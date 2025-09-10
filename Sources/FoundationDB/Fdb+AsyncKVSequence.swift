/*
 * Fdb+AsyncKVSequence.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2016-2025 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/// Provides async sequence support for iterating over FoundationDB key-value ranges.
///
/// This file implements efficient streaming iteration over large result sets from FoundationDB
/// using Swift's AsyncSequence protocol with optimized background pre-fetching.

public extension Fdb {
    /// An asynchronous sequence that efficiently streams key-value pairs from FoundationDB.
    ///
    /// `AsyncKVSequence` provides a Swift-native way to iterate over large result sets from
    /// FoundationDB range queries without loading all data into memory at once. It implements
    /// background pre-fetching to minimize network latency and maximize throughput.
    ///
    /// ## Usage
    ///
    /// ```swift
    /// let sequence = transaction.readRange(
    ///     beginSelector: .firstGreaterOrEqual("user:"),
    ///     endSelector: .firstGreaterOrEqual("user;")
    /// )
    ///
    /// for try await (key, value) in sequence {
    ///     let userId = String(bytes: key)
    ///     let userData = String(bytes: value)
    ///     // Process each key-value pair as it's received
    /// }
    /// ```
    ///
    /// ## Performance Characteristics
    ///
    /// - **Streaming**: Results are processed as they arrive, not buffered entirely in memory
    /// - **Background Pre-fetching**: Next batch is fetched concurrently while processing current batch
    /// - **Configurable Batching**: Batch size can be tuned via `batchLimit` parameter
    /// - **Snapshot Consistency**: Supports both snapshot and non-snapshot reads
    ///
    /// ## Implementation Notes
    ///
    /// The sequence uses an optimized async iterator that:
    /// 1. Starts pre-fetching the next batch immediately upon initialization
    /// 2. Continues pre-fetching in background while serving current batch items
    /// 3. Only blocks when transitioning between batches if pre-fetch isn't complete
    ///
    /// This design minimizes the impact of network latency on iteration performance.
    struct AsyncKVSequence: AsyncSequence {
        public typealias Element = KeyValue

        /// The transaction used for range queries
        let transaction: ITransaction
        /// Starting key selector for the range
        let beginSelector: Fdb.KeySelector
        /// Ending key selector for the range (exclusive)
        let endSelector: Fdb.KeySelector
        /// Whether to use snapshot reads
        let snapshot: Bool
        /// Maximum number of key-value pairs to fetch per batch (0 = use FDB default)
        let batchLimit: Int32 = 0

        /// Creates a new async iterator for this sequence.
        ///
        /// The iterator begins background pre-fetching immediately upon creation to minimize
        /// latency for the first `next()` call.
        ///
        /// - Returns: A new `AsyncIterator` configured for this sequence
        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(
                transaction: transaction,
                beginSelector: beginSelector,
                endSelector: endSelector,
                snapshot: snapshot,
                batchLimit: batchLimit
            )
        }

        /// High-performance async iterator with background pre-fetching.
        ///
        /// This iterator implements an optimized batching strategy:
        ///
        /// 1. **Immediate Pre-fetch**: Starts fetching the first batch during initialization
        /// 2. **Background Pre-fetch**: While serving items from current batch, pre-fetches next batch
        /// 3. **Minimal Blocking**: Only blocks when current batch is exhausted and next isn't ready
        ///
        /// ## Performance Benefits
        ///
        /// - **Overlapped I/O**: Network requests happen concurrently with data processing
        /// - **Reduced Latency**: Pre-fetching hides network round-trip time
        /// - **Memory Efficient**: Only keeps 1-2 batches in memory at any time
        ///
        /// ## Thread Safety
        ///
        /// This iterator is **not** thread-safe. Each iterator should be used by a single task.
        /// Multiple iterators can be created from the same sequence for concurrent processing.
        public struct AsyncIterator: AsyncIteratorProtocol {
            /// Transaction used for all range queries
            private let transaction: ITransaction
            /// Key selector for the next batch to fetch
            private var nextBeginSelector: Fdb.KeySelector
            /// End key selector (remains constant)
            private let endSelector: Fdb.KeySelector
            /// Whether to use snapshot reads
            private let snapshot: Bool
            /// Batch size limit
            private let batchLimit: Int32

            /// Current batch of records being served
            private var currentBatch: ResultRange = .init(records: [], more: true)
            /// Index of next item to return from current batch
            private var currentIndex: Int = 0
            /// Background task pre-fetching the next batch
            private var preFetchTask: Task<ResultRange?, Error>?

            /// Returns `true` when all available data has been consumed
            private var isExhausted: Bool {
                currentBatchExhausted && !currentBatch.more
            }

            /// Returns `true` when current batch has no more items to serve
            private var currentBatchExhausted: Bool {
                currentIndex >= currentBatch.records.count
            }

            /// Initializes the iterator and immediately starts pre-fetching the first batch.
            ///
            /// - Parameters:
            ///   - transaction: The transaction to use for range queries
            ///   - beginSelector: Starting key selector for the range
            ///   - endSelector: Ending key selector for the range (exclusive)
            ///   - snapshot: Whether to use snapshot reads
            ///   - batchLimit: Maximum items per batch (0 = FDB default)
            init(
                transaction: ITransaction, beginSelector: Fdb.KeySelector,
                endSelector: Fdb.KeySelector, snapshot: Bool, batchLimit: Int32
            ) {
                self.transaction = transaction
                nextBeginSelector = beginSelector
                self.endSelector = endSelector
                self.batchLimit = batchLimit
                self.snapshot = snapshot

                // Start fetching immediately to minimize latency on first next() call
                startBackgroundPreFetch()
            }

            /// Returns the next key-value pair in the sequence.
            ///
            /// This method implements the core iteration logic with optimal performance:
            ///
            /// 1. If current batch has items, return next item immediately
            /// 2. If current batch is exhausted, wait for pre-fetched batch
            /// 3. Continue pre-fetching next batch in background
            ///
            /// The method only blocks on network I/O when transitioning between batches
            /// and the next batch isn't ready yet.
            ///
            /// - Returns: The next key-value pair, or `nil` if sequence is exhausted
            /// - Throws: `FdbError` if the database operation fails
            public mutating func next() async throws -> KeyValue? {
                if isExhausted {
                    return nil
                }

                if currentBatchExhausted {
                    try await updateCurrentBatch()
                }

                if currentBatchExhausted {
                    // If last fetch didn't bring any new records, we've read everything.
                    return nil
                }

                let keyValue = currentBatch.records[currentIndex]
                currentIndex += 1
                return keyValue
            }

            /// Updates the current batch with pre-fetched data and starts next pre-fetch.
            ///
            /// This method is called when the current batch is exhausted and we need to
            /// move to the next batch. It waits for the background pre-fetch task to complete,
            /// updates the iterator state, and starts pre-fetching the subsequent batch.
            ///
            /// - Throws: `FdbError` if the pre-fetch operation failed
            private mutating func updateCurrentBatch() async throws {
                guard let nextBatch = try await preFetchTask?.value else {
                    throw FdbError(.clientError)
                }

                assert(currentIndex >= currentBatch.records.count)
                currentBatch = nextBatch
                currentIndex = 0

                if !currentBatch.records.isEmpty, currentBatch.more {
                    let lastKey = nextBatch.records.last!.0
                    nextBeginSelector = Fdb.KeySelector.firstGreaterThan(lastKey)
                    startBackgroundPreFetch()
                } else {
                    preFetchTask = nil
                }
            }

            /// Starts background pre-fetching of the next batch.
            ///
            /// This method creates a background Task that performs the next range query
            /// concurrently. The task captures all necessary values to avoid reference
            /// cycles and ensure thread safety.
            ///
            /// The pre-fetch runs independently and can complete while the iterator
            /// is serving items from the current batch, minimizing blocking time
            /// during batch transitions.
            private mutating func startBackgroundPreFetch() {
                preFetchTask = Task {
                    [transaction, nextBeginSelector, endSelector, batchLimit, snapshot] in
                    return try await transaction.getRange(
                        beginSelector: nextBeginSelector,
                        endSelector: endSelector,
                        limit: batchLimit,
                        snapshot: snapshot
                    )
                }
            }
        }
    }
}
