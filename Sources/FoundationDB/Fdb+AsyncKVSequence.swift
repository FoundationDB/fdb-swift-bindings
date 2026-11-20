/*
 * FDB+AsyncKVSequence.swift
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
/// This file implements streaming iteration over large result sets from FoundationDB using
/// Swift's AsyncSequence protocol, fetching one batch at a time as the caller consumes results.

extension FDB {
    /// An asynchronous sequence that streams key-value pairs from FoundationDB.
    ///
    /// `AsyncKVSequence` provides a Swift-native way to iterate over large result sets from
    /// FoundationDB range queries without loading all data into memory at once. Results are
    /// read in batches, and each batch is only requested once the previous one has been consumed.
    ///
    /// ## Usage
    ///
    /// ```swift
    /// let sequence = transaction.getRange(
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
    /// - **On-demand batches**: At most one batch is held at a time, and no read is made until the
    ///   caller needs more results
    /// - **Configurable Batching**: Batch size can be tuned via `batchLimit` parameter
    /// - **Snapshot Consistency**: Supports both snapshot and non-snapshot reads
    ///
    /// ## Implementation Notes
    ///
    /// The sequence uses an async iterator that:
    /// 1. Reads the first batch on the first call to `next()`
    /// 2. Serves items from the current batch without further I/O
    /// 3. Reads the following batch only when the current one is exhausted
    ///
    /// Because every read happens inside a call to `next()`, no read is ever in flight once the
    /// caller stops iterating. That means the caller can safely use the transaction afterwards,
    /// including calling `commit()`, without a stray read racing with it.
    public struct AsyncKVSequence: AsyncSequence, Sendable {
        public typealias Element = (Bytes, Bytes)

        /// The transaction used for range queries
        let transaction: TransactionProtocol
        /// Starting key selector for the range
        let beginSelector: FDB.KeySelector
        /// Ending key selector for the range (exclusive)
        let endSelector: FDB.KeySelector
        /// Whether to use snapshot reads
        let snapshot: Bool
        /// Maximum number of key-value pairs to fetch per batch (0 = use FDB default)
        let batchLimit: Int = 0

        /// Creates a new async iterator for this sequence.
        ///
        /// No read is made until the first call to `next()`.
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

        /// Async iterator that reads one batch at a time.
        ///
        /// This iterator implements a simple batching strategy:
        ///
        /// 1. **Lazy first read**: The first batch is read on the first call to `next()`
        /// 2. **Serve from memory**: Items from the current batch are returned without any I/O
        /// 3. **Read when needed**: The next batch is read only after the current one is exhausted
        ///
        /// ## Why there is no pre-fetching
        ///
        /// - **No stray reads**: A read started in the background could still be running when the
        ///   caller moves on, for example to `commit()`. FDB fails both the read and the commit
        ///   with `used_during_commit` in that case.
        /// - **Memory Efficient**: Only one batch is kept in memory at a time
        ///
        /// ## Thread Safety
        ///
        /// This iterator is **not** thread-safe. Each iterator should be used by a single task.
        /// Multiple iterators can be created from the same sequence for concurrent processing.
        public struct AsyncIterator: AsyncIteratorProtocol {
            /// Transaction used for all range queries
            private let transaction: TransactionProtocol
            /// Begin key selector (remains constant)
            private let beginSelector: FDB.KeySelector
            /// End key selector (remains constant)
            private let endSelector: FDB.KeySelector
            /// Whether to use snapshot reads
            private let snapshot: Bool
            /// Batch size limit
            private let batchLimit: Int

            /// Current batch of records being served
            private var currentBatch: ResultRange = .init(records: [], more: true)
            /// Index of next item to return from current batch
            private var currentIndex: Int = 0
            /// Key selector to begin the next iteration from (or nil if at the beginning or end)
            private var nextBatchStartSelector: FDB.KeySelector?

            /// Returns `true` when all available data has been consumed
            private var isExhausted: Bool {
                currentBatchExhausted && !currentBatch.more
            }

            /// Returns `true` when current batch has no more items to serve
            private var currentBatchExhausted: Bool {
                currentIndex >= currentBatch.records.count
            }

            /// Initializes the iterator. No read is made until `next()` is called.
            ///
            /// - Parameters:
            ///   - transaction: The transaction to use for range queries
            ///   - beginSelector: Starting key selector for the range
            ///   - endSelector: Ending key selector for the range (exclusive)
            ///   - snapshot: Whether to use snapshot reads
            ///   - batchLimit: Maximum items per batch (0 = FDB default)
            init(
                transaction: TransactionProtocol, beginSelector: FDB.KeySelector,
                endSelector: FDB.KeySelector, snapshot: Bool, batchLimit: Int
            ) {
                self.transaction = transaction
                self.beginSelector = beginSelector
                self.endSelector = endSelector
                self.batchLimit = batchLimit
                self.snapshot = snapshot
            }

            /// Returns the next key-value pair in the sequence.
            ///
            /// This method implements the core iteration logic:
            ///
            /// 1. If current batch has items, return next item immediately
            /// 2. If current batch is exhausted, read the next batch and wait for it
            ///
            /// The method only waits on network I/O when transitioning between batches.
            ///
            /// - Returns: The next key-value pair, or `nil` if sequence is exhausted
            /// - Throws: `FDBError` if the database operation fails
            public mutating func next() async throws -> Element? {
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

            /// Updates the current batch by fetching the next result.
            ///
            /// This method is called when the current batch is exhausted and we need to
            /// move to the next batch. Note that we will not begin to fetch the next result
            /// until all of the results from the previous batch are consumed. This ensures
            /// that we never read an extra page that no operation has requested.
            ///
            /// In theory, we could begin the read of the next page as soon as the previous
            /// page has been read, which would let the processing of results from the iterator
            /// happen concurrently with fetching the next page. However, then the user would
            /// be unable to reason about whether the background read had completed before
            /// trying to do some other operation. In particular, if they attempt to call `commit()`
            /// on the transaction, the FDB client will fail both the read _and_ the commit
            /// with a `used_during_commit` error. If the caller is cognizant of the hazard
            /// and cancels the background fetch, this could be avoided, but it is safer to
            /// only read when the caller asks for more results.
            ///
            /// - Throws: `FDBError` if the fetch operation fails
            private mutating func updateCurrentBatch() async throws {
                let nextBatch = try await fetchNextBatch()

                assert(currentIndex >= currentBatch.records.count)
                currentBatch = nextBatch
                currentIndex = 0

                if !currentBatch.records.isEmpty && currentBatch.more {
                    let lastKey = nextBatch.records.last!.0
                    nextBatchStartSelector = FDB.KeySelector.firstGreaterThan(lastKey)
                } else {
                    nextBatchStartSelector = nil
                }
            }

            private func fetchNextBatch() async throws -> ResultRange {
                return try await transaction.getRangeNative(
                    beginSelector: nextBatchStartSelector ?? beginSelector,
                    endSelector: endSelector,
                    limit: batchLimit,
                    snapshot: snapshot
                )
            }
        }
    }
}
