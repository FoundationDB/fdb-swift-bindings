/*
 * Fdb+KVSequence.swift
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

public extension Fdb {
    struct AsyncKVSequence: AsyncSequence {
        public typealias Element = KeyValue

        let transaction: ITransaction
        let beginSelector: Fdb.KeySelector
        let endSelector: Fdb.KeySelector
        let snapshot: Bool
        let batchLimit: Int32 = 0

        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(
                transaction: transaction,
                beginSelector: beginSelector,
                endSelector: endSelector,
                snapshot: snapshot,
                batchLimit: batchLimit
            )
        }

        public struct AsyncIterator: AsyncIteratorProtocol {
            private let transaction: ITransaction
            private var nextBeginSelector: Fdb.KeySelector
            private let endSelector: Fdb.KeySelector
            private let snapshot: Bool
            private let batchLimit: Int32

            private var currentBatch: ResultRange = .init(records: [], more: true)
            private var currentIndex: Int = 0
            private var preFetchTask: Task<ResultRange?, Error>?

            private var isExhausted: Bool {
                currentBatchExhausted && !currentBatch.more
            }

            private var currentBatchExhausted: Bool {
                currentIndex >= currentBatch.records.count
            }

            init(
                transaction: ITransaction, beginSelector: Fdb.KeySelector,
                endSelector: Fdb.KeySelector, snapshot: Bool, batchLimit: Int32
            ) {
                self.transaction = transaction
                nextBeginSelector = beginSelector
                self.endSelector = endSelector
                self.batchLimit = batchLimit
                self.snapshot = snapshot

                startBackgroundPreFetch()
            }

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
