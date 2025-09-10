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
        let beginSelector: Fdb.KeySelector?
        let endSelector: Fdb.KeySelector?
        let limit: Int32
        let snapshot: Bool

        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(
                transaction: transaction,
                beginSelector: beginSelector,
                endSelector: endSelector,
                limit: limit,
                snapshot: snapshot
            )
        }

        public struct AsyncIterator: AsyncIteratorProtocol {
            private let transaction: ITransaction
            private let beginSelector: Fdb.KeySelector?
            private let endSelector: Fdb.KeySelector?
            private let limit: Int32
            private let snapshot: Bool

            private var currentResult: ResultRange?
            private var currentIndex: Int = 0
            private var nextBeginSelector: Fdb.KeySelector?
            private var isExhausted: Bool = false

            private var preFetchedResult: ResultRange?
            private var shouldPreFetch: Bool = false

            init(transaction: ITransaction, beginSelector: Fdb.KeySelector?, endSelector: Fdb.KeySelector?, limit: Int32, snapshot: Bool) {
                self.transaction = transaction
                self.beginSelector = beginSelector
                self.endSelector = endSelector
                self.limit = limit
                self.snapshot = snapshot
            }

            public mutating func next() async throws -> KeyValue? {
                if isExhausted {
                    return nil
                }

                if currentResult == nil {
                    try await fetchInitialBatch()
                }

                guard let result = currentResult else {
                    isExhausted = true
                    return nil
                }

                if currentIndex < result.records.count {
                    let keyValue = result.records[currentIndex]
                    currentIndex += 1

                    if currentIndex == result.records.count && result.more && preFetchedResult == nil {
                        shouldPreFetch = true
                    }

                    return keyValue
                }

                if result.more || preFetchedResult != nil {
                    try await moveToNextBatch()
                    return try await next()
                } else {
                    isExhausted = true
                    return nil
                }
            }

            private mutating func fetchInitialBatch() async throws {
                if let begin = beginSelector, let end = endSelector {
                    currentResult = try await transaction.getRange(
                        beginSelector: begin,
                        endSelector: end,
                        limit: limit,
                        snapshot: snapshot
                    )
                } else {
                    throw FdbError(FdbErrorCode.clientError)
                }

                if let result = currentResult, !result.records.isEmpty {
                    let lastKey = result.records.last!.0
                    nextBeginSelector = Fdb.KeySelector.firstGreaterThan(lastKey)
                }

                currentIndex = 0
            }

            private mutating func moveToNextBatch() async throws {
                if let preFetched = preFetchedResult {
                    currentResult = preFetched
                    preFetchedResult = nil
                } else if let nextBegin = nextBeginSelector, let end = endSelector {
                    currentResult = try await transaction.getRange(
                        beginSelector: nextBegin,
                        endSelector: end,
                        limit: limit,
                        snapshot: snapshot
                    )
                } else {
                    isExhausted = true
                    return
                }

                currentIndex = 0

                if let result = currentResult, !result.records.isEmpty {
                    let lastKey = result.records.last!.0
                    nextBeginSelector = Fdb.KeySelector.firstGreaterThan(lastKey)

                    if shouldPreFetch && result.more {
                        try await preFetchNextBatch()
                        shouldPreFetch = false
                    }
                } else {
                    isExhausted = true
                }
            }

            private mutating func preFetchNextBatch() async throws {
                guard let nextBegin = nextBeginSelector, let end = endSelector else {
                    return
                }

                preFetchedResult = try await transaction.getRange(
                    beginSelector: nextBegin,
                    endSelector: end,
                    limit: limit,
                    snapshot: snapshot
                )
            }
        }
    }
}
