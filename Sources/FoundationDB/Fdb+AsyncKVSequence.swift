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
            AsyncIterator()
        }

        public struct AsyncIterator: AsyncIteratorProtocol {
            public mutating func next() async throws -> KeyValue? {
                return nil
            }
        }
    }
}
