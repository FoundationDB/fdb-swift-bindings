/*
 * Transaction.swift
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
import CFoundationDB

public class FdbTransaction: ITransaction, @unchecked Sendable {
    private let transaction: OpaquePointer

    init(transaction: OpaquePointer) {
        self.transaction = transaction
    }

    deinit {
        fdb_transaction_destroy(transaction)
    }

    public func getValue(for key: Fdb.Key, snapshot: Bool) async throws -> Fdb.Value? {
        try await key.withUnsafeBytes { keyBytes in
            Future<ResultValue>(
                fdb_transaction_get(
                    transaction,
                    keyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(key.count),
                    snapshot ? 1 : 0
                )
            )
        }.getAsync()?.value
    }

    public func setValue(_ value: Fdb.Value, for key: Fdb.Key) {
        key.withUnsafeBytes { keyBytes in
            value.withUnsafeBytes { valueBytes in
                fdb_transaction_set(
                    transaction,
                    keyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(key.count),
                    valueBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(value.count)
                )
            }
        }
    }

    public func clear(key: Fdb.Key) {
        key.withUnsafeBytes { keyBytes in
            fdb_transaction_clear(
                transaction,
                keyBytes.bindMemory(to: UInt8.self).baseAddress,
                Int32(key.count)
            )
        }
    }

    public func clearRange(beginKey: Fdb.Key, endKey: Fdb.Key) {
        beginKey.withUnsafeBytes { beginKeyBytes in
            endKey.withUnsafeBytes { endKeyBytes in
                fdb_transaction_clear_range(
                    transaction,
                    beginKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(beginKey.count),
                    endKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(endKey.count)
                )
            }
        }
    }

    public func atomicOp(key: Fdb.Key, param: Fdb.Value, mutationType: Fdb.MutationType) {
        key.withUnsafeBytes { keyBytes in
            param.withUnsafeBytes { paramBytes in
                fdb_transaction_atomic_op(
                    transaction,
                    keyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(key.count),
                    paramBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(param.count),
                    FDBMutationType(mutationType.rawValue)
                )
            }
        }
    }

    public func setOption(_ option: Fdb.TransactionOption, value: Fdb.Value?) throws {
        let error: Int32
        if let value = value {
            error = value.withUnsafeBytes { bytes in
                fdb_transaction_set_option(
                    transaction,
                    FDBTransactionOption(option.rawValue),
                    bytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(value.count)
                )
            }
        } else {
            error = fdb_transaction_set_option(transaction, FDBTransactionOption(option.rawValue), nil, 0)
        }

        if error != 0 {
            throw FdbError(code: error)
        }
    }

    public func getKey(selector: Fdb.KeySelector, snapshot: Bool) async throws -> Fdb.Key? {
        try await selector.key.withUnsafeBytes { keyBytes in
            Future<ResultKey>(
                fdb_transaction_get_key(
                    transaction,
                    keyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(selector.key.count),
                    selector.orEqual ? 1 : 0,
                    selector.offset,
                    snapshot ? 1 : 0
                )
            )
        }.getAsync()?.value
    }

    public func commit() async throws -> Bool {
        try await Future<ResultVoid>(
            fdb_transaction_commit(transaction)
        ).getAsync() != nil
    }

    public func cancel() {
        fdb_transaction_cancel(transaction)
    }

    public func getVersionstamp() async throws -> Fdb.Key? {
        try await Future<ResultKey>(
            fdb_transaction_get_versionstamp(transaction)
        ).getAsync()?.value
    }

    public func setReadVersion(_ version: Int64) {
        fdb_transaction_set_read_version(transaction, version)
    }

    public func getReadVersion() async throws -> Int64 {
        try await Future<ResultVersion>(
            fdb_transaction_get_read_version(transaction)
        ).getAsync()?.value ?? 0
    }

    public func onError(_ error: FdbError) async throws {
        try await Future<ResultVoid>(
            fdb_transaction_on_error(transaction, error.code)
        ).getAsync()
    }

    public func getEstimatedRangeSizeBytes(beginKey: Fdb.Key, endKey: Fdb.Key) async throws -> Int64 {
        try await beginKey.withUnsafeBytes { beginKeyBytes in
            endKey.withUnsafeBytes { endKeyBytes in
                Future<ResultInt64>(
                    fdb_transaction_get_estimated_range_size_bytes(
                        transaction,
                        beginKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(beginKey.count),
                        endKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(endKey.count)
                    )
                )
            }
        }.getAsync()?.value ?? 0
    }

    public func getRangeSplitPoints(beginKey: Fdb.Key, endKey: Fdb.Key, chunkSize: Int64) async throws -> [[UInt8]] {
        try await beginKey.withUnsafeBytes { beginKeyBytes in
            endKey.withUnsafeBytes { endKeyBytes in
                Future<ResultKeyArray>(
                    fdb_transaction_get_range_split_points(
                        transaction,
                        beginKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(beginKey.count),
                        endKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(endKey.count),
                        chunkSize
                    )
                )
            }
        }.getAsync()?.value ?? []
    }

    public func getCommittedVersion() throws -> Int64 {
        var version: Int64 = 0
        let err = fdb_transaction_get_committed_version(transaction, &version)
        if err != 0 {
            throw FdbError(code: err)
        }
        return version
    }

    public func getApproximateSize() async throws -> Int64 {
        try await Future<ResultInt64>(
            fdb_transaction_get_approximate_size(transaction)
        ).getAsync()?.value ?? 0
    }

    public func getRange(
        beginSelector: Fdb.KeySelector, endSelector: Fdb.KeySelector, limit: Int32 = 0,
        snapshot: Bool
    ) async throws -> ResultRange {
        let future = beginSelector.key.withUnsafeBytes { beginKeyBytes in
            endSelector.key.withUnsafeBytes { endKeyBytes in
                Future<ResultRange>(
                    fdb_transaction_get_range(
                        transaction,
                        beginKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(beginSelector.key.count),
                        beginSelector.orEqual ? 1 : 0,
                        beginSelector.offset,
                        endKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(endSelector.key.count),
                        endSelector.orEqual ? 1 : 0,
                        endSelector.offset,
                        limit,
                        0, // target_bytes = 0 (no limit)
                        FDBStreamingMode(-1), // mode = FDB_STREAMING_MODE_ITERATOR
                        1, // iteration = 1
                        snapshot ? 1 : 0,
                        0 // reverse = false
                    )
                )
            }
        }

        return try await future.getAsync() ?? ResultRange(records: [], more: false)
    }

    public func getRange(
        beginKey: Fdb.Key, endKey: Fdb.Key, limit: Int32 = 0, snapshot: Bool
    ) async throws -> ResultRange {
        let future = beginKey.withUnsafeBytes { beginKeyBytes in
            endKey.withUnsafeBytes { endKeyBytes in
                Future<ResultRange>(
                    fdb_transaction_get_range(
                        transaction,
                        beginKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(beginKey.count),
                        1, // begin_or_equal = true
                        0, // begin_offset = 0
                        endKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(endKey.count),
                        1, // end_or_equal = false (exclusive)
                        0, // end_offset = 0
                        limit,
                        0, // target_bytes = 0 (no limit)
                        FDBStreamingMode(-1), // mode = FDB_STREAMING_MODE_ITERATOR
                        1, // iteration = 1
                        snapshot ? 1 : 0,
                        0 // reverse = false
                    )
                )
            }
        }

        return try await future.getAsync() ?? ResultRange(records: [], more: false)
    }
}
