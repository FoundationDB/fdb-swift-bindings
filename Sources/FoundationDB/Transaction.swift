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

public final class FDBTransaction: TransactionProtocol, @unchecked Sendable {
    private let transaction: OpaquePointer

    init(transaction: OpaquePointer) {
        self.transaction = transaction
    }

    deinit {
        fdb_transaction_destroy(transaction)
    }

    public func getValue(for key: FDB.Bytes, snapshot: Bool) async throws -> FDB.Bytes? {
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

    public func setValue(_ value: FDB.Bytes, for key: FDB.Bytes) {
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

    public func clear(key: FDB.Bytes) {
        key.withUnsafeBytes { keyBytes in
            fdb_transaction_clear(
                transaction,
                keyBytes.bindMemory(to: UInt8.self).baseAddress,
                Int32(key.count)
            )
        }
    }

    public func clearRange(beginKey: FDB.Bytes, endKey: FDB.Bytes) {
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

    public func atomicOp(key: FDB.Bytes, param: FDB.Bytes, mutationType: FDB.MutationType) {
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

    public func setOption(to value: FDB.Bytes?, forOption option: FDB.TransactionOption) throws {
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
            throw FDBError(code: error)
        }
    }

    public func getKey(selector: FDB.KeySelector, snapshot: Bool) async throws -> FDB.Bytes? {
        try await selector.key.withUnsafeBytes { keyBytes in
            Future<ResultKey>(
                fdb_transaction_get_key(
                    transaction,
                    keyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(selector.key.count),
                    selector.orEqual ? 1 : 0,
                    Int32(selector.offset),
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

    public func getVersionstamp() async throws -> FDB.Bytes? {
        try await Future<ResultKey>(
            fdb_transaction_get_versionstamp(transaction)
        ).getAsync()?.value
    }

    public func setReadVersion(_ version: FDB.Version) {
        fdb_transaction_set_read_version(transaction, version)
    }

    public func getReadVersion() async throws -> FDB.Version {
        try await Future<ResultVersion>(
            fdb_transaction_get_read_version(transaction)
        ).getAsync()?.value ?? 0
    }

    public func onError(_ error: FDBError) async throws {
        try await Future<ResultVoid>(
            fdb_transaction_on_error(transaction, error.code)
        ).getAsync()
    }

    public func getEstimatedRangeSizeBytes(beginKey: FDB.Bytes, endKey: FDB.Bytes) async throws -> Int {
        try Int(await beginKey.withUnsafeBytes { beginKeyBytes in
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
        }.getAsync()?.value ?? 0)
    }

    public func getRangeSplitPoints(beginKey: FDB.Bytes, endKey: FDB.Bytes, chunkSize: Int) async throws -> [[UInt8]] {
        try await beginKey.withUnsafeBytes { beginKeyBytes in
            endKey.withUnsafeBytes { endKeyBytes in
                Future<ResultKeyArray>(
                    fdb_transaction_get_range_split_points(
                        transaction,
                        beginKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(beginKey.count),
                        endKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(endKey.count),
                        Int64(chunkSize)
                    )
                )
            }
        }.getAsync()?.value ?? []
    }

    public func getCommittedVersion() throws -> FDB.Version {
        var version: FDB.Version = 0
        let err = fdb_transaction_get_committed_version(transaction, &version)
        if err != 0 {
            throw FDBError(code: err)
        }
        return version
    }

    public func getApproximateSize() async throws -> Int {
        try Int(await Future<ResultInt64>(
            fdb_transaction_get_approximate_size(transaction)
        ).getAsync()?.value ?? 0)
    }

    public func addConflictRange(beginKey: FDB.Bytes, endKey: FDB.Bytes, type: FDB.ConflictRangeType) throws {
        let error = beginKey.withUnsafeBytes { beginKeyBytes in
            endKey.withUnsafeBytes { endKeyBytes in
                fdb_transaction_add_conflict_range(
                    transaction,
                    beginKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(beginKey.count),
                    endKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(endKey.count),
                    FDBConflictRangeType(rawValue: type.rawValue)
                )
            }
        }

        if error != 0 {
            throw FDBError(code: error)
        }
    }

    public func getRange(
        beginSelector: FDB.KeySelector, endSelector: FDB.KeySelector, limit: Int = 0,
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
                        Int32(beginSelector.offset),
                        endKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(endSelector.key.count),
                        endSelector.orEqual ? 1 : 0,
                        Int32(endSelector.offset),
                        Int32(limit),
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
        beginKey: FDB.Bytes, endKey: FDB.Bytes, limit: Int = 0, snapshot: Bool
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
                        Int32(limit),
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
