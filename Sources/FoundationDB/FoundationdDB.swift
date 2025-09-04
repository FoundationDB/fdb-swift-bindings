/*
 * FoundationDB.swift
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

/// Database interface for FoundationDB operations
public protocol IDatabase {
    func createTransaction() throws -> any ITransaction

    /// Executes a transaction with automatic retry logic
    func withTransaction<T: Sendable>(
        _ operation: (ITransaction) async throws -> T
    ) async throws -> T
}

/// Transaction interface for FoundationDB operations
public protocol ITransaction {
    /// Retrieves a value for the given key
    func getValue(for key: String, snapshot: Bool) async throws -> Fdb.Value?
    func getValue(for key: Fdb.Key, snapshot: Bool) async throws -> Fdb.Value?

    /// Sets a value for the given key
    func setValue(_ value: Fdb.Value, for key: Fdb.Key)
    func setValue(_ value: String, for key: String)

    /// Removes a key-value pair
    func clear(key: Fdb.Key)
    func clear(key: String)

    /// Removes all key-value pairs in the given range
    func clearRange(beginKey: Fdb.Key, endKey: Fdb.Key)
    func clearRange(beginKey: String, endKey: String)

    /// Resolves a key selector to an actual key
    func getKey(selector: Fdb.Selectable, snapshot: Bool) async throws -> Fdb.Key?
    func getKey(selector: Fdb.KeySelector, snapshot: Bool) async throws -> Fdb.Key?

    /// Retrieves key-value pairs within a range
    func getRange(
        begin: Fdb.Selectable, end: Fdb.Selectable, limit: Int32, snapshot: Bool
    ) async throws -> ResultRange

    func getRange(
        beginSelector: Fdb.KeySelector, endSelector: Fdb.KeySelector, limit: Int32, snapshot: Bool
    ) async throws -> ResultRange

    func getRange(
        beginKey: String, endKey: String, limit: Int32, snapshot: Bool
    ) async throws -> ResultRange

    func getRange(
        beginKey: Fdb.Key, endKey: Fdb.Key, limit: Int32, snapshot: Bool
    ) async throws -> ResultRange

    /// Commits the transaction
    func commit() async throws -> Bool
    /// Cancels the transaction
    func cancel()

    /// Gets the versionstamp for this transaction
    func getVersionstamp() async throws -> Fdb.Key?

    /// Sets the read version for snapshot reads
    func setReadVersion(_ version: Int64)
    /// Gets the read version used by this transaction
    func getReadVersion() async throws -> Int64

    func atomicOp(key: Fdb.Key, param: Fdb.Value, mutationType: Fdb.MutationType)

    // MARK: - Transaction option methods
    func setOption(_ option: Fdb.TransactionOption, value: Fdb.Value?) throws
    func setOption(_ option: Fdb.TransactionOption, value: String) throws
    func setOption(_ option: Fdb.TransactionOption, value: Int) throws
}

public extension IDatabase {
    func withTransaction<T: Sendable>(
        _ operation: (ITransaction) async throws -> T
    ) async throws -> T {
        let maxRetries = 100 // TODO: Remove this.

        for attempt in 0 ..< maxRetries {
            let transaction = try createTransaction()

            do {
                let result = try await operation(transaction)
                let committed = try await transaction.commit()

                if committed {
                    return result
                }
            } catch {
                // TODO: If user wants to cancel, don't retry.
                transaction.cancel()

                if let fdbError = error as? FdbError, fdbError.isRetryable {
                    if attempt < maxRetries - 1 {
                        continue
                    }
                }

                throw error
            }
        }

        throw FdbError(.transactionTooOld)
    }
}

public extension ITransaction {
    func getValue(for key: String, snapshot: Bool = false) async throws -> Fdb.Value? {
        let keyBytes = [UInt8](key.utf8)
        return try await getValue(for: keyBytes, snapshot: snapshot)
    }

    func getValue(for key: Fdb.Key, snapshot: Bool = false) async throws -> Fdb.Value? {
        try await getValue(for: key, snapshot: snapshot)
    }

    func setValue(_ value: String, for key: String) {
        let keyBytes = [UInt8](key.utf8)
        let valueBytes = [UInt8](value.utf8)
        setValue(valueBytes, for: keyBytes)
    }

    func clear(key: String) {
        let keyBytes = [UInt8](key.utf8)
        clear(key: keyBytes)
    }

    func clearRange(beginKey: String, endKey: String) {
        let beginKeyBytes = [UInt8](beginKey.utf8)
        let endKeyBytes = [UInt8](endKey.utf8)
        clearRange(beginKey: beginKeyBytes, endKey: endKeyBytes)
    }

    func getKey(selector: Fdb.Selectable, snapshot: Bool = false) async throws -> Fdb.Key? {
        try await getKey(selector: selector.toKeySelector(), snapshot: snapshot)
    }

    func getKey(selector: Fdb.KeySelector, snapshot: Bool = false) async throws -> Fdb.Key? {
        try await getKey(selector: selector, snapshot: snapshot)
    }

    func getRange(
        begin: Fdb.Selectable, end: Fdb.Selectable, limit: Int32 = 0, snapshot: Bool = false
    ) async throws -> ResultRange {
        let beginSelector = begin.toKeySelector()
        let endSelector = end.toKeySelector()
        return try await getRange(
            beginSelector: beginSelector, endSelector: endSelector, limit: limit, snapshot: snapshot
        )
    }

    func getRange(
        beginSelector: Fdb.KeySelector, endSelector: Fdb.KeySelector, limit: Int32 = 0,
        snapshot: Bool = false
    ) async throws -> ResultRange {
        try await getRange(
            beginSelector: beginSelector, endSelector: endSelector, limit: limit, snapshot: snapshot
        )
    }

    func getRange(
        beginKey: String, endKey: String, limit: Int32 = 0, snapshot: Bool = false
    ) async throws -> ResultRange {
        let beginKeyBytes = [UInt8](beginKey.utf8)
        let endKeyBytes = [UInt8](endKey.utf8)
        return try await getRange(
            beginKey: beginKeyBytes, endKey: endKeyBytes, limit: limit, snapshot: snapshot
        )
    }

    func getRange(
        beginKey: Fdb.Key, endKey: Fdb.Key, limit: Int32 = 0, snapshot: Bool = false
    ) async throws -> ResultRange {
        try await getRange(beginKey: beginKey, endKey: endKey, limit: limit, snapshot: snapshot)
    }

    // MARK: - Transaction option convenience overloads
    
    func setOption(_ option: Fdb.TransactionOption) throws {
        try setOption(option, value: nil)
    }

    public func setOption(_ option: Fdb.TransactionOption, value: String) throws {
        let valueBytes = [UInt8](value.utf8)
        try setOption(option, value: valueBytes)
    }

    public func setOption(_ option: Fdb.TransactionOption, value: Int) throws {
        let valueBytes = withUnsafeBytes(of: Int64(value)) { [UInt8]($0) }
        try setOption(option, value: valueBytes)
    }

}

public extension ITransaction {
    // MARK: - Convenience methods for common transaction options
    func setTimeout(_ milliseconds: Int) throws {
        try setOption(.timeout, value: milliseconds)
    }

    func setRetryLimit(_ limit: Int) throws {
        try setOption(.retryLimit, value: limit)
    }

    func setMaxRetryDelay(_ milliseconds: Int) throws {
        try setOption(.maxRetryDelay, value: milliseconds)
    }

    func setSizeLimit(_ bytes: Int) throws {
        try setOption(.sizeLimit, value: bytes)
    }

    func setIdempotencyId(_ id: Fdb.Value) throws {
        try setOption(.idempotencyId, value: id)
    }

    func enableAutomaticIdempotency() throws {
        try setOption(.automaticIdempotency)
    }

    func disableReadYourWrites() throws {
        try setOption(.readYourWritesDisable)
    }

    func enableSnapshotReadYourWrites() throws {
        try setOption(.snapshotRywEnable)
    }

    func disableSnapshotReadYourWrites() throws {
        try setOption(.snapshotRywDisable)
    }

    func setPriorityBatch() throws {
        try setOption(.priorityBatch)
    }

    func setPrioritySystemImmediate() throws {
        try setOption(.prioritySystemImmediate)
    }

    func enableCausalWriteRisky() throws {
        try setOption(.causalWriteRisky)
    }

    func enableCausalReadRisky() throws {
        try setOption(.causalReadRisky)
    }

    func disableCausalRead() throws {
        try setOption(.causalReadDisable)
    }

    func enableAccessSystemKeys() throws {
        try setOption(.accessSystemKeys)
    }

    func enableReadSystemKeys() throws {
        try setOption(.readSystemKeys)
    }

    func enableRawAccess() throws {
        try setOption(.rawAccess)
    }

    func addTag(_ tag: String) throws {
        try setOption(.tag, value: tag)
    }

    func addAutoThrottleTag(_ tag: String) throws {
        try setOption(.autoThrottleTag, value: tag)
    }

    func setDebugTransactionIdentifier(_ identifier: String) throws {
        try setOption(.debugTransactionIdentifier, value: identifier)
    }

    func enableLogTransaction() throws {
        try setOption(.logTransaction)
    }
}
