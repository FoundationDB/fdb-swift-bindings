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

/// Protocol defining the interface for FoundationDB database connections.
///
/// `IDatabase` provides the core database operations including transaction creation
/// and transaction retry logic. Implementations handle the underlying database
/// connection and resource management.
/// Database interface for FoundationDB operations
public protocol IDatabase {
    /// Creates a new transaction for database operations.
    ///
    /// - Returns: A new transaction instance conforming to `ITransaction`.
    /// - Throws: `FdbError` if the transaction cannot be created.
    func createTransaction() throws -> any ITransaction

    /// Executes a transaction with automatic retry logic.
    ///
    /// This method automatically handles transaction retries for retryable errors,
    /// providing a convenient way to execute transactional operations reliably.
    ///
    /// - Parameter operation: The operation to execute within the transaction context.
    /// - Returns: The result of the transaction operation.
    /// - Throws: `FdbError` if the transaction fails after all retry attempts.
    func withTransaction<T: Sendable>(
        _ operation: (ITransaction) async throws -> T
    ) async throws -> T
}

/// Protocol defining the interface for FoundationDB transactions.
///
/// `ITransaction` provides all the operations that can be performed within
/// a FoundationDB transaction, including reads, writes, atomic operations,
/// and transaction management.
/// Transaction interface for FoundationDB operations
public protocol ITransaction: Sendable {
    /// Retrieves a value for the given key.
    ///
    /// - Parameters:
    ///   - key: The key to retrieve as a byte array.
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: The value associated with the key, or nil if not found.
    /// - Throws: `FdbError` if the operation fails.
    func getValue(for key: Fdb.Key, snapshot: Bool) async throws -> Fdb.Value?

    /// Sets a value for the given key.
    ///
    /// - Parameters:
    ///   - value: The value to set as a byte array.
    ///   - key: The key to associate with the value.
    func setValue(_ value: Fdb.Value, for key: Fdb.Key)

    /// Removes a key-value pair from the database.
    ///
    /// - Parameter key: The key to remove as a byte array.
    func clear(key: Fdb.Key)

    /// Removes all key-value pairs in the given range.
    ///
    /// - Parameters:
    ///   - beginKey: The start of the range (inclusive) as a byte array.
    ///   - endKey: The end of the range (exclusive) as a byte array.
    func clearRange(beginKey: Fdb.Key, endKey: Fdb.Key)

    /// Resolves a key selector to an actual key.
    ///
    /// - Parameters:
    ///   - selector: The key selector to resolve.
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: The resolved key, or nil if no key matches.
    /// - Throws: `FdbError` if the operation fails.
    func getKey(selector: Fdb.Selectable, snapshot: Bool) async throws -> Fdb.Key?

    /// Resolves a key selector to an actual key.
    ///
    /// - Parameters:
    ///   - selector: The key selector to resolve.
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: The resolved key, or nil if no key matches.
    /// - Throws: `FdbError` if the operation fails.
    func getKey(selector: Fdb.KeySelector, snapshot: Bool) async throws -> Fdb.Key?

    /// Returns an AsyncSequence that yields key-value pairs within a range.
    ///
    /// - Parameters:
    ///   - beginSelector: The key selector for the start of the range.
    ///   - endSelector: The key selector for the end of the range.
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: An async sequence that yields key-value pairs.
    func readRange(
        beginSelector: Fdb.KeySelector, endSelector: Fdb.KeySelector, snapshot: Bool
    ) -> Fdb.AsyncKVSequence

    /// Retrieves key-value pairs within a range using selectable endpoints.
    ///
    /// - Parameters:
    ///   - begin: The start of the range (converted to key selector).
    ///   - end: The end of the range (converted to key selector).
    ///   - limit: Maximum number of key-value pairs to return (0 for no limit).
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: A `ResultRange` containing the key-value pairs and more flag.
    /// - Throws: `FdbError` if the operation fails.
    func getRange(
        begin: Fdb.Selectable, end: Fdb.Selectable, limit: Int, snapshot: Bool
    ) async throws -> ResultRange

    /// Retrieves key-value pairs within a range using key selectors.
    ///
    /// - Parameters:
    ///   - beginSelector: The key selector for the start of the range.
    ///   - endSelector: The key selector for the end of the range.
    ///   - limit: Maximum number of key-value pairs to return (0 for no limit).
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: A `ResultRange` containing the key-value pairs and more flag.
    /// - Throws: `FdbError` if the operation fails.
    func getRange(
        beginSelector: Fdb.KeySelector, endSelector: Fdb.KeySelector, limit: Int, snapshot: Bool
    ) async throws -> ResultRange

    /// Retrieves key-value pairs within a range using byte array keys.
    ///
    /// - Parameters:
    ///   - beginKey: The start key of the range as a byte array.
    ///   - endKey: The end key of the range as a byte array.
    ///   - limit: Maximum number of key-value pairs to return (0 for no limit).
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: A `ResultRange` containing the key-value pairs and more flag.
    /// - Throws: `FdbError` if the operation fails.
    func getRange(
        beginKey: Fdb.Key, endKey: Fdb.Key, limit: Int, snapshot: Bool
    ) async throws -> ResultRange

    /// Commits the transaction.
    ///
    /// - Returns: `true` if the transaction was successfully committed.
    /// - Throws: `FdbError` if the commit fails.
    func commit() async throws -> Bool

    /// Cancels the transaction.
    ///
    /// After calling this method, the transaction cannot be used for further operations.
    func cancel()

    /// Gets the versionstamp for this transaction.
    ///
    /// The versionstamp is only available after the transaction has been committed.
    ///
    /// - Returns: The transaction's versionstamp as a key, or nil if not available.
    /// - Throws: `FdbError` if the operation fails.
    func getVersionstamp() async throws -> Fdb.Key?

    /// Sets the read version for snapshot reads.
    ///
    /// - Parameter version: The version to use for snapshot reads.
    func setReadVersion(_ version: Fdb.Version)

    /// Gets the read version used by this transaction.
    ///
    /// - Returns: The transaction's read version.
    /// - Throws: `FdbError` if the operation fails.
    func getReadVersion() async throws -> Fdb.Version

    /// Handles transaction errors and implements retry logic with exponential backoff.
    ///
    /// If this method returns successfully, the transaction has been reset and can be retried.
    /// If it throws an error, the transaction should not be retried.
    ///
    /// - Parameter error: The error encountered during transaction execution.
    /// - Throws: `FdbError` if the error is not retryable or retry limits have been exceeded.
    func onError(_ error: FdbError) async throws

    /// Returns an estimated byte size of the specified key range.
    ///
    /// The estimate is calculated based on sampling done by FDB server. Larger key-value pairs
    /// are more likely to be sampled. For accuracy, use on large ranges (>3MB recommended).
    ///
    /// - Parameters:
    ///   - beginKey: The start of the range (inclusive).
    ///   - endKey: The end of the range (exclusive).
    /// - Returns: The estimated size in bytes.
    /// - Throws: `FdbError` if the operation fails.
    func getEstimatedRangeSizeBytes(beginKey: Fdb.Key, endKey: Fdb.Key) async throws -> Int

    /// Returns a list of keys that can split the given range into roughly equal chunks.
    ///
    /// The returned split points include the start and end keys of the range.
    ///
    /// - Parameters:
    ///   - beginKey: The start of the range.
    ///   - endKey: The end of the range.
    ///   - chunkSize: The desired size of each chunk in bytes.
    /// - Returns: An array of keys representing split points.
    /// - Throws: `FdbError` if the operation fails.
    func getRangeSplitPoints(beginKey: Fdb.Key, endKey: Fdb.Key, chunkSize: Int) async throws -> [[UInt8]]

    /// Returns the version number at which a committed transaction modified the database.
    ///
    /// Must only be called after a successful commit. Read-only transactions return -1.
    ///
    /// - Returns: The committed version number.
    /// - Throws: `FdbError` if called before commit or if the operation fails.
    func getCommittedVersion() throws -> Fdb.Version

    /// Returns the approximate transaction size so far.
    ///
    /// This is the sum of estimated sizes of mutations, read conflict ranges, and write conflict ranges.
    /// Can be called multiple times before commit.
    ///
    /// - Returns: The approximate size in bytes.
    /// - Throws: `FdbError` if the operation fails.
    func getApproximateSize() async throws -> Int

    /// Performs an atomic operation on a key.
    ///
    /// - Parameters:
    ///   - key: The key to operate on.
    ///   - param: The parameter for the atomic operation.
    ///   - mutationType: The type of atomic operation to perform.
    func atomicOp(key: Fdb.Key, param: Fdb.Value, mutationType: Fdb.MutationType)

    /// Adds a conflict range to the transaction.
    ///
    /// Conflict ranges are used to manually declare the read and write sets of the transaction.
    /// This can be useful for ensuring serializability when certain keys are accessed indirectly.
    ///
    /// - Parameters:
    ///   - beginKey: The start of the range (inclusive) as a byte array.
    ///   - endKey: The end of the range (exclusive) as a byte array.
    ///   - type: The type of conflict range (read or write).
    /// - Throws: `FdbError` if the operation fails.
    func addConflictRange(beginKey: Fdb.Key, endKey: Fdb.Key, type: Fdb.ConflictRangeType) throws

    // MARK: - Transaction option methods

    /// Sets a transaction option with an optional value.
    ///
    /// - Parameters:
    ///   - option: The transaction option to set.
    ///   - value: Optional byte array value for the option.
    /// - Throws: `FdbError` if the option cannot be set.
    func setOption(_ option: Fdb.TransactionOption, value: Fdb.Value?) throws

    /// Sets a transaction option with a string value.
    ///
    /// - Parameters:
    ///   - option: The transaction option to set.
    ///   - value: String value for the option.
    /// - Throws: `FdbError` if the option cannot be set.
    func setOption(_ option: Fdb.TransactionOption, value: String) throws

    /// Sets a transaction option with an integer value.
    ///
    /// - Parameters:
    ///   - option: The transaction option to set.
    ///   - value: Integer value for the option.
    /// - Throws: `FdbError` if the option cannot be set.
    func setOption(_ option: Fdb.TransactionOption, value: Int) throws
}

/// Default implementation of transaction retry logic for `IDatabase`.
public extension IDatabase {
    /// Default implementation of `withTransaction` with automatic retry logic.
    ///
    /// This implementation automatically retries transactions when they encounter
    /// retryable errors, up to a maximum number of attempts.
    ///
    /// - Parameter operation: The transaction operation to execute.
    /// - Returns: The result of the successful transaction.
    /// - Throws: `FdbError` if all retry attempts fail.
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
    func getValue(for key: Fdb.Key, snapshot: Bool = false) async throws -> Fdb.Value? {
        try await getValue(for: key, snapshot: snapshot)
    }

    func getKey(selector: Fdb.Selectable, snapshot: Bool = false) async throws -> Fdb.Key? {
        try await getKey(selector: selector.toKeySelector(), snapshot: snapshot)
    }

    func getKey(selector: Fdb.KeySelector, snapshot: Bool = false) async throws -> Fdb.Key? {
        try await getKey(selector: selector, snapshot: snapshot)
    }

    func readRange(
        beginSelector: Fdb.KeySelector, endSelector: Fdb.KeySelector, snapshot: Bool = false
    ) -> Fdb.AsyncKVSequence {
        Fdb.AsyncKVSequence(
            transaction: self,
            beginSelector: beginSelector,
            endSelector: endSelector,
            snapshot: snapshot
        )
    }

    func readRange(
        beginSelector: Fdb.KeySelector, endSelector: Fdb.KeySelector
    ) -> Fdb.AsyncKVSequence {
        readRange(
            beginSelector: beginSelector, endSelector: endSelector, snapshot: false
        )
    }

    func readRange(
        begin: Fdb.Selectable, end: Fdb.Selectable, snapshot: Bool = false
    ) -> Fdb.AsyncKVSequence {
        let beginSelector = begin.toKeySelector()
        let endSelector = end.toKeySelector()
        return readRange(
            beginSelector: beginSelector, endSelector: endSelector, snapshot: snapshot
        )
    }

    func readRange(
        beginKey: Fdb.Key, endKey: Fdb.Key, snapshot: Bool = false
    ) -> Fdb.AsyncKVSequence {
        let beginSelector = Fdb.KeySelector.firstGreaterOrEqual(beginKey)
        let endSelector = Fdb.KeySelector.firstGreaterOrEqual(endKey)
        return readRange(
            beginSelector: beginSelector, endSelector: endSelector, snapshot: snapshot
        )
    }

    func getRange(
        begin: Fdb.Selectable, end: Fdb.Selectable, limit: Int = 0, snapshot: Bool = false
    ) async throws -> ResultRange {
        let beginSelector = begin.toKeySelector()
        let endSelector = end.toKeySelector()
        return try await getRange(
            beginSelector: beginSelector, endSelector: endSelector, limit: limit, snapshot: snapshot
        )
    }

    func getRange(
        beginSelector: Fdb.KeySelector, endSelector: Fdb.KeySelector, limit: Int = 0,
        snapshot: Bool = false
    ) async throws -> ResultRange {
        try await getRange(
            beginSelector: beginSelector, endSelector: endSelector, limit: limit, snapshot: snapshot
        )
    }

    func getRange(
        beginKey: Fdb.Key, endKey: Fdb.Key, limit: Int = 0, snapshot: Bool = false
    ) async throws -> ResultRange {
        try await getRange(beginKey: beginKey, endKey: endKey, limit: limit, snapshot: snapshot)
    }

    func setOption(_ option: Fdb.TransactionOption) throws {
        try setOption(option, value: nil)
    }

    func setOption(_ option: Fdb.TransactionOption, value: String) throws {
        let valueBytes = [UInt8](value.utf8)
        try setOption(option, value: valueBytes)
    }

    func setOption(_ option: Fdb.TransactionOption, value: Int) throws {
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

    func setDebugTransactionIdentifier(_ identifier: String) throws {
        try setOption(.debugTransactionIdentifier, value: identifier)
    }

    func enableLogTransaction() throws {
        try setOption(.logTransaction)
    }
}
