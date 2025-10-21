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
/// `DatabaseProtocol` provides the core database operations including transaction creation
/// and transaction retry logic. Implementations handle the underlying database
/// connection and resource management.
/// Database interface for FoundationDB operations
public protocol DatabaseProtocol {
    /// Creates a new transaction for database operations.
    ///
    /// - Returns: A new transaction instance conforming to `TransactionProtocol`.
    /// - Throws: `FDBError` if the transaction cannot be created.
    func createTransaction() throws -> any TransactionProtocol

    /// Executes a transaction with automatic retry logic.
    ///
    /// This method automatically handles transaction retries for retryable errors,
    /// providing a convenient way to execute transactional operations reliably.
    ///
    /// - Parameter operation: The operation to execute within the transaction context.
    /// - Returns: The result of the transaction operation.
    /// - Throws: `FDBError` if the transaction fails after all retry attempts.
    func withTransaction<T: Sendable>(
        _ operation: (TransactionProtocol) async throws -> T
    ) async throws -> T
}

/// Protocol defining the interface for FoundationDB transactions.
///
/// `TransactionProtocol` provides all the operations that can be performed within
/// a FoundationDB transaction, including reads, writes, atomic operations,
/// and transaction management.
/// Transaction interface for FoundationDB operations
public protocol TransactionProtocol: Sendable {
    /// Retrieves a value for the given key.
    ///
    /// - Parameters:
    ///   - key: The key to retrieve as a byte array.
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: The value associated with the key, or nil if not found.
    /// - Throws: `FDBError` if the operation fails.
    func getValue(for key: FDB.Key, snapshot: Bool) async throws -> FDB.Value?

    /// Sets a value for the given key.
    ///
    /// - Parameters:
    ///   - value: The value to set as a byte array.
    ///   - key: The key to associate with the value.
    func setValue(_ value: FDB.Value, for key: FDB.Key)

    /// Removes a key-value pair from the database.
    ///
    /// - Parameter key: The key to remove as a byte array.
    func clear(key: FDB.Key)

    /// Removes all key-value pairs in the given range.
    ///
    /// - Parameters:
    ///   - beginKey: The start of the range (inclusive) as a byte array.
    ///   - endKey: The end of the range (exclusive) as a byte array.
    func clearRange(beginKey: FDB.Key, endKey: FDB.Key)

    /// Resolves a key selector to an actual key.
    ///
    /// - Parameters:
    ///   - selector: The key selector to resolve.
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: The resolved key, or nil if no key matches.
    /// - Throws: `FDBError` if the operation fails.
    func getKey(selector: FDB.Selectable, snapshot: Bool) async throws -> FDB.Key?

    /// Resolves a key selector to an actual key.
    ///
    /// - Parameters:
    ///   - selector: The key selector to resolve.
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: The resolved key, or nil if no key matches.
    /// - Throws: `FDBError` if the operation fails.
    func getKey(selector: FDB.KeySelector, snapshot: Bool) async throws -> FDB.Key?

    /// Returns an AsyncSequence that yields key-value pairs within a range.
    ///
    /// - Parameters:
    ///   - beginSelector: The key selector for the start of the range.
    ///   - endSelector: The key selector for the end of the range.
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: An async sequence that yields key-value pairs.
    func readRange(
        beginSelector: FDB.KeySelector, endSelector: FDB.KeySelector, snapshot: Bool
    ) -> FDB.AsyncKVSequence

    /// Retrieves key-value pairs within a range using selectable endpoints.
    ///
    /// - Parameters:
    ///   - begin: The start of the range (converted to key selector).
    ///   - end: The end of the range (converted to key selector).
    ///   - limit: Maximum number of key-value pairs to return (0 for no limit).
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: A `ResultRange` containing the key-value pairs and more flag.
    /// - Throws: `FDBError` if the operation fails.
    func getRange(
        begin: FDB.Selectable, end: FDB.Selectable, limit: Int, snapshot: Bool
    ) async throws -> ResultRange

    /// Retrieves key-value pairs within a range using key selectors.
    ///
    /// - Parameters:
    ///   - beginSelector: The key selector for the start of the range.
    ///   - endSelector: The key selector for the end of the range.
    ///   - limit: Maximum number of key-value pairs to return (0 for no limit).
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: A `ResultRange` containing the key-value pairs and more flag.
    /// - Throws: `FDBError` if the operation fails.
    func getRange(
        beginSelector: FDB.KeySelector, endSelector: FDB.KeySelector, limit: Int, snapshot: Bool
    ) async throws -> ResultRange

    /// Retrieves key-value pairs within a range using byte array keys.
    ///
    /// - Parameters:
    ///   - beginKey: The start key of the range as a byte array.
    ///   - endKey: The end key of the range as a byte array.
    ///   - limit: Maximum number of key-value pairs to return (0 for no limit).
    ///   - snapshot: Whether to perform a snapshot read.
    /// - Returns: A `ResultRange` containing the key-value pairs and more flag.
    /// - Throws: `FDBError` if the operation fails.
    func getRange(
        beginKey: FDB.Key, endKey: FDB.Key, limit: Int, snapshot: Bool
    ) async throws -> ResultRange

    /// Commits the transaction.
    ///
    /// - Returns: `true` if the transaction was successfully committed.
    /// - Throws: `FDBError` if the commit fails.
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
    /// - Throws: `FDBError` if the operation fails.
    func getVersionstamp() async throws -> FDB.Key?

    /// Sets the read version for snapshot reads.
    ///
    /// - Parameter version: The version to use for snapshot reads.
    func setReadVersion(_ version: FDB.Version)

    /// Gets the read version used by this transaction.
    ///
    /// - Returns: The transaction's read version.
    /// - Throws: `FDBError` if the operation fails.
    func getReadVersion() async throws -> FDB.Version

    /// Handles transaction errors and implements retry logic with exponential backoff.
    ///
    /// If this method returns successfully, the transaction has been reset and can be retried.
    /// If it throws an error, the transaction should not be retried.
    ///
    /// - Parameter error: The error encountered during transaction execution.
    /// - Throws: `FDBError` if the error is not retryable or retry limits have been exceeded.
    func onError(_ error: FDBError) async throws

    /// Returns an estimated byte size of the specified key range.
    ///
    /// The estimate is calculated based on sampling done by FDB server. Larger key-value pairs
    /// are more likely to be sampled. For accuracy, use on large ranges (>3MB recommended).
    ///
    /// - Parameters:
    ///   - beginKey: The start of the range (inclusive).
    ///   - endKey: The end of the range (exclusive).
    /// - Returns: The estimated size in bytes.
    /// - Throws: `FDBError` if the operation fails.
    func getEstimatedRangeSizeBytes(beginKey: FDB.Key, endKey: FDB.Key) async throws -> Int

    /// Returns a list of keys that can split the given range into roughly equal chunks.
    ///
    /// The returned split points include the start and end keys of the range.
    ///
    /// - Parameters:
    ///   - beginKey: The start of the range.
    ///   - endKey: The end of the range.
    ///   - chunkSize: The desired size of each chunk in bytes.
    /// - Returns: An array of keys representing split points.
    /// - Throws: `FDBError` if the operation fails.
    func getRangeSplitPoints(beginKey: FDB.Key, endKey: FDB.Key, chunkSize: Int) async throws -> [[UInt8]]

    /// Returns the version number at which a committed transaction modified the database.
    ///
    /// Must only be called after a successful commit. Read-only transactions return -1.
    ///
    /// - Returns: The committed version number.
    /// - Throws: `FDBError` if called before commit or if the operation fails.
    func getCommittedVersion() throws -> FDB.Version

    /// Returns the approximate transaction size so far.
    ///
    /// This is the sum of estimated sizes of mutations, read conflict ranges, and write conflict ranges.
    /// Can be called multiple times before commit.
    ///
    /// - Returns: The approximate size in bytes.
    /// - Throws: `FDBError` if the operation fails.
    func getApproximateSize() async throws -> Int

    /// Performs an atomic operation on a key.
    ///
    /// - Parameters:
    ///   - key: The key to operate on.
    ///   - param: The parameter for the atomic operation.
    ///   - mutationType: The type of atomic operation to perform.
    func atomicOp(key: FDB.Key, param: FDB.Value, mutationType: FDB.MutationType)

    /// Adds a conflict range to the transaction.
    ///
    /// Conflict ranges are used to manually declare the read and write sets of the transaction.
    /// This can be useful for ensuring serializability when certain keys are accessed indirectly.
    ///
    /// - Parameters:
    ///   - beginKey: The start of the range (inclusive) as a byte array.
    ///   - endKey: The end of the range (exclusive) as a byte array.
    ///   - type: The type of conflict range (read or write).
    /// - Throws: `FDBError` if the operation fails.
    func addConflictRange(beginKey: FDB.Key, endKey: FDB.Key, type: FDB.ConflictRangeType) throws

    // MARK: - Transaction option methods

    /// Sets a transaction option with an optional value.
    ///
    /// - Parameters:
    ///   - value: Optional byte array value for the option.
    ///   - option: The transaction option to set.
    /// - Throws: `FDBError` if the option cannot be set.
    func setOption(to value: FDB.Value?, forOption option: FDB.TransactionOption) throws

    /// Sets a transaction option with a string value.
    ///
    /// - Parameters:
    ///   - value: String value for the option.
    ///   - option: The transaction option to set.
    /// - Throws: `FDBError` if the option cannot be set.
    func setOption(to value: String, forOption option: FDB.TransactionOption) throws

    /// Sets a transaction option with an integer value.
    ///
    /// - Parameters:
    ///   - value: Integer value for the option.
    ///   - option: The transaction option to set.
    /// - Throws: `FDBError` if the option cannot be set.
    func setOption(to value: Int, forOption option: FDB.TransactionOption) throws
}

/// Default implementation of transaction retry logic for `DatabaseProtocol`.
extension DatabaseProtocol {
    /// Default implementation of `withTransaction` with automatic retry logic.
    ///
    /// This implementation automatically retries transactions when they encounter
    /// retryable errors, up to a maximum number of attempts.
    ///
    /// - Parameter operation: The transaction operation to execute.
    /// - Returns: The result of the successful transaction.
    /// - Throws: `FDBError` if all retry attempts fail.
    public func withTransaction<T: Sendable>(
        _ operation: (TransactionProtocol) async throws -> T
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

                if let fdbError = error as? FDBError, fdbError.isRetryable {
                    if attempt < maxRetries - 1 {
                        continue
                    }
                }

                throw error
            }
        }

        throw FDBError(.transactionTooOld)
    }
}

extension TransactionProtocol {
 public func getValue(for key: FDB.Key, snapshot: Bool = false) async throws -> FDB.Value? {
        try await getValue(for: key, snapshot: snapshot)
    }

 public func getKey(selector: FDB.Selectable, snapshot: Bool = false) async throws -> FDB.Key? {
        try await getKey(selector: selector.toKeySelector(), snapshot: snapshot)
    }

 public func getKey(selector: FDB.KeySelector, snapshot: Bool = false) async throws -> FDB.Key? {
        try await getKey(selector: selector, snapshot: snapshot)
    }

 public func readRange(
        beginSelector: FDB.KeySelector, endSelector: FDB.KeySelector, snapshot: Bool = false
    ) -> FDB.AsyncKVSequence {
        FDB.AsyncKVSequence(
            transaction: self,
            beginSelector: beginSelector,
            endSelector: endSelector,
            snapshot: snapshot
        )
    }

 public func readRange(
        beginSelector: FDB.KeySelector, endSelector: FDB.KeySelector
    ) -> FDB.AsyncKVSequence {
        readRange(
            beginSelector: beginSelector, endSelector: endSelector, snapshot: false
        )
    }

 public func readRange(
        begin: FDB.Selectable, end: FDB.Selectable, snapshot: Bool = false
    ) -> FDB.AsyncKVSequence {
        let beginSelector = begin.toKeySelector()
        let endSelector = end.toKeySelector()
        return readRange(
            beginSelector: beginSelector, endSelector: endSelector, snapshot: snapshot
        )
    }

 public func readRange(
        beginKey: FDB.Key, endKey: FDB.Key, snapshot: Bool = false
    ) -> FDB.AsyncKVSequence {
        let beginSelector = FDB.KeySelector.firstGreaterOrEqual(beginKey)
        let endSelector = FDB.KeySelector.firstGreaterOrEqual(endKey)
        return readRange(
            beginSelector: beginSelector, endSelector: endSelector, snapshot: snapshot
        )
    }

 public func getRange(
        begin: FDB.Selectable, end: FDB.Selectable, limit: Int = 0, snapshot: Bool = false
    ) async throws -> ResultRange {
        let beginSelector = begin.toKeySelector()
        let endSelector = end.toKeySelector()
        return try await getRange(
            beginSelector: beginSelector, endSelector: endSelector, limit: limit, snapshot: snapshot
        )
    }

 public func getRange(
        beginSelector: FDB.KeySelector, endSelector: FDB.KeySelector, limit: Int = 0,
        snapshot: Bool = false
    ) async throws -> ResultRange {
        try await getRange(
            beginSelector: beginSelector, endSelector: endSelector, limit: limit, snapshot: snapshot
        )
    }

 public func getRange(
        beginKey: FDB.Key, endKey: FDB.Key, limit: Int = 0, snapshot: Bool = false
    ) async throws -> ResultRange {
        try await getRange(beginKey: beginKey, endKey: endKey, limit: limit, snapshot: snapshot)
    }

 public func setOption(forOption option: FDB.TransactionOption) throws {
        try setOption(to: nil, forOption: option)
    }

 public func setOption(to value: String, forOption option: FDB.TransactionOption) throws {
        let valueBytes = [UInt8](value.utf8)
        try setOption(to: valueBytes, forOption: option)
    }

 public func setOption(to value: Int, forOption option: FDB.TransactionOption) throws {
        let valueBytes = withUnsafeBytes(of: Int64(value)) { [UInt8]($0) }
        try setOption(to: valueBytes, forOption: option)
    }
}
