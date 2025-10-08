/*
 * Database.swift
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

/// A FoundationDB database connection.
///
/// `FdbDatabase` represents a connection to a FoundationDB database and implements
/// the `IDatabase` protocol. It provides transaction creation capabilities and
/// automatically manages the underlying database connection resource.
///
/// ## Usage Example
/// ```swift
/// let database = try FdbClient.openDatabase()
/// let transaction = try database.createTransaction()
/// ```
public class FdbDatabase: IDatabase {
    /// The underlying FoundationDB database pointer.
    private let database: OpaquePointer

    /// Initializes a new database instance with the given database pointer.
    ///
    /// - Parameter database: The underlying FoundationDB database pointer.
    init(database: OpaquePointer) {
        self.database = database
    }

    /// Cleans up the database connection when the instance is deallocated.
    deinit {
        fdb_database_destroy(database)
    }

    /// Creates a new transaction for database operations.
    ///
    /// Creates and returns a new transaction that can be used to perform
    /// read and write operations on the database.
    ///
    /// - Returns: A new transaction instance conforming to `ITransaction`.
    /// - Throws: `FdbError` if the transaction cannot be created.
    public func createTransaction() throws -> any ITransaction {
        var transaction: OpaquePointer?
        let error = fdb_database_create_transaction(database, &transaction)
        if error != 0 {
            throw FdbError(code: error)
        }

        guard let tr = transaction else {
            throw FdbError(.internalError)
        }

        return FdbTransaction(transaction: tr)
    }

    /// Sets a database option with a byte array value.
    ///
    /// - Parameters:
    ///   - option: The database option to set.
    ///   - value: The value for the option (optional).
    /// - Throws: `FdbError` if the option cannot be set.
    public func setOption(_ option: Fdb.DatabaseOption, value: Fdb.Value? = nil) throws {
        let error: Int32
        if let value = value {
            error = value.withUnsafeBytes { bytes in
                fdb_database_set_option(
                    database,
                    FDBDatabaseOption(option.rawValue),
                    bytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(value.count)
                )
            }
        } else {
            error = fdb_database_set_option(database, FDBDatabaseOption(option.rawValue), nil, 0)
        }

        if error != 0 {
            throw FdbError(code: error)
        }
    }

    /// Sets a database option with a string value.
    ///
    /// - Parameters:
    ///   - option: The database option to set.
    ///   - value: The string value for the option.
    /// - Throws: `FdbError` if the option cannot be set.
    public func setOption(_ option: Fdb.DatabaseOption, value: String) throws {
        try setOption(option, value: Array(value.utf8))
    }

    /// Sets a database option with an integer value.
    ///
    /// - Parameters:
    ///   - option: The database option to set.
    ///   - value: The integer value for the option.
    /// - Throws: `FdbError` if the option cannot be set.
    public func setOption(_ option: Fdb.DatabaseOption, value: Int) throws {
        var val = Int64(value).littleEndian
        try withUnsafeBytes(of: &val) { bytes in
            try setOption(option, value: Array(bytes))
        }
    }
}
