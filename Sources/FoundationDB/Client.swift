/*
 * Client.swift
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

/// The main client interface for FoundationDB operations.
///
/// `FDBClient` provides the primary entry point for connecting to and interacting
/// with a FoundationDB cluster. It handles network initialization, database connections,
/// and global configuration settings.
///
/// ## Usage Example
/// ```swift
/// // Initialize the client
/// try await FDBClient.initialize()
///
/// // Open a database connection
/// let database = try FDBClient.openDatabase()
/// ```
// TODO: Remove hard-coded error codes.
public final class FDBClient: Sendable {
    /// FoundationDB API version constants.
    public static let defaultApiVersion: Int = 710

    /// Initializes the FoundationDB client with the specified API version.
    ///
    /// This method must be called before performing any other FoundationDB operations.
    /// It sets up the network layer and starts the network thread.
    ///
    /// - Parameter version: The FoundationDB API version to use. Defaults to the current version.
    /// - Throws: `FDBError` if initialization fails.
    public static func initialize(version: Int = FDBClient.defaultApiVersion) async throws {
        try FDBNetwork.shared.initialize(version: version)
    }

    /// Returns true if FDB network is initialized.
    public static var isInitialized: Bool { FDBNetwork.shared.isInitialized }

    /// Opens a connection to a FoundationDB database.
    ///
    /// Creates and returns a database handle that can be used to create transactions
    /// and perform database operations.
    ///
    /// - Parameter clusterFilePath: Optional path to the cluster file. If nil, uses the default cluster file.
    /// - Returns: An `FDBDatabase` instance for performing database operations.
    /// - Throws: `FDBError` if the database connection cannot be established.
    public static func openDatabase(clusterFilePath: String? = nil) throws -> FDBDatabase {
        var database: OpaquePointer?
        let error = fdb_create_database(clusterFilePath, &database)
        if error != 0 {
            throw FDBError(code: error)
        }

        guard let db = database else {
            throw FDBError(.clientError)
        }

        return FDBDatabase(database: db)
    }

    /// Sets a network option with an optional byte array value.
    ///
    /// - Parameters:
    ///   - value: Optional byte array value for the option.
    ///   - option: The network option to set.
    /// - Throws: `FDBError` if the option cannot be set.
    public static func setNetworkOption(to value: [UInt8]? = nil, forOption option: FDB.NetworkOption) throws {
        try FDBNetwork.shared.setNetworkOption(to: value, forOption: option)
    }

    /// Sets a network option with a string value.
    ///
    /// - Parameters:
    ///   - value: String value for the option.
    ///   - option: The network option to set.
    /// - Throws: `FDBError` if the option cannot be set.
    public static func setNetworkOption(to value: String, forOption option: FDB.NetworkOption) throws {
        try FDBNetwork.shared.setNetworkOption(to: value, forOption: option)
    }

    /// Sets a network option with an integer value.
    ///
    /// - Parameters:
    ///   - value: Integer value for the option.
    ///   - option: The network option to set.
    /// - Throws: `FDBError` if the option cannot be set.
    public static func setNetworkOption(to value: Int, forOption option: FDB.NetworkOption) throws {
        try FDBNetwork.shared.setNetworkOption(to: value, forOption: option)
    }
}
