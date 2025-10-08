/*
 * Network.swift
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

#if canImport(Darwin)
    import Darwin
#elseif canImport(Glibc)
    import Glibc
#endif

/// Singleton network manager for FoundationDB operations.
///
/// `FdbNetwork` manages the FoundationDB network layer, including initialization,
/// network thread management, and network option configuration. It follows the
/// singleton pattern to ensure only one network instance exists per process.
///
/// ## Usage Example
/// ```swift
/// let network = FdbNetwork.shared
/// try network.initialize(version: 740)
/// ```
@MainActor
class FdbNetwork {
    /// The shared singleton instance of the network manager.
    static let shared = FdbNetwork()

    /// Indicates whether the network has been set up.
    private nonisolated(unsafe) var networkSetup = false

    /// The pthread handle for the network thread.
    private nonisolated(unsafe) var networkThread: pthread_t? = nil

    /// Initializes the FoundationDB network with the specified API version.
    ///
    /// This method performs the complete network initialization sequence:
    /// selecting the API version, setting up the network, and starting the network thread.
    ///
    /// - Parameter version: The FoundationDB API version to use.
    /// - Throws: `FdbError` if any step of initialization fails.
    func initialize(version: Int32) throws {
        if networkSetup {
            // throw FdbError(code: 2201)
            return
        }

        try selectAPIVersion(version)
        try setupNetwork()
        startNetwork()
    }

    /// Stops the FoundationDB network and waits for the network thread to complete.
    deinit {
        if !networkSetup {
            return
        }

        // Call stop_network and wait for network thread to complete
        let error = fdb_stop_network()
        if error != 0 {
            print("Failed to stop network in deinit: \(FdbError(code: error).description)")
        }

        if let thread = networkThread {
            pthread_join(thread, nil)
        }
    }

    /// Selects the FoundationDB API version.
    ///
    /// - Parameter version: The API version to select.
    /// - Throws: `FdbError` if the API version cannot be selected.
    func selectAPIVersion(_ version: Int32) throws {
        let error = fdb_select_api_version_impl(version, FDB_API_VERSION)
        if error != 0 {
            throw FdbError(code: error)
        }
    }

    /// Sets up the FoundationDB network layer.
    ///
    /// This method must be called before starting the network thread.
    ///
    /// - Throws: `FdbError` if network setup fails or if already set up.X
    func setupNetwork() throws {
        guard !networkSetup else {
            throw FdbError(.networkError)
        }

        let error = fdb_setup_network()
        if error != 0 {
            throw FdbError(code: error)
        }

        networkSetup = true
    }

    /// Starts the FoundationDB network thread.
    ///
    /// Creates and starts a pthread that runs the FoundationDB network event loop.
    /// The network must be set up before calling this method.
    func startNetwork() {
        guard networkSetup else {
            fatalError("Network must be setup before starting thread network")
        }

        var thread = pthread_t(bitPattern: 0)
        let result = pthread_create(&thread, nil, { _ in
            let error = fdb_run_network()
            if error != 0 {
                print("Network thread error: \(FdbError(code: error).description)")
            }
            return nil
        }, nil)

        if result == 0 {
            networkThread = thread
        }
    }

    /// Sets a network option with an optional byte array value.
    ///
    /// - Parameters:
    ///   - option: The network option to set.
    ///   - value: Optional byte array value for the option.
    /// - Throws: `FdbError` if the option cannot be set.
    func setNetworkOption(_ option: Fdb.NetworkOption, value: [UInt8]? = nil) throws {
        let error: Int32
        if let value = value {
            error = value.withUnsafeBytes { bytes in
                fdb_network_set_option(
                    FDBNetworkOption(option.rawValue),
                    bytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(value.count)
                )
            }
        } else {
            error = fdb_network_set_option(FDBNetworkOption(option.rawValue), nil, 0)
        }

        if error != 0 {
            throw FdbError(code: error)
        }
    }

    /// Sets a network option with a string value.
    ///
    /// - Parameters:
    ///   - option: The network option to set.
    ///   - value: String value for the option (automatically converted to UTF-8 bytes).
    /// - Throws: `FdbError` if the option cannot be set.
    func setNetworkOption(_ option: Fdb.NetworkOption, value: String) throws {
        try setNetworkOption(option, value: [UInt8](value.utf8))
    }

    /// Sets a network option with an integer value.
    ///
    /// - Parameters:
    ///   - option: The network option to set.
    ///   - value: Integer value for the option (automatically converted to 64-bit bytes).
    /// - Throws: `FdbError` if the option cannot be set.
    func setNetworkOption(_ option: Fdb.NetworkOption, value: Int) throws {
        let valueBytes = withUnsafeBytes(of: Int64(value)) { [UInt8]($0) }
        try setNetworkOption(option, value: valueBytes)
    }
}
