/*
 * Future.swift
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

/// Protocol for types that can be extracted from FoundationDB C futures.
///
/// Types conforming to this protocol can be used as the result type for `Future<T>`
/// and provide the implementation for extracting their value from the underlying
/// C future object.
// TODO: Explore ways to use Span and avoid copying bytes from CFuture into Swift.

protocol FutureResult: Sendable {
    /// Extracts the result value from a C future.
    ///
    /// - Parameter fromFuture: The C future pointer to extract from.
    /// - Returns: The extracted result value, or nil if no value is present.
    /// - Throws: `FdbError` if the future contains an error.
    static func extract(fromFuture: CFuturePtr) throws -> Self?
}

/// A Swift wrapper for FoundationDB C futures that provides async/await support.
///
/// `Future<T>` bridges FoundationDB's callback-based C API with Swift's structured
/// concurrency model, allowing async operations to be awaited naturally.
///
/// ## Usage Example
/// ```swift
/// let future = Future<ResultValue>(cFuturePtr)
/// let result = try await future.getAsync()
/// ```
class Future<T: FutureResult> {
    /// The underlying C future pointer.
    private let cFuture: CFuturePtr

    /// Initializes a new Future with the given C future pointer.
    ///
    /// - Parameter cFuture: The C future pointer to wrap.
    init(_ cFuture: CFuturePtr) {
        self.cFuture = cFuture
    }

    /// Cleans up the C future when the instance is deallocated.
    deinit {
        fdb_future_destroy(cFuture)
    }

    /// Asynchronously waits for the future to complete and returns the result.
    ///
    /// This method bridges FoundationDB's callback-based API with Swift's async/await,
    /// allowing the caller to await the result of the underlying C future.
    ///
    /// - Returns: The result value extracted from the future, or nil if no value is present.
    /// - Throws: `FdbError` if the future operation failed.
    func getAsync() async throws -> T? {
        try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<T?, Error>) in
            let box = CallbackBox { [continuation] future in
                do {
                    let err = fdb_future_get_error(future)
                    if err != 0 {
                        throw FdbError(code: err)
                    }

                    let value = try T.extract(fromFuture: self.cFuture)
                    continuation.resume(returning: value)
                } catch {
                    continuation.resume(throwing: error)
                }
            }

            let userdata = Unmanaged.passRetained(box).toOpaque() // TODO: If future is canceled, this will not cleanup?
            fdb_future_set_callback(cFuture, fdbFutureCallback, userdata)
        }
    }
}

/// A container for managing callback functions in the C future system.
///
/// This class holds onto Swift callback functions that are passed to the C API,
/// ensuring they remain alive for the duration of the future operation.
private final class CallbackBox {
    /// The callback function to be invoked when the future completes.
    let callback: (CFuturePtr) -> Void

    /// Initializes a new callback box with the given callback.
    ///
    /// - Parameter callback: The callback function to store.
    init(callback: @escaping (CFuturePtr) -> Void) {
        self.callback = callback
    }
}

/// C callback function that bridges to Swift callbacks.
///
/// This function is called by the FoundationDB C API when a future completes.
/// It extracts the Swift callback from the userdata and invokes it.
///
/// - Parameters:
///   - future: The completed C future pointer.
///   - userdata: Opaque pointer containing the `CallbackBox` instance.
private func fdbFutureCallback(future: CFuturePtr?, userdata: UnsafeMutableRawPointer?) {
    guard let userdata, let future = future else { return }
    let box = Unmanaged<CallbackBox>.fromOpaque(userdata).takeRetainedValue()
    box.callback(future)
}

/// A result type for futures that return no data (void operations).
///
/// Used for operations like transaction commits that complete successfully
/// but don't return any specific value.
struct ResultVoid: FutureResult {
    /// Extracts a void result from the future (always succeeds if no error).
    ///
    /// - Parameter fromFuture: The C future to check for errors.
    /// - Returns: A `ResultVoid` instance if successful.
    /// - Throws: `FdbError` if the future contains an error.
    static func extract(fromFuture: CFuturePtr) throws -> Self? {
        let err = fdb_future_get_error(fromFuture)
        if err != 0 {
            throw FdbError(code: err)
        }

        return Self()
    }
}

/// A result type for futures that return version numbers.
///
/// Used for operations that return transaction version stamps or read versions.
struct ResultVersion: FutureResult {
    /// The extracted version value.
    let value: Fdb.Version

    /// Extracts a version from the future.
    ///
    /// - Parameter fromFuture: The C future containing the version.
    /// - Returns: A `ResultVersion` with the extracted version.
    /// - Throws: `FdbError` if the future contains an error.
    static func extract(fromFuture: CFuturePtr) throws -> Self? {
        var version: Int64 = 0
        let err = fdb_future_get_int64(fromFuture, &version)
        if err != 0 {
            throw FdbError(code: err)
        }
        return Self(value: version)
    }
}

/// A result type for futures that return key data.
///
/// Used for operations like key selectors that resolve to actual keys.
struct ResultKey: FutureResult {
    /// The extracted key, or nil if no key was returned.
    let value: Fdb.Key?

    /// Extracts a key from the future.
    ///
    /// - Parameter fromFuture: The C future containing the key data.
    /// - Returns: A `ResultKey` with the extracted key, or nil if no key present.
    /// - Throws: `FdbError` if the future contains an error.
    static func extract(fromFuture: CFuturePtr) throws -> Self? {
        var keyPtr: UnsafePointer<UInt8>?
        var keyLen: Int32 = 0

        let err = fdb_future_get_key(fromFuture, &keyPtr, &keyLen)
        if err != 0 {
            throw FdbError(code: err)
        }

        if let keyPtr {
            let key = Array(UnsafeBufferPointer(start: keyPtr, count: Int(keyLen)))
            return Self(value: key)
        }

        return Self(value: nil)
    }
}

/// A result type for futures that return value data.
///
/// Used for get operations that retrieve values associated with keys.
struct ResultValue: FutureResult {
    /// The extracted value, or nil if no value was found.
    let value: Fdb.Value?

    /// Extracts a value from the future.
    ///
    /// - Parameter fromFuture: The C future containing the value data.
    /// - Returns: A `ResultValue` with the extracted value, or nil if not present.
    /// - Throws: `FdbError` if the future contains an error.
    static func extract(fromFuture: CFuturePtr) throws -> Self? {
        var present: Int32 = 0
        var valPtr: UnsafePointer<UInt8>?
        var valLen: Int32 = 0

        let err = fdb_future_get_value(fromFuture, &present, &valPtr, &valLen)
        if err != 0 {
            throw FdbError(code: err)
        }

        if present != 0, let valPtr {
            let value = Array(UnsafeBufferPointer(start: valPtr, count: Int(valLen)))
            return Self(value: value)
        }

        return Self(value: nil)
    }
}

/// A result type for futures that return key-value ranges.
///
/// Used for range operations that retrieve multiple key-value pairs along
/// with information about whether more data is available.
public struct ResultRange: FutureResult {
    /// The array of key-value pairs returned by the range operation.
    let records: Fdb.KeyValueArray
    /// Indicates whether there are more records beyond this result.
    let more: Bool

    /// Extracts key-value pairs from a range future.
    ///
    /// - Parameter fromFuture: The C future containing the key-value array.
    /// - Returns: A `ResultRange` with the extracted records and more flag.
    /// - Throws: `FdbError` if the future contains an error.
    static func extract(fromFuture: CFuturePtr) throws -> Self? {
        var kvPtr: UnsafePointer<FDBKeyValue>?
        var count: Int32 = 0
        var more: Int32 = 0

        let err = fdb_future_get_keyvalue_array(fromFuture, &kvPtr, &count, &more)
        if err != 0 {
            throw FdbError(code: err)
        }

        guard let kvPtr = kvPtr, count > 0 else {
            return nil
        }

        var keyValueArray: Fdb.KeyValueArray = []
        for i in 0 ..< Int(count) {
            let kv = kvPtr[i]
            let key = Array(UnsafeBufferPointer(start: kv.key, count: Int(kv.key_length)))
            let value = Array(UnsafeBufferPointer(start: kv.value, count: Int(kv.value_length)))
            keyValueArray.append((key, value))
        }

        return Self(records: keyValueArray, more: more > 0)
    }
}
