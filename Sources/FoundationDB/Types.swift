/*
 * Types.swift
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

/// Type aliases for C API interoperation.
/// Pointer to FoundationDB C future objects.
typealias CFuturePtr = OpaquePointer
/// C callback function type for future completion.
typealias CCallback = @convention(c) (UnsafeRawPointer?, UnsafeRawPointer?) -> Void

/// Core FoundationDB type definitions and utilities.
///
/// The `Fdb` namespace contains all fundamental types used throughout the
/// FoundationDB Swift bindings, including keys, values, version numbers,
/// and key selector utilities.
public enum Fdb {
    /// A FoundationDB version number (64-bit integer).
    public typealias Version = Int64
    /// Raw byte data used throughout the FoundationDB API.
    public typealias Bytes = [UInt8]
    /// A FoundationDB key (sequence of bytes).
    public typealias Key = Bytes
    /// A FoundationDB value (sequence of bytes).
    public typealias Value = Bytes
    /// A key-value pair tuple.
    public typealias KeyValue = (Key, Value)
    /// An array of key-value pairs.
    public typealias KeyValueArray = [KeyValue]

    /// Protocol for types that can be converted to key selectors.
    ///
    /// Types conforming to this protocol can be used in range operations
    /// and other APIs that accept key selector parameters.
    public protocol Selectable {
        /// Converts this instance to a key selector.
        ///
        /// - Returns: A `KeySelector` representing this selectable.
        func toKeySelector() -> Fdb.KeySelector
    }

    /// A key selector that specifies a key position within the database.
    ///
    /// Key selectors provide a way to specify keys relative to other keys,
    /// allowing for flexible range queries and key resolution.
    ///
    /// ## Usage Examples
    /// ```swift
    /// // Select the first key >= "apple"
    /// let selector = Fdb.KeySelector.firstGreaterOrEqual("apple")
    ///
    /// // Select the last key < "zebra"
    /// let selector = Fdb.KeySelector.lastLessThan("zebra")
    /// ```
    public struct KeySelector: Selectable, @unchecked Sendable {
        /// The reference key for this selector.
        public let key: Key
        /// Whether to include the reference key itself in selection.
        public let orEqual: Bool
        /// Offset from the selected key position.
        public let offset: Int32

        /// Creates a new key selector.
        ///
        /// - Parameters:
        ///   - key: The reference key.
        ///   - orEqual: Whether to include the reference key itself.
        ///   - offset: Offset from the selected position.
        public init(key: Key, orEqual: Bool, offset: Int32) {
            self.key = key
            self.orEqual = orEqual
            self.offset = offset
        }

        /// Returns this key selector (identity function).
        ///
        /// - Returns: This key selector instance.
        public func toKeySelector() -> KeySelector {
            return self
        }

        /// Creates a key selector for the first key greater than or equal to the given key.
        ///
        /// This is the most commonly used key selector pattern.
        ///
        /// - Parameter key: The reference key as a byte array.
        /// - Returns: A key selector that selects the first key >= the reference key.
        public static func firstGreaterOrEqual(_ key: Key) -> KeySelector {
            return KeySelector(key: key, orEqual: false, offset: 1)
        }

        /// Creates a key selector for the first key greater than or equal to the given string.
        ///
        /// Convenience method that converts the string to UTF-8 bytes.
        ///
        /// - Parameter key: The reference key as a string.
        /// - Returns: A key selector that selects the first key >= the reference key.
        public static func firstGreaterOrEqual(_ key: String) -> KeySelector {
            return KeySelector(key: [UInt8](key.utf8), orEqual: false, offset: 1)
        }

        /// Creates a key selector for the first key greater than the given key.
        ///
        /// - Parameter key: The reference key as a byte array.
        /// - Returns: A key selector that selects the first key > the reference key.
        public static func firstGreaterThan(_ key: Key) -> KeySelector {
            return KeySelector(key: key, orEqual: true, offset: 1)
        }

        /// Creates a key selector for the first key greater than the given string.
        ///
        /// Convenience method that converts the string to UTF-8 bytes.
        ///
        /// - Parameter key: The reference key as a string.
        /// - Returns: A key selector that selects the first key > the reference key.
        public static func firstGreaterThan(_ key: String) -> KeySelector {
            return KeySelector(key: [UInt8](key.utf8), orEqual: true, offset: 1)
        }

        /// Creates a key selector for the last key less than or equal to the given key.
        ///
        /// - Parameter key: The reference key as a byte array.
        /// - Returns: A key selector that selects the last key <= the reference key.
        public static func lastLessOrEqual(_ key: Key) -> KeySelector {
            return KeySelector(key: key, orEqual: true, offset: 0)
        }

        /// Creates a key selector for the last key less than or equal to the given string.
        ///
        /// Convenience method that converts the string to UTF-8 bytes.
        ///
        /// - Parameter key: The reference key as a string.
        /// - Returns: A key selector that selects the last key <= the reference key.
        public static func lastLessOrEqual(_ key: String) -> KeySelector {
            return KeySelector(key: [UInt8](key.utf8), orEqual: true, offset: 0)
        }

        /// Creates a key selector for the last key less than the given key.
        ///
        /// - Parameter key: The reference key as a byte array.
        /// - Returns: A key selector that selects the last key < the reference key.
        public static func lastLessThan(_ key: Key) -> KeySelector {
            return KeySelector(key: key, orEqual: false, offset: 0)
        }

        /// Creates a key selector for the last key less than the given string.
        ///
        /// Convenience method that converts the string to UTF-8 bytes.
        ///
        /// - Parameter key: The reference key as a string.
        /// - Returns: A key selector that selects the last key < the reference key.
        public static func lastLessThan(_ key: String) -> KeySelector {
            return KeySelector(key: [UInt8](key.utf8), orEqual: false, offset: 0)
        }
    }
}

/// Extension making `Fdb.Key` conformant to `Selectable`.
///
/// This allows key byte arrays to be used directly in range operations
/// by converting them to "first greater or equal" key selectors.
extension Fdb.Key: Fdb.Selectable {
    /// Converts this key to a key selector using "first greater or equal" semantics.
    ///
    /// - Returns: A key selector that selects the first key >= this key.
    public func toKeySelector() -> Fdb.KeySelector {
        return Fdb.KeySelector.firstGreaterOrEqual(self)
    }
}

/// Extension making `String` conformant to `Selectable`.
///
/// This allows strings to be used directly in range operations by converting
/// them to UTF-8 bytes and then to "first greater or equal" key selectors.
extension String: Fdb.Selectable {
    /// Converts this string to a key selector using "first greater or equal" semantics.
    ///
    /// - Returns: A key selector that selects the first key >= this string (as UTF-8 bytes).
    public func toKeySelector() -> Fdb.KeySelector {
        return Fdb.KeySelector.firstGreaterOrEqual([UInt8](utf8))
    }
}
