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
/// The `FDB` namespace contains all fundamental types used throughout the
/// FoundationDB Swift bindings, including keys, values, version numbers,
/// and key selector utilities.
public enum FDB {
    /// A FoundationDB version number (64-bit integer).
    public typealias Version = Int64

    /// Raw byte data used throughout the FoundationDB API.
    public typealias Bytes = [UInt8]

    /// An array of key-value pairs.
    public typealias KeyValueArray = [(Bytes, Bytes)]

    /// Protocol for types that can be converted to key selectors.
    ///
    /// Types conforming to this protocol can be used in range operations
    /// and other APIs that accept key selector parameters.
    public protocol Selectable {
        /// Converts this instance to a key selector.
        ///
        /// - Returns: A `KeySelector` representing this selectable.
        func toKeySelector() -> FDB.KeySelector
    }

    /// A key selector that specifies a key position within the database.
    ///
    /// Key selectors provide a way to specify keys relative to other keys,
    /// allowing for flexible range queries and key resolution.
    ///
    /// ## Usage Examples
    /// ```swift
    /// // Select the first key >= "apple"
    /// let selector = FDB.KeySelector.firstGreaterOrEqual("apple")
    ///
    /// // Select the last key < "zebra"
    /// let selector = FDB.KeySelector.lastLessThan("zebra")
    /// ```
    public struct KeySelector: Selectable, Sendable {
        /// The reference key for this selector.
        public let key: Bytes
        /// Whether to include the reference key itself in selection.
        public let orEqual: Bool
        /// Offset from the selected key position.
        public let offset: Int

        /// Creates a new key selector.
        ///
        /// - Parameters:
        ///   - key: The reference key.
        ///   - orEqual: Whether to include the reference key itself.
        ///   - offset: Offset from the selected position.
        public init(key: Bytes, orEqual: Bool, offset: Int) {
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
        public static func firstGreaterOrEqual(_ key: FDB.Bytes) -> KeySelector {
            return KeySelector(key: key, orEqual: false, offset: 1)
        }

        /// Creates a key selector for the first key greater than the given key.
        ///
        /// - Parameter key: The reference key as a byte array.
        /// - Returns: A key selector that selects the first key > the reference key.
        public static func firstGreaterThan(_ key: FDB.Bytes) -> KeySelector {
            return KeySelector(key: key, orEqual: true, offset: 1)
        }

        /// Creates a key selector for the last key less than or equal to the given key.
        ///
        /// - Parameter key: The reference key as a byte array.
        /// - Returns: A key selector that selects the last key <= the reference key.
        public static func lastLessOrEqual(_ key: FDB.Bytes) -> KeySelector {
            return KeySelector(key: key, orEqual: true, offset: 0)
        }

        /// Creates a key selector for the last key less than the given key.
        ///
        /// - Parameter key: The reference key as a byte array.
        /// - Returns: A key selector that selects the last key < the reference key.
        public static func lastLessThan(_ key: FDB.Bytes) -> KeySelector {
            return KeySelector(key: key, orEqual: false, offset: 0)
        }
    }
}

/// Extension making `FDB.Key` conformant to `Selectable`.
///
/// This allows key byte arrays to be used directly in range operations
/// by converting them to "first greater or equal" key selectors.
extension FDB.Bytes: FDB.Selectable {
    /// Converts this key to a key selector using "first greater or equal" semantics.
    ///
    /// - Returns: A key selector that selects the first key >= this key.
    public func toKeySelector() -> FDB.KeySelector {
        return FDB.KeySelector.firstGreaterOrEqual(self)
    }
}

/// Extension making `String` conformant to `Selectable`.
///
/// This allows strings to be used directly in range operations by converting
/// them to UTF-8 bytes and then to "first greater or equal" key selectors.
extension String: FDB.Selectable {
    /// Converts this string to a key selector using "first greater or equal" semantics.
    ///
    /// - Returns: A key selector that selects the first key >= this string (as UTF-8 bytes).
    public func toKeySelector() -> FDB.KeySelector {
        return FDB.KeySelector.firstGreaterOrEqual([UInt8](utf8))
    }
}
