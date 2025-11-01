/*
 * Subspace.swift
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

import Foundation

/// FoundationDB subspace for key management
///
/// A Subspace represents a well-defined region of keyspace in FoundationDB.
/// It provides methods for encoding keys with a prefix and decoding them back.
///
/// Subspaces are used to partition the key space into logical regions, similar to
/// tables in a relational database. They ensure that keys from different regions
/// don't collide by prepending a unique prefix to all keys.
///
/// ## Example Usage
///
/// ```swift
/// // Create a root subspace
/// let userSpace = Subspace(rootPrefix: "users")
///
/// // Create nested subspaces
/// let activeUsers = userSpace.subspace("active")
///
/// // Pack keys with the subspace prefix
/// let key = userSpace.pack(Tuple(12345, "alice"))
///
/// // Unpack keys to get the original tuple
/// let tuple = try userSpace.unpack(key)
/// ```
public struct Subspace: Sendable {
    /// The binary prefix for this subspace
    public let prefix: FDB.Bytes

    // MARK: - Initialization

    /// Create a subspace with a binary prefix
    ///
    /// - Warning: Subspace is primarily designed for tuple-encoded prefixes.
    ///   Using raw binary prefixes may result in range queries that do not
    ///   include all keys within the subspace if the prefix ends with 0xFF bytes.
    ///
    ///   **Known Limitation**: The `range()` method uses `prefix + [0xFF]` as
    ///   the exclusive upper bound. This means keys like `[prefix, 0xFF, 0x00]`
    ///   will fall outside the returned range because they are lexicographically
    ///   greater than `[prefix, 0xFF]`.
    ///
    ///   Example:
    ///   ```swift
    ///   let subspace = Subspace(prefix: [0x01, 0xFF])
    ///   let (begin, end) = subspace.range()
    ///   // begin = [0x01, 0xFF, 0x00]
    ///   // end   = [0x01, 0xFF, 0xFF]
    ///
    ///   // Keys like [0x01, 0xFF, 0xFF, 0x00] will NOT be included
    ///   // because they are > [0x01, 0xFF, 0xFF] in lexicographical order
    ///   ```
    ///
    /// - Important: For tuple-encoded data (created via `init(rootPrefix:)` or
    ///   `subspace(_:)`), this limitation does not apply because tuple type codes
    ///   never include 0xFF.
    ///
    /// - Note: This behavior matches the official Java, C++, Python, and Go
    ///   implementations. A subspace formed with a raw byte string as a prefix
    ///   is not fully compatible with the tuple layer, and keys stored within it
    ///   cannot be unpacked as tuples unless they were originally tuple-encoded.
    ///
    /// - Recommendation: Use `init(rootPrefix:)` for tuple-encoded data whenever
    ///   possible. Reserve this initializer for special cases like system
    ///   prefixes (e.g., DirectoryLayer internal keys).
    ///
    /// - Parameter prefix: The binary prefix
    ///
    /// - SeeAlso: https://apple.github.io/foundationdb/developer-guide.html#subspaces
    public init(prefix: FDB.Bytes) {
        self.prefix = prefix
    }

    /// Create a subspace with a string prefix
    /// - Parameter rootPrefix: The string prefix (will be encoded as a Tuple)
    public init(rootPrefix: String) {
        let tuple = Tuple(rootPrefix)
        self.prefix = tuple.encode()
    }

    // MARK: - Subspace Creation

    /// Create a nested subspace by appending tuple elements
    /// - Parameter elements: Tuple elements to append
    /// - Returns: A new subspace with the extended prefix
    ///
    /// ## Example
    ///
    /// ```swift
    /// let users = Subspace(rootPrefix: "users")
    /// let activeUsers = users.subspace("active")  // prefix = users + "active"
    /// let userById = activeUsers.subspace(12345)  // prefix = users + "active" + 12345
    /// ```
    public func subspace(_ elements: any TupleElement...) -> Subspace {
        let tuple = Tuple(elements)
        return Subspace(prefix: prefix + tuple.encode())
    }

    // MARK: - Key Encoding/Decoding

    /// Encode a tuple into a key with this subspace's prefix
    /// - Parameter tuple: The tuple to encode
    /// - Returns: The encoded key with prefix
    ///
    /// The returned key will have the format: `[prefix][encoded tuple]`
    public func pack(_ tuple: Tuple) -> FDB.Bytes {
        return prefix + tuple.encode()
    }

    /// Decode a key into a tuple, removing this subspace's prefix
    /// - Parameter key: The key to decode
    /// - Returns: The decoded tuple
    /// - Throws: `TupleError.invalidDecoding` if the key doesn't start with this prefix
    ///
    /// This operation is the inverse of `pack(_:)`. It removes the subspace prefix
    /// and decodes the remaining bytes as a tuple.
    public func unpack(_ key: FDB.Bytes) throws -> Tuple {
        guard key.starts(with: prefix) else {
            throw TupleError.invalidDecoding("Key does not match subspace prefix")
        }
        let tupleBytes = Array(key.dropFirst(prefix.count))
        let elements = try Tuple.decode(from: tupleBytes)
        return Tuple(elements)
    }

    /// Check if a key belongs to this subspace
    /// - Parameter key: The key to check
    /// - Returns: true if the key starts with this subspace's prefix
    ///
    /// ## Example
    ///
    /// ```swift
    /// let userSpace = Subspace(rootPrefix: "users")
    /// let key = userSpace.pack(Tuple(12345))
    /// print(userSpace.contains(key))  // true
    ///
    /// let otherKey = Subspace(rootPrefix: "posts").pack(Tuple(1))
    /// print(userSpace.contains(otherKey))  // false
    /// ```
    public func contains(_ key: FDB.Bytes) -> Bool {
        return key.starts(with: prefix)
    }

    // MARK: - Range Operations

    /// Get the range for scanning all keys in this subspace
    ///
    /// The range is defined as `[prefix + 0x00, prefix + 0xFF)`, which:
    /// - Includes all keys that start with the subspace prefix and have additional bytes
    /// - Does NOT include the bare prefix itself (if it exists as a key)
    ///
    /// ## Important Limitation with Raw Binary Prefixes
    ///
    /// - Warning: If this subspace was created with a raw binary prefix using
    ///   `init(prefix:)`, keys that begin with `[prefix, 0xFF, ...]` may fall
    ///   outside the returned range.
    ///
    ///   This is because `prefix + [0xFF]` is used as the exclusive upper bound,
    ///   and any key starting with `[prefix, 0xFF]` followed by additional bytes
    ///   will be lexicographically greater than `[prefix, 0xFF]`.
    ///
    ///   Example of keys that will be **excluded**:
    ///   ```swift
    ///   let subspace = Subspace(prefix: [0x01, 0xFF])
    ///   let (begin, end) = subspace.range()
    ///   // begin = [0x01, 0xFF, 0x00]
    ///   // end   = [0x01, 0xFF, 0xFF]
    ///
    ///   // These keys are OUTSIDE the range:
    ///   // [0x01, 0xFF, 0xFF]          (equal to end, excluded)
    ///   // [0x01, 0xFF, 0xFF, 0x00]    (> end)
    ///   // [0x01, 0xFF, 0xFF, 0xFF]    (> end)
    ///   ```
    ///
    /// ## Why This Works for Tuple-Encoded Data
    ///
    /// For tuple-encoded data (created via `init(rootPrefix:)` or `subspace(_:)`),
    /// this limitation does not apply because:
    /// - Tuple type codes range from 0x00 to 0x33
    /// - 0xFF is not a valid tuple type code
    /// - Therefore, no tuple-encoded key will ever have 0xFF immediately after the prefix
    ///
    /// This makes `prefix + [0xFF]` a safe exclusive upper bound for all
    /// tuple-encoded keys within the subspace.
    ///
    /// ## Cross-Language Compatibility
    ///
    /// This implementation matches the canonical behavior of all official bindings:
    /// - Java: `new Range(prefix + 0x00, prefix + 0xFF)`
    /// - Python: `slice(prefix + b"\x00", prefix + b"\xff")`
    /// - Go: `(prefix + 0x00, prefix + 0xFF)`
    /// - C++: `(prefix + 0x00, prefix + 0xFF)`
    ///
    /// The limitation with raw binary prefixes exists in all these implementations.
    ///
    /// ## Recommended Usage
    ///
    /// - ✅ **Recommended**: Use with tuple-encoded data via `init(rootPrefix:)` or `subspace(_:)`
    /// - ⚠️ **Caution**: Avoid raw binary prefixes ending in 0xFF bytes
    /// - 💡 **Alternative**: For raw binary prefix ranges, consider using a strinc-based
    ///   method (to be provided in future versions)
    ///
    /// ## Example (Tuple-Encoded Data)
    ///
    /// ```swift
    /// let userSpace = Subspace(rootPrefix: "users")
    /// let (begin, end) = userSpace.range()
    ///
    /// // Scan all user keys (safe - tuple-encoded)
    /// let sequence = transaction.getRange(
    ///     beginKey: begin,
    ///     endKey: end
    /// )
    /// for try await (key, value) in sequence {
    ///     // Process each user key-value pair
    /// }
    /// ```
    ///
    /// - Returns: A tuple of (begin, end) keys for range operations
    ///
    /// - SeeAlso: `init(prefix:)` for warnings about raw binary prefixes
    public func range() -> (begin: FDB.Bytes, end: FDB.Bytes) {
        let begin = prefix + [0x00]
        let end = prefix + [0xFF]
        return (begin, end)
    }

    /// Get a range with specific start and end tuples
    /// - Parameters:
    ///   - start: Start tuple (inclusive)
    ///   - end: End tuple (exclusive)
    /// - Returns: A tuple of (begin, end) keys
    ///
    /// ## Example
    ///
    /// ```swift
    /// let userSpace = Subspace(rootPrefix: "users")
    /// // Scan users with IDs from 1000 to 2000
    /// let (begin, end) = userSpace.range(from: Tuple(1000), to: Tuple(2000))
    /// ```
    public func range(from start: Tuple, to end: Tuple) -> (begin: FDB.Bytes, end: FDB.Bytes) {
        return (pack(start), pack(end))
    }
}

// MARK: - Equatable

extension Subspace: Equatable {
    public static func == (lhs: Subspace, rhs: Subspace) -> Bool {
        return lhs.prefix == rhs.prefix
    }
}

// MARK: - Hashable

extension Subspace: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(prefix)
    }
}

// MARK: - CustomStringConvertible

extension Subspace: CustomStringConvertible {
    public var description: String {
        let hexString = prefix.map { String(format: "%02x", $0) }.joined()
        return "Subspace(prefix: \(hexString))"
    }
}

// MARK: - SubspaceError

/// Errors that can occur in Subspace operations
public enum SubspaceError: Error {
    /// The key cannot be incremented because it contains only 0xFF bytes
    case cannotIncrementKey(String)
}

// MARK: - FDB.Bytes String Increment Extension

extension FDB.Bytes {
    /// String increment for raw binary prefixes
    ///
    /// Returns the first key that would sort outside the range prefixed by this byte array.
    /// This implements the canonical strinc algorithm used in FoundationDB.
    ///
    /// The algorithm:
    /// 1. Strip all trailing 0xFF bytes
    /// 2. Increment the last remaining byte
    /// 3. Return the truncated result
    ///
    /// This matches the behavior of:
    /// - Go: `fdb.Strinc()`
    /// - Java: `ByteArrayUtil.strinc()`
    /// - Python: `fdb.strinc()`
    ///
    /// - Returns: Incremented byte array
    /// - Throws: `SubspaceError.cannotIncrementKey` if the byte array is empty
    ///   or contains only 0xFF bytes
    ///
    /// ## Example
    ///
    /// ```swift
    /// try [0x01, 0x02].strinc()       // → [0x01, 0x03]
    /// try [0x01, 0xFF].strinc()       // → [0x02]
    /// try [0x01, 0x02, 0xFF, 0xFF].strinc()  // → [0x01, 0x03]
    /// try [0xFF, 0xFF].strinc()       // throws SubspaceError.cannotIncrementKey
    /// try [].strinc()                 // throws SubspaceError.cannotIncrementKey
    /// ```
    ///
    /// - SeeAlso: `Subspace.prefixRange()` for usage with Subspace
    public func strinc() throws -> FDB.Bytes {
        // Strip trailing 0xFF bytes
        var result = self
        while result.last == 0xFF {
            result.removeLast()
        }

        // Check if result is empty (input was empty or all 0xFF)
        guard !result.isEmpty else {
            throw SubspaceError.cannotIncrementKey(
                "Key must contain at least one byte not equal to 0xFF"
            )
        }

        // Increment the last byte
        result[result.count - 1] = result[result.count - 1] &+ 1

        return result
    }
}

// MARK: - Subspace Prefix Range Extension

extension Subspace {
    /// Get range for raw binary prefix (includes prefix itself)
    ///
    /// This method is useful when working with raw binary prefixes that were not
    /// tuple-encoded. It uses the strinc algorithm to compute the exclusive upper bound,
    /// which ensures that ALL keys starting with the prefix are included in the range.
    ///
    /// Unlike `range()`, which uses `prefix + [0xFF]` as the upper bound, this method
    /// uses `strinc(prefix)`, which correctly handles prefixes ending in 0xFF bytes.
    ///
    /// ## When to Use This Method
    ///
    /// - ✅ Use this when the subspace was created with `init(prefix:)` using raw binary data
    /// - ✅ Use this when you need to ensure ALL keys with the prefix are included
    /// - ✅ Use this for non-tuple-encoded keys
    ///
    /// ## When to Use `range()` Instead
    ///
    /// - ✅ Use `range()` for tuple-encoded data (via `init(rootPrefix:)` or `subspace(_:)`)
    /// - ✅ Use `range()` for standard tuple-based data modeling
    ///
    /// ## Comparison
    ///
    /// ```swift
    /// let subspace = Subspace(prefix: [0x01, 0xFF])
    ///
    /// // range() - may miss keys
    /// let (begin1, end1) = subspace.range()
    /// // begin1 = [0x01, 0xFF, 0x00]
    /// // end1   = [0x01, 0xFF, 0xFF]
    /// // Excludes: [0x01, 0xFF, 0xFF, 0x00], [0x01, 0xFF, 0xFF, 0xFF], etc.
    ///
    /// // prefixRange() - includes all keys
    /// let (begin2, end2) = try subspace.prefixRange()
    /// // begin2 = [0x01, 0xFF]
    /// // end2   = [0x02]
    /// // Includes: ALL keys starting with [0x01, 0xFF]
    /// ```
    ///
    /// - Returns: Range from prefix (inclusive) to strinc(prefix) (exclusive)
    /// - Throws: `SubspaceError.cannotIncrementKey` if prefix cannot be incremented
    ///   (i.e., if the prefix is empty or contains only 0xFF bytes)
    ///
    /// ## Example
    ///
    /// ```swift
    /// let subspace = Subspace(prefix: [0x01, 0xFF])
    ///
    /// do {
    ///     let (begin, end) = try subspace.prefixRange()
    ///     // begin = [0x01, 0xFF]
    ///     // end   = [0x02]
    ///
    ///     let sequence = transaction.getRange(beginKey: begin, endKey: end)
    ///     for try await (key, value) in sequence {
    ///         // Process all keys starting with [0x01, 0xFF]
    ///         // Including [0x01, 0xFF, 0xFF, 0x00] and beyond
    ///     }
    /// } catch SubspaceError.cannotIncrementKey(let message) {
    ///     print("Cannot create range: \(message)")
    /// }
    /// ```
    ///
    /// - SeeAlso: `range()` for tuple-encoded data ranges
    /// - SeeAlso: `FDB.Bytes.strinc()` for the underlying algorithm
    public func prefixRange() throws -> (begin: FDB.Bytes, end: FDB.Bytes) {
        return (prefix, try prefix.strinc())
    }
}
