/*
 * Versionstamp.swift
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

/// Represents a FoundationDB versionstamp (96-bit / 12 bytes)
///
/// A versionstamp is a 12-byte value consisting of:
/// - 10 bytes: Transaction version (assigned by FDB at commit time)
/// - 2 bytes: User-defined version (for ordering within a transaction)
///
/// Versionstamps are used for:
/// - Optimistic concurrency control
/// - Creating globally unique, monotonically increasing keys
/// - Maintaining temporal ordering of records
///
/// Example usage:
/// ```swift
/// // Create an incomplete versionstamp for writing
/// let vs = Versionstamp.incomplete(userVersion: 0)
/// let tuple = Tuple("prefix", vs)
/// let key = try tuple.packWithVersionstamp()
/// transaction.atomicOp(key: key, param: [], mutationType: .setVersionstampedKey)
///
/// // After commit, read the completed versionstamp
/// let committedVersion = try await transaction.getVersionstamp()
/// let complete = Versionstamp(transactionVersion: committedVersion!, userVersion: 0)
/// ```
public struct Versionstamp: Sendable, Hashable, Equatable, CustomStringConvertible {

    // MARK: - Constants

    /// Size of transaction version in bytes (10 bytes / 80 bits)
    public static let transactionVersionSize = 10

    /// Size of user version in bytes (2 bytes / 16 bits)
    public static let userVersionSize = 2

    /// Total size of versionstamp in bytes (12 bytes / 96 bits)
    public static let totalSize = transactionVersionSize + userVersionSize

    /// Placeholder for incomplete transaction version (10 bytes of 0xFF)
    private static let incompletePlaceholder: [UInt8] = [UInt8](repeating: 0xFF, count: transactionVersionSize)

    // MARK: - Properties

    /// Transaction version (10 bytes)
    /// - nil for incomplete versionstamp (to be filled by FDB at commit time)
    /// - Non-nil for complete versionstamp (after commit)
    public let transactionVersion: [UInt8]?

    /// User-defined version (2 bytes, big-endian)
    /// Used for ordering within a single transaction
    /// Range: 0-65535
    public let userVersion: UInt16

    // MARK: - Initialization

    /// Create a versionstamp
    /// - Parameters:
    ///   - transactionVersion: 10-byte transaction version from FDB (nil for incomplete)
    ///   - userVersion: User-defined version (0-65535)
    public init(transactionVersion: [UInt8]?, userVersion: UInt16 = 0) {
        if let tv = transactionVersion {
            precondition(
                tv.count == Self.transactionVersionSize,
                "Transaction version must be exactly \(Self.transactionVersionSize) bytes"
            )
        }
        self.transactionVersion = transactionVersion
        self.userVersion = userVersion
    }

    /// Create an incomplete versionstamp
    /// - Parameter userVersion: User-defined version (0-65535)
    /// - Returns: Versionstamp with placeholder transaction version
    ///
    /// Use this when creating keys/values that will be filled by FDB at commit time.
    public static func incomplete(userVersion: UInt16 = 0) -> Versionstamp {
        return Versionstamp(transactionVersion: nil, userVersion: userVersion)
    }

    // MARK: - Properties

    /// Check if versionstamp is complete
    /// - Returns: true if transaction version has been set, false otherwise
    public var isComplete: Bool {
        return transactionVersion != nil
    }

    /// Convert to 12-byte representation
    /// - Returns: 12-byte array (10 bytes transaction version + 2 bytes user version, big-endian)
    public func toBytes() -> FDB.Bytes {
        var bytes = transactionVersion ?? Self.incompletePlaceholder

        // User version is stored as big-endian
        bytes.append(contentsOf: withUnsafeBytes(of: userVersion.bigEndian) { Array($0) })

        return bytes
    }

    /// Create from 12-byte representation
    /// - Parameter bytes: 12-byte array
    /// - Returns: Versionstamp
    /// - Throws: `TupleError.invalidEncoding` if bytes length is not 12
    public static func fromBytes(_ bytes: FDB.Bytes) throws -> Versionstamp {
        guard bytes.count == totalSize else {
            throw TupleError.invalidEncoding
        }

        let trVersionBytes = Array(bytes.prefix(transactionVersionSize))
        let userVersionBytes = bytes.suffix(userVersionSize)

        let userVersion = userVersionBytes.withUnsafeBytes {
            $0.load(as: UInt16.self).bigEndian
        }

        // Check if transaction version is incomplete (all 0xFF)
        let isIncomplete = trVersionBytes == incompletePlaceholder

        return Versionstamp(
            transactionVersion: isIncomplete ? nil : trVersionBytes,
            userVersion: userVersion
        )
    }

    // MARK: - Hashable & Equatable

    public func hash(into hasher: inout Hasher) {
        hasher.combine(transactionVersion)
        hasher.combine(userVersion)
    }

    public static func == (lhs: Versionstamp, rhs: Versionstamp) -> Bool {
        return lhs.transactionVersion == rhs.transactionVersion &&
               lhs.userVersion == rhs.userVersion
    }

    // MARK: - Comparable

    /// Versionstamps are ordered lexicographically by their byte representation
    public static func < (lhs: Versionstamp, rhs: Versionstamp) -> Bool {
        return lhs.toBytes().lexicographicallyPrecedes(rhs.toBytes())
    }

    // MARK: - CustomStringConvertible

    public var description: String {
        if let tv = transactionVersion {
            let tvHex = tv.map { String(format: "%02x", $0) }.joined()
            return "Versionstamp(tr:\(tvHex), user:\(userVersion))"
        } else {
            return "Versionstamp(incomplete, user:\(userVersion))"
        }
    }
}

// MARK: - Comparable Conformance

extension Versionstamp: Comparable {}

// MARK: - TupleElement Conformance

extension Versionstamp: TupleElement {
    public func encodeTuple() -> FDB.Bytes {
        var bytes: FDB.Bytes = [TupleTypeCode.versionstamp.rawValue]
        bytes.append(contentsOf: toBytes())
        return bytes
    }

    public static func decodeTuple(from bytes: FDB.Bytes, at offset: inout Int) throws -> Versionstamp {
        guard offset + Versionstamp.totalSize <= bytes.count else {
            throw TupleError.invalidEncoding
        }

        let versionstampBytes = Array(bytes[offset..<(offset + Versionstamp.totalSize)])
        offset += Versionstamp.totalSize

        return try Versionstamp.fromBytes(versionstampBytes)
    }
}
