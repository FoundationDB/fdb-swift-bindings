/*
 * Tuple+Versionstamp.swift
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

// MARK: - Versionstamp Support

extension Tuple {

    /// Pack tuple with an incomplete versionstamp and append offset
    ///
    /// This method packs a tuple that contains exactly one incomplete versionstamp,
    /// and appends the byte offset where the versionstamp appears.
    ///
    /// The offset is always 4 bytes (uint32, little-endian) as per API version 520+.
    /// API versions prior to 520 used 2-byte offsets but are no longer supported.
    ///
    /// The resulting key can be used with `SET_VERSIONSTAMPED_KEY` atomic operation.
    /// At commit time, FoundationDB will replace the 10-byte placeholder with the
    /// actual transaction versionstamp.
    ///
    /// - Parameter prefix: Optional prefix bytes to prepend (default: empty)
    /// - Returns: Packed bytes with offset appended
    /// - Throws: `TupleError.invalidEncoding` if:
    ///   - No incomplete versionstamp found
    ///   - Multiple incomplete versionstamps found
    ///   - Offset exceeds maximum value (65535 for API < 520, 4294967295 for API >= 520)
    ///
    /// Example usage:
    /// ```swift
    /// let vs = Versionstamp.incomplete(userVersion: 0)
    /// let tuple = Tuple("user", 12345, vs)
    /// let key = try tuple.packWithVersionstamp()
    ///
    /// transaction.atomicOp(
    ///     key: key,
    ///     param: [],
    ///     mutationType: .setVersionstampedKey
    /// )
    /// ```
    public func packWithVersionstamp(prefix: FDB.Bytes = []) throws -> FDB.Bytes {
        var packed = prefix
        var versionstampPosition: Int? = nil
        var incompleteCount = 0

        // Encode each element and track incomplete versionstamp position
        for element in elements {
            if let vs = element as? Versionstamp {
                if !vs.isComplete {
                    incompleteCount += 1
                    if versionstampPosition == nil {
                        // Position points to start of 10-byte transaction version
                        // (after type code byte and before the 10-byte placeholder)
                        versionstampPosition = packed.count + 1  // +1 for type code 0x33
                    }
                }
            }

            packed.append(contentsOf: element.encodeTuple())
        }

        // Validate exactly one incomplete versionstamp
        guard incompleteCount == 1, let position = versionstampPosition else {
            throw TupleError.invalidEncoding
        }

        // Append offset based on API version
        // Currently defaults to API 520+ behavior (4-byte offset)
        // API < 520 used 2-byte offset, but is no longer supported

        // API >= 520: Use 4-byte offset (uint32, little-endian)
        guard position <= UInt32.max else {
            throw TupleError.invalidEncoding
        }

        let offset = UInt32(position)
        packed.append(contentsOf: withUnsafeBytes(of: offset.littleEndian) { Array($0) })

        return packed
    }

    /// Check if tuple contains an incomplete versionstamp
    /// - Returns: true if any element is an incomplete versionstamp
    public func hasIncompleteVersionstamp() -> Bool {
        return elements.contains { element in
            if let vs = element as? Versionstamp {
                return !vs.isComplete
            }
            return false
        }
    }

    /// Count incomplete versionstamps in tuple
    /// - Returns: Number of incomplete versionstamps
    public func countIncompleteVersionstamps() -> Int {
        return elements.reduce(0) { count, element in
            if let vs = element as? Versionstamp, !vs.isComplete {
                return count + 1
            }
            return count
        }
    }

    /// Validate tuple for use with packWithVersionstamp()
    /// - Throws: `TupleError.invalidEncoding` if validation fails
    public func validateForVersionstamp() throws {
        let incompleteCount = countIncompleteVersionstamps()

        guard incompleteCount == 1 else {
            throw TupleError.invalidEncoding
        }
    }
}

// MARK: - Tuple Decoding Support

extension Tuple {

    /// Decode tuple that may contain versionstamps
    ///
    /// This is an enhanced version of decode() that supports TupleTypeCode.versionstamp (0x33).
    /// It maintains backward compatibility with existing decode() implementation.
    ///
    /// - Parameter bytes: Encoded tuple bytes
    /// - Returns: Array of decoded tuple elements
    /// - Throws: `TupleError.invalidEncoding` if decoding fails
    public static func decodeWithVersionstamp(from bytes: FDB.Bytes) throws -> [any TupleElement] {
        var elements: [any TupleElement] = []
        var offset = 0

        while offset < bytes.count {
            guard offset < bytes.count else { break }

            let typeCode = bytes[offset]
            offset += 1

            switch typeCode {
            case TupleTypeCode.versionstamp.rawValue:
                let element = try Versionstamp.decodeTuple(from: bytes, at: &offset)
                elements.append(element)

            // For other type codes, delegate to existing decode logic
            // This requires refactoring Tuple.decode() to be reusable
            // For now, we handle the most common cases:

            case TupleTypeCode.bytes.rawValue:
                var value: [UInt8] = []
                while offset < bytes.count && bytes[offset] != 0x00 {
                    if bytes[offset] == 0xFF {
                        offset += 1
                        if offset < bytes.count && bytes[offset] == 0xFF {
                            value.append(0x00)
                            offset += 1
                        }
                    } else {
                        value.append(bytes[offset])
                        offset += 1
                    }
                }
                offset += 1  // Skip terminating 0x00
                elements.append(value as FDB.Bytes)

            case TupleTypeCode.string.rawValue:
                var value: [UInt8] = []
                while offset < bytes.count && bytes[offset] != 0x00 {
                    if bytes[offset] == 0xFF {
                        offset += 1
                        if offset < bytes.count && bytes[offset] == 0xFF {
                            value.append(0x00)
                            offset += 1
                        }
                    } else {
                        value.append(bytes[offset])
                        offset += 1
                    }
                }
                offset += 1  // Skip terminating 0x00
                let string = String(decoding: value, as: UTF8.self)
                elements.append(string)

            default:
                // For other types, fall back to standard decode
                // This is a simplified version; full implementation should reuse Tuple.decode()
                throw TupleError.invalidEncoding
            }
        }

        return elements
    }
}
