/*
 * Tuple.swift
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

public enum TupleError: Error, Sendable {
    case invalidTupleElement
    case invalidEncoding
    case invalidDecoding(String)
    case unsupportedType
}

enum TupleTypeCode: UInt8, CaseIterable {
    case null = 0x00
    case bytes = 0x01
    case string = 0x02
    case nested = 0x05
    case negativeIntStart = 0x0B
    case intZero = 0x14
    case positiveIntEnd = 0x1D
    case float = 0x20
    case double = 0x21
    case boolFalse = 0x26
    case boolTrue = 0x27
    case uuid = 0x30
    case versionstamp = 0x33
}

public protocol TupleElement: Sendable, Hashable, Equatable {
    func encodeTuple() -> FDB.Bytes
    static func decodeTuple(from bytes: FDB.Bytes, at offset: inout Int) throws -> Self
}

// TODO: Make it a TypedTuple so that we don't have to typecast manually.
/// A tuple represents an ordered collection of elements that can be encoded to and decoded from bytes.
///
/// Tuples can be used as keys in FoundationDB, and their encoding preserves lexicographic ordering.
///
/// ## Equality and Hashing
///
/// Tuple equality is based on the encoded byte representation of each element, which matches
/// FoundationDB's tuple comparison semantics. This differs from Swift's native equality for
/// floating-point values in the following ways:
///
/// - **Positive and negative zero**: `Tuple(0.0)` and `Tuple(-0.0)` are **not equal** because
///   they have different bit patterns and encode to different bytes. This differs from Swift,
///   where `0.0 == -0.0` is `true`.
///
/// - **NaN values**: `Tuple(Float.nan)` and `Tuple(Float.nan)` **are equal** if they have the
///   same bit pattern, because they encode to the same bytes. This differs from Swift, where
///   `Float.nan == Float.nan` is `false`.
///
/// These semantic differences ensure consistency with FoundationDB's tuple ordering and are
/// important when using tuples as dictionary keys or in sets.
public struct Tuple: Sendable, Hashable, Equatable {
    private let elements: [any TupleElement]

    public init(_ elements: any TupleElement...) {
        self.elements = elements
    }

    public init(_ elements: [any TupleElement]) {
        self.elements = elements
    }

    public subscript(index: Int) -> (any TupleElement)? {
        guard index >= 0, index < elements.count else { return nil }
        return elements[index]
    }

    public var count: Int {
        return elements.count
    }

    public func encode() -> FDB.Bytes {
        var result = FDB.Bytes()
        for element in elements {
            result.append(contentsOf: element.encodeTuple())
        }
        return result
    }

    public static func decode(from bytes: FDB.Bytes) throws -> [any TupleElement] {
        var elements: [any TupleElement] = []
        var offset = 0

        while offset < bytes.count {
            let typeCode = bytes[offset]
            offset += 1

            switch typeCode {
            case TupleTypeCode.null.rawValue:
                elements.append(TupleNil())
            case TupleTypeCode.bytes.rawValue:
                let element = try FDB.Bytes.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            case TupleTypeCode.string.rawValue:
                let element = try String.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            case TupleTypeCode.boolFalse.rawValue, TupleTypeCode.boolTrue.rawValue:
                let element = try Bool.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            case TupleTypeCode.float.rawValue:
                let element = try Float.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            case TupleTypeCode.double.rawValue:
                let element = try Double.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            case TupleTypeCode.uuid.rawValue:
                let element = try UUID.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            case TupleTypeCode.intZero.rawValue:
                elements.append(0)
            case TupleTypeCode.negativeIntStart.rawValue ... TupleTypeCode.positiveIntEnd.rawValue:
                let element = try Int64.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            case TupleTypeCode.nested.rawValue:
                let element = try Tuple.decodeTuple(from: bytes, at: &offset)
                elements.append(element)
            default:
                throw TupleError.invalidDecoding("Unknown type code: \(typeCode)")
            }
        }

        return elements
    }

    public static func == (lhs: Tuple, rhs: Tuple) -> Bool {
        guard lhs.count == rhs.count else { return false }

        for i in 0..<lhs.count {
            // Swift's type system doesn't allow comparing `any Protocol` existentials directly,
            // even though TupleElement requires Equatable conformance. We compare encoded bytes
            // instead, which is semantically correct since tuple encoding is canonical:
            // equal values always produce equal encodings.
            //
            // Note: This means Float/Double comparison follows bit-pattern equality rather than
            // IEEE 754 equality (e.g., +0.0 and -0.0 are unequal, NaN values with the same bit
            // pattern are equal). See the Tuple documentation for details.
            if lhs.elements[i].encodeTuple() != rhs.elements[i].encodeTuple() {
                return false
            }
        }
        return true
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(elements.count)
        for element in elements {
            // Swift's type system doesn't allow hashing `any Protocol` existentials directly,
            // even though TupleElement requires Hashable conformance. We hash encoded bytes
            // instead, which ensures consistency with the equality implementation above.
            hasher.combine(element.encodeTuple())
        }
    }
}

struct TupleNil: TupleElement {
    func encodeTuple() -> FDB.Bytes {
        return [TupleTypeCode.null.rawValue]
    }

    static func decodeTuple(from _: FDB.Bytes, at _: inout Int) throws -> TupleNil {
        return TupleNil()
    }

    static func == (lhs: TupleNil, rhs: TupleNil) -> Bool {
        // All TupleNil instances are equal (representing null/nil)
        return true
    }

    func hash(into hasher: inout Hasher) {
        // Use a constant value for consistency with the null type code
        hasher.combine(TupleTypeCode.null.rawValue)
    }
}

extension String: TupleElement {
    public func encodeTuple() -> FDB.Bytes {
        var encoded = [TupleTypeCode.string.rawValue]
        let utf8Bytes = Array(utf8)

        for byte in utf8Bytes {
            if byte == 0x00 {
                encoded.append(contentsOf: [0x00, 0xFF])
            } else {
                encoded.append(byte)
            }
        }
        encoded.append(0x00)
        return encoded
    }

    public static func decodeTuple(from bytes: FDB.Bytes, at offset: inout Int) throws -> String {
        var decoded = FDB.Bytes()

        while offset < bytes.count {
            let byte = bytes[offset]
            offset += 1

            if byte == 0x00 {
                if offset < bytes.count && bytes[offset] == 0xFF {
                    offset += 1
                    decoded.append(0x00)
                } else {
                    break
                }
            } else {
                decoded.append(byte)
            }
        }

        return String(bytes: decoded, encoding: .utf8)!
    }
}

extension FDB.Bytes: TupleElement {
    public func encodeTuple() -> FDB.Bytes {
        var encoded = [TupleTypeCode.bytes.rawValue]
        for byte in self {
            if byte == 0x00 {
                encoded.append(contentsOf: [0x00, 0xFF])
            } else {
                encoded.append(byte)
            }
        }
        encoded.append(0x00)
        return encoded
    }

    public static func decodeTuple(from bytes: FDB.Bytes, at offset: inout Int) throws -> FDB.Bytes {
        var decoded = FDB.Bytes()

        while offset < bytes.count {
            let byte = bytes[offset]
            offset += 1

            if byte == 0x00 {
                if offset < bytes.count && bytes[offset] == 0xFF {
                    offset += 1
                    decoded.append(0x00)
                } else {
                    break
                }
            } else {
                decoded.append(byte)
            }
        }

        return decoded
    }
}

extension Bool: TupleElement {
    public func encodeTuple() -> FDB.Bytes {
        return self ? [TupleTypeCode.boolTrue.rawValue] : [TupleTypeCode.boolFalse.rawValue]
    }

    public static func decodeTuple(from bytes: FDB.Bytes, at offset: inout Int) throws -> Bool {
        guard offset > 0 else {
            throw TupleError.invalidDecoding("Bool decoding requires type code")
        }
        let typeCode = bytes[offset - 1]

        switch typeCode {
        case TupleTypeCode.boolTrue.rawValue:
            return true
        case TupleTypeCode.boolFalse.rawValue:
            return false
        default:
            throw TupleError.invalidDecoding("Invalid bool type code: \(typeCode)")
        }
    }
}

extension Float: TupleElement {
    public func encodeTuple() -> FDB.Bytes {
        var encoded = [TupleTypeCode.float.rawValue]
        let bitPattern = self.bitPattern
        let bytes = withUnsafeBytes(of: bitPattern.bigEndian) { Array($0) }
        encoded.append(contentsOf: bytes)
        return encoded
    }

    public static func decodeTuple(from bytes: FDB.Bytes, at offset: inout Int) throws -> Float {
        guard offset + 4 <= bytes.count else {
            throw TupleError.invalidDecoding("Not enough bytes for Float")
        }

        let floatBytes = Array(bytes[offset ..< offset + 4])
        offset += 4

        let bigEndianValue = floatBytes.withUnsafeBytes { bytes in
            bytes.load(as: UInt32.self)
        }
        let bitPattern = UInt32(bigEndian: bigEndianValue)
        return Float(bitPattern: bitPattern)
    }
}

extension Double: TupleElement {
    public func encodeTuple() -> FDB.Bytes {
        var encoded = [TupleTypeCode.double.rawValue]
        let bitPattern = self.bitPattern
        let bytes = withUnsafeBytes(of: bitPattern.bigEndian) { Array($0) }
        encoded.append(contentsOf: bytes)
        return encoded
    }

    public static func decodeTuple(from bytes: FDB.Bytes, at offset: inout Int) throws -> Double {
        guard offset + 8 <= bytes.count else {
            throw TupleError.invalidDecoding("Not enough bytes for Double")
        }

        let doubleBytes = Array(bytes[offset ..< offset + 8])
        offset += 8

        let bigEndianValue = doubleBytes.withUnsafeBytes { bytes in
            bytes.load(as: UInt64.self)
        }
        let bitPattern = UInt64(bigEndian: bigEndianValue)
        return Double(bitPattern: bitPattern)
    }
}

extension UUID: TupleElement {
    public func encodeTuple() -> FDB.Bytes {
        var encoded = [TupleTypeCode.uuid.rawValue]
        let (u1, u2, u3, u4, u5, u6, u7, u8, u9, u10, u11, u12, u13, u14, u15, u16) = uuid
        encoded.append(contentsOf: [
            u1, u2, u3, u4, u5, u6, u7, u8, u9, u10, u11, u12, u13, u14, u15, u16,
        ])
        return encoded
    }

    public static func decodeTuple(from bytes: FDB.Bytes, at offset: inout Int) throws -> UUID {
        guard offset + 16 <= bytes.count else {
            throw TupleError.invalidDecoding("Not enough bytes for UUID")
        }

        let uuidBytes = Array(bytes[offset ..< offset + 16])
        offset += 16

        let uuidTuple = (
            uuidBytes[0], uuidBytes[1], uuidBytes[2], uuidBytes[3],
            uuidBytes[4], uuidBytes[5], uuidBytes[6], uuidBytes[7],
            uuidBytes[8], uuidBytes[9], uuidBytes[10], uuidBytes[11],
            uuidBytes[12], uuidBytes[13], uuidBytes[14], uuidBytes[15]
        )

        return UUID(uuid: uuidTuple)
    }
}

private let sizeLimits: [UInt64] = [
    (1 << (0 * 8)) - 1,
    (1 << (1 * 8)) - 1,
    (1 << (2 * 8)) - 1,
    (1 << (3 * 8)) - 1,
    (1 << (4 * 8)) - 1,
    (1 << (5 * 8)) - 1,
    (1 << (6 * 8)) - 1,
    (1 << (7 * 8)) - 1,
    UInt64.max, // (1 << (8 * 8)) - 1 would overflow, so use UInt64.max instead
]

private func bisectLeft(_ value: UInt64) -> Int {
    var n = 0
    while n < sizeLimits.count && sizeLimits[n] < value {
        n += 1
    }
    return n
}

extension Int64: TupleElement {
    public func encodeTuple() -> FDB.Bytes {
        return encodeInt(self)
    }

    private func encodeInt(_ value: Int64) -> FDB.Bytes {
        if value == 0 {
            return [TupleTypeCode.intZero.rawValue]
        }

        var encoded = FDB.Bytes()
        if value > 0 {
            let n = bisectLeft(UInt64(value))
            encoded.append(TupleTypeCode.intZero.rawValue + UInt8(n))
            let bigEndianValue = UInt64(bitPattern: value).bigEndian
            let bytes = withUnsafeBytes(of: bigEndianValue) { Array($0) }
            encoded.append(contentsOf: bytes.suffix(n))
        } else {
            let n = bisectLeft(UInt64(-value))
            encoded.append(TupleTypeCode.intZero.rawValue - UInt8(n))

            if n < 8 {
                let offset = UInt64(sizeLimits[n]) &+ UInt64(bitPattern: value)
                let bigEndianValue = offset.bigEndian
                let bytes = withUnsafeBytes(of: bigEndianValue) { Array($0) }
                encoded.append(contentsOf: bytes.suffix(n))
            } else {
                // n == 8 case
                let offset = UInt64(bitPattern: value)
                let bigEndianValue = offset.bigEndian
                let bytes = withUnsafeBytes(of: bigEndianValue) { Array($0) }
                encoded.append(contentsOf: bytes)
            }
        }

        return encoded
    }

    public static func decodeTuple(from bytes: FDB.Bytes, at offset: inout Int) throws -> Int64 {
        guard offset > 0 else {
            throw TupleError.invalidDecoding("Int64 decoding requires type code")
        }
        let typeCode = bytes[offset - 1]

        if typeCode == TupleTypeCode.intZero.rawValue {
            return 0
        }

        var n = Int(typeCode) - Int(TupleTypeCode.intZero.rawValue)
        var neg = false
        if n < 0 {
            n = -n
            neg = true
        }

        var bp = [UInt8](repeating: 0, count: 8)
        bp.replaceSubrange((8 - n) ..< 8, with: bytes[offset ... (offset + n - 1)])
        offset += n

        var ret: Int64 = 0
        for byte in bp {
            ret = (ret << 8) | Int64(byte)
        }

        if neg {
            if n == 8 {
                return ret
            } else {
                return ret - Int64(sizeLimits[n])
            }
        }

        if ret > 0 {
            return ret
        }

        return ret
    }
}

extension Tuple: TupleElement {
    public func encodeTuple() -> FDB.Bytes {
        var encoded = [TupleTypeCode.nested.rawValue]
        for element in elements {
            let elementBytes = element.encodeTuple()
            for byte in elementBytes {
                if byte == 0x00 {
                    encoded.append(contentsOf: [0x00, 0xFF])
                } else {
                    encoded.append(byte)
                }
            }
        }
        encoded.append(0x00)
        return encoded
    }

    public static func decodeTuple(from bytes: FDB.Bytes, at offset: inout Int) throws -> Tuple {
        var nestedBytes = FDB.Bytes()

        while offset < bytes.count {
            let byte = bytes[offset]
            offset += 1

            if byte == 0x00 {
                if offset < bytes.count && bytes[offset] == 0xFF {
                    offset += 1
                    nestedBytes.append(0x00)
                } else {
                    break
                }
            } else {
                nestedBytes.append(byte)
            }
        }

        let nestedElements = try Tuple.decode(from: nestedBytes)
        return Tuple(nestedElements)
    }
}

extension Int: TupleElement {
    public func encodeTuple() -> FDB.Bytes {
        return Int64(self).encodeTuple()
    }

    public static func decodeTuple(from bytes: FDB.Bytes, at offset: inout Int) throws -> Int {
        let value = try Int64.decodeTuple(from: bytes, at: &offset)
        guard value >= Int.min && value <= Int.max else {
            throw TupleError.invalidDecoding("Int64 value \(value) out of range for Int")
        }
        return Int(value)
    }
}

extension Int32: TupleElement {
    public func encodeTuple() -> FDB.Bytes {
        return Int64(self).encodeTuple()
    }

    public static func decodeTuple(from bytes: FDB.Bytes, at offset: inout Int) throws -> Int32 {
        let value = try Int64.decodeTuple(from: bytes, at: &offset)
        guard value >= Int32.min && value <= Int32.max else {
            throw TupleError.invalidDecoding("Int64 value \(value) out of range for Int32")
        }
        return Int32(value)
    }
}

extension UInt64: TupleElement {
    public func encodeTuple() -> FDB.Bytes {
        if self <= Int64.max {
            return Int64(self).encodeTuple()
        } else {
            return Int64.max.encodeTuple()
        }
    }

    public static func decodeTuple(from bytes: FDB.Bytes, at offset: inout Int) throws -> UInt64 {
        let value = try Int64.decodeTuple(from: bytes, at: &offset)
        guard value >= 0 else {
            throw TupleError.invalidDecoding(
                "Negative value \(value) cannot be converted to UInt64")
        }
        return UInt64(value)
    }
}
