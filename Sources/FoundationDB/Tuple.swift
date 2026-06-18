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

// MARK: Elements

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

@nonexhaustive
public enum TupleElement: Sendable, Hashable, Equatable, Comparable {
    case null
    case bytes(FDB.Bytes)
    case string(String)
    case nested(Tuple)
    case int(Int64)
    case float(Float)
    case double(Double)
    case bool(Bool)
    case uuid(UUID)
    case versionstamp(Versionstamp)

    var encodedCount: Int {
        switch self {
        case .null:
            return 1
        case let .bytes(b):
            return b.encodedTupleCount
        case let .string(s):
            return s.encodedTupleCount
        case let .nested(t):
            return t.encodedTupleCount
        case let .int(i):
            return i.encodedTupleCount
        case let .float(f):
            return f.encodedTupleCount
        case let .double(d):
            return d.encodedTupleCount
        case let .bool(b):
            return b.encodedTupleCount
        case let .uuid(u):
            return u.encodedTupleCount
        case let .versionstamp(v):
            return v.encodedTupleCount
        }
    }

    func encode(into encoded: inout FDB.Bytes) {
        switch self {
        case .null:
            encoded.append(TupleTypeCode.null.rawValue)
        case let .bytes(b):
            b.encodeTuple(into: &encoded)
        case let .string(s):
            s.encodeTuple(into: &encoded)
        case let .nested(t):
            t.encodeTuple(into: &encoded)
        case let .int(i):
            i.encodeTuple(into: &encoded)
        case let .float(f):
            f.encodeTuple(into: &encoded)
        case let .double(d):
            d.encodeTuple(into: &encoded)
        case let .bool(b):
            b.encodeTuple(into: &encoded)
        case let .uuid(u):
            u.encodeTuple(into: &encoded)
        case let .versionstamp(v):
            v.encodeTuple(into: &encoded)
        }
    }

    public static func == (lhs: TupleElement, rhs: TupleElement) -> Bool {
        switch lhs {
        case .null:
            switch rhs {
            case .null:
                return true
            default:
                return false
            }
        case let .bytes(bl):
            switch rhs {
            case let .bytes(br):
                return bl == br
            default:
                return false
            }
        case let .string(sl):
            switch rhs {
            case let .string(sr):
                return sl == sr
            default:
                return false
            }
        case let .nested(tl):
            switch rhs {
            case let .nested(tr):
                return tl == tr
            default:
                return false
            }
        case let .int(il):
            switch rhs {
            case let .int(ir):
                return il == ir
            default:
                return false
            }
        case let .float(fl):
            switch rhs {
            case let .float(fr):
                return fl.bitPattern == fr.bitPattern
            default:
                return false
            }
        case let .double(dl):
            switch rhs {
            case let .double(dr):
                return dl.bitPattern == dr.bitPattern
            default:
                return false
            }
        case let .bool(bl):
            switch rhs {
            case let .bool(br):
                return bl == br
            default:
                return false
            }
        case let .uuid(ul):
            switch rhs {
            case let .uuid(ur):
                return ul == ur
            default:
                return false
            }
        case let .versionstamp(vl):
            switch rhs {
            case let .versionstamp(vr):
                return vl == vr
            default:
                return false
            }
        }
    }

    public static func < (lhs: TupleElement, rhs: TupleElement) -> Bool {
        var le = FDB.Bytes()
        lhs.encode(into: &le)
        var re = FDB.Bytes()
        rhs.encode(into: &re)
        return le.lexicographicallyPrecedes(re)
    }
}

/// public protocol for converting between Swift native types and Tuple element types.
public protocol TupleElementConvertible {
    func tupleElement() -> TupleElement

    static func fromTuple(element: TupleElement) -> Self?
}

extension TupleElement: TupleElementConvertible {
    public func tupleElement() -> TupleElement {
        self
    }

    public static func fromTuple(element: TupleElement) -> TupleElement? {
        element
    }
}

/// internal protocol for converting between Tuple elements and byte strings.
protocol TupleCodable {
    var encodedTupleCount: Int { get }
    func encodeTuple(into: inout FDB.Bytes)
    static func decodeTuple<B: Collection<UInt8>>(from: B, at: inout B.Index, typeCode: UInt8) throws -> Self
}

// MARK: Tuple object

/// A tuple represents an ordered collection of elements that can be encoded to and decoded from bytes.
///
/// Tuples can be used as keys in FoundationDB, and their encoding preserves lexicographic ordering.
///
public struct Tuple: Sendable, Hashable, Equatable, Comparable, CustomStringConvertible {
    public let elements: [TupleElement]

    public init(_ elements: [TupleElement]) {
        self.elements = elements
    }

    public init(_ elements: any Sequence<TupleElementConvertible>) {
        self.init(elements.map { $0.tupleElement() })
    }

    public init(_ elements: TupleElementConvertible...) {
        self.init(elements)
    }

    public var count: Int {
        elements.count
    }

    public subscript(index: Int) -> TupleElement {
        return elements[index]
    }

    public var encodedCount: Int {
        var result = 0
        for element in elements {
            result += element.encodedCount
        }
        return result
    }

    public func encode() -> FDB.Bytes {
        let count = encodedCount
        var encoded = FDB.Bytes()
        encoded.reserveCapacity(count)
        for element in elements {
            element.encode(into: &encoded)
        }
        return encoded
    }

    public static func decode<B: Collection<UInt8>>(from bytes: B) throws -> Self {
        var offset = bytes.startIndex
        return try decode(from: bytes, at: &offset, nested: false)
    }

    static func decode<B: Collection<UInt8>>(from bytes: B, at offset: inout B.Index, nested: Bool) throws -> Self {
        var elements: [TupleElement] = []

        while offset != bytes.endIndex {
            let typeCode = bytes[offset]
            offset = bytes.index(after: offset)

            if nested && typeCode == 0 {
                if offset != bytes.endIndex && bytes[offset] == 0xFF {
                    offset = bytes.index(after: offset)
                } else {
                    break
                }
            }

            switch typeCode {
            case TupleTypeCode.null.rawValue:
                elements.append(TupleElement.null)
            case TupleTypeCode.bytes.rawValue:
                let element = try FDB.Bytes.decodeTuple(from: bytes, at: &offset, typeCode: typeCode)
                elements.append(element.tupleElement())
            case TupleTypeCode.string.rawValue:
                let element = try String.decodeTuple(from: bytes, at: &offset, typeCode: typeCode)
                elements.append(element.tupleElement())
            case TupleTypeCode.boolFalse.rawValue, TupleTypeCode.boolTrue.rawValue:
                let element = try Bool.decodeTuple(from: bytes, at: &offset, typeCode: typeCode)
                elements.append(element.tupleElement())
            case TupleTypeCode.float.rawValue:
                let element = try Float.decodeTuple(from: bytes, at: &offset, typeCode: typeCode)
                elements.append(element.tupleElement())
            case TupleTypeCode.double.rawValue:
                let element = try Double.decodeTuple(from: bytes, at: &offset, typeCode: typeCode)
                elements.append(element.tupleElement())
            case TupleTypeCode.uuid.rawValue:
                let element = try UUID.decodeTuple(from: bytes, at: &offset, typeCode: typeCode)
                elements.append(element.tupleElement())
            case TupleTypeCode.versionstamp.rawValue:
                let element = try Versionstamp.decodeTuple(from: bytes, at: &offset, typeCode: typeCode)
                elements.append(element.tupleElement())
            case TupleTypeCode.intZero.rawValue:
                elements.append(TupleElement.int(0))
            case TupleTypeCode.negativeIntStart.rawValue ... TupleTypeCode.positiveIntEnd.rawValue:
                let element = try Int64.decodeTuple(from: bytes, at: &offset, typeCode: typeCode)
                elements.append(element.tupleElement())
            case TupleTypeCode.nested.rawValue:
                let element = try Tuple.decodeTuple(from: bytes, at: &offset, typeCode: typeCode)
                elements.append(element.tupleElement())
            default:
                throw TupleError.invalidDecoding("Unknown type code: \(typeCode)")
            }
        }

        return Self(elements)
    }

    public static func < (lhs: Tuple, rhs: Tuple) -> Bool {
        let lc = lhs.count
        let rc = rhs.count
        for i in 0..<min(lc, rc) {
            let le = lhs.elements[i]
            let re = rhs.elements[i]
            if le < re {
                return true
            }
            if le > re {
                return false
            }
        }
        return lc < rc
    }

    public var description: String {
        elements.description
    }
}

extension TupleElement {
    public func convert<T: TupleElementConvertible>(_: T.Type) -> T? {
        T.fromTuple(element: self)
    }
}

extension Tuple {
    public subscript<T: TupleElementConvertible>(index: Int, as type: T.Type) -> T? {
        guard index >= 0, index < elements.count else { return nil }
        return elements[index].convert(type)
    }
}

// MARK: Specific element types

extension String: TupleElementConvertible {
    public func tupleElement() -> TupleElement {
        .string(self)
    }

    public static func fromTuple(element: TupleElement) -> String? {
        switch element {
        case let .string(s):
            return s
        default:
            return nil
        }
    }
}

extension String: TupleCodable {
    var encodedTupleCount: Int {
        utf8.count + utf8.count { $0 == 0 } + 2
    }

    func encodeTuple(into encoded: inout FDB.Bytes) {
        encoded.append(TupleTypeCode.string.rawValue)
        let utf8Bytes = utf8

        for byte in utf8Bytes {
            if byte == 0x00 {
                encoded.append(contentsOf: [0x00, 0xFF])
            } else {
                encoded.append(byte)
            }
        }
        encoded.append(0x00)
    }

    static func decodeTuple<B: Collection<UInt8>>(from bytes: B, at offset: inout B.Index, typeCode: UInt8) throws -> String {
        var decoded = FDB.Bytes()

        while offset != bytes.endIndex {
            let byte = bytes[offset]
            offset = bytes.index(after: offset)

            if byte == 0x00 {
                if offset != bytes.endIndex && bytes[offset] == 0xFF {
                    offset = bytes.index(after: offset)
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

extension FDB.Bytes: TupleElementConvertible {
    public func tupleElement() -> TupleElement {
        .bytes(self)
    }

    public static func fromTuple(element: TupleElement) -> FDB.Bytes? {
        switch element {
        case let .bytes(b):
            return b
        default:
            return nil
        }
    }
}

extension FDB.Bytes: TupleCodable {
    var encodedTupleCount: Int {
        self.count + self.count { $0 == 0 } + 2
    }

    func encodeTuple(into encoded: inout FDB.Bytes) {
        encoded.append(TupleTypeCode.bytes.rawValue)
        for byte in self {
            if byte == 0x00 {
                encoded.append(contentsOf: [0x00, 0xFF])
            } else {
                encoded.append(byte)
            }
        }
        encoded.append(0x00)
    }

    static func decodeTuple<B: Collection<UInt8>>(from bytes: B, at offset: inout B.Index, typeCode: UInt8) throws -> FDB.Bytes {
        var decoded = FDB.Bytes()

        while offset != bytes.endIndex {
            let byte = bytes[offset]
            offset = bytes.index(after: offset)

            if byte == 0x00 {
                if offset != bytes.endIndex && bytes[offset] == 0xFF {
                    offset = bytes.index(after: offset)
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

extension Bool: TupleElementConvertible {
    public func tupleElement() -> TupleElement {
        .bool(self)
    }

    public static func fromTuple(element: TupleElement) -> Bool? {
        switch element {
        case let .bool(b):
            return b
        default:
            return nil
        }
    }
}

extension Bool: TupleCodable {
    var encodedTupleCount: Int {
        2
    }

    func encodeTuple(into encoded: inout FDB.Bytes) {
        encoded.append(self ? TupleTypeCode.boolTrue.rawValue : TupleTypeCode.boolFalse.rawValue)
    }

    static func decodeTuple<B: Collection<UInt8>>(from bytes: B, at offset: inout B.Index, typeCode: UInt8) throws -> Bool {
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

extension Float: TupleElementConvertible {
    public func tupleElement() -> TupleElement {
        .float(self)
    }

    public static func fromTuple(element: TupleElement) -> Float? {
        switch element {
        case let .float(f):
            return f
        default:
            return nil
        }
    }
}

extension Float: TupleCodable {
    var encodedTupleCount: Int {
        5
    }

    func encodeTuple(into encoded: inout FDB.Bytes) {
        encoded.append(TupleTypeCode.float.rawValue)
        let bitPattern = self.bitPattern
        withUnsafeBytes(of: bitPattern.bigEndian) { bytes in
            encoded.append(contentsOf: bytes)
        }
    }

    static func decodeTuple<B: Collection<UInt8>>(from bytes: B, at offset: inout B.Index, typeCode: UInt8) throws -> Float {
        var offset4 = offset
        guard bytes.formIndex(&offset4, offsetBy: 4, limitedBy: bytes.endIndex) else {
            throw TupleError.invalidDecoding("Not enough bytes for Float")
        }

        let floatBytes = Array(bytes[offset ..< offset4])
        offset = offset4

        let bigEndianValue = floatBytes.withUnsafeBytes { bytes in
            bytes.load(as: UInt32.self)
        }
        let bitPattern = UInt32(bigEndian: bigEndianValue)
        return Float(bitPattern: bitPattern)
    }
}

extension Double: TupleElementConvertible {
    public func tupleElement() -> TupleElement {
        .double(self)
    }

    public static func fromTuple(element: TupleElement) -> Double? {
        switch element {
        case let .double(d):
            return d
        default:
            return nil
        }
    }
}

extension Double: TupleCodable {
    var encodedTupleCount: Int {
        9
    }

    func encodeTuple(into encoded: inout FDB.Bytes) {
        encoded.append(TupleTypeCode.double.rawValue)
        let bitPattern = self.bitPattern
        withUnsafeBytes(of: bitPattern.bigEndian) { bytes in
            encoded.append(contentsOf: bytes)
        }
    }

    static func decodeTuple<B: Collection<UInt8>>(from bytes: B, at offset: inout B.Index, typeCode: UInt8) throws -> Double {
        var offset8 = offset
        guard bytes.formIndex(&offset8, offsetBy: 8, limitedBy: bytes.endIndex) else {
            throw TupleError.invalidDecoding("Not enough bytes for Double")
        }

        let doubleBytes = Array(bytes[offset ..< offset8])
        offset = offset8

        let bigEndianValue = doubleBytes.withUnsafeBytes { bytes in
            bytes.load(as: UInt64.self)
        }
        let bitPattern = UInt64(bigEndian: bigEndianValue)
        return Double(bitPattern: bitPattern)
    }
}

extension UUID: TupleElementConvertible {
    public func tupleElement() -> TupleElement {
        .uuid(self)
    }

    public static func fromTuple(element: TupleElement) -> UUID? {
        switch element {
        case let .uuid(u):
            return u
        default:
            return nil
        }
    }
}

extension UUID: TupleCodable {
    var encodedTupleCount: Int {
        17
    }

    func encodeTuple(into encoded: inout FDB.Bytes) {
        encoded.append(TupleTypeCode.uuid.rawValue)
        let (u1, u2, u3, u4, u5, u6, u7, u8, u9, u10, u11, u12, u13, u14, u15, u16) = uuid
        encoded.append(contentsOf: [
            u1, u2, u3, u4, u5, u6, u7, u8, u9, u10, u11, u12, u13, u14, u15, u16,
        ])
    }

    static func decodeTuple<B: Collection<UInt8>>(from bytes: B, at offset: inout B.Index, typeCode: UInt8) throws -> UUID {
        var offset16 = offset
        guard bytes.formIndex(&offset16, offsetBy: 16, limitedBy: bytes.endIndex) else {
            throw TupleError.invalidDecoding("Not enough bytes for UUID")
        }

        let uuidBytes = Array(bytes[offset ..< offset16])
        offset = offset16

        let uuidTuple = (
            uuidBytes[0], uuidBytes[1], uuidBytes[2], uuidBytes[3],
            uuidBytes[4], uuidBytes[5], uuidBytes[6], uuidBytes[7],
            uuidBytes[8], uuidBytes[9], uuidBytes[10], uuidBytes[11],
            uuidBytes[12], uuidBytes[13], uuidBytes[14], uuidBytes[15]
        )

        return UUID(uuid: uuidTuple)
    }
}

extension Versionstamp: TupleElementConvertible {
    public func tupleElement() -> TupleElement {
        .versionstamp(self)
    }

    public static func fromTuple(element: TupleElement) -> Versionstamp? {
        switch element {
        case let .versionstamp(v):
            return v
        default:
            return nil
        }
    }
}

extension Versionstamp: TupleCodable {
    var  encodedTupleCount: Int {
        Versionstamp.totalSize
    }

    func encodeTuple(into encoded: inout FDB.Bytes) {
        encoded.append(TupleTypeCode.versionstamp.rawValue)
        encoded.append(contentsOf: toBytes())
    }

    public static func decodeTuple<B: Collection<UInt8>>(from bytes: B, at offset: inout B.Index, typeCode: UInt8) throws -> Versionstamp {
        var offsetV = offset
        guard bytes.formIndex(&offsetV, offsetBy: Versionstamp.totalSize, limitedBy: bytes.endIndex) else {
            throw TupleError.invalidDecoding("Not enough bytes for versionstamp")
        }

        let versionstampBytes = Array(bytes[offset..<offsetV])
        offset = offsetV

        return try Versionstamp.fromBytes(versionstampBytes)
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

extension Int64: TupleElementConvertible {
    public func tupleElement() -> TupleElement {
        .int(self)
    }

    public static func fromTuple(element: TupleElement) -> Int64? {
        switch element {
        case let .int(i):
            return i
        default:
            return nil
        }
    }
}

extension Int64: TupleCodable {
    var encodedTupleCount: Int {
        if self == 0 {
            return 2
        } else {
            return bisectLeft(UInt64((self < 0) ? -self : self)) + 1
        }
    }

    func encodeTuple(into encoded: inout FDB.Bytes) {
        if self == 0 {
            encoded.append(TupleTypeCode.intZero.rawValue)
        } else if self > 0 {
            let n = bisectLeft(UInt64(self))
            encoded.append(TupleTypeCode.intZero.rawValue + UInt8(n))
            let bigEndianValue = UInt64(bitPattern: self).bigEndian
            withUnsafeBytes(of: bigEndianValue) { bytes in
                encoded.append(contentsOf: bytes.suffix(n))
            }
        } else {
            let n = bisectLeft(UInt64(-self))
            encoded.append(TupleTypeCode.intZero.rawValue - UInt8(n))

            if n < 8 {
                let offset = UInt64(sizeLimits[n]) &+ UInt64(bitPattern: self)
                let bigEndianValue = offset.bigEndian
                withUnsafeBytes(of: bigEndianValue) { bytes in
                    encoded.append(contentsOf: bytes.suffix(n))
                }
            } else {
                // n == 8 case
                let offset = UInt64(bitPattern: self)
                let bigEndianValue = offset.bigEndian
                withUnsafeBytes(of: bigEndianValue) { bytes in
                    encoded.append(contentsOf: bytes)
                }
            }
        }
    }

    static func decodeTuple<B: Collection<UInt8>>(from bytes: B, at offset: inout B.Index, typeCode: UInt8) throws -> Int64 {
        if typeCode == TupleTypeCode.intZero.rawValue {
            return 0
        }

        var n = Int(typeCode) - Int(TupleTypeCode.intZero.rawValue)
        var neg = false
        if n < 0 {
            n = -n
            neg = true
        }

        var offsetN = offset
        guard bytes.formIndex(&offsetN, offsetBy: n, limitedBy: bytes.endIndex) else {
            throw TupleError.invalidDecoding("Not enough bytes for integer")
        }

        var bp = [UInt8](repeating: 0, count: 8)
        bp.replaceSubrange((8 - n) ..< 8, with: bytes[offset..<offsetN])
        offset = offsetN

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

extension Tuple: TupleElementConvertible {
    public func tupleElement() -> TupleElement {
        .nested(self)
    }

    public static func fromTuple(element: TupleElement) -> Tuple? {
        switch element {
        case let .nested(t):
            return t
        default:
            return nil
        }
    }
}

extension Tuple: TupleCodable {
    var encodedTupleCount: Int {
        var result = 2
        for element in elements {
            switch element {
            case .null:
                result += 2
            default:
                result += element.encodedCount
            }
        }
        return result
    }

    func encodeTuple(into encoded: inout FDB.Bytes) {
        encoded.append(TupleTypeCode.nested.rawValue)
        for element in elements {
            switch element {
            case .null:
                encoded.append(contentsOf: [0x00, 0xFF])
            default:
                element.encode(into: &encoded)
            }
        }
        encoded.append(0x00)
    }

    static func decodeTuple<B: Collection<UInt8>>(from bytes: B, at offset: inout B.Index, typeCode: UInt8) throws -> Tuple {
        return try decode(from: bytes, at: &offset, nested: true)
    }
}

extension Int: TupleElementConvertible {
    public func tupleElement() -> TupleElement {
        .int(Int64(self))
    }

    public static func fromTuple(element: TupleElement) -> Int? {
        switch element {
        case let .int(i):
            return Int(i)
        default:
            return nil
        }
    }
}

extension Int32: TupleElementConvertible {
    public func tupleElement() -> TupleElement {
        .int(Int64(self))
    }

    public static func fromTuple(element: TupleElement) -> Int32? {
        switch element {
        case let .int(i):
            return Int32(i)
        default:
            return nil
        }
    }
}

// MARK: Versionstamp support

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
    /// let key = try tuple.encodeWithVersionstamp()
    ///
    /// transaction.atomicOp(
    ///     key: key,
    ///     param: [],
    ///     mutationType: .setVersionstampedKey
    /// )
    /// ```
    public func encodeWithVersionstamp(prefix: FDB.Bytes = []) throws -> FDB.Bytes {
        var encoded = FDB.Bytes()
        encoded.reserveCapacity(prefix.count + encodedCount + 4)
        encoded.append(contentsOf: prefix)

        var versionstampPosition: Int? = nil
        var incompleteCount = 0

        // Encode each element and track incomplete versionstamp position
        for element in elements {
            switch element {
            case let .versionstamp(vs):
                if !vs.isComplete {
                    incompleteCount += 1
                    if versionstampPosition == nil {
                        // Position points to start of 10-byte transaction version
                        // (after type code byte and before the 10-byte placeholder)
                        versionstampPosition = encoded.count + 1  // +1 for type code (0x33)
                    }
                }
            default:
                break
            }
            element.encode(into: &encoded)
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
        withUnsafeBytes(of: offset.littleEndian) { encoded.append(contentsOf: $0) }

        return encoded
    }

    /// Check if tuple contains an incomplete versionstamp
    /// - Returns: true if any element is an incomplete versionstamp
    public func hasIncompleteVersionstamp() -> Bool {
        return elements.contains { element in
            switch element {
            case let .versionstamp(vs):
                return !vs.isComplete
            default:
                return false
            }
        }
    }

    /// Count incomplete versionstamps in tuple
    /// - Returns: Number of incomplete versionstamps
    public func countIncompleteVersionstamps() -> Int {
        return elements.count { element in
            switch element {
            case let .versionstamp(vs):
                return !vs.isComplete
            default:
                return false
            }
        }
    }

    /// Validate tuple for use with encodeWithVersionstamp()
    /// - Throws: `TupleError.invalidEncoding` if validation fails
    public func validateForVersionstamp() throws {
        let incompleteCount = countIncompleteVersionstamps()

        if incompleteCount != 1 {
            throw TupleError.invalidEncoding
        }
    }
}

// MARK: Type-safe tuples

extension Tuple {
    init<each T: TupleElementConvertible>(_ elements: (repeat each T)) {
        var tupleElements: [TupleElement] = []
        for e in repeat each elements {
            tupleElements.append(e.tupleElement())
        }
        self.init(tupleElements)
    }

    public static func encode<each T: TupleElementConvertible>(_ elements: (repeat each T)) -> FDB.Bytes {
        // TODO: The DRY version of this that uses the above crashes / hangs the compiler.
        var tupleElements: [TupleElement] = []
        for e in repeat each elements {
            tupleElements.append(e.tupleElement())
        }
        return Tuple(tupleElements).encode()
    }

    public func convert<each T: TupleElementConvertible>(_ type:(repeat each T).Type) -> (repeat each T) {
        var i = 0
        func _next() -> TupleElement {
            let e = self[i]
            i += 1
            return e
        }
        let st = (repeat (each T).fromTuple(element: _next())!)
        return st
    }

    public static func decode<each T: TupleElementConvertible>(from bytes: FDB.Bytes, as type:(repeat each T).Type = (repeat each T).self) throws -> (repeat each T) {
        let t = try Tuple.decode(from: bytes)
        return t.convert(type)
    }
}
