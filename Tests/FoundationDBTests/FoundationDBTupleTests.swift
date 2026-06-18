/*
 * FoundationDBTupleTests.swift
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
import Testing

@testable import FoundationDB

@Test("TupleNil encoding and decoding")
func testTupleNil() throws {
    let tupleNil = Tuple(TupleElement.null)
    let encoded = tupleNil.encode()
    #expect(encoded == [TupleTypeCode.null.rawValue], "TupleNil should encode to null type code")

    let decoded = try Tuple.decode(from:encoded)
    #expect(decoded == tupleNil, "Should decode back to original null tuple")
}

@Test("TupleString encoding and decoding")
func tupleString() throws {
    let testString = "Hello, World!"
    var encoded = FDB.Bytes()
    testString.encodeTuple(into: &encoded)

    #expect(encoded.first == TupleTypeCode.string.rawValue, "TupleString should start with string type code")
    #expect(encoded.last == 0x00, "TupleString should end with null terminator")

    var offset = 1
    let decoded = try String.decodeTuple(from: encoded, at: &offset, typeCode: encoded[0])
    #expect(decoded == testString, "Should decode back to original string")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleString with null bytes")
func tupleStringWithNulls() throws {
    let testString = "Hello\u{0}World"
    var encoded = FDB.Bytes()
    testString.encodeTuple(into: &encoded)

    #expect(encoded.contains(0x00), "Encoded string should contain null bytes")
    #expect(encoded.contains(0xFF), "Null bytes should be escaped with 0xFF")

    var offset = 1
    let decoded = try String.decodeTuple(from: encoded, at: &offset, typeCode: encoded[0])
    #expect(decoded == testString, "Should decode back to original string with nulls")
}

@Test("TupleBool encoding and decoding")
func tupleBool() throws {
    let testTrue = true
    let testFalse = false

    var encodedTrue = FDB.Bytes()
    testTrue.encodeTuple(into: &encodedTrue)
    var encodedFalse = FDB.Bytes()
    testFalse.encodeTuple(into: &encodedFalse)

    #expect(encodedTrue == [TupleTypeCode.boolTrue.rawValue], "true should encode to boolTrue type code")
    #expect(encodedFalse == [TupleTypeCode.boolFalse.rawValue], "false should encode to boolFalse type code")

    var offsetTrue = 1
    var offsetFalse = 1

    let decodedTrue = try Bool.decodeTuple(from: encodedTrue, at: &offsetTrue, typeCode: encodedTrue[0])
    let decodedFalse = try Bool.decodeTuple(from: encodedFalse, at: &offsetFalse, typeCode: encodedFalse[0])

    #expect(decodedTrue == true, "Should decode back to true")
    #expect(decodedFalse == false, "Should decode back to false")
}

@Test("TupleFloat encoding and decoding")
func tupleFloat() throws {
    let testFloat: Float = 3.14159
    var encoded = FDB.Bytes()
    testFloat.encodeTuple(into: &encoded)

    #expect(encoded.first == TupleTypeCode.float.rawValue, "Float should start with float type code")
    #expect(encoded.count == 5, "Float should be 5 bytes (1 type code + 4 data)")

    var offset = 1
    let decoded = try Float.decodeTuple(from: encoded, at: &offset, typeCode: encoded[0])
    #expect(decoded == testFloat, "Should decode back to original float")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleDouble encoding and decoding")
func tupleDouble() throws {
    let testDouble = 3.141592653589793
    var encoded = FDB.Bytes()
    testDouble.encodeTuple(into: &encoded)

    #expect(encoded.first == TupleTypeCode.double.rawValue, "Double should start with double type code")
    #expect(encoded.count == 9, "Double should be 9 bytes (1 type code + 8 data)")

    var offset = 1
    let decoded = try Double.decodeTuple(from: encoded, at: &offset, typeCode: encoded[0])
    #expect(decoded == testDouble, "Should decode back to original double")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleUUID encoding and decoding")
func tupleUUID() throws {
    let testUUID = UUID()
    var encoded = FDB.Bytes()
    testUUID.encodeTuple(into: &encoded)

    #expect(encoded.first == TupleTypeCode.uuid.rawValue, "UUID should start with uuid type code")
    #expect(encoded.count == 17, "UUID should be 17 bytes (1 type code + 16 data)")

    var offset = 1
    let decoded = try UUID.decodeTuple(from: encoded, at: &offset, typeCode: encoded[0])
    #expect(decoded == testUUID, "Should decode back to original UUID")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - Zero")
func tupleInt64Zero() throws {
    let testInt: Int64 = 0
    var encoded = FDB.Bytes()
    testInt.encodeTuple(into: &encoded)

    #expect(encoded == [TupleTypeCode.intZero.rawValue], "Zero should encode to intZero type code")

    var offset = 1
    let decoded = try Int64.decodeTuple(from: encoded, at: &offset, typeCode: encoded[0])
    #expect(decoded == testInt, "Should decode back to original zero")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - Small positive")
func tupleInt64SmallPositive() throws {
    let testInt: Int64 = 42
    var encoded = FDB.Bytes()
    testInt.encodeTuple(into: &encoded)

    #expect(encoded.first == 0x15, "Small positive should use 0x15 type code (positiveInt1)")

    var offset = 1
    let decoded = try Int64.decodeTuple(from: encoded, at: &offset, typeCode: encoded[0])
    #expect(decoded == testInt, "Should decode back to original positive integer")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - Very small negative")
func tupleInt64VerySmallNegative() throws {
    let testInt: Int64 = -42
    var encoded = FDB.Bytes()
    testInt.encodeTuple(into: &encoded)

    #expect(encoded.first == 0x13)

    var offset = 1
    let decoded = try Int64.decodeTuple(from: encoded, at: &offset, typeCode: encoded[0])
    #expect(decoded == testInt, "Should decode back to original positive integer")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - Large negative")
func tupleInt64LargeNegative() throws {
    let testInt: Int64 = -89_034_333_444
    var encoded = FDB.Bytes()
    testInt.encodeTuple(into: &encoded)

    var offset = 1
    let decoded = try Int64.decodeTuple(from: encoded, at: &offset, typeCode: encoded[0])
    #expect(decoded == testInt, "Should decode back to original negative integer")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - Very Large negative")
func tupleInt64VeryLargeNegative() throws {
    let testInt: Int64 = -(1 << 55) - 34_897_432
    var encoded = FDB.Bytes()
    testInt.encodeTuple(into: &encoded)

    var offset = 1
    let decoded = try Int64.decodeTuple(from: encoded, at: &offset, typeCode: encoded[0])
    #expect(decoded == testInt, "Should decode back to original negative integer")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - VeryVery Large negative")
func tupleInt64VeryLargeNegative2() throws {
    let testInt: Int64 = -(1 << 60) - 34_897_432
    var encoded = FDB.Bytes()
    testInt.encodeTuple(into: &encoded)

    var offset = 1
    let decoded = try Int64.decodeTuple(from: encoded, at: &offset, typeCode: encoded[0])
    #expect(decoded == testInt, "Should decode back to original negative integer")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - Large values")
func tupleInt64LargeValues() throws {
    let largePositive = Int64.max
    let largeNegative = Int64.min + 1

    var encodedPos = FDB.Bytes()
    largePositive.encodeTuple(into: &encodedPos)
    var encodedNeg = FDB.Bytes()
    largeNegative.encodeTuple(into: &encodedNeg)

    var offsetPos = 1
    var offsetNeg = 1

    let decodedPos = try Int64.decodeTuple(from: encodedPos, at: &offsetPos, typeCode: encodedPos[0])
    let decodedNeg = try Int64.decodeTuple(from: encodedNeg, at: &offsetNeg, typeCode: encodedNeg[0])

    #expect(decodedPos == largePositive, "Should decode back to Int64.max")
    #expect(decodedNeg == largeNegative, "Should decode back to Int64.min")
}

@Test("TupleInt32 encoding and decoding")
func tupleInt32() throws {
    let testInt: Int32 = -2_034_333_444
    let testTuple = Tuple(testInt)
    let encoded = testTuple.encode()

    let decoded = try Tuple.decode(from: encoded)
    #expect(decoded == testTuple, "Should decode back to original tuple")
}

@Test("TupleInt encoding and decoding")
func tupleInt() throws {
    let testInt = 123_456
    let testTuple = Tuple(testInt)
    let encoded = testTuple.encode()

    let decoded = try Tuple.decode(from: encoded)
    #expect(decoded == testTuple, "Should decode back to original tuple")
}

@Test("TupleNested encoding and decoding")
func tupleNested() throws {
    let innerTuple = Tuple("hello", 42, true)
    let outerTuple = Tuple("outer", innerTuple, "end")

    let encoded = outerTuple.encode()
    let decoded = try Tuple.decode(from: encoded)

    #expect(decoded.count == 3, "Should have 3 elements")

    let decodedString1  = String.fromTuple(element: decoded[0])
    #expect(decodedString1 == "outer", "First element should be 'outer'")

    let decodedNested = Tuple.fromTuple(element: decoded[1])
    #expect(decodedNested != nil, "Second element should be a Tuple")
    #expect(decodedNested?.count == 3, "Nested tuple should have 3 elements")

    let decodedString2 = String.fromTuple(element: decoded[2])
    #expect(decodedString2 == "end", "Third element should be 'end'")
}

@Test("Tuple with a zero integer")
func tupleWithZero() throws {
    let tuple = Tuple("hello", 0, "foo")

    let encoded = tuple.encode()
    let decoded = try Tuple.decode(from: encoded)

    #expect(decoded.count == 3, "Should have 3 elements")
    let decodedString1 = decoded[0].convert(String.self)
    #expect(decodedString1 == "hello")

    let decodedInt = decoded[1].convert(Int64.self)
    #expect(decodedInt == 0)

    let decodedString2 = decoded[2, as:String.self]
    #expect(decodedString2 == "foo")
}

@Test("TupleNested deep nesting")
func tupleNestedDeep() throws {
    let level3 = Tuple("deep", 123)
    let level2 = Tuple("middle", level3)
    let level1 = Tuple("top", level2, "bottom")

    let encoded = level1.encode()
    let decoded = try Tuple.decode(from: encoded)

    #expect(decoded.count == 3, "Top level should have 3 elements")

    let topString = String.fromTuple(element: decoded[0])
    #expect(topString == "top", "First element should be 'top'")

    let middleTuple = Tuple.fromTuple(element: decoded[1])
    #expect(middleTuple != nil, "Second element should be a Tuple")
    #expect(middleTuple?.count == 2, "Middle tuple should have 2 elements")

    let bottomString = String.fromTuple(element: decoded[2])
    #expect(bottomString == "bottom", "Third element should be 'bottom'")
}

@Test("TupleInt64 encoding and decoding - 1 million distributed integers")
func tupleInt64DistributedIntegers() throws {
    // Deterministic random number generator using LCG algorithm
    var seed: UInt64 = 12345
    func nextRandom() -> Int64 {
        // Generate full 64-bit value
        seed = seed &* 6_364_136_223_846_793_005 &+ 1_442_695_040_888_963_407
        return Int64(bitPattern: seed)
    }

    // Test 10000 integers
    var positive = 0
    var negative = 0
    for _ in 0 ..< 1_000_000 {
        let testInt = nextRandom()
        var encoded = FDB.Bytes()
        testInt.encodeTuple(into: &encoded)

        if testInt > 0 {
            positive += 1
        } else if testInt < 0 {
            negative += 1
        }

        var offset = 1
        let decoded = try Int64.decodeTuple(from: encoded, at: &offset, typeCode: encoded[0])
        #expect(decoded == testInt, "Integer \(testInt) should encode and decode correctly")
        #expect(offset == encoded.count, "Offset should advance to end of encoded data")
    }

    print("tested with n_positives = \(positive), n_negatives = \(negative)")
}

@Test("Tuple equality - same values")
func tupleEquality() throws {
    let tuple1 = Tuple("hello", 42, true, 3.14)
    let tuple2 = Tuple("hello", 42, true, 3.14)

    #expect(tuple1 == tuple2, "Tuples with same values should be equal")
}

@Test("Tuple equality - different values")
func tupleInequality() throws {
    let tuple1 = Tuple("hello", 42, true)
    let tuple2 = Tuple("hello", 43, true)

    #expect(tuple1 != tuple2, "Tuples with different values should not be equal")
}

@Test("Tuple equality - different lengths")
func tupleInequalityDifferentLengths() throws {
    let tuple1 = Tuple("hello", 42)
    let tuple2 = Tuple("hello", 42, true)

    #expect(tuple1 != tuple2, "Tuples with different lengths should not be equal")
}

@Test("Tuple equality - nested tuples")
func tupleEqualityNested() throws {
    let inner1 = Tuple("nested", 123)
    let inner2 = Tuple("nested", 123)
    let tuple1 = Tuple("outer", inner1, "end")
    let tuple2 = Tuple("outer", inner2, "end")

    #expect(tuple1 == tuple2, "Tuples with equal nested tuples should be equal")
}

@Test("Tuple hashability - same values produce same hash")
func tupleHashabilitySameValues() throws {
    let tuple1 = Tuple("hello", 42, true, 3.14)
    let tuple2 = Tuple("hello", 42, true, 3.14)

    #expect(tuple1.hashValue == tuple2.hashValue, "Tuples with same values should have same hash")
}

@Test("Tuple hashability - can be used in Set")
func tupleHashabilitySet() throws {
    let tuple1 = Tuple("hello", 42)
    let tuple2 = Tuple("world", 99)
    let tuple3 = Tuple("hello", 42)  // duplicate of tuple1

    var set = Set<Tuple>()
    set.insert(tuple1)
    set.insert(tuple2)
    set.insert(tuple3)

    #expect(set.count == 2, "Set should contain only 2 unique tuples")
    #expect(set.contains(tuple1), "Set should contain tuple1")
    #expect(set.contains(tuple2), "Set should contain tuple2")
    #expect(set.contains(tuple3), "Set should contain tuple3 (same as tuple1)")
}

@Test("Tuple hashability - can be used as Dictionary key")
func tupleHashabilityDictionary() throws {
    let key1 = Tuple("user", 123)
    let key2 = Tuple("user", 456)
    let key3 = Tuple("user", 123)  // duplicate of key1

    var dict: [Tuple: String] = [:]
    dict[key1] = "Alice"
    dict[key2] = "Bob"
    dict[key3] = "Charlie"  // should overwrite key1

    #expect(dict.count == 2, "Dictionary should contain 2 entries")
    #expect(dict[key1] == "Charlie", "key1 value should be overwritten to 'Charlie'")
    #expect(dict[key2] == "Bob", "key2 value should be 'Bob'")
    #expect(dict[key3] == "Charlie", "key3 (same as key1) should retrieve 'Charlie'")
}

// MARK: - Edge Cases

@Test("Tuple equality - Float positive and negative zero are unequal")
func tupleFloatZeroInequality() throws {
    let tuple1 = Tuple(Float(0.0))
    let tuple2 = Tuple(Float(-0.0))

    // Note: These are unequal because they have different bit patterns
    // and encode to different bytes. This differs from Swift's Float equality
    // where 0.0 == -0.0 is true.
    #expect(tuple1 != tuple2, "Positive and negative zero have different encodings")

    // It is okay that they have the same hash.
    #expect(tuple1.hashValue == tuple2.hashValue, "Different values can still have the same hashes")
}

@Test("Tuple equality - Double positive and negative zero are unequal")
func tupleDoubleZeroInequality() throws {
    let tuple1 = Tuple(Double(0.0))
    let tuple2 = Tuple(Double(-0.0))

    #expect(tuple1 != tuple2, "Positive and negative zero have different encodings")
    #expect(tuple1.hashValue == tuple2.hashValue, "Different values can still have the same hashes")
}

@Test("Tuple equality - Float NaN values are equal")
func tupleFloatNaNEquality() throws {
    let tuple1 = Tuple(Float.nan)
    let tuple2 = Tuple(Float.nan)

    // Note: These are equal because they encode to the same bytes
    // (same bit pattern). This differs from Swift's Float equality
    // where Float.nan == Float.nan is false.
    #expect(tuple1 == tuple2, "NaN values with same bit pattern encode to same bytes")
    #expect(tuple1.hashValue == tuple2.hashValue, "Equal values must have same hash")
}

@Test("Tuple equality - Double NaN values are equal")
func tupleDoubleNaNEquality() throws {
    let tuple1 = Tuple(Double.nan)
    let tuple2 = Tuple(Double.nan)

    #expect(tuple1 == tuple2, "NaN values with same bit pattern encode to same bytes")
    #expect(tuple1.hashValue == tuple2.hashValue, "Equal values must have same hash")
}

@Test("Tuple equality - Float infinity values")
func tupleFloatInfinity() throws {
    let tuple1 = Tuple(Float.infinity)
    let tuple2 = Tuple(Float.infinity)
    let tuple3 = Tuple(-Float.infinity)

    #expect(tuple1 == tuple2, "Same infinity values should be equal")
    #expect(tuple1 != tuple3, "Positive and negative infinity should be unequal")
}

@Test("Tuple equality - Double infinity values")
func tupleDoubleInfinity() throws {
    let tuple1 = Tuple(Double.infinity)
    let tuple2 = Tuple(Double.infinity)
    let tuple3 = Tuple(-Double.infinity)

    #expect(tuple1 == tuple2, "Same infinity values should be equal")
    #expect(tuple1 != tuple3, "Positive and negative infinity should be unequal")
}

@Test("Tuple equality - empty tuples")
func tupleEmptyEquality() throws {
    let tuple1 = Tuple()
    let tuple2 = Tuple([])

    #expect(tuple1 == tuple2, "Empty tuples should be equal")
    #expect(tuple1.hashValue == tuple2.hashValue, "Empty tuples should have same hash")
    #expect(tuple1.count == 0, "Empty tuple should have count 0")
}

@Test("Tuple hashability - empty tuples in Set")
func tupleEmptySet() throws {
    let tuple1 = Tuple()
    let tuple2 = Tuple([])

    var set = Set<Tuple>()
    set.insert(tuple1)
    set.insert(tuple2)

    #expect(set.count == 1, "Empty tuples should be deduplicated in Set")
}

@Test("Tuple with nil values")
func tupleWithNil() throws {
    let tuple1 = Tuple(TupleElement.null, "hello", TupleElement.null)
    let tuple2 = Tuple(TupleElement.null, "hello", TupleElement.null)

    #expect(tuple1 == tuple2, "Tuples with nil values should be equal")
    #expect(tuple1.hashValue == tuple2.hashValue, "Tuples with nil values should have same hash")
    #expect(tuple1.count == 3, "Tuple should have 3 elements including nils")
}

@Test("Tuple equality - nil values in different positions are unequal")
func tupleNilPositions() throws {
    let tuple1 = Tuple(TupleElement.null, "hello")
    let tuple2 = Tuple("hello", TupleElement.null)

    #expect(tuple1 != tuple2, "Tuples with nils in different positions should be unequal")
}

@Test("Tuple to Swift Tuple")
func tupleToTuple() throws {
    typealias TupleType = (String, Int, String)

    let swiftTuple: TupleType = ("hello", 2, "foo")
    let tuple = Tuple(swiftTuple)

    let encoded = tuple.encode()
    let decodedTuple = try Tuple.decode(from: encoded, as: TupleType.self)
    #expect(decodedTuple == swiftTuple)

    let decodedTuple1a = try Tuple.decode(from: encoded, as: (String, Int, String).self)
    #expect(decodedTuple == decodedTuple1a)

    let encoded2 = Tuple.encode(swiftTuple)
    #expect(encoded == encoded2)

    let decodedTuple2: TupleType = try Tuple.decode(from: encoded)
    #expect(decodedTuple == decodedTuple2)

    let decodedTuple2a: (String, Int, String) = try Tuple.decode(from: encoded)
    #expect(decodedTuple2 == decodedTuple2a)

    let (s1, i2, s3) = try Tuple.decode<String, Int, String>(from: encoded)
    #expect(decodedTuple == (s1, i2, s3))
}
