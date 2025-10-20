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
    let tupleNil = TupleNil()
    let encoded = tupleNil.encodeTuple()
    #expect(encoded == [TupleTypeCode.null.rawValue], "TupleNil should encode to null type code")

    var offset = 1
    let decoded = try TupleNil.decodeTuple(from: encoded, at: &offset)
    #expect(decoded is TupleNil, "Should decode back to TupleNil")
    #expect(offset == 1, "Offset should not advance for TupleNil")
}

@Test("TupleString encoding and decoding")
func tupleString() throws {
    let testString = "Hello, World!"
    let encoded = testString.encodeTuple()

    #expect(encoded.first == TupleTypeCode.string.rawValue, "TupleString should start with string type code")
    #expect(encoded.last == 0x00, "TupleString should end with null terminator")

    var offset = 1
    let decoded = try String.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testString, "Should decode back to original string")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleString with null bytes")
func tupleStringWithNulls() throws {
    let testString = "Hello\u{0}World"
    let encoded = testString.encodeTuple()

    #expect(encoded.contains(0x00), "Encoded string should contain null bytes")
    #expect(encoded.contains(0xFF), "Null bytes should be escaped with 0xFF")

    var offset = 1
    let decoded = try String.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testString, "Should decode back to original string with nulls")
}

@Test("TupleBool encoding and decoding")
func tupleBool() throws {
    let testTrue = true
    let testFalse = false

    let encodedTrue = testTrue.encodeTuple()
    let encodedFalse = testFalse.encodeTuple()

    #expect(encodedTrue == [TupleTypeCode.boolTrue.rawValue], "true should encode to boolTrue type code")
    #expect(encodedFalse == [TupleTypeCode.boolFalse.rawValue], "false should encode to boolFalse type code")

    var offsetTrue = 1
    var offsetFalse = 1

    let decodedTrue = try Bool.decodeTuple(from: encodedTrue, at: &offsetTrue)
    let decodedFalse = try Bool.decodeTuple(from: encodedFalse, at: &offsetFalse)

    #expect(decodedTrue == true, "Should decode back to true")
    #expect(decodedFalse == false, "Should decode back to false")
}

@Test("TupleFloat encoding and decoding")
func tupleFloat() throws {
    let testFloat: Float = 3.14159
    let encoded = testFloat.encodeTuple()

    #expect(encoded.first == TupleTypeCode.float.rawValue, "Float should start with float type code")
    #expect(encoded.count == 5, "Float should be 5 bytes (1 type code + 4 data)")

    var offset = 1
    let decoded = try Float.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testFloat, "Should decode back to original float")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleDouble encoding and decoding")
func tupleDouble() throws {
    let testDouble = 3.141592653589793
    let encoded = testDouble.encodeTuple()

    #expect(encoded.first == TupleTypeCode.double.rawValue, "Double should start with double type code")
    #expect(encoded.count == 9, "Double should be 9 bytes (1 type code + 8 data)")

    var offset = 1
    let decoded = try Double.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testDouble, "Should decode back to original double")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleUUID encoding and decoding")
func tupleUUID() throws {
    let testUUID = UUID()
    let encoded = testUUID.encodeTuple()

    #expect(encoded.first == TupleTypeCode.uuid.rawValue, "UUID should start with uuid type code")
    #expect(encoded.count == 17, "UUID should be 17 bytes (1 type code + 16 data)")

    var offset = 1
    let decoded = try UUID.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testUUID, "Should decode back to original UUID")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - Zero")
func tupleInt64Zero() throws {
    let testInt: Int64 = 0
    let encoded = testInt.encodeTuple()

    #expect(encoded == [TupleTypeCode.intZero.rawValue], "Zero should encode to intZero type code")

    var offset = 1
    let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testInt, "Should decode back to original zero")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - Small positive")
func tupleInt64SmallPositive() throws {
    let testInt: Int64 = 42
    let encoded = testInt.encodeTuple()

    #expect(encoded.first == 0x15, "Small positive should use 0x15 type code (positiveInt1)")

    var offset = 1
    let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testInt, "Should decode back to original positive integer")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - Very small negative")
func tupleInt64VerySmallNegative() throws {
    let testInt: Int64 = -42
    let encoded = testInt.encodeTuple()

    #expect(encoded.first == 0x13)

    var offset = 1
    let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testInt, "Should decode back to original positive integer")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - Large negative")
func tupleInt64LargeNegative() throws {
    let testInt: Int64 = -89_034_333_444
    let encoded = testInt.encodeTuple()

    var offset = 1
    let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testInt, "Should decode back to original negative integer")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - Very Large negative")
func tupleInt64VeryLargeNegative() throws {
    let testInt: Int64 = -(1 << 55) - 34_897_432
    let encoded = testInt.encodeTuple()

    var offset = 1
    let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testInt, "Should decode back to original negative integer")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - VeryVery Large negative")
func tupleInt64VeryLargeNegative2() throws {
    let testInt: Int64 = -(1 << 60) - 34_897_432
    let encoded = testInt.encodeTuple()

    var offset = 1
    let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testInt, "Should decode back to original negative integer")
    #expect(offset == encoded.count, "Offset should advance to end of encoded data")
}

@Test("TupleInt64 encoding and decoding - Large values")
func tupleInt64LargeValues() throws {
    let largePositive = Int64.max
    let largeNegative = Int64.min + 1

    let encodedPos = largePositive.encodeTuple()
    let encodedNeg = largeNegative.encodeTuple()

    var offsetPos = 1
    var offsetNeg = 1

    let decodedPos = try Int64.decodeTuple(from: encodedPos, at: &offsetPos)
    let decodedNeg = try Int64.decodeTuple(from: encodedNeg, at: &offsetNeg)

    #expect(decodedPos == largePositive, "Should decode back to Int64.max")
    #expect(decodedNeg == largeNegative, "Should decode back to Int64.min")
}

@Test("TupleInt32 encoding and decoding")
func tupleInt32() throws {
    let testInt: Int32 = -2_034_333_444
    let encoded = testInt.encodeTuple()

    var offset = 1
    let decoded = try Int32.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testInt, "Should decode back to original Int32")
}

@Test("TupleInt encoding and decoding")
func tupleInt() throws {
    let testInt = 123_456
    let encoded = testInt.encodeTuple()

    var offset = 1
    let decoded = try Int.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testInt, "Should decode back to original Int")
}

@Test("TupleUInt64 encoding and decoding")
func tupleUInt64() throws {
    let testUInt: UInt64 = 999_999
    let encoded = testUInt.encodeTuple()

    var offset = 1
    let decoded = try UInt64.decodeTuple(from: encoded, at: &offset)
    #expect(decoded == testUInt, "Should decode back to original UInt64")
}

@Test("TupleNested encoding and decoding")
func tupleNested() throws {
    let innerTuple = Tuple("hello", 42, true)
    let outerTuple = Tuple("outer", innerTuple, "end")

    let encoded = outerTuple.encode()
    let decoded = try Tuple.decode(from: encoded)

    #expect(decoded.count == 3, "Should have 3 elements")

    let decodedString1 = decoded[0] as? String
    #expect(decodedString1 == "outer", "First element should be 'outer'")

    let decodedNested = decoded[1] as? Tuple
    #expect(decodedNested != nil, "Second element should be a Tuple")
    #expect(decodedNested?.count == 3, "Nested tuple should have 3 elements")

    let decodedString2 = decoded[2] as? String
    #expect(decodedString2 == "end", "Third element should be 'end'")
}

@Test("Tuple with a zero integer")
func tupleWithZero() throws {
    let tuple = Tuple("hello", 0, "foo")

    let encoded = tuple.encode()
    let decoded = try Tuple.decode(from: encoded)

    #expect(decoded.count == 3, "Should have 3 elements")
    let decodedString1 = decoded[0] as? String
    #expect(decodedString1 == "hello")

    let decodedInt = decoded[1] as? Int
    #expect(decodedInt == 0)

    let decodedString2 = decoded[2] as? String
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

    let topString = decoded[0] as? String
    #expect(topString == "top", "First element should be 'top'")

    let middleTuple = decoded[1] as? Tuple
    #expect(middleTuple != nil, "Second element should be a Tuple")
    #expect(middleTuple?.count == 2, "Middle tuple should have 2 elements")

    let bottomString = decoded[2] as? String
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
        let encoded = testInt.encodeTuple()

        if testInt > 0 {
            positive += 1
        } else if testInt < 0 {
            negative += 1
        }

        var offset = 1
        let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == testInt, "Integer \(testInt) should encode and decode correctly")
        #expect(offset == encoded.count, "Offset should advance to end of encoded data")
    }

    print("tested with n_positives = \(positive), n_negatives = \(negative)")
}
