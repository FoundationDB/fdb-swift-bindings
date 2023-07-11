/*
 * TupleTests.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2016-2018 Apple Inc. and the FoundationDB project authors
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
@testable import FoundationDB
import XCTest

class TupleTests: XCTestCase {
	static var allTests: [(String, (TupleTests) -> () throws -> Void)] {
		return [
			("testDefaultInitializerCreatesEmptyTuple", testDefaultInitializerCreatesEmptyTuple),
			("testInitializationWithDataItemPutsDataItemInTuple", testInitializationWithDataItemPutsDataItemInTuple),
			("testInitializationWithStringPutsStringInTuple", testInitializationWithStringPutsStringInTuple),
			("testInitializationWithMultipleEntriesAddsEntries", testInitializationWithMultipleEntriesAddsEntries),
			("testInitializationWithRawDataReadsAllFields", testInitializationWithRawDataReadsAllFields),
			("testInitializationWithRawDataWithSingleStringReadsString", testInitializationWithRawDataWithSingleStringReadsString),
			("testInitializationWithRawDataWithStringAndRangeEndByteIgnoresByte", testInitializationWithRawDataWithStringAndRangeEndByteIgnoresByte),
			("testInitializationWithRawDataWithIntegerAndRangeEndByteIgnoresByte", testInitializationWithRawDataWithIntegerAndRangeEndByteIgnoresByte),
			("testAppendStringAddsStringBytes", testAppendStringAddsStringBytes),
			("testAppendStringWithUnicodeCharactersAddsUTF8Bytes", testAppendStringWithUnicodeCharactersAddsUTF8Bytes),
			("testAppendStringWithNullByteInStringEscapesNullByte", testAppendStringWithNullByteInStringEscapesNullByte),
			("testAppendNullAddsNullByte", testAppendNullAddsNullByte),
			("testAppendingNullAddsNullByte", testAppendingNullAddsNullByte),
			("testAppendDataAppendsBytes", testAppendDataAppendsBytes),
			("testAppendDataWithNullByteInDataEscapesNullByte", testAppendDataWithNullByteInDataEscapesNullByte),
			("testAppendIntegerAppendsBytes", testAppendIntegerAppendsBytes),
			("testAppendIntegerWithSmallNumberAppendsNecessaryBytes", testAppendIntegerWithSmallNumberAppendsNecessaryBytes),
			("testAppendIntegerWithZeroAppendsHeaderByte", testAppendIntegerWithZeroAppendsHeaderByte),
			("testAppendIntegerWithNegativeIntegerAppendsBytes", testAppendIntegerWithNegativeIntegerAppendsBytes),
			("testAppendIntegerWith64BitIntegerAppendsBytes", testAppendIntegerWith64BitIntegerAppendsBytes),
			("testAppendingMultipleTimesAddsAllValues", testAppendingMultipleTimesAddsAllValues),
			("testReadWithStringWithValidDataReadsString", testReadWithStringWithValidDataReadsString),
			("testReadWithStringWithIntegerValueThrowsError", testReadWithStringWithIntegerValueThrowsError),
			("testReadWithStringWithInvalidUTF8DataThrowsError", testReadWithStringWithInvalidUTF8DataThrowsError),
			("testReadWithMultipleValuesCanReadFromDifferentIndices", testReadWithMultipleValuesCanReadFromDifferentIndices),
			("testReadWithMultipleValuesWithIndexBeyondBoundsThrowsError", testReadWithMultipleValuesWithIndexBeyondBoundsThrowsError),
			("testTypeAtWithStringReturnsString", testTypeAtWithStringReturnsString),
			("testTypeAtWithIntegerReturnsInteger", testTypeAtWithIntegerReturnsInteger),
			("testTypeAtWithIndexBeyondBoundsReturnsNil", testTypeAtWithIndexBeyondBoundsReturnsNil),
			("testChildRangeGetsRangeContainingChildren", testChildRangeGetsRangeContainingChildren),
			("testHasPrefixWithSameTupleIsTrue", testHasPrefixWithSameTupleIsTrue),
			("testHasPrefixWithPrefixTupleIsTrue", testHasPrefixWithPrefixTupleIsTrue),
			("testHasPrefixWithSiblingKeyIsFalse", testHasPrefixWithSiblingKeyIsFalse),
			("testHasPrefixWithChildKeyIsFalse", testHasPrefixWithChildKeyIsFalse),
            ("testHasPrefixReadRangeAndEvaluateHasPrefixIsTrue", testHasPrefixReadRangeAndEvaluateHasPrefixIsTrue),
			("testIncrementLastEntryWithIntegerEntryIncrementsValue", testIncrementLastEntryWithIntegerEntryIncrementsValue),
			("testIncrementLastEntryWithIntegerWithCarryCarriesIncrement", testIncrementLastEntryWithIntegerWithCarryCarriesIncrement),
			("testIncrementLastEntryWithIntegerOverflowResetsToZero", testIncrementLastEntryWithIntegerOverflowResetsToZero),
			("testIncrementLastEntryWithNegativeIntegerIncrementsNumber", testIncrementLastEntryWithNegativeIntegerIncrementsNumber),
			("testIncrementLastEntryWithStringPerformsCharacterIncrement", testIncrementLastEntryWithStringPerformsCharacterIncrement),
			("testIncrementLastEntryWithDataPerformsByteIncrement", testIncrementLastEntryWithDataPerformsByteIncrement),
			("testIncrementLastEntryWithNullByteDoesNothing", testIncrementLastEntryWithNullByteDoesNothing),
			("testIncrementLastEntryWithRangeEndByteDoesNothing", testIncrementLastEntryWithRangeEndByteDoesNothing),
			("testIncrementLastEntryWithEmptyTupleDoesNothing", testIncrementLastEntryWithEmptyTupleDoesNothing),
			("testDescriptionReturnsDescriptionOfTupleElements", testDescriptionReturnsDescriptionOfTupleElements),
			("testTuplesWithSameDataAreEqual", testTuplesWithSameDataAreEqual),
			("testTuplesWithDifferentDataAreNotEqual", testTuplesWithDifferentDataAreNotEqual),
			("testTuplesWithSameDataHaveSameHash", testTuplesWithSameDataHaveSameHash),
			("testTuplesWithDifferentDataHaveDifferentHash", testTuplesWithDifferentDataHaveDifferentHash),
			("testCompareTuplesWithMatchingValuesReturnsFalse", testCompareTuplesWithMatchingValuesReturnsFalse),
			("testCompareTuplesWithAscendingValuesReturnsTrue", testCompareTuplesWithAscendingValuesReturnsTrue),
			("testCompareTuplesWithDescendingValuesReturnsFalse", testCompareTuplesWithDescendingValuesReturnsFalse),
			("testCompareTuplesWithPrefixAsFirstReturnsTrue", testCompareTuplesWithPrefixAsFirstReturnsTrue),
			("testCompareTuplesWithPrefixAsSecondReturnsFalse", testCompareTuplesWithPrefixAsSecondReturnsFalse),
		]
	}
	
	func testDefaultInitializerCreatesEmptyTuple() {
		let tuple = Tuple()
		XCTAssertEqual(tuple.count, 0)
	}
	
	func testInitializationWithDataItemPutsDataItemInTuple() {
		let tuple = Tuple(Data([0x10, 0x01, 0x19]))
		XCTAssertEqual(tuple.count, 1)
		XCTAssertEqual(tuple.data, Data([0x01, 0x10, 0x01, 0x19, 0x00]))
	}
	
	func testInitializationWithStringPutsStringInTuple() {
		let tuple = Tuple("Test Value")
		XCTAssertEqual(tuple.count, 1)
		XCTAssertEqual(tuple.data, Data([0x02, 0x54, 0x65, 0x73, 0x74, 0x20, 0x56, 0x61, 0x6C, 0x75, 0x65, 0x00]))
	}
	
	func testInitializationWithMultipleEntriesAddsEntries() {
		let tuple = Tuple("Test Value", 45)
		XCTAssertEqual(tuple.count, 2)
		XCTAssertEqual(tuple.data, Data([0x02, 0x54, 0x65, 0x73, 0x74, 0x20, 0x56, 0x61, 0x6C, 0x75, 0x65, 0x00, 0x15, 0x2D]))
	}
	
	func testInitializationWithRawDataReadsAllFields() throws {
		let tuple = Tuple(rawData: Data([
			0x02, 0x54, 0x65, 0x73, 0x74, 0x20, 0x56, 0x61, 0x6C, 0x75, 0x65, 0x00,
			0x01, 0x16, 0x05, 0xAB, 0x00,
			0x00,
			0x26,
			0x17, 0x95, 0xCC, 0x92,
			0x02, 0x54, 0x65, 0x73, 0x00, 0xFF, 0x74, 0x00,
			0x27,
			
			0x30, 0x43, 0xF5, 0xA5, 0x8A, 0x1F, 0xD8, 0x11, 0xE8, 0x88, 0xA7, 0x98, 0x01, 0xA7, 0xA4, 0x26, 0x5B,
			0x14,
			0x15, 0x18,
			0x20, 0xC2, 0x28, 0x00, 0x00,
			0x21, 0xC0, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
			]))
		
		XCTAssertEqual(tuple.count, 12)
		XCTAssertEqual(try tuple.read(at: 0) as String, "Test Value")
		XCTAssertEqual(try tuple.read(at: 1) as Data, Data([0x16, 0x05, 0xAB]))
		XCTAssertEqual(try tuple.read(at: 3) as Bool, false)
		XCTAssertEqual(try tuple.read(at: 4) as Int, 9817234)
		#if os(OSX)
		XCTAssertEqual(try tuple.read(at: 5) as String, "Tes\u{0}t")
		#else
		XCTAssertEqual(try tuple.read(at: 5) as String, "Tes")
		#endif
		XCTAssertEqual(try tuple.read(at: 6) as Bool, true)
		XCTAssertEqual(try tuple.read(at: 7) as UUID, UUID(uuidString: "43f5a58a-1fd8-11e8-88a7-9801a7a4265b"))
		XCTAssertEqual(try tuple.read(at: 8) as Int, 0)
		XCTAssertEqual(try tuple.read(at: 9) as Int, 24)
		XCTAssertEqual(try tuple.read(at: 10) as Float, 42.0, accuracy: 0.01)
		XCTAssertEqual(try tuple.read(at: 11) as Double, 42.0, accuracy: 0.01)
	}
	
	func testInitializationWithRawDataWithSingleStringReadsString() {
		let tuple = Tuple(rawData: Data([0x02, 0x54, 0x65, 0x73, 0x74, 0x20, 0x4B, 0x65, 0x79, 0x00]))
		XCTAssertEqual(tuple.count, 1)
		XCTAssertEqual(try tuple.read(at: 0) as String, "Test Key")
	}
	
	func testInitializationWithRawDataWithStringAndRangeEndByteIgnoresByte() {
		let tuple = Tuple(rawData: Data([
			0x02, 0x54, 0x65, 0x73, 0x74, 0x20, 0x4B, 0x65, 0x79, 0x00,
			0x02, 0x54, 0x65, 0x73, 0x74, 0x20, 0x56, 0x61, 0x6C, 0x75, 0x65, 0x00,
			0xFF]))
		XCTAssertEqual(tuple.count, 3)
		XCTAssertEqual(try tuple.read(at: 0) as String, "Test Key")
		XCTAssertEqual(try tuple.read(at: 1) as String, "Test Value")
		XCTAssertEqual(tuple.data.count, 23)
	}
	
	func testInitializationWithRawDataWithIntegerAndRangeEndByteIgnoresByte() {
		let tuple = Tuple(rawData: Data([
			0x17, 0x95, 0xCC, 0x92,
			0xFF
			]))
		XCTAssertEqual(tuple.count, 2)
		XCTAssertEqual(try tuple.read(at: 0) as Int, 9817234)
	}
	
	func testAppendStringAddsStringBytes() {
		var tuple = Tuple()
		tuple.append("Test Key")
		XCTAssertEqual(tuple.data, Data([0x02, 0x54, 0x65, 0x73, 0x74, 0x20, 0x4B, 0x65, 0x79, 0x00]))
		XCTAssertEqual(tuple.count, 1)
	}
	
	func testAppendStringWithUnicodeCharactersAddsUTF8Bytes() {
		var tuple = Tuple()
		tuple.append("Niño")
		XCTAssertEqual(tuple.data, Data([0x02, 0x4E, 0x69, 0xC3, 0xB1, 0x6F, 0x00]))
	}
	
	func testAppendStringWithNullByteInStringEscapesNullByte() {
		var tuple = Tuple()
		tuple.append("Test \u{0}Key")
		XCTAssertEqual(tuple.data, Data([0x02, 0x54, 0x65, 0x73, 0x74, 0x20, 0x00, 0xFF, 0x4B, 0x65, 0x79, 0x00]))
		XCTAssertEqual(tuple.count, 1)
	}
	
	func testAppendNullAddsNullByte() {
		var tuple = Tuple()
		tuple.appendNullByte()
		XCTAssertEqual(tuple.data, Data([0x00]))
		XCTAssertEqual(tuple.count, 1)
	}
	
	func testAppendingNullAddsNullByte() {
		let tuple = Tuple().appendingNullByte()
		XCTAssertEqual(tuple.data, Data([0x00]))
		XCTAssertEqual(tuple.count, 1)
	}
	
	func testAppendDataAppendsBytes() {
		var tuple = Tuple()
		tuple.append(Data([0x10, 0x01, 0x19]))
		XCTAssertEqual(tuple.data, Data([0x01, 0x10, 0x01, 0x19, 0x00]))
		XCTAssertEqual(tuple.count, 1)
	}
	
	func testAppendDataWithNullByteInDataEscapesNullByte() {
		var tuple = Tuple()
		tuple.append(Data([0x10, 0x00, 0x19]))
		XCTAssertEqual(tuple.data, Data([0x01, 0x10, 0x00, 0xFF, 0x19, 0x00]))
		XCTAssertEqual(tuple.count, 1)
	}
	
	func testAppendIntegerAppendsBytes() {
		var tuple = Tuple()
		tuple.append(8174509123489079081)
		XCTAssertEqual(tuple.data, Data([0x1C, 0x71, 0x71, 0xB0, 0xBC, 0xC6, 0xC1, 0x9F, 0x29]))
		XCTAssertEqual(tuple.count, 1)
	}
	
	func testAppendIntegerWithSmallNumberAppendsNecessaryBytes() {
		var tuple = Tuple()
		tuple.append(1451)
		XCTAssertEqual(tuple.data, Data([0x16, 0x05, 0xAB]))
		XCTAssertEqual(tuple.count, 1)
	}
	
	func testAppendIntegerWithZeroAppendsHeaderByte() {
		var tuple = Tuple()
		tuple.append(0)
		XCTAssertEqual(tuple.data, Data([0x14]))
		XCTAssertEqual(tuple.count, 1)
	}
	
	func testAppendIntegerWithNegativeIntegerAppendsBytes() {
		var tuple = Tuple()
		tuple.append(-89127348907)
		XCTAssertEqual(tuple.data, Data([0x0F, 0xEB, 0x3F, 0x98, 0x95, 0x54]))
		XCTAssertEqual(tuple.count, 1)
	}
	
	func testAppendIntegerWith64BitIntegerAppendsBytes() {
		var tuple = Tuple()
		tuple.append(14732181464251135039 as UInt64)
		XCTAssertEqual(tuple.data, Data([0x1C, 0xCC, 0x73, 0x38, 0xFC, 0xBF, 0x48, 0x74, 0x3F]))
		XCTAssertEqual(tuple.count, 1)
	}
	
	func testAppendingMultipleTimesAddsAllValues() {
		var tuple = Tuple()
		tuple.append("Test Value")
		tuple.append(Data([0x16, 0x05, 0xAB]))
		tuple.appendNullByte()
		tuple.append(9817234)
		XCTAssertEqual(tuple.data, Data([
			
			0x02, 0x54, 0x65, 0x73, 0x74, 0x20, 0x56, 0x61, 0x6C, 0x75, 0x65, 0x00,
			0x01, 0x16, 0x05, 0xAB, 0x00,
			0x00,
			0x17, 0x95, 0xCC, 0x92
			]))
		XCTAssertEqual(tuple.count, 4)
	}
	
	func testReadWithStringWithValidDataReadsString() throws {
		var tuple = Tuple()
		tuple.append("Test Value")
		let result: String = try tuple.read(at: 0)
		XCTAssertEqual(result, "Test Value")
	}
	
	func testReadWithStringWithIntegerValueThrowsError() throws {
		var tuple = Tuple()
		tuple.append(10897)
		XCTAssertThrowsError(try tuple.read(at: 0) as String) {
			error in
			switch(error) {
			case let TupleDecodingError.incorrectTypeCode(index: index, desired: desired, actual: actual):
				XCTAssertEqual(index, 0)
				XCTAssertEqual(desired, Set([Tuple.EntryType.string.rawValue]))
				XCTAssertEqual(actual, 0x16)
			default:
				XCTFail("Got unexpected error: \(error)")
			}
		}
	}
	
	func testReadWithStringWithInvalidUTF8DataThrowsError() throws {
		let tuple = Tuple(rawData: Data([0x02, 0x54, 0xC0, 0x65, 0x73, 0x74, 0x20, 0x4B, 0x65, 0x79, 0x00]))
		
		XCTAssertThrowsError(try tuple.read(at: 0) as String) {
			error in
			switch(error) {
			case TupleDecodingError.invalidString:
				return
			default:
				XCTFail("Got unexpected error: \(error)")
			}
		}
	}
	
	func testReadWithMultipleValuesCanReadFromDifferentIndices() throws {
		var tuple = Tuple()
		tuple.append("Test")
		tuple.appendNullByte()
		tuple.append(15)
		XCTAssertEqual(try tuple.read(at: 0) as String, "Test")
		XCTAssertEqual(try tuple.read(at: 2) as Int, 15)
	}
	
	func testReadWithMultipleValuesWithIndexBeyondBoundsThrowsError() throws {
		var tuple = Tuple()
		tuple.append("Test")
		tuple.appendNullByte()
		tuple.append(15)
		XCTAssertThrowsError(try tuple.read(at: 3) as Int) {
			error in
			switch(error) {
			case let TupleDecodingError.missingField(index):
				XCTAssertEqual(index, 3)
				break
			default:
				XCTFail("Threw unexpected error: \(error)")
			}
		}
	}
	
	func testTypeAtWithStringReturnsString() {
		var tuple = Tuple()
		tuple.append("Test")
		tuple.append(15)
		XCTAssertEqual(tuple.type(at: 0), .string)
	}
	
	func testTypeAtWithIntegerReturnsInteger() {
		var tuple = Tuple()
		tuple.append("Test")
		tuple.append(15)
		XCTAssertEqual(tuple.type(at: 1), .integer)
	}
	
	func testTypeAtWithIndexBeyondBoundsReturnsNil() {
		var tuple = Tuple()
		tuple.append("Test")
		tuple.append(15)
		XCTAssertNil(tuple.type(at: 2))
	}
	
	func testParsingComplexNestedTuple() throws {
		let data = Data([
			0x02, 0x50, 0x55, 0x53, 0x48, 0x00, 0x05, 0x21, 0x45, 0xF0, 0x6D,
			0x8A, 0x84, 0xD9, 0xD1, 0x5B, 0x05, 0x00, 0x01, 0x22, 0x0D, 0x23,
			0x03, 0x52, 0x59, 0x4F, 0x9F, 0xFB, 0x82, 0xF0, 0xA0, 0x2D, 0x4C,
			0x85, 0x1C, 0x29, 0x1F, 0x12, 0x96, 0xB7, 0xFC, 0x34, 0x6F, 0xAE,
			0x6C, 0xEB, 0x84, 0xFF, 0xF6, 0x73, 0xBE, 0xAF, 0xF6, 0x38, 0x11,
			0x6E, 0x51, 0x74, 0x54, 0x10, 0x64, 0xEB, 0xAE, 0x3F, 0x1F, 0x65,
			0xFC, 0x1B, 0xFF, 0x5F, 0x9E, 0x0E, 0xAF, 0x1B, 0x6D, 0xFE, 0x84,
			0xB3, 0x83, 0x9C, 0xED, 0x05, 0xD3, 0x00, 0x00])
		let tuple = Tuple(rawData: data)
		XCTAssertEqual(tuple.count, 2)
		XCTAssertEqual(try tuple.read(at: 0), "PUSH")
		let tuple2 = try tuple.read(at: 1) as Tuple
		XCTAssertEqual(3, tuple2.count)
		XCTAssertEqual(-4.981199884715445e-29, try tuple2.read(at: 0), accuracy: 0.00001)
		XCTAssertEqual(Tuple(), try tuple2.read(at: 1))
		let innerData = try tuple2.read(at: 2) as Data
		XCTAssertEqual(64, innerData.count)
	}
	
	func testChildRangeGetsRangeContainingChildren() {
		let data = Data([
			0x02, 0x54, 0x65, 0x73, 0x74, 0x20, 0x4B, 0x65, 0x79, 0x00,
			0x02, 0x54, 0x65, 0x73, 0x74, 0x20, 0x56, 0x61, 0x6C, 0x75, 0x65, 0x00]
		)
		let tuple = Tuple(rawData: data)
		let range = tuple.childRange
		var startData = data
		startData.append(0x00)
		XCTAssertEqual(range.lowerBound.data, startData)
		var endData = data
		endData.append(0xFF)
		XCTAssertEqual(range.upperBound.data, endData)
		
		var child = tuple
		child.append(5)
		XCTAssertTrue(range.contains(child))
		XCTAssertFalse(range.contains(Tuple("Test Key", "Test Value 5")))
	}
	
	func testHasPrefixWithSameTupleIsTrue() {
		let key1 = Tuple("Test", "Key")
		XCTAssertTrue(key1.hasPrefix(key1))
	}
	
	func testHasPrefixWithPrefixTupleIsTrue() {
		let key1 = Tuple("Test", "Key")
		let key2 = Tuple("Test", "Key", 2)
		XCTAssertTrue(key2.hasPrefix(key1))
	}
	
	func testHasPrefixWithSiblingKeyIsFalse() {
		let key1 = Tuple("Test", "Key")
		let key2 = Tuple("Test", "Keys")
		XCTAssertFalse(key2.hasPrefix(key1))
	}
	
	func testHasPrefixWithChildKeyIsFalse() {
		let key1 = Tuple("Test", "Key")
		let key2 = Tuple("Test", "Key", 2)
		XCTAssertFalse(key1.hasPrefix(key2))
	}

    func testHasPrefixReadRangeAndEvaluateHasPrefixIsTrue() {
        do {
            let key1 = try Tuple("Prefix", "Test", "Key").read(range: 1..<3)
            let key2 = Tuple("Test", "Key")
            XCTAssertTrue(key2.hasPrefix(key1))
        } catch let e {
            XCTFail(e.localizedDescription)
        }
    }
	
	func testIncrementLastEntryWithIntegerEntryIncrementsValue() {
		var key = Tuple("Test", "Key", 5)
		key.incrementLastEntry()
		XCTAssertEqual(key, Tuple("Test", "Key", 6))
	}
	
	func testIncrementLastEntryWithIntegerWithCarryCarriesIncrement() {
		var key = Tuple("Test", "Key", 511)
		key.incrementLastEntry()
		XCTAssertEqual(key, Tuple("Test", "Key", 512))
	}
	
	func testIncrementLastEntryWithIntegerOverflowResetsToZero() {
		var key = Tuple("Test", "Key", 255)
		key.incrementLastEntry()
		XCTAssertEqual(try key.read(at: 2) as Int, 0)
	}
	
	func testIncrementLastEntryWithNegativeIntegerIncrementsNumber() {
		var key = Tuple("Test", "Key", -5)
		key.incrementLastEntry()
		XCTAssertEqual(key, Tuple("Test", "Key", -4))
	}
	
	func testIncrementLastEntryWithStringPerformsCharacterIncrement() {
		var key = Tuple("Test", "Key", "C")
		key.incrementLastEntry()
		XCTAssertEqual(key, Tuple("Test", "Key", "D"))
	}
	
	func testIncrementLastEntryWithDataPerformsByteIncrement() {
		var key = Tuple("Test", "Key", Data([1, 2, 3]))
		key.incrementLastEntry()
		XCTAssertEqual(key, Tuple("Test", "Key", Data([1, 2, 4])))
	}
	
	func testIncrementLastEntryWithNullByteDoesNothing() {
		var key = Tuple("Test", "Key").appendingNullByte()
		key.incrementLastEntry()
		XCTAssertEqual(key, Tuple("Test", "Key").appendingNullByte())
	}
	
	func testIncrementLastEntryWithRangeEndByteDoesNothing() {
		var key = Tuple("Test", "Key").childRange.upperBound
		key.incrementLastEntry()
		XCTAssertEqual(key, Tuple("Test", "Key").childRange.upperBound)
	}
	
	func testIncrementLastEntryWithEmptyTupleDoesNothing() {
		var key = Tuple()
		key.incrementLastEntry()
		XCTAssertEqual(key, Tuple())
	}
	
	func testDescriptionReturnsDescriptionOfTupleElements() {
		let key1 = Tuple("Test", 5)
		XCTAssertEqual(key1.description, "(Test, 5)")
		let key2 = key1.appendingNullByte()
		XCTAssertEqual(key2.description, "(Test, 5, \\x00)")
		let key3 = key1.childRange.upperBound
		XCTAssertEqual(key3.description, "(Test, 5, \\xFF)")
		var key4 = key1
		key4.append(-5)
		XCTAssertEqual(key4.description, "(Test, 5, -5)")
		var key5 = key1
		key5.append(Data([1,2,3,4]))
		XCTAssertEqual(key5.description, "(Test, 5, 4 bytes)")
	}
	
	func testTuplesWithSameDataAreEqual() {
		let tuple1 = Tuple(Data([0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x31]))
		let tuple2 = Tuple(Data([0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x31]))
		XCTAssertEqual(tuple1, tuple2)
	}
	
	func testTuplesWithDifferentDataAreNotEqual() {
		let tuple1 = Tuple(Data([0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x31]))
		let tuple2 = Tuple(Data([0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x32]))
		XCTAssertNotEqual(tuple1, tuple2)
	}
	
	func testTuplesWithSameDataHaveSameHash() {
		let tuple1 = Tuple(Data([0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x31]))
		let tuple2 = Tuple(Data([0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x31]))
		XCTAssertEqual(tuple1.hashValue, tuple2.hashValue)
	}
	
	func testTuplesWithDifferentDataHaveDifferentHash() {
		let tuple1 = Tuple(Data([0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x31]))
		let tuple2 = Tuple(Data([0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x32]))
		XCTAssertNotEqual(tuple1.hashValue, tuple2.hashValue)
	}
	
	func testCompareTuplesWithMatchingValuesReturnsFalse() {
		XCTAssertFalse(Tuple("Value1") < Tuple("Value1"))
	}
	
	func testCompareTuplesWithAscendingValuesReturnsTrue() {
		XCTAssertTrue(Tuple("Value1") < Tuple("Value2"))
	}
	
	func testCompareTuplesWithDescendingValuesReturnsFalse() {
		XCTAssertFalse(Tuple("Value2") < Tuple("Value1"))
	}
	
	func testCompareTuplesWithPrefixAsFirstReturnsTrue() {
		XCTAssertTrue(Tuple("Value") < Tuple("Value2"))
	}
	
	func testCompareTuplesWithPrefixAsSecondReturnsFalse() {
		XCTAssertFalse(Tuple("Value2") < Tuple("Value"))
	}
}
