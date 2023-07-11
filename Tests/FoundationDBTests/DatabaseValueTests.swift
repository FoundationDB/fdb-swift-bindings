/*
 * DatabaseValueTests.swift
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

class DatabaseValueTests: XCTestCase {
	static var allTests: [(String, (DatabaseValueTests) -> () throws -> Void)] {
		return [
			("testInitializationWithDataPutsDataInValue", testInitializationWithDataPutsDataInValue),
			("testInitializationWithStringPutsStringInValue", testInitializationWithStringPutsStringInValue),
			("testInitializationWithUnicodeLiteralPutsStringInValue", testInitializationWithUnicodeLiteralPutsStringInValue),
			("testInitializationWithStringLiteralPutsStringInValue", testInitializationWithStringLiteralPutsStringInValue),
			("testInitializationWithGraphemeLiteralPutsStringInValue", testInitializationWithGraphemeLiteralPutsStringInValue),
			("testHasPrefixWithSameValueIsTrue", testHasPrefixWithSameValueIsTrue),
			("testHasPrefixWithPrefixValueIsTrue", testHasPrefixWithPrefixValueIsTrue),
			("testHasPrefixWithSiblingKeyIsFalse", testHasPrefixWithSiblingKeyIsFalse),
			("testHasPrefixWithChildKeyIsFalse", testHasPrefixWithChildKeyIsFalse),
            ("testHasPrefixWithSubrangePrefixValueIsTrue", testHasPrefixWithSubrangePrefixValueIsTrue),
			("testIncrementIncrementsLastByte", testIncrementIncrementsLastByte),
			("testIncrementCanCarryIntoEarlierBytes", testIncrementCanCarryIntoEarlierBytes),
			("testIncrementWithMaxValueWrapsToZero", testIncrementWithMaxValueWrapsToZero),
			("testValuesWithSameDataAreEqual", testValuesWithSameDataAreEqual),
			("testValuesWithDifferentDataAreNotEqual", testValuesWithDifferentDataAreNotEqual),
			("testValuesWithSameDataHaveSameHash", testValuesWithSameDataHaveSameHash),
			("testValuesWithDifferentDataHaveDifferentHash", testValuesWithDifferentDataHaveDifferentHash),
			("testCompareValuesWithMatchingValuesReturnsFalse", testCompareValuesWithMatchingValuesReturnsFalse),
			("testCompareValuesWithAscendingValuesReturnsTrue", testCompareValuesWithAscendingValuesReturnsTrue),
			("testCompareValuesWithDescendingValuesReturnsFalse", testCompareValuesWithDescendingValuesReturnsFalse),
			("testCompareValuesWithPrefixAsFirstReturnsTrue", testCompareValuesWithPrefixAsFirstReturnsTrue),
			("testCompareValuesWithPrefixAsSecondReturnsFalse", testCompareValuesWithPrefixAsSecondReturnsFalse),
		]
	}
	
	func testInitializationWithDataPutsDataInValue() {
		let value = DatabaseValue(Data([0x10, 0x01, 0x19]))
		XCTAssertEqual(value.data, Data([0x10, 0x01, 0x19]))
	}
	
	func testInitializationWithStringPutsStringInValue() {
		let value = DatabaseValue(string: "Test Value")
		XCTAssertEqual(value.data, Data([0x54, 0x65, 0x73, 0x74, 0x20, 0x56, 0x61, 0x6C, 0x75, 0x65]))
	}
	
	func testInitializationWithUnicodeLiteralPutsStringInValue() {
		let value = DatabaseValue(unicodeScalarLiteral: "Test Value")
		XCTAssertEqual(value.data, Data([0x54, 0x65, 0x73, 0x74, 0x20, 0x56, 0x61, 0x6C, 0x75, 0x65]))
	}
	
	func testInitializationWithStringLiteralPutsStringInValue() {
		let value = DatabaseValue(stringLiteral: "Test Value")
		XCTAssertEqual(value.data, Data([0x54, 0x65, 0x73, 0x74, 0x20, 0x56, 0x61, 0x6C, 0x75, 0x65]))
	}
	
	func testInitializationWithGraphemeLiteralPutsStringInValue() {
		let value = DatabaseValue(extendedGraphemeClusterLiteral: "Test Value")
		XCTAssertEqual(value.data, Data([0x54, 0x65, 0x73, 0x74, 0x20, 0x56, 0x61, 0x6C, 0x75, 0x65]))
	}
	
	func testHasPrefixWithSameValueIsTrue() {
		let key1 = DatabaseValue(bytes: [1,2,3,4])
		XCTAssertTrue(key1.hasPrefix(key1))
	}
	
	func testHasPrefixWithPrefixValueIsTrue() {
		let key1 = DatabaseValue(bytes: [1,2,3,4])
		let key2 = DatabaseValue(bytes: [1,2,3,4,5])
		XCTAssertTrue(key2.hasPrefix(key1))
	}
	
	func testHasPrefixWithSiblingKeyIsFalse() {
		let key1 = DatabaseValue(bytes: [1,2,3,4,0])
		let key2 = DatabaseValue(bytes: [1,2,3,4,5])
		XCTAssertFalse(key2.hasPrefix(key1))
	}
	
	func testHasPrefixWithChildKeyIsFalse() {
		let key1 = DatabaseValue(bytes: [1,2,3,4])
		let key2 = DatabaseValue(bytes: [1,2,3,4,5])
		XCTAssertFalse(key1.hasPrefix(key2))
	}

    func testHasPrefixWithSubrangePrefixValueIsTrue() {
        let key1 = DatabaseValue(Data([0,1,2,3,4])[1...])
        let key2 = DatabaseValue(bytes: [1,2,3,4,5])
        XCTAssertTrue(key2.hasPrefix(key1))
    }
	
	func testIncrementIncrementsLastByte() {
		var key = DatabaseValue(bytes: [1,2,3,4])
		key.increment()
		XCTAssertEqual(key, DatabaseValue(bytes: [1,2,3,5]))
	}
	
	func testIncrementCanCarryIntoEarlierBytes() {
		var key = DatabaseValue(bytes: [1,2,0xFF,0xFF])
		key.increment()
		XCTAssertEqual(key, DatabaseValue(bytes: [1,3,0,0]))
	}
	
	func testIncrementWithMaxValueWrapsToZero() {
		var key = DatabaseValue(bytes: [0xFF,0xFF,0xFF,0xFF])
		key.increment()
		XCTAssertEqual(key, DatabaseValue(bytes: [0,0,0,0]))
	}
	
	func testValuesWithSameDataAreEqual() {
		let value1 = DatabaseValue(bytes: [0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x31])
		let value2 = DatabaseValue(bytes: [0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x31])
		XCTAssertEqual(value1, value2)
	}
	
	func testValuesWithDifferentDataAreNotEqual() {
		let value1 = DatabaseValue(bytes: [0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x31])
		let value2 = DatabaseValue(bytes: [0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x32])
		XCTAssertNotEqual(value1, value2)
	}
	
	func testValuesWithSameDataHaveSameHash() {
		let value1 = DatabaseValue(bytes: [0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x31])
		let value2 = DatabaseValue(bytes: [0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x31])
		XCTAssertEqual(value1.hashValue, value2.hashValue)
	}
	
	func testValuesWithDifferentDataHaveDifferentHash() {
		let value1 = DatabaseValue(bytes: [0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x31])
		let value2 = DatabaseValue(bytes: [0x54, 0x65, 0x73, 0x74, 0x4B, 0x65, 0x79, 0x32])
		XCTAssertNotEqual(value1.hashValue, value2.hashValue)
	}
	
	func testCompareValuesWithMatchingValuesReturnsFalse() {
		XCTAssertFalse(DatabaseValue(string: "Value1") < DatabaseValue(string: "Value1"))
	}
	
	func testCompareValuesWithAscendingValuesReturnsTrue() {
		XCTAssertTrue(DatabaseValue(string: "Value1") < DatabaseValue(string: "Value2"))
	}
	
	func testCompareValuesWithDescendingValuesReturnsFalse() {
		XCTAssertFalse(DatabaseValue(string: "Value2") < DatabaseValue(string: "Value1"))
	}
	
	func testCompareValuesWithPrefixAsFirstReturnsTrue() {
		XCTAssertTrue(DatabaseValue(string: "Value") < DatabaseValue(string: "Value2"))
	}
	
	func testCompareValuesWithPrefixAsSecondReturnsFalse() {
		XCTAssertFalse(DatabaseValue(string: "Value2") < DatabaseValue(string: "Value"))
	}
}
