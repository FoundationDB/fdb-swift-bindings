/*
 * TupleConvertibleTests.swift
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

class TupleConvertibleTests: XCTestCase {
	var data = Data()
	
	override func setUp() {
		data = Data()
	}
	
	func testIntegerEncoding() throws {
		Int.FoundationDBTupleAdapter.write(value: -5551212, into: &data)
		XCTAssertEqual([0x11, 0xAB, 0x4B, 0x93], Array(data))
		var value = try Int.FoundationDBTupleAdapter.read(from: data, at: 0)
		XCTAssertEqual(-5551212, value)
		
		Int.FoundationDBTupleAdapter.write(value: 1273, into: &data)
		XCTAssertEqual([0x11, 0xAB, 0x4B, 0x93, 0x16, 0x04, 0xF9], Array(data))
		value = try Int.FoundationDBTupleAdapter.read(from: data, at: 4)
		XCTAssertEqual(1273, value)
	}
	
	func testUnsignedIntegerEncoding() throws {
		UInt64.FoundationDBTupleAdapter.write(value: 5551212, into: &data)
		XCTAssertEqual([0x17, 0x54, 0xB4, 0x6C], Array(data))
		let value = try UInt64.FoundationDBTupleAdapter.read(from: data, at: 0)
		XCTAssertEqual(5551212, value)
		
		Int.FoundationDBTupleAdapter.write(value: -5551212, into: &data)
		XCTAssertThrowsError(try UInt64.FoundationDBTupleAdapter.read(from: data, at: 4))
	}
	
	func testByteEncoding() throws {
		UInt8.FoundationDBTupleAdapter.write(value: 0x24, into: &data)
		XCTAssertEqual([0x15, 0x24], Array(data))
		let value = try UInt8.FoundationDBTupleAdapter.read(from: data, at: 0)
		XCTAssertEqual(0x24, value)
		
		Int.FoundationDBTupleAdapter.write(value: 1454, into: &data)
		XCTAssertThrowsError(try UInt8.FoundationDBTupleAdapter.read(from: data, at: 2))
	}
	
	func testDataEncoding() {
		let sample = Data([0x66, 0x6F, 0x6F, 0x00, 0x62, 0x61, 0x72])
		Data.FoundationDBTupleAdapter.write(value: sample, into: &data)
		XCTAssertEqual([0x01, 0x66, 0x6F, 0x6F, 0x00, 0xFF, 0x62, 0x61, 0x72, 0x00], Array(data))
		let value = Data.FoundationDBTupleAdapter.read(from: data, at: 0)
		XCTAssertEqual(Array(sample), Array(value))
	}
	
	func testStringEncoding() throws {
		let string = "F\u{00d4}O\u{0000}bar"
		String.FoundationDBTupleAdapter.write(value: string, into: &data)
		XCTAssertEqual([0x02, 0x46, 0xC3, 0x94, 0x4F, 0x00, 0xFF, 0x62, 0x61, 0x72, 0x00], Array(data))
		let value = try String.FoundationDBTupleAdapter.read(from: data, at: 0)
		XCTAssertEqual(string, value)
	}
	
	func testBooleanEncoding() {
		Bool.FoundationDBTupleAdapter.write(value: false, into: &data)
		XCTAssertEqual([0x26], Array(data))
		XCTAssertFalse(Bool.FoundationDBTupleAdapter.read(from: data, at: 0))
		
		Bool.FoundationDBTupleAdapter.write(value: true, into: &data)
		XCTAssertEqual([0x26, 0x27], Array(data))
		XCTAssertTrue(Bool.FoundationDBTupleAdapter.read(from: data, at: 1))
	}
	
	func testUUIDEncoding() throws {
		let uuid = UUID(uuidString: "3c7498fa-4e90-11e8-9615-9801a7a4265b")!
		UUID.FoundationDBTupleAdapter.write(value: uuid, into: &data)
		XCTAssertEqual([0x30, 0x3c, 0x74, 0x98, 0xfa, 0x4e, 0x90, 0x11, 0xe8, 0x96, 0x15, 0x98, 0x01, 0xa7, 0xa4, 0x26, 0x5b], Array(data))
		let value = try UUID.FoundationDBTupleAdapter.read(from: data, at: 0)
		XCTAssertEqual(uuid, value)
	}
	
	func testFloatEncoding() throws {
		var float: Float32 = 42.0
		Float32.FoundationDBTupleAdapter.write(value: float, into: &data)
		XCTAssertEqual([0x20, 0xC2, 0x28, 0x00, 0x00], Array(data))
		var value = try Float32.FoundationDBTupleAdapter.read(from: data, at: 0)
		XCTAssertEqual(float, value)
		
		data = Data()
		float = -42.0
		Float32.FoundationDBTupleAdapter.write(value: float, into: &data)
		XCTAssertEqual([0x20, 0x3D, 0xD7, 0xFF, 0xFF], Array(data))
		value = try Float32.FoundationDBTupleAdapter.read(from: data, at: 0)
		XCTAssertEqual(float, value)
	}
	
	func testDoubleEncoding() throws {
		var double: Float64 = 42.0
		Float64.FoundationDBTupleAdapter.write(value: double, into: &data)
		XCTAssertEqual([0x21, 0xC0, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], Array(data))
		var value = try Float64.FoundationDBTupleAdapter.read(from: data, at: 0)
		XCTAssertEqual(double, value, accuracy: 0.01)
		
		data = Data()
		double = -42.0
		Float64.FoundationDBTupleAdapter.write(value: double, into: &data)
		XCTAssertEqual([0x21, 0x3F, 0xBA, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], Array(data))
		value = try Float64.FoundationDBTupleAdapter.read(from: data, at: 0)
		XCTAssertEqual(double, value)
	}
	
	func testNullEncoding() {
		let null = NSNull()
		NSNull.FoundationDBTupleAdapter.write(value: null, into: &data)
		XCTAssertEqual([0x00], Array(data))
		let value = NSNull.FoundationDBTupleAdapter.read(from: data, at: 0)
		XCTAssertEqual(null, value)
	}
	
	func testNestedTupleEncoding() throws {
		let tuple = Tuple(Data([0x66, 0x6F, 0x6F, 0x00, 0x62, 0x61, 0x72]), NSNull(), Tuple())
		Tuple.FoundationDBTupleAdapter.write(value: tuple, into: &data)
		XCTAssertEqual([0x05, 0x01, 0x66, 0x6F, 0x6F, 0x00, 0xFF, 0x62, 0x61, 0x72, 0x00, 0x00, 0xFF, 0x05, 0x00, 0x00], Array(data))
		let value = Tuple.FoundationDBTupleAdapter.read(from: data, at: 0)
		XCTAssertEqual(3, value.count)
		XCTAssertEqual(Data([0x66, 0x6F, 0x6F, 0x00, 0x62, 0x61, 0x72]), try tuple.read(at: 0))
		XCTAssertEqual(NSNull(), try tuple.read(at: 1))
		
		let nested: Tuple = try tuple.read(at: 2)
		XCTAssertEqual(0, nested.count)
	}
}
