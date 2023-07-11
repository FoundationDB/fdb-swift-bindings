/*
 * TransactionTests.swift
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

import XCTest
@testable import FoundationDB
import NIO

class TransactionTests: XCTestCase {
	let eventLoop = EmbeddedEventLoop()
	var connection: InMemoryDatabaseConnection!
	var transaction: InMemoryTransaction!
	
	static var allTests: [(String, (TransactionTests) -> () throws -> Void)] {
		return [
			("testReadWithSelectorsGetsValuesInRange", testReadWithSelectorsGetsValuesInRange),
			("testReadWithRangeGetsValuesInRange", testReadWithRangeGetsValuesInRange),
			("testAddReadConflictOnKeyAddsConflict", testAddReadConflictOnKeyAddsConflict),
			("testAddWriteConflictOnKeyAddsConflict", testAddWriteConflictOnKeyAddsConflict),
		]
	}
	
	override func setUp() {
		super.setUp()
		connection = InMemoryDatabaseConnection(eventLoop: eventLoop)
		transaction = InMemoryTransaction(version: connection.currentVersion, database: connection)
		connection["Test Key 1"] = "Test Value 1"
		connection["Test Key 2"] = "Test Value 2"
		connection["Test Key 3"] = "Test Value 3"
		connection["Test Key 4"] = "Test Value 4"
	}
	
	func testReadWithSelectorsGetsValuesInRange() throws {
		self.runLoop(eventLoop) {
			self.transaction.read(from: KeySelector(greaterThan: "Test Key 1"), to: KeySelector(lessThan: "Test Key 5")).map {
				let results = $0.rows
				XCTAssertEqual(results.count, 2)
				if results.count < 2 { return }
				XCTAssertEqual(results[0].key, "Test Key 2")
				XCTAssertEqual(results[0].value, "Test Value 2")
				XCTAssertEqual(results[1].key, "Test Key 3")
				XCTAssertEqual(results[1].value, "Test Value 3")
				}.catch(self)
		}
	}
	
	func testReadWithRangeGetsValuesInRange() throws {
		self.runLoop(eventLoop) {
			self.transaction.read(range: "Test Key 1" ..< "Test Key 3").map {
				let results = $0.rows
				XCTAssertEqual(results.count, 2)
				if results.count < 2 { return }
				XCTAssertEqual(results[0].key, "Test Key 1")
				XCTAssertEqual(results[0].value, "Test Value 1")
				XCTAssertEqual(results[1].key, "Test Key 2")
				XCTAssertEqual(results[1].value, "Test Value 2")
				}.catch(self)
		}
	}
	
	func testAddReadConflictOnKeyAddsConflict() throws {
		self.runLoop(eventLoop) {
			let transaction2 = self.connection.startTransaction()
			self.transaction.getReadVersion().flatMap { _ -> EventLoopFuture<Void> in
				self.transaction.addReadConflict(key: "Test Key 1")
				self.transaction.store(key: "Test Key 2", value: "Test Value 2")
				transaction2.store(key: "Test Key 1", value: "Test Value 1")
				return self.connection.commit(transaction: transaction2)
				}.map { _ in
					_ = self.connection.commit(transaction: self.transaction).map { _ in XCTFail() }
				}.catch(self)
		}
	}
	
	func testAddWriteConflictOnKeyAddsConflict() throws {
		self.runLoop(eventLoop) {
			let transaction2 = self.connection.startTransaction()
			transaction2.read("Test Key 1").flatMap { _ -> EventLoopFuture<Void> in
				self.transaction.addWriteConflict(key: "Test Key 1")
				self.transaction.store(key: "Test Key 2", value: "Test Value 2")
				return self.connection.commit(transaction: self.transaction)
				}.map { _ in
					_ = self.connection.commit(transaction: transaction2).map { XCTFail() }
				}.catch(self)
		}
	}
}
