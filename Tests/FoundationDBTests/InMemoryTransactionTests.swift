/*
 * InMemoryTransactionTests.swift
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
import Foundation
import NIO

class InMemoryTransactionTests: XCTestCase {
	let eventLoop = EmbeddedEventLoop()
	
	static var allTests: [(String, (InMemoryTransactionTests) -> () throws -> Void)] {
		return [
			("testReadGetsValueFromConnection", testReadGetsValueFromConnection),
			("testReadWithMissingKeyReturnsNil", testReadWithMissingKeyReturnsNil),
			("testReadAddsReadConflict", testReadAddsReadConflict),
			("testFindKeyWithGreaterThanOrEqualWithMatchingKeyFindsKey", testFindKeyWithGreaterThanOrEqualWithMatchingKeyFindsKey),
			("testFindKeyWithGreaterThanOrEqualWithNoExactMatchFindsNextKey", testFindKeyWithGreaterThanOrEqualWithNoExactMatchFindsNextKey),
			("testFindKeyWithGreaterThanOrEqualWithNoMatchingKeyIsNil", testFindKeyWithGreaterThanOrEqualWithNoMatchingKeyIsNil),
			("testFindKeyWithGreaterThanOrEqualWithOffsetReturnsOffsetKey", testFindKeyWithGreaterThanOrEqualWithOffsetReturnsOffsetKey),
			("testFindKeyWithGreaterThanWithFindsNextKey", testFindKeyWithGreaterThanWithFindsNextKey),
			("testFindKeyWithGreaterThanWithNoMatchingKeyIsNil", testFindKeyWithGreaterThanWithNoMatchingKeyIsNil),
			("testFindKeyWithGreaterThanWithOffsetReturnsOffsetKey", testFindKeyWithGreaterThanWithOffsetReturnsOffsetKey),
			("testFindKeyWithLessThanOrEqualWithMatchingKeyFindsKey", testFindKeyWithLessThanOrEqualWithMatchingKeyFindsKey),
			("testFindKeyWithLessThanOrEqualWithNoExactMatchFindsPreviousKey", testFindKeyWithLessThanOrEqualWithNoExactMatchFindsPreviousKey),
			("testFindKeyWithLessThanOrEqualWithNoMatchingKeyReturnsNil", testFindKeyWithLessThanOrEqualWithNoMatchingKeyReturnsNil),
			("testFindKeyWithLessThanOrEqualWithOffsetReturnsOffsetKey", testFindKeyWithLessThanOrEqualWithOffsetReturnsOffsetKey),
			("testFindKeyWithLessThanFindsPreviousKey", testFindKeyWithLessThanFindsPreviousKey),
			("testFindKeyWithLessThanWithNoMatchingKeyReturnsNil", testFindKeyWithLessThanWithNoMatchingKeyReturnsNil),
			("testFindKeyWithLessThanWithOffsetReturnsOffsetKey", testFindKeyWithLessThanWithOffsetReturnsOffsetKey),
			("testReadSelectorsReadsMatchingKeysAndValues", testReadSelectorsReadsMatchingKeysAndValues),
			("testReadSelectorCanReadLargeRanges", testReadSelectorCanReadLargeRanges),
			("testReadSelectorCanReadWithLimits", testReadSelectorCanReadWithLimits),
			("testReadSelectorsCanReadValuesInReverse", testReadSelectorsCanReadValuesInReverse),
			("testStorePutsPairInChangeSet", testStorePutsPairInChangeSet),
			("testStoreAddsWriteConflict", testStoreAddsWriteConflict),
			("testClearAddsNilValueToChangeSet", testClearAddsNilValueToChangeSet),
			("testClearAddsWriteConflict", testClearAddsWriteConflict),
			("testClearWithRangeAddsNilValuesToChangeSet", testClearWithRangeAddsNilValuesToChangeSet),
			("testClearRangeAddsWriteConflict", testClearRangeAddsWriteConflict),
			("testAddReadConflictAddsPairToReadConflictList", testAddReadConflictAddsPairToReadConflictList),
			("testAddWriteConflictAddsPairToWriteConflictList", testAddWriteConflictAddsPairToWriteConflictList),
			("testGetReadVersionReturnsVersionFromInitialization", testGetReadVersionReturnsVersionFromInitialization),
			("testSetReadVersionSetsReadVersion", testSetReadVersionSetsReadVersion),
			("testGetCommittedVersionGetsVersion", testGetCommittedVersionGetsVersion),
			("testGetCommittedVersionWithUncommittedTransactionReturnsNegativeOne", testGetCommittedVersionWithUncommittedTransactionReturnsNegativeOne),
			("testAttemptRetryResetsTransaction", testAttemptRetryResetsTransaction),
			("testAttemptRetryWithNonFdbErrorRethrowsError", testAttemptRetryWithNonFdbErrorRethrowsError),
			("testResetResetsTransaction", testResetResetsTransaction),
			("testCancelFlagsTransactionAsCancelled", testCancelFlagsTransactionAsCancelled),
			("testPerformAtomicOperationWithBitwiseAndPerformsOperation", testPerformAtomicOperationWithBitwiseAndPerformsOperation),
			("testGetVersionStampReturnsVersionStampAfterCommit", testGetVersionStampReturnsVersionStampAfterCommit),
			("testSetOptionWithNoWriteConflictOptionPreventsCausingWriteConflicts", testSetOptionWithNoWriteConflictOptionPreventsCausingWriteConflicts),
		]
	}
	
	var connection: InMemoryDatabaseConnection!
	var transaction: InMemoryTransaction!
	
	override func setUp() {
		super.setUp()
		connection = InMemoryDatabaseConnection(eventLoop: eventLoop)
		transaction = InMemoryTransaction(version: 5, database: connection)
		connection["Test Key 1"] = "Test Value 1"
		connection["Test Key 2"] = "Test Value 2"
		connection["Test Key 3"] = "Test Value 3"
		connection["Test Key 4"] = "Test Value 4"
	}
	
	override func tearDown() {
	}
	
	func testReadGetsValueFromConnection() throws {
		self.runLoop(eventLoop) {
			self.transaction.read("Test Key 1").map { XCTAssertEqual($0, "Test Value 1") }.catch(self)
		}
	}
	
	func testReadWithMissingKeyReturnsNil() throws {
		self.runLoop(eventLoop) {
			self.transaction.read("Test Key 5").map { XCTAssertNil($0) }.catch(self)
		}
	}
	
	func testReadAddsReadConflict() throws {
		self.runLoop(eventLoop) {
			self.transaction.read("Test Key 1").map { _ in
				XCTAssertEqual(self.transaction.readConflicts.count, 1)
				XCTAssertEqual(self.transaction.readConflicts.first?.lowerBound, "Test Key 1")
				XCTAssertEqual(self.transaction.readConflicts.first?.upperBound, "Test Key 1\u{0}")
				}.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanOrEqualWithMatchingKeyFindsKey() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(greaterThan: "Test Key 1", orEqual: true), snapshot: false).map {
				XCTAssertEqual($0, "Test Key 1")
				}.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanOrEqualWithNoExactMatchFindsNextKey() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(greaterThan: "Test Key 11", orEqual: true), snapshot: false).map {
				XCTAssertEqual($0, "Test Key 2")
				}.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanOrEqualWithNoMatchingKeyIsNil() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(greaterThan: "Test Key 5", orEqual: true), snapshot: false).map {
				XCTAssertNil($0)
				}.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanOrEqualWithOffsetReturnsOffsetKey() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(greaterThan: "Test Key 1", orEqual: true, offset: 2), snapshot: false).map {
				XCTAssertEqual($0, "Test Key 3")
				}.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanWithFindsNextKey() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(greaterThan: "Test Key 11", orEqual: false), snapshot: false).map {
				XCTAssertEqual($0, "Test Key 2")
				}.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanWithNoMatchingKeyIsNil() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(greaterThan: "Test Key 5", orEqual: false), snapshot: false).map {
				XCTAssertNil($0)
				}.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanWithOffsetReturnsOffsetKey() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(greaterThan: "Test Key 1", offset: 2), snapshot: false).map {
				XCTAssertEqual($0, "Test Key 4")
				}.catch(self)
		}
	}
	
	func testFindKeyWithLessThanOrEqualWithMatchingKeyFindsKey() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(lessThan: "Test Key 1", orEqual: true), snapshot: false).map {
				XCTAssertEqual($0, "Test Key 1")
				}.catch(self)
		}
	}
	
	func testFindKeyWithLessThanOrEqualWithNoExactMatchFindsPreviousKey() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(lessThan: "Test Key 11", orEqual: true), snapshot: false).map {
				XCTAssertEqual($0, "Test Key 1")
				}.catch(self)
		}
	}
	
	func testFindKeyWithLessThanOrEqualWithNoMatchingKeyReturnsNil() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(lessThan: "Test Key 0", orEqual: true), snapshot: false).map {
				XCTAssertNil($0)
				}.catch(self)
		}
	}
	
	func testFindKeyWithLessThanOrEqualWithOffsetReturnsOffsetKey() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(lessThan: "Test Key 4", orEqual: true, offset: 2), snapshot: false).map {
				XCTAssertEqual($0, "Test Key 2")
				}.catch(self)
		}
	}
	
	func testFindKeyWithLessThanFindsPreviousKey() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(lessThan: "Test Key 2"), snapshot: false).map {
				XCTAssertEqual($0, "Test Key 1")
				}.catch(self)
		}
	}
	
	func testFindKeyWithLessThanWithNoMatchingKeyReturnsNil() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(lessThan: "Test Key 1"), snapshot: false).map {
				XCTAssertNil($0)
				}.catch(self)
		}
	}
	
	func testFindKeyWithLessThanWithOffsetReturnsOffsetKey() throws {
		self.runLoop(eventLoop) {
			self.transaction.findKey(selector: KeySelector(lessThan: "Test Key 4", offset: 2), snapshot: false).map {
				XCTAssertEqual($0, "Test Key 1")
				}.catch(self)
		}
	}
	
	func testReadSelectorsReadsMatchingKeysAndValues() throws {
		self.runLoop(eventLoop) {
			self.transaction.readSelectors(from: KeySelector(greaterThan: "Test Key 1"), to: KeySelector(greaterThan: "Test Key 4"), limit: nil, mode: .iterator, snapshot: false, reverse: false).map {
				let results = $0.rows
				XCTAssertEqual(results.count, 3)
				if results.count < 3 { return }
				XCTAssertEqual(results[0].key, "Test Key 2")
				XCTAssertEqual(results[0].value, "Test Value 2")
				XCTAssertEqual(results[1].key, "Test Key 3")
				XCTAssertEqual(results[1].value, "Test Value 3")
				XCTAssertEqual(results[2].key, "Test Key 4")
				XCTAssertEqual(results[2].value, "Test Value 4")
				}.catch(self)
		}
	}
	
	func testReadSelectorCanReadLargeRanges() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction { (transaction) -> Void in
				for index in 0..<500 {
					let key = DatabaseValue(string: String(format: "Range Key %03i", index))
					let value = DatabaseValue(string: String(format: "Range Value %03i", index))
					transaction.store(key: key, value: value)
				}
				}.flatMap {
					return self.transaction.readSelectors(from: KeySelector(greaterThan: "Range Key"), to: KeySelector(greaterThan: "T"), limit: nil, mode: .iterator, snapshot: false, reverse: false).map {
						let results = $0.rows
						XCTAssertEqual(results.count, 500)
						XCTAssertEqual(results.first?.key, "Range Key 000")
						XCTAssertEqual(results.first?.value, "Range Value 000")
						XCTAssertEqual(results.last?.key, "Range Key 499")
						XCTAssertEqual(results.last?.value, "Range Value 499")
					}
				}.catch(self)
		}
	}
	
	func testReadSelectorCanReadWithLimits() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction { (transaction: Transaction) -> Void in
				for index in 0..<500 {
					let key = DatabaseValue(string: String(format: "Range Key %03i", index))
					let value = DatabaseValue(string: String(format: "Range Value %03i", index))
					transaction.store(key: key, value: value)
				}
				}.flatMap {
					self.transaction.readSelectors(from: KeySelector(greaterThan: "Range Key"), to: KeySelector(greaterThan: "T"), limit: 5, mode: .iterator, snapshot: false, reverse: false).map {
						let results = $0.rows
						XCTAssertEqual(results.count, 5)
						XCTAssertEqual(results.first?.key, "Range Key 000")
						XCTAssertEqual(results.first?.value, "Range Value 000")
						XCTAssertEqual(results.last?.key, "Range Key 004")
						XCTAssertEqual(results.last?.value, "Range Value 004")
					}
				}.catch(self)
		}
	}
	
	func testReadSelectorsCanReadValuesInReverse() throws {
		self.runLoop(eventLoop) {
			self.transaction.readSelectors(from: KeySelector(greaterThan: "Test Key 1"), to: KeySelector(greaterThan: "Test Key 4"), limit: nil, mode: .iterator, snapshot: false, reverse: true).map {
				let results = $0.rows
				XCTAssertEqual(results.count, 3)
				if results.count < 3 { return }
				XCTAssertEqual(results[0].key, "Test Key 4")
				XCTAssertEqual(results[0].value, "Test Value 4")
				XCTAssertEqual(results[1].key, "Test Key 3")
				XCTAssertEqual(results[1].value, "Test Value 3")
				XCTAssertEqual(results[2].key, "Test Key 2")
				XCTAssertEqual(results[2].value, "Test Value 2")
				}.catch(self)
		}
	}
	
	func testStorePutsPairInChangeSet() {
		transaction.store(key: "Key1", value: "Value1")
		transaction.store(key: "Key2", value: "Value2")
		XCTAssertEqual(transaction.changes.keys.count, 2)
		XCTAssertEqual(transaction.changes["Key1"] ?? nil, "Value1")
		XCTAssertEqual(transaction.changes["Key2"] ?? nil, "Value2")
	}
	
	func testStoreAddsWriteConflict() {
		transaction.store(key: "Key1", value: "Value1")
		transaction.store(key: "Key2", value: "Value2")
		XCTAssertEqual(transaction.writeConflicts.count, 2)
		if transaction.writeConflicts.count < 2 { return }
		XCTAssertEqual(transaction.writeConflicts[0].lowerBound, "Key1")
		XCTAssertEqual(transaction.writeConflicts[0].upperBound, "Key2")
		XCTAssertEqual(transaction.writeConflicts[1].lowerBound, "Key2")
		XCTAssertEqual(transaction.writeConflicts[1].upperBound, "Key3")
	}
	
	func testClearAddsNilValueToChangeSet() {
		transaction.clear(key: "Key1")
		XCTAssertNotNil(transaction.changes["Key1"] as Any)
		if(!transaction.changes.keys.contains("Key1")) {
			return
		}
		XCTAssertNil(transaction.changes["Key1"]!)
	}
	
	func testClearAddsWriteConflict() {
		transaction.clear(key: "Key1")
		XCTAssertEqual(transaction.writeConflicts.count, 1)
		if transaction.writeConflicts.count < 1 { return }
		XCTAssertEqual(transaction.writeConflicts[0].lowerBound, "Key1")
		XCTAssertEqual(transaction.writeConflicts[0].upperBound, "Key2")
	}
	
	func testClearWithRangeAddsNilValuesToChangeSet() {
		transaction.clear(range: "Test Key 1" ..< "Test Key 3")
		XCTAssertTrue(transaction.changes.keys.contains("Test Key 1"))
		XCTAssertNil(transaction.changes["Test Key 1"]!)
		XCTAssertTrue(transaction.changes.keys.contains("Test Key 2"))
		XCTAssertNil(transaction.changes["Test Key 2"]!)
		XCTAssertFalse(transaction.changes.keys.contains("Test Key 3"))
	}
	
	func testClearRangeAddsWriteConflict() {
		transaction.clear(range: "Test Key 1" ..< "Test Key 3")
		XCTAssertEqual(transaction.writeConflicts.count, 1)
		if transaction.writeConflicts.count < 1 { return }
		XCTAssertEqual(transaction.writeConflicts[0].lowerBound, "Test Key 1")
		XCTAssertEqual(transaction.writeConflicts[0].upperBound, "Test Key 3")
	}
	
	func testAddReadConflictAddsPairToReadConflictList() {
		transaction.addReadConflict(on: "Key1" ..< "Key2")
		XCTAssertEqual(transaction.readConflicts.count, 1)
		if transaction.readConflicts.count < 1 { return }
		XCTAssertEqual(transaction.readConflicts[0].lowerBound, "Key1")
		XCTAssertEqual(transaction.readConflicts[0].upperBound, "Key2")
	}
	
	func testAddWriteConflictAddsPairToWriteConflictList() {
		transaction.addWriteConflict(on: "Key1" ..< "Key2")
		XCTAssertEqual(transaction.writeConflicts.count, 1)
		if transaction.writeConflicts.count < 1 { return }
		XCTAssertEqual(transaction.writeConflicts[0].lowerBound, "Key1")
		XCTAssertEqual(transaction.writeConflicts[0].upperBound, "Key2")
	}
	
	func testGetReadVersionReturnsVersionFromInitialization() throws {
		self.runLoop(eventLoop) {
			self.transaction.getReadVersion().map {
				XCTAssertEqual($0, self.transaction.readVersion)
				}.catch(self)
		}
	}
	
	func testSetReadVersionSetsReadVersion() throws {
		transaction.setReadVersion(151)
		XCTAssertEqual(transaction.readVersion, 151)
	}
	
	func testGetCommittedVersionGetsVersion() throws {
		self.runLoop(eventLoop) {
			self.connection.commit(transaction: self.transaction).flatMap {
				self.transaction.getCommittedVersion().map {
					XCTAssertEqual($0, 1)
				}
				}.catch(self)
		}
	}
	
	func testGetCommittedVersionWithUncommittedTransactionReturnsNegativeOne() throws {
		self.runLoop(eventLoop) {
			self.transaction.getCommittedVersion().map {
				XCTAssertEqual($0, -1)
				}.catch(self)
		}
	}
	
	func testAttemptRetryResetsTransaction() throws {
		self.runLoop(eventLoop) {
			_ = self.transaction.read("Test Key")
			self.transaction.store(key: "Test Key", value: "Test Value")
			self.transaction.attemptRetry(error: ClusterDatabaseConnection.FdbApiError(1020)).map { _ in
				XCTAssertEqual(self.transaction.changes.count, 0)
				XCTAssertEqual(self.transaction.readConflicts.count, 0)
				}.catch(self)
		}
	}
	
	func testAttemptRetryWithNonFdbErrorRethrowsError() throws {
		self.runLoop(eventLoop) {
			_ = self.transaction.read("Test Key")
			self.transaction.store(key: "Test Key", value: "Test Value")
			self.transaction.attemptRetry(error: TestError.test).map { XCTFail() }
				.recover {
					switch($0) {
					case is TestError: break
					default: XCTFail("Unexpected error: \($0)")
					}
					XCTAssertEqual(self.transaction.changes.count, 1)
					XCTAssertEqual(self.transaction.readConflicts.count, 1)
				}.catch(self)
		}
	}
	
	func testResetResetsTransaction() {
		self.runLoop(eventLoop) {
			self.transaction.read("Test Key").map { _ in
				self.transaction.store(key: "Test Key", value: "Test Value")
				self.transaction.reset()
				XCTAssertEqual(self.transaction.changes.count, 0)
				XCTAssertEqual(self.transaction.readConflicts.count, 0)
				}.catch(self)
		}
	}
	
	func testCancelFlagsTransactionAsCancelled() {
		transaction.cancel()
		XCTAssertTrue(transaction.cancelled)
	}
	
	func testPerformAtomicOperationWithBitwiseAndPerformsOperation() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction { $0.store(key: "Test Key", value: DatabaseValue(Data([0xC3]))) }.flatMap { _ -> EventLoopFuture<Void> in
				self.transaction.performAtomicOperation(operation: .bitAnd, key: "Test Key", value: DatabaseValue(Data([0xA9])))
				return self.connection.commit(transaction: self.transaction).flatMap {
					self.connection.transaction {
						$0.read("Test Key").map {
							XCTAssertEqual($0, DatabaseValue(Data([0x81])))
						}
					}
				}
				}.catch(self)
		}
	}
	
	func testGetVersionStampReturnsVersionStampAfterCommit() throws {
		self.runLoop(eventLoop) {
			let future = self.transaction.getVersionStamp()
			self.transaction.store(key: "Test Key", value: "Test Value")
			self.connection.commit(transaction: self.transaction).flatMap {
				future.map {
					XCTAssertEqual($0.data, Data([0,0,0,0,0,0,0,1,0,0]))
				}
				}.catch(self)
		}
	}
	
	func testSetOptionWithNoWriteConflictOptionPreventsCausingWriteConflicts() throws {
		self.runLoop(eventLoop) {
			let transaction2 = self.connection.startTransaction()
			self.transaction.setOption(.nextWriteNoWriteConflictRange)
			self.transaction.store(key: "Test Key", value: "Test Value")
			_ = transaction2.read("Test Key")
			transaction2.store(key: "Test Key 2", value: "Test Value 2")
			self.connection.commit(transaction: self.transaction).flatMap {
				self.connection.commit(transaction: transaction2)
				}.catch(self)
		}
	}
}
