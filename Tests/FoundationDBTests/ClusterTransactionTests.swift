/*
 * ClusterTransactionTests.swift
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
import Foundation
@testable import FoundationDB
import CFoundationDB
import CFoundationDB
import NIO

class ClusterTransactionTests: XCTestCase {
	let eventLoop = EmbeddedEventLoop()
	var connection: ClusterDatabaseConnection? = nil
	var transaction: ClusterTransaction? = nil
	
	static var allTests : [(String, (ClusterTransactionTests) -> () throws -> Void)] {
		return [
			("testReadKeyReadsValueForKey", testReadKeyReadsValueForKey),
			("testReadKeyWithMissingValueReturnsNil", testReadKeyWithMissingValueReturnsNil),
			("testReadKeyKeepsMultipleReadsConsistent", testReadKeyKeepsMultipleReadsConsistent),
			("testReadKeyWithSnapshotOnDoesNotAddReadConflict", testReadKeyWithSnapshotOnDoesNotAddReadConflict),
			("testFindKeyWithGreaterThanOrEqualWithMatchingKeyFindsKey", testFindKeyWithGreaterThanOrEqualWithMatchingKeyFindsKey),
			("testFindKeyWithGreaterThanOrEqualWithNoExactMatchFindsNextKey", testFindKeyWithGreaterThanOrEqualWithNoExactMatchFindsNextKey),
			("testFindKeyWithGreaterThanOrEqualWithNoMatchingKeyReturnsFFKey", testFindKeyWithGreaterThanOrEqualWithNoMatchingKeyReturnsFFKey),
			("testFindKeyWithGreaterThanOrEqualWithOffsetReturnsOffsetKey", testFindKeyWithGreaterThanOrEqualWithOffsetReturnsOffsetKey),
			("testFindKeyWithGreaterThanWithFindsNextKey", testFindKeyWithGreaterThanWithFindsNextKey),
			("testFindKeyWithGreaterThanWithNoMatchingKeyReturnsFFKey", testFindKeyWithGreaterThanWithNoMatchingKeyReturnsFFKey),
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
			("testClearCanClearKey", testClearCanClearKey),
			("testClearCanClearRange", testClearCanClearRange),
			("testAddReadConflictAddsReadConflict", testAddReadConflictAddsReadConflict),
			("testGetReadVersionGetsReadVersion", testGetReadVersionGetsReadVersion),
			("testSetReadVersionSetsReadVersion", testSetReadVersionSetsReadVersion),
			("testGetCommittedVersionGetsVersion", testGetCommittedVersionGetsVersion),
			("testGetCommittedVersionWithUncommittedTransactionReturnsNegativeOne", testGetCommittedVersionWithUncommittedTransactionReturnsNegativeOne),
			("testAttemptRetryWithTransactionNotCommittedErrorDoesNotThrowError", testAttemptRetryWithTransactionNotCommittedErrorDoesNotThrowError),
			("testAttemptRetryWithNoMoreServersRethrowsError", testAttemptRetryWithNoMoreServersRethrowsError),
			("testAttemptRetryWithNonApiErrorRethrowsError", testAttemptRetryWithNonApiErrorRethrowsError),
			("testResetResetsTransaction", testResetResetsTransaction),
			("testResetWithCommittedTransactionAllowsCommittingAgain", testResetWithCommittedTransactionAllowsCommittingAgain),
			("testResetWithCancelledTransactionAllowsCommitting", testResetWithCancelledTransactionAllowsCommitting),
			("testCancelPreventsCommittingTransaction", testCancelPreventsCommittingTransaction),
			("testPerformAtomicOperationWithBitwiseAndPerformsOperation", testPerformAtomicOperationWithBitwiseAndPerformsOperation),
			("testGetVersionStampReturnsVersionStampAfterCommit", testGetVersionStampReturnsVersionStampAfterCommit),
			("testAddWriteConflictAddsWriteConflict", testAddWriteConflictAddsWriteConflict),
			("testSetOptionWithNoWriteConflictOptionPreventsCausingWriteConflicts", testSetOptionWithNoWriteConflictOptionPreventsCausingWriteConflicts),
		]
	}
	
	override func setUp() {
		super.setUp()
		setFdbApiVersion(FDB_API_VERSION)
		
		if connection == nil {
			do {
				connection = try ClusterDatabaseConnection(eventLoop: eventLoop)
			}
			catch {
				print("Error creating database connection for testing: \(error)")
			}
		}
		
		transaction = connection.flatMap { ClusterTransaction(database: $0) }
		
		runLoop(eventLoop) {
			self.connection?.transaction {
				$0.clear(range: Tuple().childRange)
				$0.store(key: "Test Key 1", value: "Test Value 1")
				$0.store(key: "Test Key 2", value: "Test Value 2")
				$0.store(key: "Test Key 3", value: "Test Value 3")
				$0.store(key: "Test Key 4", value: "Test Value 4")
				}.catch(self)
		}
	}
	
	func testReadKeyReadsValueForKey() throws {
		guard let transaction = transaction else { return XCTFail() }
		self.runLoop(eventLoop) {
			transaction.read("Test Key 1")
				.map {
					XCTAssertEqual($0, "Test Value 1") }
				.catch(self)
		}
	}
	
	func testReadKeyWithMissingValueReturnsNil() throws {
		guard let transaction = transaction else { return XCTFail() }
		self.runLoop(eventLoop) {
			transaction.read("Test Key 5").map {
				XCTAssertNil($0)
				}.catch(self)
		}
	}
	
	func testReadKeyKeepsMultipleReadsConsistent() throws {
		guard let transaction = transaction else { return XCTFail() }
		guard let connection = connection else { return XCTFail() }
		self.runLoop(eventLoop) {
			transaction.read("Test Key 1").map {
				XCTAssertEqual($0, "Test Value 1")
				}.catch(self)
			
			connection.transaction { (t: Transaction) -> Void in
				t.store(key: "Test Key 1", value: "Test Value 3")
				return Void()
				}.flatMap { _ -> EventLoopFuture<Void> in
					transaction.store(key: "Test Key 2", value: "Test Value 2")
					return transaction.read("Test Key 1").map {
						XCTAssertEqual($0, "Test Value 1")
						}.map { _ in
							_ = connection.commit(transaction: transaction).map { XCTFail() }
					}
				}.catch(self)
		}
	}
	
	func testReadKeyWithSnapshotOnDoesNotAddReadConflict() throws {
		guard let transaction = transaction else { return XCTFail() }
		guard let connection = connection else { return XCTFail() }
		self.runLoop(eventLoop) {
			transaction.read("Test Key 1", snapshot: true)
				.map { XCTAssertEqual($0, "Test Value 1") }
				.catch(self)
			connection.transaction {
				$0.store(key: "Test Key 1", value: "Test Value 3")
				}.flatMap { _ -> EventLoopFuture<Void> in
					transaction.read("Test Key 1", snapshot: true)
						.map { XCTAssertEqual($0, "Test Value 1") }
						.catch(self)
					transaction.store(key: "Test Key 2", value: "Test Value 2")
					return connection.commit(transaction: transaction)
				}.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanOrEqualWithMatchingKeyFindsKey() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			transaction.findKey(selector: KeySelector(greaterThan: "Test Key 1", orEqual: true), snapshot: false).map { XCTAssertEqual($0, "Test Key 1") }.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanOrEqualWithNoExactMatchFindsNextKey() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			transaction.findKey(selector: KeySelector(greaterThan: "Test Key 11", orEqual: true), snapshot: false).map { XCTAssertEqual($0, "Test Key 2") }.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanOrEqualWithNoMatchingKeyReturnsFFKey() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			let key = transaction.findKey(selector: KeySelector(greaterThan: "Test Key 5", orEqual: true), snapshot: false)
			key.map { XCTAssertEqual($0, DatabaseValue(bytes: [0xFF])) }.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanOrEqualWithOffsetReturnsOffsetKey() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			let key = transaction.findKey(selector: KeySelector(greaterThan: "Test Key 1", orEqual: true, offset: 2), snapshot: false)
			key.map { XCTAssertEqual($0, "Test Key 3") }.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanWithFindsNextKey() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			let key = transaction.findKey(selector: KeySelector(greaterThan: "Test Key 11", orEqual: false), snapshot: false)
			key.map { XCTAssertEqual($0, "Test Key 2") }.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanWithNoMatchingKeyReturnsFFKey() throws {
		guard let transaction = transaction else { return XCTFail() }
		self.runLoop(eventLoop) {
			let key = transaction.findKey(selector: KeySelector(greaterThan: "Test Key 5", orEqual: false), snapshot: false)
			key.map { XCTAssertEqual($0, DatabaseValue(bytes: [0xFF])) }.catch(self)
		}
	}
	
	func testFindKeyWithGreaterThanWithOffsetReturnsOffsetKey() throws {
		guard let transaction = transaction else { return XCTFail() }
		self.runLoop(eventLoop) {
			let key = transaction.findKey(selector: KeySelector(greaterThan: "Test Key 1", offset: 2), snapshot: false)
			key.map { XCTAssertEqual($0, "Test Key 4") }.catch(self)
		}
	}
	
	func testFindKeyWithLessThanOrEqualWithMatchingKeyFindsKey() throws {
		guard let transaction = transaction else { return XCTFail() }
		self.runLoop(eventLoop) {
			let key = transaction.findKey(selector: KeySelector(lessThan: "Test Key 1", orEqual: true), snapshot: false)
			key.map { XCTAssertEqual($0, "Test Key 1") }.catch(self)
		}
	}
	
	func testFindKeyWithLessThanOrEqualWithNoExactMatchFindsPreviousKey() throws {
		guard let transaction = transaction else { return XCTFail() }
		self.runLoop(eventLoop) {
			let key = transaction.findKey(selector: KeySelector(lessThan: "Test Key 11", orEqual: true), snapshot: false)
			key.map { XCTAssertEqual($0, "Test Key 1") }.catch(self)
		}
	}
	
	func testFindKeyWithLessThanOrEqualWithNoMatchingKeyReturnsNil() throws {
		guard let transaction = transaction else { return XCTFail() }
		self.runLoop(eventLoop) {
			let key = transaction.findKey(selector: KeySelector(lessThan: "Test Key 0", orEqual: true), snapshot: false)
			key.map { XCTAssertNil($0) }.catch(self)
		}
	}
	
	func testFindKeyWithLessThanOrEqualWithOffsetReturnsOffsetKey() throws {
		guard let transaction = transaction else { return XCTFail() }
		self.runLoop(eventLoop) {
			let key = transaction.findKey(selector: KeySelector(lessThan: "Test Key 4", orEqual: true, offset: 2), snapshot: false)
			key.map { XCTAssertEqual($0, "Test Key 2") }.catch(self)
		}
	}
	
	func testFindKeyWithLessThanFindsPreviousKey() throws {
		guard let transaction = transaction else { return XCTFail() }
		self.runLoop(eventLoop) {
			let key = transaction.findKey(selector: KeySelector(lessThan: "Test Key 2"), snapshot: false)
			key.map { XCTAssertEqual($0, "Test Key 1") }.catch(self)
		}
	}
	
	func testFindKeyWithLessThanWithNoMatchingKeyReturnsNil() throws {
		guard let transaction = transaction else { return XCTFail() }
		self.runLoop(eventLoop) {
			let key = transaction.findKey(selector: KeySelector(lessThan: "Test Key 1"), snapshot: false)
			key.map { XCTAssertNil($0) }.catch(self)
		}
	}
	
	func testFindKeyWithLessThanWithOffsetReturnsOffsetKey() throws {
		guard let transaction = transaction else { return XCTFail() }
		self.runLoop(eventLoop) {
			let key = transaction.findKey(selector: KeySelector(lessThan: "Test Key 4", offset: 2), snapshot: false)
			key.map { XCTAssertEqual($0, "Test Key 1") }.catch(self)
		}
	}
	
	func testReadSelectorsReadsMatchingKeysAndValues() throws {
		guard let transaction = transaction else { return XCTFail() }
		self.runLoop(eventLoop) {
			transaction.readSelectors(from: KeySelector(greaterThan: "Test Key 1"), to: KeySelector(greaterThan: "Test Key 4"), limit: nil, mode: .iterator, snapshot: false, reverse: false).map {
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
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			connection.transaction { transaction -> Void in
				for index in 0..<500 {
					let key = DatabaseValue(string: String(format: "Range Key %03i", index))
					let value = DatabaseValue(string: String(format: "Range Value %03i", index))
					transaction.store(key: key, value: value)
				}
				}.flatMap {
					transaction.readSelectors(from: KeySelector(greaterThan: "Range Key"), to: KeySelector(greaterThan: "T"), limit: nil, mode: .iterator, snapshot: false, reverse: false).map {
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
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			connection.transaction {
				for index in 0..<500 {
					let key = DatabaseValue(string: String(format: "Range Key %03i", index))
					let value = DatabaseValue(string: String(format: "Range Value %03i", index))
					$0.store(key: key, value: value)
				}
				}.flatMap { _ in
					transaction.readSelectors(from: KeySelector(greaterThan: "Range Key"), to: KeySelector(greaterThan: "T"), limit: 5, mode: .iterator, snapshot: false, reverse: false).map {
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
			guard let transaction = self.transaction else { return XCTFail() }
			transaction.readSelectors(from: KeySelector(greaterThan: "Test Key 1"), to: KeySelector(greaterThan: "Test Key 4"), limit: nil, mode: .iterator, snapshot: false, reverse: true).map {
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
	
	func testClearCanClearKey() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			transaction.clear(key: "Test Key 1")
			connection.commit(transaction: transaction)
				.map { _ in
					connection.transaction { $0.read("Test Key 1") }.map {
						XCTAssertNil($0)
						}.catch(self)
					connection.transaction { $0.read("Test Key 2") }.map {
						XCTAssertNotNil($0)
						}.catch(self)
				}.catch(self)
		}
	}
	
	func testClearCanClearRange() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			transaction.clear(range: "Test Key 1" ..< "Test Key 3")
			connection.commit(transaction: transaction)
				.map { _ in
					connection.transaction { $0.read("Test Key 1") }.map {
						XCTAssertNil($0)
						}.catch(self)
					connection.transaction { $0.read("Test Key 2") }.map {
						XCTAssertNil($0)
						}.catch(self)
					connection.transaction { $0.read("Test Key 3") }.map {
						XCTAssertNotNil($0)
						}.catch(self)
				}
				.catch(self)
		}
	}
	
	func testAddReadConflictAddsReadConflict() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			_ = transaction.read("Test Key 1")
			transaction.store(key: "Test Key 4", value: "Test Value 4")
			transaction.addReadConflict(on: "Test Key 2" ..< "Test Key 3")
			connection.transaction { $0.store(key: "Test Key 2", value: "Conflict!") }
				.map { _ in
					connection.commit(transaction: transaction).map { XCTFail() }
						.recover { switch($0) {
						case let error as ClusterDatabaseConnection.FdbApiError:
							XCTAssertEqual(1020, error.errorCode)
						default: XCTFail("\($0)")
							}}
						.catch(self)
				}.catch(self)
		}
	}
	
	func testAddWriteConflictAddsWriteConflict() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			
			_ = transaction.read("Test Key 1")
			transaction.store(key: "A", value: "B")
			connection.transaction {
				$0.store(key: "C", value: "D")
				$0.addWriteConflict(on: "Test Key" ..< "Test Kez")
				}.map { _ in
					connection.commit(transaction: transaction).map { XCTFail() }
						.recover { switch($0) {
						case let error as ClusterDatabaseConnection.FdbApiError:
							XCTAssertEqual(1020, error.errorCode)
						default: XCTFail("\($0)")
							}}
				}.catch(self)
		}
	}
	
	func testGetReadVersionGetsReadVersion() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			transaction.getReadVersion().map {
				XCTAssertGreaterThan($0, 0)
				}.catch(self)
		}
	}
	
	func testSetReadVersionSetsReadVersion() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			transaction.setReadVersion(151)
			transaction.getReadVersion().map {
				XCTAssertEqual($0, 151)
				}.catch(self)
		}
	}
	
	func testGetCommittedVersionGetsVersion() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			transaction.store(key: "Test Key 5", value: "Test Value 5")
			connection.commit(transaction: transaction)
				.flatMap {
					transaction.getCommittedVersion().map {
						XCTAssertGreaterThan($0, 0)
					}
				}.catch(self)
		}
	}
	
	func testGetCommittedVersionWithUncommittedTransactionReturnsNegativeOne() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			transaction.store(key: "Test Key 5", value: "Test Value 5")
			
			transaction.getCommittedVersion().map {
				XCTAssertEqual($0, -1)
				}.catch(self)
		}
	}
	
	func testAttemptRetryWithTransactionNotCommittedErrorDoesNotThrowError() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			transaction.attemptRetry(error: ClusterDatabaseConnection.FdbApiError(1020)).catch(self)
		}
	}
	
	func testAttemptRetryWithNoMoreServersRethrowsError() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			transaction.attemptRetry(error: ClusterDatabaseConnection.FdbApiError(1008))
				.map { XCTFail() }
				.recover {
					switch($0) {
					case let error as ClusterDatabaseConnection.FdbApiError:
						XCTAssertEqual(error.errorCode, 1008)
					default:
						XCTFail("Unexpected error: \($0)")
					}
				}.catch(self)
		}
	}
	
	func testAttemptRetryWithNonApiErrorRethrowsError() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			
			transaction.attemptRetry(error: TestError.test)
				.map { XCTFail() }
				.recover {
					XCTAssertTrue($0 is TestError)
				}.catch(self)
		}
	}
	
	func testResetResetsTransaction() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			transaction.store(key: "Test Key 5", value: "Test Value 5")
			transaction.reset()
			transaction.store(key: "Test Key 6", value: "Test Value 6")
			connection.commit(transaction: transaction).flatMap {
				connection.transaction {
					$0.read("Test Key 5").map {
						XCTAssertNil($0)
						}.catch(self)
					$0.read("Test Key 6").map {
						XCTAssertEqual($0, "Test Value 6")
						}.catch(self)
				}
				}.catch(self)
		}
	}
	
	func testResetWithCommittedTransactionAllowsCommittingAgain() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			transaction.store(key: "Test Key 5", value: "Test Value 5")
			connection.commit(transaction: transaction)
				.map { _ in
					transaction.reset()
					transaction.store(key: "Test Key 6", value: "Test Value 6")
					connection.commit(transaction: transaction).flatMap {
						connection.transaction {
							$0.read("Test Key 5").map { XCTAssertEqual($0, "Test Value 5") }.catch(self)
							$0.read("Test Key 6").map { XCTAssertEqual($0, "Test Value 6") }.catch(self)
						}
						}.catch(self)
				}.catch(self)
		}
	}
	
	func testResetWithCancelledTransactionAllowsCommitting() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			transaction.store(key: "Test Key 5", value: "Test Value 5")
			transaction.cancel()
			transaction.reset()
			transaction.store(key: "Test Key 6", value: "Test Value 6")
			connection.commit(transaction: transaction).map { _ in
				connection.transaction {
					$0.read("Test Key 5").map { XCTAssertNil($0) }.catch(self)
					$0.read("Test Key 6").map { XCTAssertEqual($0, "Test Value 6") }.catch(self)
					}.catch(self)
				}.catch(self)
		}
	}
	
	func testCancelPreventsCommittingTransaction() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			transaction.store(key: "Test Key 5", value: "Test Value 5")
			transaction.cancel()
			connection.commit(transaction: transaction).map { XCTFail() }
				.recover {
					switch($0) {
					case let error as ClusterDatabaseConnection.FdbApiError:
						XCTAssertEqual(error.errorCode, 1025)
					default:
						XCTFail("Unexpected error: \($0)")
					}
				}.catch(self)
			connection.transaction {
				$0.read("Test Key 5").map { XCTAssertNil($0) }.catch(self)
				}.catch(self)
		}
	}
	
	func testPerformAtomicOperationWithBitwiseAndPerformsOperation() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			connection.transaction { $0.store(key: "Test Key", value: DatabaseValue(Data([0xC3]))) }.map { _ in
				transaction.performAtomicOperation(operation: .bitAnd, key: "Test Key", value: DatabaseValue(Data([0xA9])))
				connection.commit(transaction: transaction).map { _ in
					connection.transaction {
						$0.read("Test Key").map {
							XCTAssertEqual($0, DatabaseValue(Data([0x81])))
							}.catch(self)
						}.catch(self)
					}.catch(self)
				}.catch(self)
		}
	}
	
	func testGetVersionStampReturnsVersionStampAfterCommit() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			let future = transaction.getVersionStamp()
			transaction.store(key: "Test Key", value: "Test Value")
			connection.commit(transaction: transaction).map { _ in
				future.map { stamp in
					transaction.getCommittedVersion().map { version in
						var bytes: [UInt8] = [0x00, 0x00]
						var versionBytes = version
						for _ in 0 ..< 8 {
							bytes.insert(UInt8(versionBytes & 0xFF), at: 0)
							versionBytes = versionBytes >> 8
						}
						XCTAssertEqual(stamp.data, Data(bytes: bytes))
						}.catch(self)
					}.catch(self)
				}.catch(self)
		}
	}
	
	func testSetOptionWithNoWriteConflictOptionPreventsCausingWriteConflicts() throws {
		self.runLoop(eventLoop) {
			guard let transaction = self.transaction else { return XCTFail() }
			guard let connection = self.connection else { return XCTFail() }
			let transaction2 = connection.startTransaction()
			transaction.setOption(.nextWriteNoWriteConflictRange)
			transaction.store(key: "Test Key", value: "Test Value")
			_ = transaction2.read("Test Key")
			transaction2.store(key: "Test Key 2", value: "Test Value 2")
			connection.commit(transaction: transaction).catch(self)
			connection.commit(transaction: transaction2).catch(self)
		}
	}
}
