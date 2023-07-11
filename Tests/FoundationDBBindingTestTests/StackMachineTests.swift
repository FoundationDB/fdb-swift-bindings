/*
 * StackMachineTests.swift
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

@testable import FoundationDBBindingTest
import FoundationDB
import XCTest
import Foundation
import NIO

class StackMachineTests: XCTestCase {
	let eventLoop = EmbeddedEventLoop()
	var machine: StackMachine!
	var connection: DatabaseConnection!
	
	static var allTests: [(String, (StackMachineTests) -> () throws -> Void)] {
		return [
			("testExecutePushAddsItemToStack", testExecutePushAddsItemToStack),
			("testExecuteDupAddsItemToStack", testExecuteDupAddsItemToStack),
			("testExecuteDupWithEmptyStackThrowsError", testExecuteDupWithEmptyStackThrowsError),
			("testExecuteEmptyStackWipesStack", testExecuteEmptyStackWipesStack),
			("testExecuteSwapSwapsItemsOnStack", testExecuteSwapSwapsItemsOnStack),
			("testExecuteSwapWithEmptyStackThrowsError", testExecuteSwapWithEmptyStackThrowsError),
			("testExecuteSwapWithIndexBeyondBoundsThrowsError", testExecuteSwapWithIndexBeyondBoundsThrowsError),
			("testExecutePopRemovesTopItemFromStack", testExecutePopRemovesTopItemFromStack),
			("testExecuteSubtractWithPositiveDifferenceAddsDifferenceToStack", testExecuteSubtractWithPositiveDifferenceAddsDifferenceToStack),
			("testExecuteSubtractWithNegativeDifferenceAddsDifferenceToStack", testExecuteSubtractWithNegativeDifferenceAddsDifferenceToStack),
			("testExecuteSubtractWithStringInFirstPositionThrowsError", testExecuteSubtractWithStringInFirstPositionThrowsError),
			("testExecuteSubtractWithStringInSecondPositionThrowsError", testExecuteSubtractWithStringInSecondPositionThrowsError),
			("testExecuteConcatWithStringsConcatenatesString", testExecuteConcatWithStringsConcatenatesString),
			("testExecuteConcatWithDataItemsConcatenatesData", testExecuteConcatWithDataItemsConcatenatesData),
			("testExecuteConcatWithStringAndDataItemThrowsError", testExecuteConcatWithStringAndDataItemThrowsError),
			("testExecuteConcatWithIntegersThrowsError", testExecuteConcatWithIntegersThrowsError),
			("testExecuteLogAddsTuplesForStack", testExecuteLogAddsTuplesForStack),
			("testExecuteNewTransactionPutsTransactionInTransactionMap", testExecuteNewTransactionPutsTransactionInTransactionMap),
			("testExecuteUseTransactionSetsTransactionName", testExecuteUseTransactionSetsTransactionName),
			("testExecuteOnErrorWithUnretryableErrorPushesEmptyFutureOntoStack", testExecuteOnErrorWithUnretryableErrorPushesEmptyFutureOntoStack),
			("testExecuteOnErrorWithRetryableErrorPushesEmptyFutureOntoStack", testExecuteOnErrorWithRetryableErrorPushesEmptyFutureOntoStack),
			("testExecuteGetReadsValueFromDatabase", testExecuteGetReadsValueFromDatabase),
			("testExecuteGetWithMissingKeyPushesResultNotPresentToStack", testExecuteGetWithMissingKeyPushesResultNotPresentToStack),
			("testExecuteGetKeyPutsWithMatchingPrefixPutsKeyOnStack", testExecuteGetKeyPutsWithMatchingPrefixPutsKeyOnStack),
			("testExecuteGetKeyPutsWithEarlierPrefixPutsIncrementedPrefixOnStack", testExecuteGetKeyPutsWithEarlierPrefixPutsIncrementedPrefixOnStack),
			("testExecuteGetKeyPutsWithLaterPrefixPutsPrefixOnStack", testExecuteGetKeyPutsWithLaterPrefixPutsPrefixOnStack),
			("testExecuteGetKeyWithMissingKeyPutsEmptyDataOnStack", testExecuteGetKeyWithMissingKeyPutsEmptyDataOnStack),
			("testExecuteGetRangeReadsValuesFromDatabase", testExecuteGetRangeReadsValuesFromDatabase),
			("testExecuteGetRangeWithReverseFlagReadsValuesInReverse", testExecuteGetRangeWithReverseFlagReadsValuesInReverse),
			("testExecuteGetRangeReadsValuesWithLimitLimitsReturnSize", testExecuteGetRangeReadsValuesWithLimitLimitsReturnSize),
			("testExecuteGetRangeWithInvalidStreamingModeThrowsError", testExecuteGetRangeWithInvalidStreamingModeThrowsError),
			("testExecuteGetRangeStartingWithFindsItemsWithThatPrefix", testExecuteGetRangeStartingWithFindsItemsWithThatPrefix),
			("testExecuteGetRangeStartingWithWithInvalidStreamingModeThrowsError", testExecuteGetRangeStartingWithWithInvalidStreamingModeThrowsError),
			("testExecuteGetRangeSelectorsPushesMatchingKeysOntoStack", testExecuteGetRangeSelectorsPushesMatchingKeysOntoStack),
			("testExecuteGetReadVersionSetsLastSeenVersion", testExecuteGetReadVersionSetsLastSeenVersion),
			("testExecuteSetReadVersionSetsTransactionReadVersion", testExecuteSetReadVersionSetsTransactionReadVersion),
			("testExecuteGetVersionStampPutsVersionStampFutureOnStack", testExecuteGetVersionStampPutsVersionStampFutureOnStack),
			("testExecuteSetStoresValueInDatabase", testExecuteSetStoresValueInDatabase),
			("testExecuteClearClearsValueForKey", testExecuteClearClearsValueForKey),
			("testExecuteClearRangeClearsKeysInRange", testExecuteClearRangeClearsKeysInRange),
			("testExecuteClearRangeWithEmptyStackThrowsError", testExecuteClearRangeWithEmptyStackThrowsError),
			("testExecuteClearRangeWithPrefixClearsKeysWithPrefix", testExecuteClearRangeWithPrefixClearsKeysWithPrefix),
			("testExecuteClearRangeWithPrefixWithEmptyStackThrowsError", testExecuteClearRangeWithPrefixWithEmptyStackThrowsError),
			("testExecuteAtomicOperationExecutesOperation", testExecuteAtomicOperationExecutesOperation),
			("testExecuteReadConflictWithKeyAddsReadConflict", testExecuteReadConflictWithKeyAddsReadConflict),
			("testExecuteReadConflictWithKeyWithEmptyStackThrowsError", testExecuteReadConflictWithKeyWithEmptyStackThrowsError),
			("testExecuteReadConflictRangeAddsReadConflictRange", testExecuteReadConflictRangeAddsReadConflictRange),
			("testExecuteReadConflictRangeWithEmptyStackThrowsError", testExecuteReadConflictRangeWithEmptyStackThrowsError),
			("testExecuteWriteConflictWithKeyAddsReadConflict", testExecuteWriteConflictWithKeyAddsReadConflict),
			("testExecuteWriteConflictWithKeyWithEmptyStackThrowsError", testExecuteWriteConflictWithKeyWithEmptyStackThrowsError),
			("testExecuteWriteConflictRangeAddsWriteConflictRange", testExecuteWriteConflictRangeAddsWriteConflictRange),
			("testExecuteWriteConflictRangeWithEmptyStackThrowsError", testExecuteWriteConflictRangeWithEmptyStackThrowsError),
			("testExecuteDisableWriteConflictPreventsNextWriteConflict", testExecuteDisableWriteConflictPreventsNextWriteConflict),
			("testExecuteCommitCommitsTransaction", testExecuteCommitCommitsTransaction),
			("testExecuteCancelCancelsTransaction", testExecuteCancelCancelsTransaction),
			("testExecuteResetResetsTransaction", testExecuteResetResetsTransaction),
			("testExecuteGetCommittedVersionGetsVersionFromTransaction", testExecuteGetCommittedVersionGetsVersionFromTransaction),
			("testExecuteWaitForFutureWaitsForFutureToBeRead", testExecuteWaitForFutureWaitsForFutureToBeRead),
			("testExecuteWaitForFutureWithEmptyStackThrowsError", testExecuteWaitForFutureWithEmptyStackThrowsError),
			("testExecutePackCombinesEntriesFromStack", testExecutePackCombinesEntriesFromStack),
			("testExecutePackWithEmptyStackThrowsError", testExecutePackWithEmptyStackThrowsError),
			("testExecuteUnpackAddsEntriesToStack", testExecuteUnpackAddsEntriesToStack),
			("testExecuteUnpackWithEmptyStackThrowsError", testExecuteUnpackWithEmptyStackThrowsError),
			("testExecuteTupleRangePutsRangeEndpointsOnStack", testExecuteTupleRangePutsRangeEndpointsOnStack),
			("testExecuteTupleRangeWithEmptyStackThrowsError", testExecuteTupleRangeWithEmptyStackThrowsError),
			("testExecuteUnitTestsDoesNothing", testExecuteUnitTestsDoesNothing),
			("testExecuteThreadStartsNewMachineOperatingOnThread", testExecuteThreadStartsNewMachineOperatingOnThread),
			("testExecuteWaitEmptyWaitsForValueToBeSet", testExecuteWaitEmptyWaitsForValueToBeSet),
			("testExecuteEncodeFloatPutsFloatOnStack", testExecuteEncodeFloatPutsFloatOnStack),
			("testExecuteEncodeDoublePutsDoubleOnStack", testExecuteEncodeDoublePutsDoubleOnStack),
			("testExecuteDecodeFloatPutsBytesOnStack", testExecuteDecodeFloatPutsBytesOnStack),
			("testExecuteDecodeDoublePutsBytesOnStack", testExecuteDecodeDoublePutsBytesOnStack),
			("testExecuteTupleSortSortsTuples", testExecuteTupleSortSortsTuples),
		]
	}
	
	override func setUp() {
		super.setUp()
		connection = InMemoryDatabaseConnection(eventLoop: eventLoop)
		StackMachine.connection = connection
		machine = StackMachine(connection: connection, transactionName: "transaction")
		self.machine.commandCount = 1
		self.machine.push(value: "Item1")
		self.machine.commandCount = 2
		self.machine.push(value: "Item2")
		self.machine.commandCount = 3
	}
	
	private func hexify(_ data: Data) -> String {
		return data.map { String(format: "%02x", $0) }.joined(separator: "")
	}
	
	override func tearDown() {
	}
	
	func execute(command: Command) -> EventLoopFuture<Void> {
		do {
			return try self.machine.execute(command: command) ?? eventLoop.makeSucceededFuture(Void())
		}
		catch {
			return eventLoop.makeFailedFuture(error)
		}
	}
	
	@discardableResult
	func _testWithEmptyStack(_ operation: Command.Operation, file: String = #file, line: Int = #line) -> EventLoopFuture<Void> {
		self.machine.stack = []
		return self.machine.execute(operation: operation).map { _ in
			self.recordFailure(withDescription: "", inFile: file, atLine: line, expected: true)
			}.recover { error in
				switch(error) {
				case StackMachine.ExecutionError.PoppedEmptyStack: break
				default: self.recordFailure(withDescription: "Threw unexpected error: \(error)", inFile: file, atLine: line, expected: true)
				}
		}
	}
	
	func testExecutePushAddsItemToStack() throws {
		self.runLoop(eventLoop) {
			self.execute(command: Command(operation: .push, argument: "My Data")!).flatMap { _ -> EventLoopFuture<Void> in
				XCTAssertEqual(self.machine.stack.count, 3)
				guard self.machine.stack.count > 0 else {
					return self.eventLoop.makeSucceededFuture(Void())
				}
				XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
				return self.machine.stack.last!.value.map {
					XCTAssertEqual($0 as? String, "My Data")
				}
				}.catch(self)
		}
	}
	
	func testExecuteDupAddsItemToStack() throws {
		self.runLoop(eventLoop) {
			self.machine.execute(operation: .dup).flatMap { _ -> EventLoopFuture<Void> in
				XCTAssertEqual(self.machine.stack.count, 3)
				XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 2)
				return self.machine.stack.last!.value.map {
					XCTAssertEqual($0 as? String, "Item2")
					return Void()
				}
				}.catch(self)
		}
	}
	
	func testExecuteDupWithEmptyStackThrowsError() throws {
		self.runLoop(eventLoop) {
			self._testWithEmptyStack(.dup).catch(self)
		}
	}
	
	
	func testExecuteEmptyStackWipesStack() throws {
		self.runLoop(eventLoop) {
			self.machine.execute(operation: .empty).map { _ in
				XCTAssertEqual(self.machine.stack.count, 0)
				}.catch(self)
		}
	}
	
	func testExecuteSwapSwapsItemsOnStack() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: "Item3")
			self.machine.push(value: "Item4")
			self.machine.push(value: 2)
			self.machine.execute(operation: .swap).map { _ in
				XCTAssertEqual(self.machine.stack.count, 4)
				if self.machine.stack.count < 4 { return }
				self.machine.stack[0].value.map { XCTAssertEqual($0 as? String, "Item1") }.catch(self)
				self.machine.stack[1].value.map { XCTAssertEqual($0 as? String, "Item4") }.catch(self)
				self.machine.stack[2].value.map { XCTAssertEqual($0 as? String, "Item3") }.catch(self)
				self.machine.stack[3].value.map { XCTAssertEqual($0 as? String, "Item2") }.catch(self)
				XCTAssertEqual(self.machine.stack[1].metadata.commandNumber, 3)
				XCTAssertEqual(self.machine.stack[3].metadata.commandNumber, 2)
				}.catch(self)
		}
	}
	
	func testExecuteSwapWithEmptyStackThrowsError() throws {
		self.runLoop(eventLoop) {
			self._testWithEmptyStack(.swap).catch(self)
		}
	}
	
	func testExecuteSwapWithIndexBeyondBoundsThrowsError() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: 3)
			self.machine.execute(operation: .swap).map { _ in XCTFail() }.recover {
				error in
				switch(error) {
				case StackMachine.ExecutionError.SwappedBeyondBounds(index: 3, count: 2): break
				default: XCTFail("Threw unexpected error: \(error)")
				}
				}.catch(self)
		}
	}
	
	func testExecutePopRemovesTopItemFromStack() throws {
		self.runLoop(eventLoop) {
			self.machine.execute(operation: .pop).map { _ in
				XCTAssertEqual(self.machine.stack.count, 1)
				self.machine.stack[0].value.map { XCTAssertEqual($0 as? String, "Item1") }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteSubtractWithPositiveDifferenceAddsDifferenceToStack() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: 4)
			self.machine.push(value: 15)
			self.machine.execute(operation: .sub).map { _ in
				XCTAssertEqual(self.machine.stack.count, 3)
				XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
				self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Int, 11) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteSubtractWithNegativeDifferenceAddsDifferenceToStack() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: 314)
			self.machine.push(value: 15)
			self.machine.execute(operation: .sub).map { _ in
				XCTAssertEqual(self.machine.stack.count, 3)
				XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
				self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Int, -299) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteSubtractWithStringInFirstPositionThrowsError() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: 15)
			self.machine.push(value: "New Item")
			self.machine.execute(operation: .sub).map { _ in
				XCTAssertEqual(self.machine.stack.count, 3)
				XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
				_ = self.machine.stack.last!.value.map { _ in XCTFail() }
					.recover { error in
						switch(error) {
						case StackMachine.ExecutionError.IllegalValueType: break
						default: XCTFail("Threw unexpected error: \(error)")
						}
				}
				}.catch(self)
		}
	}
	
	func testExecuteSubtractWithStringInSecondPositionThrowsError() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: "New Item")
			self.machine.push(value: 15)
			self.machine.execute(operation: .sub).map { _ in
				XCTAssertEqual(self.machine.stack.count, 3)
				XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
				_ = self.machine.stack.last!.value.map { _ in XCTFail() }
					.recover { error in
						switch(error) {
						case StackMachine.ExecutionError.IllegalValueType: break
						default: XCTFail("Threw unexpected error: \(error)")
						}
				}
				}.catch(self)
		}
	}
	
	func testExecuteConcatWithStringsConcatenatesString() throws {
		self.runLoop(eventLoop) {
			self.machine.execute(operation: .concat).map { _ in
				XCTAssertEqual(self.machine.stack.count, 1)
				XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
				self.machine.stack.last!.value.map { XCTAssertEqual($0 as? String, "Item2Item1") }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteConcatWithDataItemsConcatenatesData() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: Data(bytes: [1,2,3,4]))
			self.machine.push(value: Data(bytes: [5,6,7,8]))
			self.machine.execute(operation: .concat).map { _ in
				XCTAssertEqual(self.machine.stack.count, 3)
				XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
				self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, Data(bytes: [5,6,7,8,1,2,3,4])) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteConcatWithStringAndDataItemThrowsError() {
		self.runLoop(eventLoop) {
			self.machine.push(value: Data(bytes: [1,2,3,4]))
			self.machine.push(value: "Hi")
			self.machine.execute(operation: .concat).map { _ in
				XCTAssertEqual(self.machine.stack.count, 3)
				XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
				_ = self.machine.stack.last!.value.map { _ in XCTFail() }.recover { error in
					switch(error) {
					case StackMachine.ExecutionError.IllegalValueType: break
					default: XCTFail("Threw unexpected error: \(error)")
					}
				}
				}.catch(self)
		}
	}
	
	func testExecuteConcatWithIntegersThrowsError() {
		self.runLoop(eventLoop) {
			self.machine.push(value: 1)
			self.machine.push(value: 2)
			self.machine.execute(operation: .concat).map { _ in
				_ = self.machine.stack.last!.value.map { _ in XCTFail() }.recover { error in
					switch(error) {
					case StackMachine.ExecutionError.IllegalValueType: break
					default: XCTFail("Threw unexpected error: \(error)")
					}
				}
				}.catch(self)
		}
	}
	
	func testExecuteLogAddsTuplesForStack() throws {
		self.runLoop(eventLoop) {
			let prefix = "bindingTestLogKeys".utf8.data
			self.machine.push(value: prefix)
			self.machine.execute(operation: .log).flatMap { _ -> EventLoopFuture<Void> in
				var end = prefix
				end.append(0xFF)
				XCTAssertEqual(self.machine.stack.count, 0)
				return self.connection.transaction {$0.read(range: DatabaseValue(prefix) ..< DatabaseValue(end))}.map {
					let rows = $0.rows
					XCTAssertEqual(rows.count, 2)
					if rows.count < 2 { return }
					
					var key1 = prefix
					key1.append(Tuple(0, 1).databaseValue.data)
					var key2 = prefix
					key2.append(Tuple(1, 2).databaseValue.data)
					XCTAssertEqual(rows[0].key, DatabaseValue(key1))
					XCTAssertEqual(rows[0].value, Tuple("Item1").databaseValue)
					XCTAssertEqual(rows[1].key, DatabaseValue(key2))
					XCTAssertEqual(rows[1].value, Tuple("Item2").databaseValue)
				}
				}.catch(self)
		}
	}
	
	func testExecuteNewTransactionPutsTransactionInTransactionMap() throws {
		self.runLoop(eventLoop) {
			self.machine.currentTransactionName = "newTransactionTest"
			self.machine.execute(operation: .newTransaction).map { _ in
				XCTAssertNotNil(self.machine.transactionMap[self.machine.currentTransactionName])
				XCTAssertEqual(self.machine.stack.count, 2)
				}.catch(self)
		}
	}
	
	func testExecuteUseTransactionSetsTransactionName() throws {
		self.runLoop(eventLoop) {
			self.machine.execute(operation: .useTransaction).map { _ in
				XCTAssertEqual(self.machine.currentTransactionName, "Item2")
				XCTAssertNotNil(self.machine.transactionMap["Item2"])
				
				XCTAssertEqual(self.machine.stack.count, 1)
				}.catch(self)
		}
	}
	
	func testExecuteOnErrorWithUnretryableErrorPushesEmptyFutureOntoStack() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: -1)
			self.machine.execute(operation: .onError).map { _ in
				XCTAssertEqual(self.machine.stack.count, 3)
				self.machine.popAndCast().map { (value: Data) in
					XCTAssertEqual(value, Tuple("ERROR".utf8.data, "-1".utf8.data).databaseValue.data)
					}.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteOnErrorWithRetryableErrorPushesEmptyFutureOntoStack() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: 1020)
			self.machine.execute(operation: .onError).map { _ in
				XCTAssertEqual(self.machine.stack.count, 3)
				self.machine.pop().map { XCTAssertEqual($0 as? Data, StackMachine.resultNotPresent ) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteGetReadsValueFromDatabase() throws {
		self.runLoop(eventLoop) {
			let key = DatabaseValue(string: "Test Key 1")
			self.connection.transaction { transaction -> Void in
				transaction.store(key: key, value: "Test Value 1")
				}.flatMap { _ -> EventLoopFuture<Void> in
					self.machine.push(value: key.data)
					return self.machine.execute(operation: .get)
				}.map { _ -> Void in
					XCTAssertEqual(self.machine.stack.count, 3)
					XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, "Test Value 1".utf8.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteGetWithMissingKeyPushesResultNotPresentToStack() throws {
		self.runLoop(eventLoop) {
			let key = DatabaseValue(string: "Test Key 1")
			self.machine.push(value: key.data)
			self.machine.execute(operation: .get).map { _ in
				XCTAssertEqual(self.machine.stack.count, 3)
				XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
				self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, StackMachine.resultNotPresent) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteGetKeyPutsWithMatchingPrefixPutsKeyOnStack() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction {
				$0.store(key: "Test Key 1", value: "Test Value 1")
				$0.store(key: "Test Key 2", value: "Test Value 2")
				$0.store(key: "Test Key 3", value: "Test Value 3")
				$0.store(key: "Test Key 4", value: "Test Value 4")
				}.map { _ in
					self.machine.push(value: "Test Key".utf8.data)
					self.machine.push(value: 2)
					self.machine.push(value: 1)
					self.machine.push(value: "Test Key 1".utf8.data)
				}.flatMap { _ in
					self.machine.execute(operation: .getKey)
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, "Test Key 3".utf8.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteGetKeyPutsWithEarlierPrefixPutsIncrementedPrefixOnStack() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction {
				$0.store(key: "Test Key 1", value: "Test Value 1")
				$0.store(key: "Test Key 2", value: "Test Value 2")
				$0.store(key: "Test Key 3", value: "Test Value 3")
				$0.store(key: "Test Key 4", value: "Test Value 4")
				}.map { _ in
					self.machine.push(value: "Test Key 1".utf8.data)
					self.machine.push(value: 2)
					self.machine.push(value: 1)
					self.machine.push(value: "Test Key 1".utf8.data)
				}.flatMap { _ in
					self.machine.execute(operation: .getKey)
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, "Test Key 2".utf8.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteGetKeyPutsWithLaterPrefixPutsPrefixOnStack() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction {
				$0.store(key: "Test Key 1", value: "Test Value 1")
				$0.store(key: "Test Key 2", value: "Test Value 2")
				$0.store(key: "Test Key 3", value: "Test Value 3")
				$0.store(key: "Test Key 4", value: "Test Value 4")
				}.map { _ in
					self.machine.push(value: "Test Key 5".utf8.data)
					self.machine.push(value: 2)
					self.machine.push(value: 1)
					self.machine.push(value: "Test Key 1".utf8.data)
				}.flatMap { _ in
					self.machine.execute(operation: .getKey)
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, "Test Key 5".utf8.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteGetKeyWithMissingKeyPutsEmptyDataOnStack() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction {
				$0.store(key: "Test Key 1", value: "Test Value 1")
				$0.store(key: "Test Key 2", value: "Test Value 2")
				$0.store(key: "Test Key 3", value: "Test Value 3")
				$0.store(key: "Test Key 4", value: "Test Value 4")
				}.map { _ in
					self.machine.push(value: "Test Key".utf8.data)
					self.machine.push(value: 2)
					self.machine.push(value: 1)
					self.machine.push(value: "Test Key 5".utf8.data)
				}.flatMap { _ in
					self.machine.execute(operation: .getKey)
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, Data()) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteGetRangeReadsValuesFromDatabase() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction {
				$0.store(key: "Test Key 1", value: "Test Value 1")
				$0.store(key: "Test Key 2", value: "Test Value 2")
				$0.store(key: "Test Key 3", value: "Test Value 3")
				$0.store(key: "Test Key 4", value: "Test Value 4")
				}.map { _ in
					self.machine.push(value: -1)
					self.machine.push(value: 0)
					self.machine.push(value: 0)
					self.machine.push(value: "Test Key 4".utf8.data)
					self.machine.push(value: "Test Key 2".utf8.data)
				}.flatMap { _ in
					self.machine.execute(operation: .getRange)
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					
					let result = Tuple(
						"Test Key 2".utf8.data,
						"Test Value 2".utf8.data,
						"Test Key 3".utf8.data,
						"Test Value 3".utf8.data
					)
					XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, result.databaseValue.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteGetRangeWithReverseFlagReadsValuesInReverse() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction {
				$0.store(key: "Test Key 1", value: "Test Value 1")
				$0.store(key: "Test Key 2", value: "Test Value 2")
				$0.store(key: "Test Key 3", value: "Test Value 3")
				$0.store(key: "Test Key 4", value: "Test Value 4")
				}.map { _ in
					self.machine.push(value: -1)
					self.machine.push(value: 1)
					self.machine.push(value: 0)
					self.machine.push(value: "Test Key 4".utf8.data)
					self.machine.push(value: "Test Key 2".utf8.data)
				}.flatMap { _ in
					self.machine.execute(operation: .getRange)
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					let result = Tuple(
						"Test Key 3".utf8.data,
						"Test Value 3".utf8.data,
						"Test Key 2".utf8.data,
						"Test Value 2".utf8.data
					)
					XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, result.databaseValue.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteGetRangeReadsValuesWithLimitLimitsReturnSize() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction {
				$0.store(key: "Test Key 1", value: "Test Value 1")
				$0.store(key: "Test Key 2", value: "Test Value 2")
				$0.store(key: "Test Key 3", value: "Test Value 3")
				$0.store(key: "Test Key 4", value: "Test Value 4")
				}.map { _ in
					self.machine.push(value: -1)
					self.machine.push(value: 0)
					self.machine.push(value: 3)
					self.machine.push(value: "Test Key 5".utf8.data)
					self.machine.push(value: "Test Key".utf8.data)
				}.flatMap { _ in
					self.machine.execute(operation: .getRange)
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					
					let result = Tuple(
						"Test Key 1".utf8.data,
						"Test Value 1".utf8.data,
						"Test Key 2".utf8.data,
						"Test Value 2".utf8.data,
						"Test Key 3".utf8.data,
						"Test Value 3".utf8.data
					)
					XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, result.databaseValue.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteGetRangeWithInvalidStreamingModeThrowsError() {
		self.runLoop(eventLoop) {
			self.machine.push(value: -5)
			self.machine.push(value: 0)
			self.machine.push(value: 0)
			self.machine.push(value: "Test Key 4".utf8.data)
			self.machine.push(value: "Test Key 2".utf8.data)
			_ = self.machine.execute(operation: .getRange).map {
				_ = self.machine.stack.last!.value.map { _ in XCTFail() }.recover {
					error in
					switch(error) {
					case StackMachine.ExecutionError.IllegalStreamingMode: break
					default:
						XCTFail("Got unexpected error: \(error)")
					}
				}
				}.map { _ in XCTFail() }.recover { error in
					switch(error) {
					case StackMachine.ExecutionError.IllegalStreamingMode: break
					default:
						XCTFail("Got unexpected error: \(error)")
					}
			}
		}
	}
	
	func testExecuteGetRangeStartingWithFindsItemsWithThatPrefix() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction {
				$0.store(key: "Test Key 1", value: "Test Value 1")
				$0.store(key: "Test Key 2", value: "Test Value 2")
				$0.store(key: "Test Key 3", value: "Test Value 3")
				$0.store(key: "Test Key 4", value: "Test Value 4")
				$0.store(key: "Test Keys", value: "Test Value 5")
				$0.store(key: "foo", value: "Test Value 6")
				}.map { _ in
					self.machine.push(value: Int(StreamingMode.iterator.rawValue))
					self.machine.push(value: 0)
					self.machine.push(value: 0)
					self.machine.push(value: "Test Key".utf8.data)
				}.flatMap { _ in
					self.machine.execute(operation: .getRangeStartingWith)
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					var result = Tuple(
						"Test Key 1".utf8.data,
						"Test Value 1".utf8.data,
						"Test Key 2".utf8.data,
						"Test Value 2".utf8.data,
						"Test Key 3".utf8.data,
						"Test Value 3".utf8.data
					)
					
					result.append("Test Key 4".utf8.data)
					result.append("Test Value 4".utf8.data)
					result.append("Test Keys".utf8.data)
					result.append("Test Value 5".utf8.data)
					XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
					
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, result.databaseValue.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteGetRangeStartingWithWithInvalidStreamingModeThrowsError() {
		self.runLoop(eventLoop) {
			self.machine.push(value: -5)
			self.machine.push(value: 0)
			self.machine.push(value: 0)
			self.machine.push(value: "Test Key".utf8.data)
			_ = self.machine.execute(operation: .getRangeStartingWith)
				.map { _ in XCTFail() }.recover { error in
					switch(error) {
					case StackMachine.ExecutionError.IllegalStreamingMode: break
					default:
						XCTFail("Got unexpected error: \(error)")
					}
			}
		}
	}
	
	func testExecuteGetRangeSelectorsPushesMatchingKeysOntoStack() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction {
				$0.store(key: "Test Key 1", value: "Test Value 1")
				$0.store(key: "Test Key 2", value: "Test Value 2")
				$0.store(key: "Test Key 3", value: "Test Value 3")
				$0.store(key: "Test Key 4", value: "Test Value 4")
				$0.store(key: "Test Keys", value: "Test Value 5")
				$0.store(key: "Foo", value: "Test Value 6")
				}.map { _ in
					self.machine.push(value: Data())
					self.machine.push(value: Int(StreamingMode.iterator.rawValue))
					self.machine.push(value: 0)
					self.machine.push(value: 0)
					self.machine.push(value: 1)
					self.machine.push(value: 0)
					self.machine.push(value: "Z".utf8.data)
					self.machine.push(value: 1)
					self.machine.push(value: 0)
					self.machine.push(value: "M".utf8.data)
				}.flatMap { _ in
					self.machine.execute(operation: .getRangeSelector)
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					var result = Tuple(
						"Test Key 1".utf8.data,
						"Test Value 1".utf8.data,
						"Test Key 2".utf8.data,
						"Test Value 2".utf8.data,
						"Test Key 3".utf8.data,
						"Test Value 3".utf8.data
					)
					result.append("Test Key 4".utf8.data)
					result.append("Test Value 4".utf8.data)
					result.append("Test Keys".utf8.data)
					result.append("Test Value 5".utf8.data)
					
					XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, result.databaseValue.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteGetReadVersionSetsLastSeenVersion() throws {
		self.runLoop(eventLoop) {
			self.machine.execute(operation: .getReadVersion).map { _ in
				self.machine.currentTransaction.getReadVersion().map {
					XCTAssertEqual(self.machine.lastSeenVersion, $0)
					}.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteSetReadVersionSetsTransactionReadVersion() throws {
		self.runLoop(eventLoop) {
			self.machine.lastSeenVersion = 27
			self.machine.execute(operation: .setReadVersion).map { _ in
				self.machine.currentTransaction.getReadVersion().map { XCTAssertEqual($0, 27) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteGetVersionStampPutsVersionStampFutureOnStack() throws {
		self.runLoop(eventLoop) {
			self.machine.execute(operation: .getVersionStamp).flatMap { _ -> EventLoopFuture<Void> in
				XCTAssertEqual(self.machine.stack.count, 3)
				
				return self.connection.commit(transaction: self.machine.currentTransaction).map { _ in
					self.machine.stack[2].value.map { XCTAssertNotNil($0 as? Data) }.catch(self)
				}
				}.catch(self)
		}
	}
	
	func testExecuteSetStoresValueInDatabase() throws {
		self.runLoop(eventLoop) {
			let key = DatabaseValue(string: "Test Key 1")
			self.machine.push(value: "Set Test Value".utf8.data)
			self.machine.push(value: key.data)
			self.machine.execute(operation: .set)
				.flatMap { _ -> EventLoopFuture<Void> in
					return self.connection.transaction { $0.read(key) }
						.map { XCTAssertNil($0) }
				}.flatMap { _ in
					self.connection.commit(transaction: self.machine.currentTransaction)
				}.map { _ in
					self.connection.transaction {$0.read(key)}
						.map { XCTAssertEqual($0, "Set Test Value") }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteClearClearsValueForKey() throws {
		self.runLoop(eventLoop) {
			let key = DatabaseValue(string: "Test Key 1")
			self.connection.transaction {
				$0.store(key: key, value: "Test Value")
				}.map { _ in
					self.machine.push(value: key.data)
				}.flatMap { _ in
					self.machine.execute(operation: .clear)
				}.map { _ in
					self.connection.transaction {$0.read(key)}.map { XCTAssertNil($0) }.catch(self)
				}.flatMap { _ in
					self.connection.commit(transaction: self.machine.currentTransaction)
				}.map { _ in
					self.connection.transaction {$0.read(key)}.map { XCTAssertNil($0) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteClearRangeClearsKeysInRange() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction {
				$0.store(key: "Test Key 1", value: "Test Value 1")
				$0.store(key: "Test Key 2", value: "Test Value 2")
				$0.store(key: "Test Key 3", value: "Test Value 3")
				$0.store(key: "Test Key 4", value: "Test Value 4")
				}.map { _ in
					self.machine.push(value: "Test Key 4".utf8.data)
					self.machine.push(value: "Test Key 2".utf8.data)
				}.flatMap { _ in
					self.machine.execute(operation: .clearRange)
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 2)
				}.flatMap { _ in
					self.connection.transaction {$0.read("Test Key 1")}.map { XCTAssertNotNil($0) }
				}.flatMap { _ in
					self.connection.commit(transaction: self.machine.currentTransaction)
				}.map { _ in
					self.connection.transaction {$0.read("Test Key 1")}.map { XCTAssertNotNil($0) }.catch(self)
					self.connection.transaction {$0.read("Test Key 2")}.map { XCTAssertNil($0) }.catch(self)
					self.connection.transaction {$0.read("Test Key 3")}.map { XCTAssertNil($0) }.catch(self)
					self.connection.transaction {$0.read("Test Key 4")}.map { XCTAssertNotNil($0) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteClearRangeWithEmptyStackThrowsError() {
		self.runLoop(eventLoop) {
			self._testWithEmptyStack(.clearRange)
		}
	}
	
	func testExecuteClearRangeWithPrefixClearsKeysWithPrefix() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction {
				$0.store(key: "Test Key 1", value: "Test Value 1")
				$0.store(key: "Test Key 2", value: "Test Value 2")
				$0.store(key: "Test Key 3", value: "Test Value 3")
				$0.store(key: "Test Key 4", value: "Test Value 4")
				$0.store(key: "Test Keys", value: "Test Value 5")
				}.map { _ in
					self.machine.push(value: "Test Key ".utf8.data)
				}.flatMap { _ in
					self.machine.execute(operation: .clearRangeStartingWith)
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 2)
				}.flatMap { _ in
					self.connection.transaction {$0.read("Test Key 2")}.map { XCTAssertNotNil($0) }
				}.flatMap { _ in
					self.connection.commit(transaction: self.machine.currentTransaction)
				}.map { _ in
					self.connection.transaction {$0.read("Test Key 1")}.map { XCTAssertNil($0) }.catch(self)
					self.connection.transaction {$0.read("Test Key 2")}.map { XCTAssertNil($0) }.catch(self)
					self.connection.transaction {$0.read("Test Key 3")}.map { XCTAssertNil($0) }.catch(self)
					self.connection.transaction {$0.read("Test Key 4")}.map { XCTAssertNil($0) }.catch(self)
					self.connection.transaction {$0.read("Test Keys")}.map { XCTAssertNotNil($0) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteClearRangeWithPrefixWithEmptyStackThrowsError() {
		self.runLoop(eventLoop) {
			self._testWithEmptyStack(.clearRangeStartingWith)
		}
	}
	
	func testExecuteAtomicOperationExecutesOperation() throws {
		self.runLoop(eventLoop) {
			self.connection.transaction { $0.store(key: "Test Key", value: DatabaseValue(Data(bytes: [0xAA]))) }
				.map { _ in
					self.machine.push(value: Data(bytes: [0x93]))
					self.machine.push(value: "Test Key".utf8.data)
					self.machine.push(value: "BIT_AND")
				}.flatMap { _ in
					self.machine.execute(operation: .atomicOperation)
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 2)
					self.connection.transaction { $0.read("Test Key") }
						.map { XCTAssertEqual($0, DatabaseValue(Data(bytes: [0x82]))) }.catch(self)
				}.catch(self)
			
		}
	}
	
	func testExecuteReadConflictWithKeyAddsReadConflict() throws {
		self.runLoop(eventLoop) {
			let key1 = DatabaseValue(string: "Test Key 1")
			self.machine.push(value: key1.data)
			self.machine.execute(operation: .addReadConflictOnKey).flatMap { _ in
				self.connection.transaction {
					$0.store(key: key1, value: "TestValue")
				}
				}.map { _ in
					self.machine.currentTransaction.store(key: "Test Key 3", value: "TestValue3")
				}
				.map { _ in
					_ = self.connection.commit(transaction: self.machine.currentTransaction).map { _ in XCTFail() }
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, "SET_CONFLICT_KEY".utf8.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteReadConflictWithKeyWithEmptyStackThrowsError() throws {
		self.runLoop(eventLoop) {
			self._testWithEmptyStack(.addReadConflictOnKey)
		}
	}
	
	func testExecuteReadConflictRangeAddsReadConflictRange() throws {
		self.runLoop(eventLoop) {
			let key1 = DatabaseValue(string: "Test Key 1")
			let key2 = DatabaseValue(string: "Test Key 2")
			self.machine.push(value: key2.data)
			self.machine.push(value: key1.data)
			
			self.machine.execute(operation: .addReadConflictOnRange).flatMap { _ in
				self.connection.transaction {
					$0.store(key: key1, value: "TestValue")
				}
				}.map { _ in
					self.machine.currentTransaction.store(key: "Test Key 3", value: "TestValue3")
					_ = self.connection.commit(transaction: self.machine.currentTransaction).map { _ in XCTFail() }.recover { _ in }
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, "SET_CONFLICT_RANGE".utf8.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteReadConflictRangeWithEmptyStackThrowsError() throws {
		self.runLoop(eventLoop) {
			self._testWithEmptyStack(.addReadConflictOnRange)
		}
	}
	
	func testExecuteWriteConflictWithKeyAddsReadConflict() throws {
		self.runLoop(eventLoop) {
			let key1 = DatabaseValue(string: "Test Key 1")
			self.machine.push(value: key1.data)
			
			self.machine.execute(operation: .addWriteConflictOnKey).flatMap { _ -> EventLoopFuture<Void> in
				self.machine.currentTransaction.store(key: "Test Key 2", value: "Test Value 2")
				let transaction2 = self.connection.startTransaction()
				return transaction2.read(key1).map { _ in
					transaction2.store(key: "Write Key", value: "TestValue")
					}.flatMap { _ in
						self.connection.commit(transaction: self.machine.currentTransaction)
					}.map { _ in
						_ = self.connection.commit(transaction: transaction2).map { _ in XCTFail() }.recover { _ in }
				}
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, "SET_CONFLICT_KEY".utf8.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteWriteConflictWithKeyWithEmptyStackThrowsError() throws {
		self.runLoop(eventLoop) {
			self._testWithEmptyStack(.addWriteConflictOnKey)
		}
	}
	
	func testExecuteWriteConflictRangeAddsWriteConflictRange() throws {
		self.runLoop(eventLoop) {
			let key1 = DatabaseValue(string: "Test Key 1")
			self.machine.push(value: "Test Key 2".utf8.data)
			self.machine.push(value: key1.data)
			
			self.machine.execute(operation: .addWriteConflictOnRange).flatMap { _ -> EventLoopFuture<Void> in
				self.machine.currentTransaction.store(key: "Test Key 3", value: "Test Value 3")
				let transaction2 = self.connection.startTransaction()
				return transaction2.read(key1).flatMap { _ -> EventLoopFuture<Void> in
					transaction2.store(key: "Write Key", value: "TestValue")
					return self.connection.commit(transaction: self.machine.currentTransaction)
					}.flatMap { _ in
						self.connection.commit(transaction: transaction2).map { _ in XCTFail() }.recover { _ in }
				}
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, "SET_CONFLICT_RANGE".utf8.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteWriteConflictRangeWithEmptyStackThrowsError() throws {
		self.runLoop(eventLoop) {
			self._testWithEmptyStack(.addWriteConflictOnRange)
		}
	}
	
	func testExecuteDisableWriteConflictPreventsNextWriteConflict() throws {
		self.runLoop(eventLoop) {
			self.machine.execute(operation: .disableWriteConflict).map { _ in
				let transaction2 = self.connection.startTransaction()
				transaction2.read("Test Key 1").map { _ in
					transaction2.store(key: "Test Key 2", value: "Test Value 2")
					self.machine.currentTransaction.store(key: "Test Key 1", value: "Test Value 1")
					}.flatMap { _ in
						self.connection.commit(transaction: self.machine.currentTransaction)
					}.flatMap { _ in
						self.connection.commit(transaction: transaction2)
					}.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteCommitCommitsTransaction() throws {
		self.runLoop(eventLoop) {
			let key: DatabaseValue = "Test Key"
			self.machine.currentTransaction.store(key: key, value: "Test Value")
			self.machine.execute(operation: .commit).flatMap { _ in
				self.connection.transaction {$0.read(key)}.map { XCTAssertNotNil($0) }
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.stack.last!.value.map { XCTAssertEqual($0 as? Data, "RESULT_NOT_PRESENT".utf8.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteCancelCancelsTransaction() throws {
		self.runLoop(eventLoop) {
			self.machine.execute(operation: .cancel).map { _ in
				_ = self.connection.commit(transaction: self.machine.currentTransaction).map { _ in XCTFail() }
				XCTAssertEqual(self.machine.stack.count, 2)
				}.catch(self)
		}
	}
	
	func testExecuteResetResetsTransaction() throws {
		self.runLoop(eventLoop) {
			self.machine.currentTransaction.store(key: "Test Key 1", value: "Test Value 1")
			self.machine.execute(operation: .reset).map { _ in
				self.machine.currentTransaction.store(key: "Test Key 2", value: "Test Value 2")
				}.flatMap { _ in
					self.connection.commit(transaction: self.machine.currentTransaction)
				}.flatMap { _ in
					self.connection.transaction { (transaction: Transaction) in
						transaction.read("Test Key 1").map { XCTAssertNil($0) }.catch(self)
						transaction.read("Test Key 2").map { XCTAssertEqual($0, "Test Value 2") }.catch(self)
					}
				}.map { _ in
					XCTAssertEqual(self.machine.stack.count, 2)
				}.catch(self)
		}
	}
	
	func testExecuteGetCommittedVersionGetsVersionFromTransaction() throws {
		self.runLoop(eventLoop) {
			self.machine.currentTransaction.store(key: "Test Key", value: "Test Value")
			self.connection.commit(transaction: self.machine.currentTransaction)
				.flatMap { _ in
					self.machine.execute(operation: .getCommittedVersion)
				}.map { _ in
					self.machine.currentTransaction.getCommittedVersion().map { XCTAssertEqual($0, self.machine.lastSeenVersion) }.catch(self)
					self.machine.pop().map { XCTAssertEqual($0 as? Data, "GOT_COMMITTED_VERSION".utf8.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteWaitForFutureWaitsForFutureToBeRead() throws {
		self.runLoop(eventLoop) {
			let future = self.eventLoop.submit {
				return "Test Value" as Any
			}
			self.machine.stack.append(StackMachine.Item(
				value: future,
				metadata: .init(commandNumber: 2)
			))
			
			self.machine.execute(operation: .waitFuture).flatMapThrowing { _ in
				let future = self.machine.stack.last!.value
				XCTAssertEqual(self.machine.stack.count, 3)
				XCTAssertEqual(try future.wait() as? String, "Test Value")
				XCTAssertEqual(self.machine.stack.last?.metadata.commandNumber, 2)
				}.catch(self)
		}
	}
	
	func testExecuteWaitForFutureWithEmptyStackThrowsError() throws {
		self.runLoop(eventLoop) {
			self._testWithEmptyStack(.waitFuture)
		}
	}
	
	func testExecutePackCombinesEntriesFromStack() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: "Item3")
			self.machine.push(value: "Item4")
			self.machine.push(value: 3)
			self.machine.execute(operation: .tuplePack)
				.map { _ in
					XCTAssertEqual(self.machine.stack.count, 2)
					XCTAssertEqual(self.machine.stack[1].metadata.commandNumber, 3)
					self.machine.stack[1].value.map { XCTAssertEqual($0 as? Data, Tuple("Item4", "Item3", "Item2").databaseValue.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecutePackWithEmptyStackThrowsError() throws {
		self.runLoop(eventLoop) {
			self._testWithEmptyStack(.tuplePack)
		}
	}
	
	func testExecuteUnpackAddsEntriesToStack() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: Tuple("Item3", "Item4"))
			self.machine.execute(operation: .tupleUnpack)
				.map { _ in
					XCTAssertEqual(self.machine.stack.count, 4)
					if self.machine.stack.count < 4 { return }
					
					self.machine.stack[2].value.map { XCTAssertEqual($0 as? Data, Tuple("Item3").databaseValue.data) }.catch(self)
					XCTAssertEqual(self.machine.stack[2].metadata.commandNumber, 3)
					self.machine.stack[3].value.map { XCTAssertEqual($0 as? Data, Tuple("Item4").databaseValue.data) }.catch(self)
					XCTAssertEqual(self.machine.stack[3].metadata.commandNumber, 3)
				}.catch(self)
		}
	}
	
	func testExecuteUnpackWithEmptyStackThrowsError() throws {
		self.runLoop(eventLoop) {
			self._testWithEmptyStack(.tupleUnpack)
		}
	}
	
	func testExecuteTupleRangePutsRangeEndpointsOnStack() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: "Bar")
			self.machine.push(value: "Baz")
			self.machine.push(value: "Foo")
			self.machine.push(value: 3)
			let range = Tuple("Foo", "Baz", "Bar").childRange
			self.machine.execute(operation: .tupleRange)
				.map { _ in
					XCTAssertEqual(self.machine.stack.count, 4)
					self.machine.stack[2].value.map { XCTAssertEqual($0 as? Data, range.lowerBound.databaseValue.data) }.catch(self)
					self.machine.stack[3].value.map { XCTAssertEqual($0 as? Data, range.upperBound.databaseValue.data) }.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteTupleRangeWithEmptyStackThrowsError() {
		self.runLoop(eventLoop) {
			self._testWithEmptyStack(.tupleRange)
		}
	}
	
	
	func testExecuteUnitTestsDoesNothing() throws {
		self.runLoop(eventLoop) {
			self.machine.execute(operation: .unitTests)
				.map { _ in
					XCTAssertEqual(self.machine.stack.count, 2)
				}.catch(self)
		}
	}
	
	func testExecuteThreadStartsNewMachineOperatingOnThread() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: "New Command Prefix".utf8.data)
			self.connection.transaction {
				$0.store(key: Tuple("New Command Prefix".utf8.data, 1), value: Tuple("PUSH", "Thread Value 1".utf8.data))
				$0.store(key: Tuple("New Command Prefix".utf8.data, 2), value: Tuple("PUSH", "Thread Key 1".utf8.data))
				$0.store(key: Tuple("New Command Prefix".utf8.data, 3), value: Tuple("PUSH", "Thread Value 2".utf8.data))
				$0.store(key: Tuple("New Command Prefix".utf8.data, 4), value: Tuple("PUSH", "Thread Key 2".utf8.data))
				$0.store(key: Tuple("New Command Prefix".utf8.data, 5), value: Tuple("SET"))
				$0.store(key: Tuple("New Command Prefix".utf8.data, 6), value: Tuple("SET"))
				$0.store(key: Tuple("New Command Prefix".utf8.data, 7), value: Tuple("COMMIT"))
				}.flatMap { _ in
					self.machine.execute(operation: .startThread)
				}.flatMap { _ in
					self.connection.transaction {
						$0.read("Thread Key 2").flatMapThrowing { value in
							if value == nil {
								throw ClusterDatabaseConnection.FdbApiError(1020)
							}
							XCTAssertEqual(value, "Thread Value 2")
						}
					}
				}.catch(self)
		}
	}
	
	func testExecuteWaitEmptyWaitsForValueToBeSet() throws {
		self.runLoop(eventLoop) {
			self.machine.push(value: "Wait Empty Test".utf8.data)
			let longTransaction = self.connection.transaction { tr in
				self.connection.eventLoop.submit {
					tr.store(key: "Wait Empty Test 2", value: "Test Value")
				}
			}
			self.machine.execute(operation: .waitEmpty)
				.flatMap { _ in
					self.connection.transaction {
						return $0.read("Wait Empty Test 2").map { XCTAssertNotNil($0) }
					}
				}
				.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.pop().map { XCTAssertEqual($0 as? Data, "WAITED_FOR_EMPTY".utf8.data) }.catch(self)
				}.flatMap { _ in
					longTransaction
				}
				.catch(self)
		}
	}
	
	func testExecuteEncodeFloatPutsFloatOnStack() {
		self.runLoop(eventLoop) {
			self.machine.push(value: Data(bytes: [
				0x42, 0xC3, 0x28, 0xF6
			]))
			self.machine.execute(operation: .encodeFloat)
				.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.pop().map {
						XCTAssertEqual($0 as! Float32, 97.58, accuracy: 0.01)
						}.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteEncodeDoublePutsDoubleOnStack() {
		self.runLoop(eventLoop) {
			self.machine.push(value: Data(bytes: [
				0x40, 0x58, 0x65, 0x1E, 0xB8, 0x51, 0xEB, 0x85
			]))
			self.machine.execute(operation: .encodeDouble)
				.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.pop().map {
						XCTAssertEqual($0 as! Float64, 97.58, accuracy: 0.01)
					}.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteDecodeFloatPutsBytesOnStack() {
		self.runLoop(eventLoop) {
			self.machine.push(value: 18.19 as Float32)
			self.machine.execute(operation: .decodeFloat)
				.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.pop().map {
						XCTAssertEqual(self.hexify($0 as! Data), "4191851f")
						}.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteDecodeDoublePutsBytesOnStack() {
		self.runLoop(eventLoop) {
			self.machine.push(value: 18.19)
			self.machine.execute(operation: .decodeDouble)
				.map { _ in
					XCTAssertEqual(self.machine.stack.count, 3)
					self.machine.pop().map {
						XCTAssertEqual(self.hexify($0 as! Data), "403230a3d70a3d71")
					}.catch(self)
				}.catch(self)
		}
	}
	
	func testExecuteTupleSortSortsTuples() {
		self.runLoop(eventLoop) {
			self.machine.push(value: Tuple(1, 2).databaseValue.data)
			self.machine.push(value: Tuple(1, 3).databaseValue.data)
			self.machine.push(value: Tuple(4, 3).databaseValue.data)
			self.machine.push(value: Tuple(1, 2, 3).databaseValue.data)
			self.machine.push(value: 4)
			self.machine.execute(operation: .tupleSort)
				.map { _ in
					XCTAssertEqual(self.machine.stack.count, 6)
					self.machine.popTuple().map {
						(data4: Data, data3: Data, data2: Data, data1: Data) in
						XCTAssertEqual(Tuple(databaseValue: DatabaseValue(data4)), Tuple(4, 3))
						XCTAssertEqual(Tuple(databaseValue: DatabaseValue(data3)), Tuple(1, 3))
						
						XCTAssertEqual(Tuple(databaseValue: DatabaseValue(data2)), Tuple(1, 2, 3))
						
						XCTAssertEqual(Tuple(databaseValue: DatabaseValue(data1)), Tuple(1, 2))
						}.catch(self)
				}.catch(self)
		}
	}
}
