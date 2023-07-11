/*
 * InMemoryDatabaseConnectionTests.swift
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
import NIO
import CFoundationDB
@testable import FoundationDB

class InMemoryDatabaseConnectionTests: XCTestCase {
	let eventLoop = EmbeddedEventLoop()
	var connection: InMemoryDatabaseConnection!
	
	static var allTests : [(String, (InMemoryDatabaseConnectionTests) -> () throws -> Void)] {
		return [
			("testSubscriptAllowsGettingAndSettingValues", testSubscriptAllowsGettingAndSettingValues),
			("testKeysReturnsKeysInRange", testKeysReturnsKeysInRange),
			("testStartTransactionCreatesInMemoryTransaction", testStartTransactionCreatesInMemoryTransaction),
			("testCommitTransactionCommitsChanges", testCommitTransactionCommitsChanges),
			("testCommitTransactionWithWrongTransactionTypeThrowsError", testCommitTransactionWithWrongTransactionTypeThrowsError),
			("testCommitTransactionWithPreviouslyCommittedTransactionThrowsError", testCommitTransactionWithPreviouslyCommittedTransactionThrowsError),
			("testCommitWithReadConflictThrowsError", testCommitWithReadConflictThrowsError),
			("testCommitWithPotentialReadConflictAcceptsTransactionWithoutOverlap", testCommitWithPotentialReadConflictAcceptsTransactionWithoutOverlap),
			("testCommitWithCancelledTransactionThrowsError", testCommitWithCancelledTransactionThrowsError),
		]
	}
	
	override func setUp() {
		super.setUp()
		connection = InMemoryDatabaseConnection(eventLoop: eventLoop)
	}
	
	override func tearDown() {
	}
	
	func testSubscriptAllowsGettingAndSettingValues() {
		connection["Test Key 1"] = "Test Value 1"
		connection["Test Key 2"] = "Test Value 2"
		XCTAssertEqual(connection["Test Key 1"], "Test Value 1")
		XCTAssertEqual(connection["Test Key 2"], "Test Value 2")
	}
	
	func testKeysReturnsKeysInRange() {
		connection["Test Key 1"] = "Test Value 1"
		connection["Test Key 2"] = "Test Value 2"
		connection["Test Key 3"] = "Test Value 3"
		connection["Test Key 4"] = "Test Value 4"
		let keys = connection.keys(from: "Test Key 1", to: "Test Key 3")
		XCTAssertEqual(keys, ["Test Key 1", "Test Key 2"])
	}
	
	func testStartTransactionCreatesInMemoryTransaction() {
		let transaction = connection.startTransaction() as? InMemoryTransaction
		XCTAssertNotNil(transaction)
		XCTAssertEqual(transaction?.readVersion, connection.currentVersion)
	}
	
	func testCommitTransactionCommitsChanges() throws {
		self.runLoop(eventLoop) {
			let transaction1 = self.connection.startTransaction()
			transaction1.store(key: "Test Key", value: "Test Value")
			self.connection.commit(transaction: transaction1).map { () -> Void in
				let transaction2 = self.connection.startTransaction()
				transaction2.read("Test Key").map { XCTAssertEqual($0, "Test Value") }.catch(self)
				XCTAssertEqual(self.connection.currentVersion, 1)
				}.catch(self)
		}
	}
	
	func testCommitTransactionWithWrongTransactionTypeThrowsError() throws {
		setFdbApiVersion(FDB_API_VERSION)
		self.runLoop(eventLoop) {
			let connection2 = try! ClusterDatabaseConnection(eventLoop: self.eventLoop)
			let transaction2 = connection2.startTransaction()
			transaction2.store(key: "Test Key", value: "Test Value")
			self.connection.commit(transaction: transaction2).map { XCTFail() }
				.recover {
					switch($0) {
					case let error as ClusterDatabaseConnection.FdbApiError:
						XCTAssertEqual(error.errorCode, 1000)
					default:
						XCTFail("Unexpected error: \($0)")
					}
				}.catch(self)
		}
	}
	
	func testCommitTransactionWithPreviouslyCommittedTransactionThrowsError() throws {
		self.runLoop(eventLoop) {
			let transaction = self.connection.startTransaction()
			transaction.store(key: "Test Key", value: "Test Value")
			self.connection.commit(transaction: transaction).flatMap {
				self.connection.commit(transaction: transaction).map { XCTFail() }
					.recover {
						switch($0) {
						case let error as ClusterDatabaseConnection.FdbApiError:
							XCTAssertEqual(error.errorCode, 2017)
						default:
							XCTFail("Unexpected error: \($0)")
						}
				}
				}.catch(self)
		}
	}
	
	func testCommitWithReadConflictThrowsError() throws {
		self.runLoop(eventLoop) {
			let transaction1 = self.connection.startTransaction()
			transaction1.read("Test Key 1").flatMap { _ -> EventLoopFuture<()> in
				transaction1.store(key: "Test Key 2", value: "Test Value 2")
				let transaction2 = self.connection.startTransaction()
				transaction2.store(key: "Test Key 1", value: "Test Value 1")
				return self.connection.commit(transaction: transaction2).flatMap {
					_ = self.connection.commit(transaction: transaction1).map { _ in XCTFail() }
						.recover {
							switch($0) {
							case let error as ClusterDatabaseConnection.FdbApiError:
								XCTAssertEqual(error.errorCode, 1020)
							default:
								XCTFail("Unexpected error: \($0)")
							}
					}
					
					let transaction3 = self.connection.startTransaction()
					return transaction3.read("Test Key 2").map { XCTAssertNil($0) }
				}
				}.catch(self)
		}
	}
	
	func testCommitWithPotentialReadConflictAcceptsTransactionWithoutOverlap() throws {
		self.runLoop(eventLoop) {
			let transaction1 = self.connection.startTransaction()
			transaction1.store(key: "Test Key 1", value: "Test Value 1")
			self.connection.commit(transaction: transaction1).flatMap { _ -> EventLoopFuture<()> in
				let transaction2 = self.connection.startTransaction()
				_ = transaction2.read("Test Key 1")
				transaction2.store(key: "Test Key 2", value: "Test Value 2")
				let transaction3 = self.connection.startTransaction()
				transaction3.store(key: "Test Key 3", value: "Test Value 3")
				return self.connection.commit(transaction: transaction3).flatMap {
					self.connection.commit(transaction: transaction2).map {
						let transaction4 = self.connection.startTransaction()
						transaction4.read("Test Key 1").map { XCTAssertEqual($0, "Test Value 1") }.catch(self)
						transaction4.read("Test Key 2").map { XCTAssertEqual($0, "Test Value 2") }.catch(self)
						transaction4.read("Test Key 3").map { XCTAssertEqual($0, "Test Value 3") }.catch(self)
					}
				}
				}.catch(self)
		}
	}
	
	func testCommitWithCancelledTransactionThrowsError() throws {
		self.runLoop(eventLoop) {
			let transaction = self.connection.startTransaction()
			transaction.store(key: "Test Key 1", value: "Test Value 1")
			transaction.cancel()
			
			self.connection.commit(transaction: transaction).map { XCTFail() }.recover {
				switch($0) {
				case let error as ClusterDatabaseConnection.FdbApiError:
					XCTAssertEqual(error.errorCode, 1025)
				default:
					XCTFail("Unexpected error: \($0)")
				}
				}.catch(self)
		}
	}
}
