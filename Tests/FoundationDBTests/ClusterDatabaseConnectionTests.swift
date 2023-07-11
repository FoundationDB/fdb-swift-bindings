/*
 * ClusterDatabaseConnectionTests.swift
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
import NIO

class ClusterDatabaseConnectionTests: XCTestCase {
	let eventLoop = EmbeddedEventLoop()
	var connection: ClusterDatabaseConnection? = nil
	
	static var allTests : [(String, (ClusterDatabaseConnectionTests) -> () throws -> Void)] {
		return [
			("testStartTransactionCreatesClusterTransaction", testStartTransactionCreatesClusterTransaction),
			("testCommitTransactionCommitsChanges", testCommitTransactionCommitsChanges),
			("testCommitTransactionWithWrongTransactionTypeThrowsError", testCommitTransactionWithWrongTransactionTypeThrowsError),
			("testCommitTransactionWithPreviouslyCommittedTransactionThrowsError", testCommitTransactionWithPreviouslyCommittedTransactionThrowsError),
			("testCommitWithReadConflictThrowsError", testCommitWithReadConflictThrowsError),
			("testCommitWithPotentialReadConflictAcceptsTransactionWithoutOverlap", testCommitWithPotentialReadConflictAcceptsTransactionWithoutOverlap),
		]
	}
	
	override func setUp() {
		super.setUp()
		setFdbApiVersion(FDB_API_VERSION)
		
		if connection == nil {
			do {
				connection = try ClusterDatabaseConnection(eventLoop: eventLoop)
			}
			catch let error {
				print("Error creating database connection for testing: \(error)")
			}
		}
		
		self.runLoop(eventLoop) {
			_ = self.connection?.transaction {
				$0.clear(range: Tuple().childRange)
			}
		}
	}
	
	func testStartTransactionCreatesClusterTransaction() throws {
		self.runLoop(eventLoop) {
			guard let connection = self.connection else { return XCTFail() }
			
			let transaction = connection.startTransaction()
			XCTAssertTrue(transaction is ClusterTransaction)
			connection.commit(transaction: transaction).recover { XCTFail("\($0)") }.catch(self)
		}
	}
	
	func testCommitTransactionCommitsChanges() throws {
		self.runLoop(eventLoop) {
			guard let connection = self.connection else { return XCTFail() }
			let transaction1 = connection.startTransaction()
			transaction1.store(key: "Test Key", value: "Test Value")
			connection.commit(transaction: transaction1).map { _ in
				let transaction2 = connection.startTransaction()
				transaction2.read("Test Key").map {
					XCTAssertEqual($0, "Test Value")
					}.catch(self)
				}.catch(self)
		}
	}
	
	func testCommitTransactionWithWrongTransactionTypeThrowsError() throws {
		self.runLoop(eventLoop) {
			guard let connection = self.connection else { return XCTFail() }
			let connection2 = InMemoryDatabaseConnection(eventLoop: self.eventLoop)
			let transaction2 = connection2.startTransaction()
			transaction2.store(key: "Test Key", value: "Test Value")
			connection.commit(transaction: transaction2).map { _ in XCTFail() }
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
			guard let connection = self.connection else { return XCTFail() }
			let transaction = connection.startTransaction()
			transaction.store(key: "Test Key", value: "Test Value")
			connection.commit(transaction: transaction).flatMap {
				connection.commit(transaction: transaction).map { XCTFail() }
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
			guard let connection = self.connection else { return XCTFail() }
			let transaction1 = connection.startTransaction()
			transaction1.read("Test Key 1").flatMap { _ -> EventLoopFuture<Void> in
				transaction1.store(key: "Test Key 2", value: "Test Value 2")
				let transaction2 = connection.startTransaction()
				transaction2.store(key: "Test Key 1", value: "Test Value 1")
				return connection.commit(transaction: transaction2)
					.flatMap { _ in
						connection.commit(transaction: transaction1).map { _ in XCTFail() }
							.recover {
								switch($0) {
								case let error as ClusterDatabaseConnection.FdbApiError:
									XCTAssertEqual(error.errorCode, 1020)
								default:
									XCTFail("Unexpected error: \($0)")
								}
						}
				}
				}.catch(self)
			
			let transaction3 = connection.startTransaction()
			transaction3.read("Test Key 2").map {
				XCTAssertNil($0)
				}.catch(self)
		}
		print("Done")
	}
	
	func testCommitWithPotentialReadConflictAcceptsTransactionWithoutOverlap() throws {
		self.runLoop(eventLoop) {
			guard let connection = self.connection else { return XCTFail() }
			let transaction1 = connection.startTransaction()
			transaction1.store(key: "Test Key 1", value: "Test Value 1")
			
			connection.commit(transaction: transaction1)
				.flatMap { _ -> EventLoopFuture<Void> in
					let transaction2 = connection.startTransaction()
					_ = transaction2.read("Test Key 1")
					transaction2.store(key: "Test Key 2", value: "Test Value 2")
					let transaction3 = connection.startTransaction()
					transaction3.store(key: "Test Key 3", value: "Test Value 3")
					return connection.commit(transaction: transaction3).flatMap {
						connection.commit(transaction: transaction2)
					}
				}
				.map { _ in
					let transaction4 = connection.startTransaction()
					transaction4.read("Test Key 1").map {
						XCTAssertEqual($0, "Test Value 1")
						}.catch(self)
					transaction4.read("Test Key 2").map {
						XCTAssertEqual($0, "Test Value 2")
						}.catch(self)
					transaction4.read("Test Key 3").map {
						XCTAssertEqual($0, "Test Value 3")
						}.catch(self)
				}.catch(self)
		}
	}
}
