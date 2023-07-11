/*
 * DatabaseConnection.swift
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
import NIO

/**
This protocol describes a handle to a database.

This is implemented by both our real database connections and our
in-memory database connection for testing.
*/
public protocol DatabaseConnection {
	/** The event loop that the database uses to handle async work. */
	var eventLoop: EventLoop { get }
	
	/**
	This method starts a new transaction.
	
	- returns:		The new transaction.
	*/
	func startTransaction() -> Transaction
	
	/**
	This method commits a transaction to the database.
	
	The database will check the read conflict ranges on the transaction for
	conflicts with recent changes, and if it detects any, it will fail the
	transaction. Otherwise, the transaction's changes will be committed into
	the database and will be available for subsequent reads.
	
	- parameter transaction:	The transaction we are committing.
	- returns:					A future that will fire when the transaction
	is finished committing. If the transaction
	cannot be committed, the future will throw
	an error.
	*/
	func commit(transaction: Transaction) -> EventLoopFuture<()>
}

extension DatabaseConnection {
	/**
	This method starts a transaction, runs a block of code, and commits the
	transaction.
	
	The block will be run asynchronously.
	
	- parameter block:	The block to run with the transaction.
	- returns:			A future providing the result of the block.
	*/
	public func transaction<T>(_ block: @escaping (Transaction) throws -> T) -> EventLoopFuture<T> {
		return self.transaction {
			transaction in
			return self.eventLoop.submit {
				try block(transaction)
			}
		}
	}
	
	/**
	This method starts a transaction, runs a block of code, and commits the
	transaction.
	
	The block will be run asynchronously.
	
	In this version, the block provides a future, and the value provided by
	that future will also be provided by the future that this method
	returns.
	
	- parameter block:	The block to run with the transaction.
	- returns:			A future providing the result of the block.
	*/
	public func transaction<T>(_ block: @escaping (Transaction) throws -> EventLoopFuture<T>) -> EventLoopFuture<T> {
		let transaction = self.startTransaction()
		
		return EventLoopFuture<T>.retrying(eventLoop: eventLoop, onError: transaction.attemptRetry) {
			return try block(transaction)
				.flatMap { v in return self.commit(transaction: transaction)
					.map { _ in return v }
			}
		}
	}
}
