/*
 * InMemoryDatabaseConnection.swift
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
This class provides a database connection to an in-memory database.
*/
public final class InMemoryDatabaseConnection: DatabaseConnection {
	public let eventLoop: EventLoop
	
	/** The internal data storage. */
	private var data: [DatabaseValue: DatabaseValue]
	
	/** The keys changed by previous commit versions. */
	private var changeHistory: [(Int64, [(Range<DatabaseValue>)])]
	
	/** The last committed transaction version. */
	public private(set) var currentVersion: Int64
	
	/** This initializer creates a new database. */
	public init(eventLoop: EventLoop) {
		self.eventLoop = eventLoop
		data = .init()
		currentVersion = 0
		changeHistory = []
	}
	
	/**
	This method reads a value from the database.
	
	- parameter key:		The key that we are reading.
	*/
	internal subscript(key: DatabaseValue) -> DatabaseValue? {
		get {
			return data[key]
		}
		set {
			data[key] = newValue
		}
	}
	
	/**
	This method gets all the keys that we currently have in a given range.
	
	- parameter start:		The beginning of the range.
	- parameter end:		The end of the range. This will not be included
	in the results.
	- returns:				The keys in that range.
	*/
	internal func keys(from start: DatabaseValue, to end: DatabaseValue) -> [DatabaseValue] {
		let keys: [DatabaseValue] = data.keys.filter {
			(key) in
			return key >= start && key < end
			}.sorted()
		return keys
	}
	
	/**
	This method starts a transaction on the database.
	
	- returns:		The new transaction.
	*/
	public func startTransaction() -> Transaction {
		return InMemoryTransaction(version: currentVersion, database: self)
	}
	
	/**
	This method commits a transaction to the database.
	
	The transaction must be one that was created by calling startTransaction
	on this database. Otherwise, it will be rejected.
	
	If the transaction has added a read conflict on any keys that have
	changed since the transaction's readVersion, this will reject the
	transaction.
	
	If the transaction is valid this will merge in the keys and values that
	were changed in the transaction.
	
	- parameter transaction:	The transaction to commit.
	- returns:					A future that will fire when the
	transaction has finished committing. If
	the transaction is rejected, the future
	will throw an error.
	*/
	public func commit(transaction: Transaction) -> EventLoopFuture<()> {
		guard let memoryTransaction = transaction as? InMemoryTransaction else {
			return eventLoop.makeFailedFuture( ClusterDatabaseConnection.FdbApiError(1000))
		}
		if memoryTransaction.committed {
			return eventLoop.makeFailedFuture( ClusterDatabaseConnection.FdbApiError(2017))
		}
		if memoryTransaction.cancelled {
			return eventLoop.makeFailedFuture( ClusterDatabaseConnection.FdbApiError(1025))
		}
		for (version, changes) in changeHistory {
			if version <= memoryTransaction.readVersion { continue }
			for changedRange in changes {
				for readRange in memoryTransaction.readConflicts {
					if changedRange.contains(readRange.lowerBound) || (changedRange.contains(readRange.lowerBound) && readRange.upperBound != changedRange.lowerBound) {
						return eventLoop.makeFailedFuture( ClusterDatabaseConnection.FdbApiError(1020))
					}
				}
			}
		}
		for (key, value) in memoryTransaction.changes {
			data[key] = value
		}
		self.currentVersion += 1
		self.changeHistory.append((self.currentVersion, memoryTransaction.writeConflicts))
		memoryTransaction.committed = true
		memoryTransaction.committedVersion = self.currentVersion
		return eventLoop.makeSucceededFuture(())
	}
}
