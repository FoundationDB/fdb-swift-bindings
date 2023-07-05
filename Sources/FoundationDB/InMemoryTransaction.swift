/*
 * InMemoryTransaction.swift
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
This class represents a transaction for an in-memory database.
*/
public final class InMemoryTransaction: Transaction {
	public let eventLoop: EventLoop
	
	/**
	The version of the database that the reads for this transaction start
	from.
	*/
	public var readVersion: Int64
	
	/**
	The database we are reading from.
	*/
	public let database: InMemoryDatabaseConnection
	
	/** Whether we've committed this transaction. */
	public var committed: Bool
	
	/** Whether we've cancelled this transaction. */
	public var cancelled: Bool
	
	/** The version at which we committed this transaction. */
	public var committedVersion: Int64?
	
	/** The changes that have been made in this transaction. */
	internal private(set) var changes: [DatabaseValue: DatabaseValue?]
	
	/** The key ranges that this transaction requests read consistency for. */
	internal private(set) var readConflicts: [Range<DatabaseValue>]
	
	/** The key ranges that this transaction has written to. */
	internal private(set) var writeConflicts: [Range<DatabaseValue>]
	
	internal private(set) var options: Set<TransactionOption>
	
	/**
	This initializer creates a new transaction.
	
	- parameter version:		The version our reads start from.
	*/
	internal init(version: Int64, database: InMemoryDatabaseConnection) {
		self.readVersion = version
		self.database = database
		self.changes = [:]
		self.readConflicts = []
		self.writeConflicts = []
		self.eventLoop = database.eventLoop
		options = []
		committed = false
		cancelled = false
	}
	
	/**
	This method reads a value from the data store.
	
	- parameter: key		The key that we are reading.
	- parameter snapshot:	Whether we should perform a snapshot read.
	- returns:				The value that we are reading, if any exists.
	*/
	public func read(_ key: DatabaseValue, snapshot: Bool) -> EventLoopFuture<DatabaseValue?> {
		var end = key
		end.data.append(0x00)
		self.addReadConflict(on: key ..< end)
		return self.eventLoop.newSucceededFuture(result: self.database[key])
	}
	
	
	/**
	This method finds a key using a key selector.
	
	- parameter selector:		The selector telling us where to find the
	key.
	- parameter snapshot:		Whether we should perform a snapshot read
	when finding the key.
	- returns:					The first key matching this selector.
	*/
	public func findKey(selector: KeySelector, snapshot: Bool) -> EventLoopFuture<DatabaseValue?> {
		let keys = self.database.keys(from: DatabaseValue(Data([0x00])), to: DatabaseValue(Data([0xFF])))
		let index = self.keyMatching(selector: selector, from: keys)
		if index >= keys.startIndex && index < keys.endIndex {
			return eventLoop.newSucceededFuture(result: keys[index])
		}
		else {
			return eventLoop.newSucceededFuture(result: nil)
		}
	}
	
	/**
	This method gets the index of the first key matching a selector.
	
	- parameter selector:		The key selector
	- parameter keys:			The list of keys to search in.
	- returns:					The index of the key that matches the
	selector. If there is no matching key, this
	will return the end index of the list.
	*/
	private func keyMatching(selector: KeySelector, from keys: [DatabaseValue]) -> Int {
		var index = keys.firstIndex (where: { selector.orEqual == 0 ? $0 >= selector.anchor : $0 > selector.anchor }) ?? keys.endIndex
		index += Int(selector.offset) - 1
		index = min(max(index, keys.startIndex - 1), keys.endIndex)
		return index
	}
	
	/**
	This method reads a range of values for a range of keys matching two
	key selectors.
	
	The keys included in the result range will be from the first key
	matching the start key selector to the first key matching the end key
	selector. The start key will be included in the results, but the end key
	will not.
	
	The results will be ordered in lexographic order by their keys.
	
	This will automatically add a read conflict for the range, so that if
	any key has changed in this range since the start of this transaction
	this transaction will not be accepted.
	
	- parameter from:		The key selector for the beginning of the range.
	- parameter end:		The key selector for the end of the range.
	- parameter limit:		The maximum number of results to return.
	- parameter mode:		The streaming mode to use.
	- parameter snapshot:	Whether we should perform a snapshot read.
	- parameter reverse:	Whether we should return the rows in reverse
	order.
	- returns:				A list of rows with the keys and their
	corresponding values.
	*/
	public func readSelectors(from start: KeySelector, to end: KeySelector, limit: Int?, mode: StreamingMode, snapshot: Bool, reverse: Bool) ->	EventLoopFuture<ResultSet> {
		return eventLoop.submit {
			let allKeys = self.database.keys(from: DatabaseValue(Data([0x00])), to: DatabaseValue(Data([0xFF])))
			
			var startIndex = self.keyMatching(selector: start, from: allKeys)
			startIndex = max(startIndex, allKeys.startIndex)
			let endIndex = self.keyMatching(selector: end, from: allKeys)
			if startIndex >= endIndex || allKeys.isEmpty || startIndex < allKeys.startIndex {
				return ResultSet(rows: [])
			}
			
			let range: Range<Int> = startIndex ..< endIndex
			let rangeKeys = allKeys[range]
			var rows = rangeKeys.compactMap {
				(key: DatabaseValue) -> (key: DatabaseValue, value: DatabaseValue)? in
				return self.database[key].flatMap { (key: key, value: $0) }
			}
			if reverse {
				rows.reverse()
			}
			if let _limit = limit, _limit < rows.count && _limit > 0 {
				rows = Array(rows.prefix(_limit))
			}
			
			return ResultSet(rows: rows)
		}
	}
	
	/**
	This method adds a change to the transaction.
	
	- parameter key:		The key we are changing.
	- parameter value:		The new value for the key.
	*/
	public func store(key: DatabaseValue, value: DatabaseValue) {
		self.changes[key] = value
		if !options.contains(.nextWriteNoWriteConflictRange) {
			self.addWriteConflict(key: key)
		}
		else {
			options.remove(.nextWriteNoWriteConflictRange)
		}
	}
	
	/**
	This method clears a value in the database.
	
	- parameter key:	The key to clear.
	*/
	public func clear(key: DatabaseValue) {
		self.changes[key] = nil as DatabaseValue?
		if !options.contains(.nextWriteNoWriteConflictRange) {
			self.addWriteConflict(key: key)
		}
		else {
			options.remove(.nextWriteNoWriteConflictRange)
		}
	}
	
	/**
	This method clears a range of keys in the database.
	
	- parameter start:		The beginning of the range to clear.
	- parameter end:		The end of the range to clear. This will not be
	included in the range.
	*/
	public func clear(range: Range<DatabaseValue>) {
		for key in self.database.keys(from: range.lowerBound, to: range.upperBound) {
			self.changes[key] = nil as DatabaseValue?
		}
		if !options.contains(.nextWriteNoWriteConflictRange) {
			self.addWriteConflict(on: range)
		}
		else {
			options.remove(.nextWriteNoWriteConflictRange)
		}
	}
	
	/**
	This method adds a read conflict for a key range.
	
	- parameter range:		The range of keys we are adding the conflict on.
	*/
	public func addReadConflict(on range: Range<DatabaseValue>) {
		self.readConflicts.append(range)
	}
	
	/**
	This method adds a range of keys that we want to reserve for writing.
	
	If the system commits this transaction, and another transaction has a
	read conflict on one of these keys, that second transaction will then
	fail to commit.
	
	- parameter range:		The range of keys to add the conflict on.
	*/
	public func addWriteConflict(on range: Range<DatabaseValue>) {
		self.writeConflicts.append(range)
	}
	
	/**
	This method gets the version of the database that this transaction is
	reading from.
	*/
	public func getReadVersion() -> EventLoopFuture<Int64> {
		return eventLoop.newSucceededFuture(result: readVersion)
	}
	
	/**
	This method gets the version of the database that this transaction
	should read from.
	
	- parameter version:		The new version.
	*/
	public func setReadVersion(_ version: Int64) {
		self.readVersion = version
	}
	
	/**
	This method gets the version of the database that this transaction
	committed its changes at.
	
	If the transaction has not committed, this will return -1.
	*/
	public func getCommittedVersion() -> EventLoopFuture<Int64> {
		return eventLoop.newSucceededFuture(result: self.committedVersion ?? -1)
	}
	
	/**
	This method resets the transaction to its initial state.
	*/
	public func reset() {
		self.committed = false
		self.cancelled = false
		self.readVersion = database.currentVersion
		self.changes = [:]
		self.readConflicts = []
		self.writeConflicts = []
	}
	
	/**
	This method cancels the transaction, preventing it from being committed
	and freeing up some associated resources.
	*/
	public func cancel() {
		self.cancelled = true
	}
	
	/**
	This method attempts to retry a transaction after an error.
	
	If the error is retryable, this will reset the transaction and fire the
	returned future when the transaction is ready to use again. If the error
	is not retryable, the returned future will rethrow the error.
	
	- parameter error:		The error that the system encountered.
	- returns:				A future indicating when the transaction is
	ready again.
	*/
	public func attemptRetry(error: Error) -> EventLoopFuture<Void> {
		if let apiError = error as? ClusterDatabaseConnection.FdbApiError, apiError.errorCode != -1 {
			reset()
			return self.eventLoop.newSucceededFuture(result: Void())
		}
		else {
			return self.eventLoop.newFailedFuture(error: error)
		}
	}
	
	private func performBitwiseOperation(operation: MutationType, left: DatabaseValue, right: DatabaseValue) -> DatabaseValue {
		var left = left
		var right = right
		while left.data.count < right.data.count {
			left.data.append(0x00)
		}
		while right.data.count < left.data.count {
			right.data.append(0x00)
		}
		var resultData = Data()
		for index in left.data.indices {
			let result: Int
			switch(operation) {
			case .bitAnd:
				result = Int(left.data[index] & right.data[index])
			default:
				result = Int(left.data[index])
			}
			resultData.append(UInt8(result % 256))
		}
		return DatabaseValue(resultData)
	}
	
	/**
	This method performs an atomic operation against a key and value.
	
	- parameter operation:		The operation to perform.
	- parameter key:			The key to read for the operation.
	- parameter value:			The new value to provide to the operation.
	*/
	public func performAtomicOperation(operation: MutationType, key: DatabaseValue, value: DatabaseValue) {
		let currentValue = self.database[key] ?? DatabaseValue(Data())
		let result: DatabaseValue
		switch(operation) {
		case .bitAnd:
			result = performBitwiseOperation(operation: operation, left: currentValue, right: value)
		default:
			result = currentValue
			print("Atomic operation not yet supported in InMemoryDatabase: \(operation)")
		}
		self.database[key] = result
	}
	
	private func checkVersionStamp(promise: EventLoopPromise<DatabaseValue>) {
		if(self.committedVersion == nil) {
			self.eventLoop.execute {
				self.checkVersionStamp(promise: promise)
			}
			return
		}
		var bytes: [UInt8] = [0,0]
		var versionCopy = self.committedVersion!
		for _ in 0 ..< 8 {
			bytes.insert(UInt8(versionCopy & 0xFF), at: 0)
			versionCopy = versionCopy >> 8
		}
		promise.succeed(result: DatabaseValue(Data(bytes)))
	}
	
	/**
	This method gets a version stamp, which is a key segment containing the
	committed version of the transaction.
	
	This can be called before the transaction is committed, and it will only
	return a value once the transaction is committed.
	*/
	public func getVersionStamp() -> EventLoopFuture<DatabaseValue> {
		let promise: EventLoopPromise<DatabaseValue> = eventLoop.newPromise()
		self.checkVersionStamp(promise: promise)
		return promise.futureResult
	}
	
	/**
	This method sets an option on the transaction.
	
	- parameter option:		The option to set.
	- parameter value:		The value to set for the option.
	*/
	public func setOption(_ option: TransactionOption, value: DatabaseValue?) {
		options.insert(option)
	}
}
