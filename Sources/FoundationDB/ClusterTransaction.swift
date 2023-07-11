/*
 * ClusterTransaction.swift
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
import CFoundationDB
import NIO

/**
This class represents a transaction against a real database cluster.
*/
public class ClusterTransaction: Transaction {
	public let eventLoop: EventLoop
	
	/** The database this transaction is on. */
	public let database: ClusterDatabaseConnection
	
	/** The internal C representation of the transaction. */
	internal let transaction: EventLoopFuture<OpaquePointer>
	
	/**
	This initializer creates a transaction against a database.
	
	- parameter database:		The database that we are working with.
	- throws:					If the API cannot give us a transaction, this
	will throw a FutureError.
	*/
	init(database: ClusterDatabaseConnection) {
		self.database = database
		self.eventLoop = database.eventLoop
		
		transaction = database.database.flatMapThrowing {
			var pointer: OpaquePointer? = nil
			try ClusterDatabaseConnection.FdbApiError.wrapApiError(fdb_database_create_transaction($0, &pointer))
			if let transaction = pointer {
				return transaction
			}
			else {
				throw FdbFutureError.FutureDidNotProvideValue
			}
		}
	}
	
	/**
	This method cleans up resources for the transaction.
	*/
	deinit {
		_ = transaction.map {
			fdb_transaction_destroy($0)
		}
	}
	
	/**
	This method reads a single value from the database.
	
	This will automatically add a read conflict, so if another transaction
	has changed the value since this transaction started reading, this
	transaction will not commit.
	
	- parameter key:		The key to read.
	- parameter snapshot:	Whether we should perform a snapshot read.
	- returns:				The value that we read.
	*/
	public func read(_ key: DatabaseValue, snapshot: Bool) -> EventLoopFuture<DatabaseValue?> {
		return key.data.withUnsafeBytes {
			bytes in
			return self.withTransaction { transaction in
				return EventLoopFuture<DatabaseValue?>.fromFoundationFuture(eventLoop: self.eventLoop, future: fdb_transaction_get(transaction, bytes.baseAddress, Int32(key.data.count), snapshot ? 1 : 0)) {
					(future: OpaquePointer) -> DatabaseValue? in
					var present: Int32 = 0
					var bytes: UnsafePointer<UInt8>? = nil
					var count: Int32 = 0
					
					fdb_future_get_value(future, &present, &bytes, &count)
					
					if present != 0 {
						return DatabaseValue(Data(bytes: bytes!, count: Int(count)))
					}
					else {
						return nil
					}
				}
			}
		}
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
		return withTransaction { transaction in
			let cFuture = selector.anchor.data.withUnsafeBytes {
				bytes -> OpaquePointer in
				fdb_transaction_get_key(transaction, bytes.baseAddress, Int32(selector.anchor.data.count), selector.orEqual, selector.offset, snapshot ? 1 : 0)
			}
			return EventLoopFuture<DatabaseValue?>.fromFoundationFuture(eventLoop: self.eventLoop, future: cFuture) {
				(future: OpaquePointer) -> DatabaseValue? in
				
				var bytes: UnsafePointer<UInt8>? = nil
				var count: Int32 = 0
				
				fdb_future_get_key(future, &bytes, &count)
				
				if let _bytes = bytes, count > 0 {
					let data = Data(bytes: _bytes, count: Int(count))
					return DatabaseValue(data)
				}
				else {
					return nil
				}
			}
		}
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
	- returns:				A list of keys and their corresponding values.
	*/
	public func readSelectors(from start: KeySelector, to end: KeySelector, limit: Int?, mode: StreamingMode, snapshot: Bool, reverse: Bool) ->	EventLoopFuture<ResultSet> {
		var rows = [(key: DatabaseValue, value: DatabaseValue)]()
		var start = start
		var iteration: Int32 = 1
		
		return EventLoopFuture<Void>.retrying(eventLoop: eventLoop, onError: { (error: Error) -> EventLoopFuture<Void> in
			switch(error) {
			case FdbFutureError.ContinueStream: return self.eventLoop.makeSucceededFuture(Void())
			default: return self.eventLoop.makeFailedFuture(error)
			}
		}) { () -> EventLoopFuture<Void> in
			let endData = end.anchor.data
			let endBytes = UnsafeMutablePointer<UInt8>.allocate(capacity: endData.count)
			_ = endData.copyBytes(to: UnsafeMutableBufferPointer(start: endBytes, count: endData.count))
			
			let startData = start.anchor.data
			let startBytes = UnsafeMutablePointer<UInt8>.allocate(capacity: startData.count)
			_ = startData.copyBytes(to: UnsafeMutableBufferPointer(start: startBytes, count: startData.count))
			
			return self.withTransaction { transaction in
				defer {
					free(startBytes)
					free(endBytes)
				}
				
				guard let cFuture = fdb_transaction_get_range(transaction, startBytes, Int32(startData.count), start.orEqual, start.offset, endBytes, Int32(endData.count), end.orEqual, end.offset, Int32(limit ?? 0), -1, FDBStreamingMode(Int32(mode.rawValue)), iteration, snapshot ? 1 : 0, reverse ? 1 : 0) else {
					throw FdbFutureError.FutureDidNotProvideValue
				}
				return EventLoopFuture<Void>.fromFoundationFuture(eventLoop: self.eventLoop, future: cFuture) { cFuture -> Void in
					var moreAvailable: Int32 = 0
					var buffer: UnsafePointer<FDBKeyValue>? = nil
					var bufferSize: Int32 = 0
					
					fdb_future_get_keyvalue_array(cFuture, &buffer, &bufferSize, &moreAvailable)
					guard let _buffer = buffer else {
						return
					}
					var entry = _buffer
					for _ in 0..<bufferSize {
						let keyData = Data(bytes: entry.pointee.key, count: Int(entry.pointee.key_length))
						let valueData = Data(bytes: entry.pointee.value, count: Int(entry.pointee.value_length))
						rows.append((key: DatabaseValue(keyData), value: DatabaseValue(valueData)))
						entry = entry.advanced(by: 1)
					}
					if let _limit = limit, bufferSize >= Int32(_limit) {
						moreAvailable = 0
					}
					if rows.count > 0 {
						start = KeySelector(greaterThan: rows[rows.count - 1].key)
					}
					iteration += 1
					if moreAvailable != 0 {
						throw FdbFutureError.ContinueStream
					}
				}
			}
			}.map { _ in
				return ResultSet(rows: rows)
		}
	}
	
	/**
	This method stores a value in the database.
	
	- parameter key:		The key to store.
	- parameter value:		The value to store for the key.
	*/
	public func store(key: DatabaseValue, value: DatabaseValue) {
		key.data.withUnsafeBytes {
			keyBytes in
			value.data.withUnsafeBytes {
				valueBytes in
				withTransaction { transaction in
					fdb_transaction_set(transaction, keyBytes.baseAddress, Int32(key.data.count), valueBytes.baseAddress, Int32(value.data.count))
				}
			}
		}
	}
	
	/**
	This method clears the value for a key in the database.
	
	- parameter key:		The key to clear.
	*/
	public func clear(key: DatabaseValue) {
		withTransaction { transaction in
			key.data.withUnsafeBytes {
				fdb_transaction_clear(transaction, $0.baseAddress, Int32(key.data.count))
			}
		}
	}
	
	/**
	This method clears the value for a range of keys in the database.
	
	This will not clear the last value in the range.
	
	- parameter range:		The range of keys to clear.
	*/
	public func clear(range: Range<DatabaseValue>) {
		withTransaction { transaction in
			range.lowerBound.data.withUnsafeBytes { lowerBytes in
				range.upperBound.data.withUnsafeBytes { upperBytes in
					fdb_transaction_clear_range(transaction, lowerBytes.baseAddress, Int32(range.lowerBound.data.count), upperBytes.baseAddress, Int32(range.upperBound.data.count))
				}
			}
		}
	}
	
	/**
	This method adds a read conflict for this transaction.
	
	This will cause the transaction to fail if any of the keys in the range
	have been changed since this transaction started.
	
	- parameter range:		The range of keys to add the conflict on.
	*/
	public func addReadConflict(on range: Range<DatabaseValue>) {
		withTransaction { transaction in
			range.lowerBound.data.withUnsafeBytes { lowerBytes in
				range.upperBound.data.withUnsafeBytes { upperBytes in
					_ = fdb_transaction_add_conflict_range(transaction, lowerBytes.baseAddress, Int32(range.lowerBound.data.count), upperBytes.baseAddress, Int32(range.upperBound.data.count), FDB_CONFLICT_RANGE_TYPE_READ)
				}
			}
		}
	}
	
	/**
	This method adds a range of keys that we want to reserve for writing.
	
	If the system commits this transaction, and another transaction has a
	read conflict on one of these keys, that second transaction will then
	fail to commit.
	
	- parameter range:		The range of keys to add the conflict on.
	*/
	public func addWriteConflict(on range: Range<DatabaseValue>) {
		withTransaction { transaction in
			range.lowerBound.data.withUnsafeBytes {
				lowerBytes in
				range.upperBound.data.withUnsafeBytes {
					upperBytes in
					_ = fdb_transaction_add_conflict_range(transaction, lowerBytes.baseAddress, Int32(range.lowerBound.data.count), upperBytes.baseAddress, Int32(range.upperBound.data.count), FDBConflictRangeType(rawValue: 1))
				}
			}
		}
	}
	
	/**
	This method gets the version of the database that this transaction is
	reading from.
	*/
	public func getReadVersion() -> EventLoopFuture<Int64> {
		return withTransaction { transaction in
			return EventLoopFuture<Int64>.fromFoundationFuture(eventLoop: self.eventLoop, future: fdb_transaction_get_read_version(transaction), default: 0, fetch: fdb_future_get_version)
		}
	}
	
	/**
	This method gets the version of the database that this transaction
	should read from.
	
	- parameter version:		The new version.
	*/
	public func setReadVersion(_ version: Int64) {
		withTransaction { transaction in
			fdb_transaction_set_read_version(transaction, version)
		}
	}
	
	/**
	This method gets the version of the database that this transaction
	committed its changes at.
	
	If the transaction has not committed, this will return -1.
	*/
	public func getCommittedVersion() -> EventLoopFuture<Int64> {
		return transaction.flatMap { transaction in
			return self.eventLoop.submit {
				var result: Int64 = 0
				try ClusterDatabaseConnection.FdbApiError.wrapApiError(fdb_transaction_get_committed_version(transaction, &result))
				return result
			}
		}
	}
	
	/**
	This method attempts to retry a transaction after an error.
	
	If the error is retryable, this will reset the transaction and fire the
	returned future when the transaction is ready to use again. If the error
	is not retryable, the returned future will rethrow the error.
	
	- parameter error:	The error that the system encountered.
	- returns:			A future indicating when the transaction is
						ready again.
	*/
	public func attemptRetry(error: Error) -> EventLoopFuture<()> {
		if let apiError = error as? ClusterDatabaseConnection.FdbApiError {
			return withTransaction { transaction in
				return EventLoopFuture<Void>.fromFoundationFuture(eventLoop: self.eventLoop, future: fdb_transaction_on_error(transaction, apiError.errorCode))
			}
		}
		else {
			return self.eventLoop.makeFailedFuture(error)
		}
	}
	
	/**
	This method resets the transaction to its initial state.
	*/
	public func reset() -> Void {
		withTransaction { transaction in
			fdb_transaction_reset(transaction)
		}
	}
	
	/**
	This method cancels the transaction, preventing it from being committed
	and freeing up some associated resources.
	*/
	public func cancel() {
		withTransaction { transaction in
			fdb_transaction_cancel(transaction)
		}
	}
	
	/**
	This method performs an atomic operation against a key and value.
	
	- parameter operation:		The operation to perform.
	- parameter key:			The key to read for the operation.
	- parameter value:			The new value to provide to the operation.
	*/
	public func performAtomicOperation(operation: MutationType, key: DatabaseValue, value: DatabaseValue) -> Void {
		withTransaction { transaction in
			key.data.withUnsafeBytes {
				keyBytes in
				value.data.withUnsafeBytes {
					valueBytes in
					fdb_transaction_atomic_op(transaction, keyBytes.baseAddress, Int32(key.data.count), valueBytes.baseAddress, Int32(value.data.count), FDBMutationType(UInt32(operation.rawValue)))
				}
			}
		}
	}
	
	/**
	This method gets a version stamp, which is a key segment containing the
	committed version of the transaction.
	
	This can be called before the transaction is committed, and it will only
	return a value once the transaction is committed.
	*/
	public func getVersionStamp() -> EventLoopFuture<DatabaseValue> {
		return withTransaction { transaction in
			return EventLoopFuture<DatabaseValue>.fromFoundationFuture(eventLoop: self.eventLoop, future: fdb_transaction_get_versionstamp(transaction)) {
				future in
				var key: UnsafePointer<UInt8>? = nil
				var length: Int32 = 0
				try ClusterDatabaseConnection.FdbApiError.wrapApiError(fdb_future_get_key(future, &key, &length))
				guard let _key = key else {
					throw FdbFutureError.FutureDidNotProvideValue
				}
				return DatabaseValue(Data(bytes: _key, count: Int(length)))
			}
		}
	}
	
	/**
	This method sets an option on the transaction.
	
	- parameter option:		The option to set.
	- parameter value:		The value to set for the option.
	*/
	public func setOption(_ option: TransactionOption, value: DatabaseValue?) {
		withTransaction { transaction in
			if let _value = value {
				_value.data.withUnsafeBytes {
					bytes in
					_ = fdb_transaction_set_option(transaction, FDBTransactionOption(rawValue: option.rawValue), bytes.baseAddress, Int32(_value.data.count))
				}
			}
			else {
				fdb_transaction_set_option(transaction, FDBTransactionOption(rawValue: option.rawValue), nil, 0)
			}
		}
	}
	
	internal func withTransaction<T>(block: @escaping (OpaquePointer) throws -> T) -> EventLoopFuture<T> {
		return transaction.flatMapThrowing {
			let value = try block($0)
			_ = self
			return value
			
		}
	}
	
	internal func withTransaction<T>(block: @escaping (OpaquePointer) throws -> EventLoopFuture<T>) -> EventLoopFuture<T> {
		return transaction.thenThrowingFuture {
			try block($0 as! OpaquePointer).map {
				_ = self
				return $0
			}
		}
	}
	
	internal func withTransaction(block: @escaping (OpaquePointer) throws -> Void) rethrows -> Void {
		_ = transaction.flatMapThrowing {
			_ = self
			try block($0)
		}
	}
}
