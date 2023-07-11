/*
 * StackMachine.swift
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
import FoundationDB
import CFoundationDB
import NIO

/**
This type represents a stack machine that can execute the binding tests.
*/
public final class StackMachine {
	/**
	This type represents a stack item's metadata.
	*/
	struct Metadata {
		/** The index of the command that produced the item. */
		let commandNumber: Int
	}
	
	/**
	This type represents an entry in the stack.
	*/
	struct Item {
		/** The value for this entry. */
		let value: EventLoopFuture<Any>
		
		/** The metadata for the entry. */
		let metadata: Metadata
	}
	
	/**
	The errors that are thrown when a command cannot be executed.
	
	The `execute` method can also thrown errors beyond these, because it
	rethrows errors from the DatabaseValue value extraction.
	*/
	enum ExecutionError: Error {
		/** The command required popping a value, but the stack is empty. */
		case PoppedEmptyStack
		
		/** The command was supposed to have an argument, but did not. */
		case PushedEmptyValue
		
		/**
		The system tried to execute a swap command with an index beyond the
		bounds of the stack.
		*/
		case SwappedBeyondBounds(index: Int, count: Int)
		
		/**
		The system tried to execute an operation with a value on top of the
		stack that was not what the operation required.
		*/
		case IllegalValueType
		
		/**
		The system attempted to make a range read request using a streaming
		mode that we don't support.
		*/
		case IllegalStreamingMode
		
		/**
		The system tried to execute a command that we do not support yet.
		*/
		case CommandNotSupported
	}
	
	/** The maximum size of a value produced by the LOG_STACK command. */
	private static let maxLogEntrySize=40000
	
	/** The number of commands that we have executed. */
	var commandCount = 0
	
	/**
	The transactions that we have committed.
	
	The keys in this dictionary are the labels assigned to the transactions
	when they were created.
	*/
	var transactionMap: [String: Transaction] = [:]
	
	/**
	The stack of data.
	*/
	var stack: [Item] = []
	
	/**
	The name of the transaction we are currently executing.
	*/
	var currentTransactionName: String
	
	/**
	The version of the database we saw from the last version-related
	command.
	*/
	var lastSeenVersion: Int64 = -1
	
	/**
	Whether this stack machine has finished executing its commands.
	*/
	var finished = false
	
	var commands: [Command] = []
	
	/** A value that we set when a stack operation does not produce a result. */
	static let resultNotPresent = Data("RESULT_NOT_PRESENT".utf8)
	
	let connection: DatabaseConnection
	
	/**
	This initializer creates an empty new state machine.
	*/
	public init(connection: DatabaseConnection, transactionName: String) {
		self.connection = connection
		self.currentTransactionName = transactionName
		if StackMachine.connection == nil {
			StackMachine.connection = connection
		}
	}
	
	/**
	This method gets the transaction that we are currently executing.
	*/
	var currentTransaction: Transaction {
		if let transaction = transactionMap[currentTransactionName] {
			return transaction
		}
		else {
			let transaction = connection.startTransaction()
			transactionMap[currentTransactionName] = transaction
			return transaction
		}
	}
	
	/**
	This initializer creates a stack machine that will run commands from the
	database.
	
	- parameter commandPrefix:	The prefix that the keys for the commands
	will start with.
	*/
	public convenience init(connection: DatabaseConnection, commandPrefix: String) {
		self.init(connection: connection, transactionName: commandPrefix)
		STACK_MACHINE_LIST.append(self)
		finished = false
	}
	
	/**
	This method fetches commands from the database and runs them.
	
	- parameter connection:		The connection that the system should fetch
								the connection from.
	*/
	public func run() {
		let commandPrefix = currentTransactionName
		_ = connection.transaction { (transaction) -> EventLoopFuture<TupleResultSet> in
			let range = Tuple(Data(bytes: Array(commandPrefix.utf8))).childRange
			return transaction.read(range: range)
			}.map {
				self.commands = $0.rows.map { $0.value }.compactMap { Command(data: $0) }
				_ = self.executeNextCommand().map { _ in
					self.finished = true;
					}.recover {
						self.finished = true
						print("\($0)")
				}
			}.recover {
				print("\($0)")
		}
	}
	
	/**
	This method runs a stack machine.
	
	- parameter arguments:	The command-line arguments.
	*/
	public static func run(eventLoop: EventLoop, arguments: [String], projectDirectory: String = ".") {
		var arguments = arguments
		arguments.remove(at: 0)
		
		if arguments.count < 2 {
			fatalError("Process must be invoked with at least two arguments")
		}
		
		let commandPrefix = arguments[0]
		
		let apiVersion = Int32(arguments[1])!
		if apiVersion > FDB_API_VERSION {
			print("Refusing to use API version \(apiVersion); max is \(FDB_API_VERSION)")
			setFdbApiVersion(FDB_API_VERSION)
		}
		else {
			setFdbApiVersion(apiVersion)
		}
		
		let clusterFilePath: String
		if arguments.count >= 3 {
			clusterFilePath = arguments[2]
		}
		else {
			clusterFilePath = "\(projectDirectory)/fdb.cluster"
		}
		
		do {
			let connection = try ClusterDatabaseConnection(fromClusterFile: clusterFilePath, eventLoop: eventLoop)
			let machine = StackMachine(connection: connection, commandPrefix: commandPrefix)
			machine.run()
		}
		catch {
			fatalError("Failed to start stack machine: \(error)")
		}
	}
	
	
	/**
	This method prints a warning about attempting to run a command.
	
	- parameter command:		The command we are trying to run.
	- parameter message:		The message explaining the problem.
	*/
	func printWarning(command: Command, message: String) {
		print("Warning: \(message)")
		print("Command index: \(commandCount)")
		print("Command: \(command)")
	}
	
	/**
	This method pushes an item onto the stack.
	
	- parameter data:		The data for the item we are creating.
	*/
	func push(value: Any) {
		self.push(future: connection.eventLoop.makeSucceededFuture(value))
	}
	
	/**
	This method pushes a future onto the stack.
	
	- parameter future:		The future to push.
	*/
	func push<T>(future: EventLoopFuture<T>) {
		self.stack.append(Item(value: future.map { $0 as Any }, metadata: self.currentMetadata))
	}
	
	func handleFutureError(_ error: Error, metadata: StackMachine.Metadata) throws -> Any {
		switch(error) {
		case let error as ClusterDatabaseConnection.FdbApiError:
			print("Got error \(error) for metadata \(metadata)")
			return Tuple(
				Data(Array("ERROR".utf8)),
						 Data(bytes: Array(String(error.errorCode).utf8))
				).databaseValue.data
		default:
			throw error
			
		}
	}
	
	/**
	This method pops a future off the stack.
	*/
	func pop() -> EventLoopFuture<Any> {
		if self.stack.isEmpty {
			return connection.eventLoop.makeFailedFuture(ExecutionError.PoppedEmptyStack)
		}
		let metadata = self.stack.last!.metadata
		return self.stack.removeLast().value.flatMapErrorThrowing { error in
			return try self.handleFutureError(error, metadata: metadata)
		}
	}
	
	/**
	This method pops a value of the stack and casts it to a specific type.
	
	If the value is of a different type, this will throw an IllegalValueType
	error.
	*/
	func popAndCast<T>() -> EventLoopFuture<T> {
		return self.pop().flatMapThrowing {
			if let value = $0 as? T {
				return value
			}
			else if T.self is DatabaseValue.Type {
				if let data = $0 as? Data {
					return DatabaseValue(data) as! T
				}
				else if let tuple = $0 as? Tuple {
					return tuple.databaseValue as! T
				}
			}
			print("Cannot cast \($0) to \(T.self)")
			throw ExecutionError.IllegalValueType
		}
	}
	
	func popTuple<A,B>() -> EventLoopFuture<(A,B)> {
		let lhs = self.popAndCast() as EventLoopFuture<A>
		let rhs = self.popAndCast() as EventLoopFuture<B>
		return lhs.flatMap { lhs in rhs.map { rhs in (lhs,rhs) } }
	}
	
	func popTuple<A,B,C>() -> EventLoopFuture<(A,B,C)> {
		let lhs = self.popTuple() as EventLoopFuture<(A,B)>
		let rhs = self.popAndCast() as EventLoopFuture<C>
		return lhs.flatMap { lhs in rhs.map { rhs in (lhs.0, lhs.1, rhs) } }
	}
	
	func popTuple<A,B,C,D>() -> EventLoopFuture<(A,B,C,D)> {
		let lhs = self.popTuple() as EventLoopFuture<(A,B,C)>
		let rhs = self.popAndCast() as EventLoopFuture<D>
		return lhs.flatMap { lhs in rhs.map { rhs in (lhs.0, lhs.1, lhs.2, rhs) } }
	}
	
	func popTuple<A,B,C,D,E>() -> EventLoopFuture<(A,B,C,D,E)> {
		let lhs = self.popTuple() as EventLoopFuture<(A,B,C,D)>
		let rhs = self.popAndCast() as EventLoopFuture<E>
		return lhs.flatMap { lhs in rhs.map { rhs in (lhs.0, lhs.1, lhs.2, lhs.3, rhs) } }
	}
	
	func unsafeTuplePack(_ value: Any) throws -> Tuple {
		switch(value) {
		case let i as Int:
			return Tuple(i)
		case let s as String:
			return Tuple(s)
		case let d as Data:
			return Tuple(d)
		case let b as Bool:
			return Tuple(b)
		case let u as UUID:
			return Tuple(u)
		case let t as Tuple:
			return t
		default:
			print("Cannot log non-tuple-compatible type \(type(of: value))")
			throw ExecutionError.IllegalValueType
		}
	}
	
	/**
	The metadata for a newly created item.
	*/
	var currentMetadata: Metadata {
		return Metadata(commandNumber: self.commandCount)
	}
	
	private func performOperation(_ command: Command, providesValue: Bool = true, block: @escaping (Transaction,Bool) throws -> EventLoopFuture<Data>) rethrows {
		if command.direct {
			let future = connection.transaction { try block($0, false) }
			self.push(future: future.map { $0 as Any })
		}
		else {
			let future = try block(self.currentTransaction, command.snapshot).map { $0 as Any }
			if providesValue {
				return self.push(future: future)
			}
		}
	}
	
	func executeNextCommand() -> EventLoopFuture<Void> {
		if commandCount < commands.count {
			do {
				if let signal = try self.execute(command: commands[commandCount]) {
					return signal.flatMap {
						self.commandCount += 1
						return self.executeNextCommand()
					}
				}
				else {
					self.commandCount += 1
					return self.executeNextCommand()
				}
			}
			catch {
				return connection.eventLoop.makeFailedFuture(error)
			}
		}
		else {
			return connection.eventLoop.makeSucceededFuture(Void())
		}
	}
	
	func execute(operation: Command.Operation) -> EventLoopFuture<Void> {
		guard let command = Command(operation: operation) else {
			return connection.eventLoop.makeFailedFuture(ExecutionError.CommandNotSupported)
		}
		do {
			return try self.execute(command: command) ?? connection.eventLoop.makeSucceededFuture(Void())
		}
		catch {
			return connection.eventLoop.makeFailedFuture(error)
		}
	}
	
	func execute(command: Command) throws -> EventLoopFuture<Void>? {
		var signal: EventLoopFuture<Void>? = nil
		print("Executing \(command) \(commandCount) - Stack \(self.stack.count)")
		
		switch(command.operation) {
		case .push:
			if let argument = command.argument {
				self.push(value: argument)
			}
			else {
				throw ExecutionError.PushedEmptyValue
			}
		case .dup:
			if let item = self.stack.last {
				self.stack.append(item)
			}
			else {
				throw ExecutionError.PoppedEmptyStack
			}
		case .empty:
			self.stack = []
		case .swap:
			signal = self.popAndCast().flatMapThrowing { (distance: Int) in
				guard distance < self.stack.count else {
					throw ExecutionError.SwappedBeyondBounds(index: distance, count: self.stack.count)
				}
				
				let endIndex = self.stack.endIndex - 1
				let startIndex = endIndex - distance
				let value = self.stack[endIndex]
				self.stack[endIndex] = self.stack[startIndex]
				self.stack[startIndex] = value
			}
		case .pop:
			_ = self.pop()
		case .sub:
			let val1 = self.popAndCast() as EventLoopFuture<Int>
			let val2 = self.popAndCast() as EventLoopFuture<Int>
			let result = val1.flatMap { val1 in
				val2.map { val2 in val1 - val2 }
			}
			self.push(future: result)
		case .concat:
			let val1 = self.pop()
			let val2 = self.pop()
			
			let pair = val1.flatMap { val1 in val2.map { val2 in (val1, val2) } }
			let result = pair.flatMapThrowing { (pair: (Any,Any)) -> Any in
				switch(pair) {
				case let (s1,s2) as (String,String):
					return s1 + s2
				case let (d1, d2) as (Data,Data):
					return d1 + d2
				default:
					print("Cannot concat types \(type(of: pair.0)) and \(type(of: pair.1))")
					throw ExecutionError.IllegalValueType
				}
			}
			self.push(future: result)
		case .log:
			let prefix = self.popAndCast() as EventLoopFuture<Data>
			let indices = Array(self.stack.indices).reversed()
			let futureList = indices.map { index -> EventLoopFuture<(Data, Any)> in
				let item = self.stack[index]
				let key = prefix.map { prefix -> Data in
					var key = prefix
					key.append(Tuple(index, item.metadata.commandNumber).databaseValue.data)
					return key
				}
				return key.flatMap { key in
					item.value
						.flatMapErrorThrowing { try self.handleFutureError($0, metadata: item.metadata) }
						.map { (key, $0) }
				}
			}
			
			let future: EventLoopFuture<[(Data,Any)]> = EventLoopFuture<(Data,Any)>.accumulating(futures: futureList, eventLoop: connection.eventLoop)
			
			signal = future.flatMap { (items: [(Data, Any)]) -> EventLoopFuture<Void> in
				self.connection.transaction { transaction -> Void in
					for (key,value) in items {
						var tupleData = try self.unsafeTuplePack(value).databaseValue
						if tupleData.data.count > StackMachine.maxLogEntrySize {
							tupleData = DatabaseValue(tupleData.data.subdata(in: 0 ..< StackMachine.maxLogEntrySize))
						}
						
						transaction.store(key: DatabaseValue(key), value: tupleData)
					}
				}
				}.map { _ in
					self.stack = []
			}
		case .newTransaction:
			self.transactionMap[self.currentTransactionName] = connection.startTransaction()
		case .useTransaction:
			signal = self.popAndCast().map { (name: String) in
				self.currentTransactionName = name
				if self.transactionMap[self.currentTransactionName] == nil {
					self.transactionMap[self.currentTransactionName] = self.connection.startTransaction()
				}
			}
		case .onError:
			let errorCode = self.popAndCast() as EventLoopFuture<Int>
			let result = errorCode.flatMap { error in
				self.currentTransaction.attemptRetry(error: ClusterDatabaseConnection.FdbApiError(Int32(error)))
				}.map { _ -> Any in StackMachine.resultNotPresent }
			self.push(future: result)
			signal = result.map { _ in Void() }.recover { _ in Void() }
		case .get:
			signal = self.popAndCast().map { (key: DatabaseValue) in
				self.performOperation(command) {
					$0.read(key, snapshot: $1).map {
						$0?.data ?? StackMachine.resultNotPresent
					}
				}
			}
		case .getKey:
			let values = self.popTuple() as EventLoopFuture<(DatabaseValue, Int, Int, DatabaseValue)>
			signal = values.map { (anchor, orEqual, offset, prefix) in
				let selector = KeySelector(anchor: anchor, orEqual: orEqual, offset: offset)
				self.performOperation(command) {
					(transaction: Transaction, snapshot: Bool) -> EventLoopFuture<Data> in
					let valueFuture = transaction.findKey(selector: selector, snapshot: snapshot)
					return valueFuture.map {
						(_key: DatabaseValue?) -> Data in
						if let key = _key {
							if key.hasPrefix(prefix) {
								return key.data
							}
							else if key < prefix {
								return prefix.data
							}
							else {
								var result = Data(prefix.data)
								result.withUnsafeMutableBytes {
									(bytes: UnsafeMutablePointer<UInt8>) in
									bytes.advanced(by: prefix.data.count - 1).pointee += 1
								}
								return result
							}
						}
						else {
							return Data()
						}
					}
				}
			}
		case .getRange:
			let values = popTuple() as EventLoopFuture<(DatabaseValue, DatabaseValue, Int, Int, Int)>
			signal = values.flatMapThrowing { (begin, end, limit, reverse, streamingModeNumber) in
				try self.performOperation(command) {
					(transaction, snapshot) in
					guard let streamingMode = StreamingMode(rawValue: Int32(streamingModeNumber)) else {
						throw ExecutionError.IllegalStreamingMode
					}
					
					let rangeFuture = transaction.readSelectors(from: KeySelector(greaterThan: begin, orEqual: true), to: KeySelector(greaterThan: end, orEqual: true), limit: limit, mode: streamingMode, snapshot: snapshot, reverse: reverse == 1)
					let results = rangeFuture.map {
						(results: ResultSet) -> Data in
						var resultTuple = Tuple()
						for (key,value) in results.rows {
							resultTuple.append(key.data)
							resultTuple.append(value.data)
						}
						return resultTuple.databaseValue.data
					}
					return results
				}
			}
		case .getRangeStartingWith:
			let values = self.popTuple() as EventLoopFuture<(DatabaseValue, Int, Int, Int)>
			signal = values.flatMapThrowing { (prefix, limit, reverse, streamingModeNumber) in
				try self.performOperation(command) {
					guard let streamingMode = StreamingMode(rawValue: Int32(streamingModeNumber)) else {
						throw ExecutionError.IllegalStreamingMode
					}
					
					var upperBound = prefix
					upperBound.data.append(0xFF)
					
					let rows = $0.readSelectors(from: KeySelector(greaterThan: prefix), to: KeySelector(greaterThan: upperBound, orEqual: true), limit: limit, mode: streamingMode, snapshot: $1, reverse: reverse == 1)
					let result = rows.map {
						(results: ResultSet) -> Data in
						var resultTuple = Tuple()
						for (key,value) in results.rows {
							resultTuple.append(key.data)
							resultTuple.append(value.data)
						}
						return resultTuple.databaseValue.data
					}
					return result
				}
			}
		case .getRangeSelector:
			let beginSelector = popTuple().map { (anchor: DatabaseValue, orEqual: Int, offset: Int) in
				KeySelector(anchor: anchor, orEqual: orEqual, offset: offset)
			}
			let endSelector = popTuple().map { (anchor: DatabaseValue, orEqual: Int, offset: Int) in
				KeySelector(anchor: anchor, orEqual: orEqual, offset: offset)
			}
			let otherValues = popTuple() as EventLoopFuture<(Int, Int, Int, DatabaseValue)>
			let allValues = beginSelector.flatMap { lhs in endSelector.map { rhs in (lhs, rhs) } }
				.flatMap { lhs in otherValues.map { rhs in (lhs.0, lhs.1, rhs.0, rhs.1, rhs.2, rhs.3) } }

			signal = allValues.flatMapThrowing { (from, to, limit, reverse, streamingModeNumber, prefix) in
				try self.performOperation(command) {
					guard let streamingMode = StreamingMode(rawValue: Int32(streamingModeNumber)) else {
						throw ExecutionError.IllegalStreamingMode
					}
					
					let results = $0.readSelectors(from: from, to: to, limit: limit,
												mode: streamingMode, snapshot: $1, reverse: reverse == 1
					)
					let tuple = results.map {
						(results: ResultSet) -> Data in
						var resultTuple = Tuple()
						for (key,value) in results.rows {
							if !key.hasPrefix(prefix) { continue }
							resultTuple.append(key.data)
							resultTuple.append(value.data)
						}
						return resultTuple.databaseValue.data
					}
					return tuple
				}
			}
		case .getReadVersion:
			signal = self.currentTransaction.getReadVersion().map {
				self.lastSeenVersion = $0
				self.push(value: Data(Array("GOT_READ_VERSION".utf8)))
				}.recover {
					self.push(future: self.connection.eventLoop.makeFailedFuture($0) as EventLoopFuture<String>)
			}
		case .setReadVersion:
			currentTransaction.setReadVersion(self.lastSeenVersion)
		case .getVersionStamp:
			self.performOperation(command) {
				transaction, _ in
				transaction.getVersionStamp().map { $0.data }
			}
		case .set:
			signal = self.popTuple().map { (key: DatabaseValue, value: DatabaseValue) in
				self.performOperation(command, providesValue: false) {
					transaction, _ in
					
					transaction.store(key: key, value: value)
					return self.connection.eventLoop.makeSucceededFuture(StackMachine.resultNotPresent)
				}
			}
		case .clear:
			signal = self.popAndCast().map { (key: DatabaseValue) in
				self.performOperation(command, providesValue: false) {
					transaction, _ in
					transaction.clear(key: key)
					return self.connection.eventLoop.makeSucceededFuture(StackMachine.resultNotPresent)
				}
			}
		case .clearRange:
			signal = self.popTuple().map { (key1: DatabaseValue, key2: DatabaseValue) in
				self.performOperation(command, providesValue: false) {
					transaction, _ in
					if key2 < key1 {
						return self.connection.eventLoop.makeFailedFuture(ClusterDatabaseConnection.FdbApiError(2005))
					}
					transaction.clear(range: key1 ..< key2)
					return self.connection.eventLoop.makeSucceededFuture(StackMachine.resultNotPresent)
				}
			}
		case .clearRangeStartingWith:
			signal = self.popAndCast().map { (start: DatabaseValue) in
				var end = start
				end.data.append(0xFF)
				self.performOperation(command, providesValue: false) {
					transaction, _ in
					transaction.clear(range: start ..< end)
					return self.connection.eventLoop.makeSucceededFuture(StackMachine.resultNotPresent)
				}
			}
		case .atomicOperation:
			signal = self.popTuple().flatMapThrowing { (operationNameCaps: String, key: DatabaseValue, value: DatabaseValue) in
				var capitalizeNext = false
				
				var operationName = ""
				for character in operationNameCaps.lowercased() {
					if character == "_" {
						capitalizeNext = true
					}
					else if capitalizeNext {
						operationName += String(character).uppercased()
						capitalizeNext = false
					}
					else {
						operationName.append(character)
					}
				}
				
				let numbers: CountableClosedRange<Int> = 0...20
				let allOps = numbers.compactMap {
					(num: Int) -> MutationType? in
					return MutationType(rawValue: UInt32(num))
				}
				let _operation = allOps.filter {
					String(describing: $0) == operationName
					}.first
				guard let operation = _operation else {
					print("Cannot perform atomic operation \(operationName)")
					throw ExecutionError.IllegalValueType
				}
				self.currentTransaction.performAtomicOperation(operation: operation, key: key, value: value)
				if command.direct {
					self.push(value: StackMachine.resultNotPresent)
				}
			}
		case .addReadConflictOnKey:
			signal = self.popAndCast().map { (key: DatabaseValue) in
				self.push(value: Data(Array("SET_CONFLICT_KEY".utf8)))
				self.currentTransaction.addReadConflict(key: key)
			}
		case .addReadConflictOnRange:
			signal = self.popTuple().map { (key1: DatabaseValue, key2: DatabaseValue) in
				if key2 < key1 {
					self.push(future: self.connection.eventLoop.makeFailedFuture(ClusterDatabaseConnection.FdbApiError(2005)) as EventLoopFuture<Data>)
				}
				else {
					self.currentTransaction.addReadConflict(on: key1 ..< key2)
					self.push(value: Data(Array("SET_CONFLICT_RANGE".utf8)))
				}
			}
		case .addWriteConflictOnKey:
			signal = self.popAndCast().map { (key: DatabaseValue) in
				self.currentTransaction.addWriteConflict(key: key)
				self.push(value: Data(Array("SET_CONFLICT_KEY".utf8)))
			}
		case .addWriteConflictOnRange:
			signal = self.popTuple().map { (key1: DatabaseValue, key2: DatabaseValue) in
				if key2 < key1 {
					self.push(future: self.connection.eventLoop.makeFailedFuture(ClusterDatabaseConnection.FdbApiError(2005)) as EventLoopFuture<Data>)
				}
				else {
					self.currentTransaction.addWriteConflict(on: key1 ..< key2)
					self.push(value: Data(Array("SET_CONFLICT_RANGE".utf8)))
				}
			}
		case .disableWriteConflict:
			currentTransaction.setOption(.nextWriteNoWriteConflictRange)
		case .commit:
			self.push(future: connection.commit(transaction: currentTransaction).map { _ in StackMachine.resultNotPresent })
		case .reset:
			currentTransaction.reset()
		case .cancel:
			currentTransaction.cancel()
		case .getCommittedVersion:
			signal = currentTransaction.getCommittedVersion().map {
				self.lastSeenVersion = $0
				self.push(value: Data(Array("GOT_COMMITTED_VERSION".utf8)))
			}
		case .waitFuture:
			guard let future = self.stack.last?.value else {
				throw ExecutionError.PoppedEmptyStack
			}
			signal = future.map { _ in }
				.flatMapErrorThrowing {
					if $0 is ClusterDatabaseConnection.FdbApiError {
						return Void()
					}
					else {
						throw $0
					}
			}
		case .tuplePack:
			signal = self.popAndCast().map { (count: Int) in
				let futures = (0 ..< count).map { _ in self.pop() }
				let combinedValues = EventLoopFuture<Any>.accumulating(futures: futures, eventLoop: self.connection.eventLoop)
				let result = combinedValues.flatMapThrowing { entries -> Data in
					var result = Tuple()
					for entry in entries {
						result.append(contentsOf: try self.unsafeTuplePack(entry))
					}
					return result.databaseValue.data
				}
				self.push(future: result)
			}
		case .tupleUnpack:
			signal = popAndCast().flatMapThrowing { (tuple: Tuple) in
				for index in 0 ..< tuple.count {
					let subTuple = try tuple.read(range: index ..< index + 1)
					self.push(value: subTuple.databaseValue.data)
				}
			}
		case .tupleRange:
			signal = popAndCast().map { (count: Int) in
				let futures = (0 ..< count).map { _ in self.pop() as EventLoopFuture<Any> }
				let combinedFuture = EventLoopFuture<Any>.accumulating(futures: futures, eventLoop: self.connection.eventLoop)
				let tuple = combinedFuture.flatMapThrowing { (entries: [Any]) -> Range<Tuple> in
					var tuple = Tuple()
					for entry in entries {
						tuple.append(contentsOf: try self.unsafeTuplePack(entry))
					}
					return tuple.childRange
				}
				
				self.push(future: tuple.map { $0.lowerBound.databaseValue.data })
				self.push(future: tuple.map { $0.upperBound.databaseValue.data })
			}
		case .tupleSort:
			let values = popAndCast().flatMap { (count: Int) -> EventLoopFuture<[Data]> in
				let futures = (0 ..< count).map { _ in
					self.pop().map { $0 as! Data }
				}
				return EventLoopFuture<[Data]>.accumulating(futures: futures, eventLoop: self.connection.eventLoop)
			}
			signal = values.map {
				let tuples = $0.map { Tuple(databaseValue: DatabaseValue($0)) }.sorted()
				for tuple in tuples {
					self.push(value: tuple.databaseValue.data)
				}
			}
		case .encodeFloat:
			signal = self.popAndCast().map { (value: Data) in
				assert(value.count == 4)
				var bits: UInt32 = 0
				for byte in value {
					bits = (bits << 8) | UInt32(byte)
				}
				self.push(value: Float32(bitPattern: bits))
			}
		case .encodeDouble:
			signal = self.popAndCast().map { (value: Data) in
				assert(value.count == 8)
				var bits: UInt64 = 0
				for byte in value {
					bits = (bits << 8) | UInt64(byte)
				}
				self.push(value: Float64(bitPattern: bits))
			}
		case .decodeFloat:
			signal = self.popAndCast().map { (value: Float32) in
				let bits = value.bitPattern
				let data = Data([
					UInt8((bits >> 24) & 0xFF),
					UInt8((bits >> 16) & 0xFF),
					UInt8((bits >> 8) & 0xFF),
					UInt8(bits & 0xFF),
					])
				self.push(value: data)
			}
		case .decodeDouble:
			signal = self.popAndCast().map { (value: Float64) in
				let bits = value.bitPattern
				let data = Data([
					UInt8((bits >> 56) & 0xFF),
					UInt8((bits >> 48) & 0xFF),
					UInt8((bits >> 40) & 0xFF),
					UInt8((bits >> 32) & 0xFF),
					UInt8((bits >> 24) & 0xFF),
					UInt8((bits >> 16) & 0xFF),
					UInt8((bits >> 8) & 0xFF),
					UInt8(bits & 0xFF),
				] as [UInt8])
				self.push(value: data)
			}
		case .startThread:
			signal = self.popAndCast().map { (prefixData: DatabaseValue) in
				startStackMachineInThread(prefix: prefixData.data)
			}
		case .waitEmpty:
			signal = self.popAndCast().flatMap { (start: DatabaseValue) -> EventLoopFuture<Void> in
				var end = DatabaseValue(start.data)
				end.data.append(0xFF)
				return self.connection.transaction {
					return $0.read(range: start ..< end).flatMapThrowing { results in
						if results.rows.count == 0 {
							throw ClusterDatabaseConnection.FdbApiError(1020)
						}
					}
					}.map { _ in
						self.push(value: Data(Array("WAITED_FOR_EMPTY".utf8)))
				}
			}
		case .unitTests:
			print("Unit tests are handled in their own binary")
		}
		return signal
	}
	
	public static var connection: DatabaseConnection?
}

private var STACK_MACHINE_LIST = [StackMachine]()
