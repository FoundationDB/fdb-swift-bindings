/*
 * ClusterDatabaseConnection.swift
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
This class provides a connection to a real database cluster.
*/
public class ClusterDatabaseConnection: DatabaseConnection {
	public let eventLoop: EventLoop
	
	/** The reference to the cluster for the C API. */
	internal let cluster: EventLoopFuture<OpaquePointer>
	
	/** The reference to the database for the C API. */
	internal let database: EventLoopFuture<OpaquePointer>
	
	/**
	This type wraps an error thrown by the FoundationDB C API.
	*/
	public struct FdbApiError: Error {
		/** The error code from the API. */
		public let errorCode: Int32
		
		/** The description of the error code. */
		public let description: String
		
		/**
		This initializer creates an API Error.
		
		- parameter errorCode:		The error code from the API.
		*/
		public init(_ errorCode: Int32) {
			self.errorCode = errorCode
			self.description = String(cString: fdb_get_error(errorCode))
		}
		
		/**
		This method wraps around a block that calls an API function that
		can return an error.
		
		If the API function returns an error, this will wrap it in this
		type and throw the error.
		
		- parameter block:		The block that can return the error.
		- throws:				The error from the block.
		*/
		public static func wrapApiError(_ block: @autoclosure () -> fdb_error_t) throws -> Void {
			let result = block()
			if result != 0 {
				throw FdbApiError(result)
			}
		}
	}
	
	/**
	This method opens a connection to a database cluster.
	
	- parameter clusterPath:	The path to the cluster file.
	- throws:					If we're not able to connect to the
								database, this will throw a FutureError.
	*/
	public init(fromClusterFile clusterPath: String? = nil, eventLoop: EventLoop) throws {
		let error = fdb_setup_network()
		if error != 2009 {
			guard error == 0 else { throw FdbApiError(error) }
			fdb_run_network_in_thread()
		}
		
		self.eventLoop = eventLoop
		self.cluster = EventLoopFuture<OpaquePointer>.fromFoundationFuture(eventLoop: eventLoop, future: fdb_create_cluster(clusterPath), fetch: fdb_future_get_cluster)
			.recover {
				fatalError("\($0)")
		}
		self.database = cluster.flatMap {
			EventLoopFuture<OpaquePointer>.fromFoundationFuture(eventLoop: eventLoop, future: fdb_cluster_create_database($0, "DB", 2), fetch: fdb_future_get_database)
			}.recover {
				fatalError("\($0)")
		}
	}
	
	/**
	This method deallocates resources for this connection.
	*/
	deinit {
		_ = self.database.map {
			fdb_database_destroy($0)
		}
		_ = self.cluster.map {
			fdb_cluster_destroy($0)
		}
	}
	
	/**
	This method starts a transaction.
	
	- returns:	The transaction.
	*/
	public func startTransaction() -> Transaction {
		return ClusterTransaction(database: self)
	}
	
	/**
	This method commits a transaction.
	
	This must be a transaction createe on this database.
	
	- parameter transaction:		The transaction to commit.
	- returns:						A future that will fire when the
									transaction has finished committing. If
									the transaction is rejected, the future
									will throw an error.
	*/
	public func commit(transaction: Transaction) -> EventLoopFuture<()> {
		guard let clusterTransaction = transaction as? ClusterTransaction else { return self.eventLoop.makeFailedFuture( FdbApiError(1000)) }
		return clusterTransaction.transaction
			.flatMap { transaction in
				return EventLoopFuture<Void>.fromFoundationFuture(eventLoop: self.eventLoop, future: fdb_transaction_commit(transaction)).map {
					_ = clusterTransaction
				}
		}
	}
}

private var ClusterConnectionNetworkQueue = OperationQueue()

/**
This method sets the runtime API version, which controls whether new client
behavior is adopted when the server is upgraded to a new version.

- parameter version:		The desired API version.
*/
public func setFdbApiVersion(_ version: Int32) {
	fdb_select_api_version_impl(version, FDB_API_VERSION)
}
