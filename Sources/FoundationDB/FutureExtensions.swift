/*
 * FutureExtensions.swift
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

import CFoundationDB
import Foundation
import NIO

extension EventLoopFuture {
	internal static func retrying<T>(eventLoop: EventLoop, onError errorFilter: @escaping (Error) -> EventLoopFuture<Void>, retryBlock: @escaping () throws -> EventLoopFuture<T>) -> EventLoopFuture<T> {
		return eventLoop.submit {
			try retryBlock()
			}.flatMap { return $0 }
			.flatMapError { error in
				let t1: EventLoopFuture<Void> = errorFilter(error)
				let t2: EventLoopFuture<T> = t1.flatMap { _ in self.retrying(eventLoop: eventLoop, onError: errorFilter, retryBlock: retryBlock) }
				return t2
		}
	}
	
	public func thenThrowingFuture<U>(_ callback: @escaping (Any) throws -> EventLoopFuture<U>) -> EventLoopFuture<U> {
		return self.flatMap {
			do {
				return try callback($0)
			}
			catch {
				return self.eventLoop.makeFailedFuture(error)
			}
		}
	}
	
	private static func check<T>(future: OpaquePointer, eventLoop: EventLoop, promise: EventLoopPromise<T>, fetch: @escaping (OpaquePointer) throws -> T) {
		if(fdb_future_is_ready(future) == 0) {
			eventLoop.execute {
				check(future: future, eventLoop: eventLoop, promise: promise, fetch: fetch)
			}
			return
		}
		
		let result: T
		do {
			try ClusterDatabaseConnection.FdbApiError.wrapApiError(fdb_future_get_error(future))
			result = try fetch(future)
		}
		catch {
			fdb_future_destroy(future)
			return promise.fail(error)
		}
		
		fdb_future_destroy(future)
		promise.succeed(result)
	}
	
	internal static func fromFoundationFuture<T>(eventLoop: EventLoop, future: OpaquePointer, fetch: @escaping (OpaquePointer) throws -> T) -> EventLoopFuture<T> {

		let promise: EventLoopPromise<T> = eventLoop.makePromise()
		self.check(future: future, eventLoop: eventLoop, promise: promise, fetch: fetch)
		return promise.futureResult
	}
	
	internal static func fromFoundationFuture<T>(eventLoop: EventLoop, future: OpaquePointer, fetch: @escaping (OpaquePointer, UnsafeMutablePointer<T?>) -> fdb_error_t) -> EventLoopFuture<T> {
		return self.fromFoundationFuture(eventLoop: eventLoop, future: future) { readyFuture in
			var result: T? = nil
			try ClusterDatabaseConnection.FdbApiError.wrapApiError(fetch(readyFuture, &result))
			return result!
		}
	}
	
	internal static func fromFoundationFuture<T>(eventLoop: EventLoop, future: OpaquePointer, default: T, fetch: @escaping (OpaquePointer, UnsafeMutablePointer<T>) -> fdb_error_t) -> EventLoopFuture<T> {
		return self.fromFoundationFuture(eventLoop: eventLoop, future: future) {
			future -> T in
			var result: T = `default`
			try ClusterDatabaseConnection.FdbApiError.wrapApiError(fetch(future, &result))
			return result
		}
	}
	
	public static func accumulating<T>(futures: [EventLoopFuture<T>], eventLoop: EventLoop) -> EventLoopFuture<[T]> {
		return accumulating(futures: futures, base: eventLoop.makeSucceededFuture([]), offset: 0)
	}
	
	private static func accumulating<T>(futures: [EventLoopFuture<T>], base: EventLoopFuture<[T]>, offset: Int) -> EventLoopFuture<[T]> {
		if(offset == futures.count) {
			return base;
		}
		return accumulating(futures: futures, base: base.flatMap { initial in
			futures[offset].map {
				var result = initial
				result.append($0)
				return result
			}
		}, offset: offset + 1)
	}
}

extension EventLoopFuture{
	internal static func fromFoundationFuture(eventLoop: EventLoop, future: OpaquePointer) -> EventLoopFuture<Void> {
		return self.fromFoundationFuture(eventLoop: eventLoop, future: future) { _ in return Void() }
	}
}

/**
This type describes the errors that are thrown by futures in their internal
workings.
*/
enum FdbFutureError: Error {
	/**
	This error is thrown when we want to retry a future that was set up with
	a retryable block.
	*/
	case Retry
	
	/**
	This error is thrown to tell a stream-based future to continue with the
	next iteration.
	*/
	case ContinueStream
	
	/**
	This error is thrown when a future finishes its work block but does not
	have a value.
	*/
	case FutureDidNotProvideValue
}
