/*
 * TestHelpers.swift
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
@testable import FoundationDB
import XCTest
import NIO

enum TestHelpers {
	static func resourceFolder() -> String {
		let environment = PlatformShims.environment
		return environment["PROJECT_DIR"] ?? "."
	}
}

extension XCTestCase {
	public func configure() {
	}
	
	public func runLoop(_ loop: EmbeddedEventLoop, block: @escaping () -> Void) {
		loop.execute(block)
		loop.run()
	}
}


#if os(macOS) || os(iOS) || os(tvOS) || os(watchOS)
#else
extension XCTestCase {
	fileprivate func recordFailure(withDescription description: String, inFile file: String, atLine line: Int, expected: Bool) {
		self.recordFailure(withDescription: description, inFile: file, atLine: line, expected: expected)
	}
}
#endif

extension EventLoopFuture {
	/**
	This method catches errors from this future by recording them on a test
	case.
	
	- parameter testCase:   The test case that is running.
	- parameter file:       The file that the errors should appear on.
	- parameter line:       The line that the errors should appear on.
	*/
	public func `catch`(_ testCase: XCTestCase, file: String = #file, line: Int = #line) {
		_ = self.map { _ in Void() }
			.recover {
				testCase.recordFailure(withDescription: "\($0)", inFile: file, atLine: line, expected: true)
		}
	}
}

enum TestError: Error {
	case test
}
