/*
 * Error.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2016-2025 Apple Inc. and the FoundationDB project authors
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

// TODO: These should be auto-generated like other bindings
enum FDBErrorCode: Int32, CaseIterable {
    case notCommitted = 1007
    case transactionTooOld = 1020
    case futureVersion = 1021
    case transactionCancelled = 1025
    case transactionTimedOut = 1031
    case processBehind = 1037
    case operationCancelled = 1101
    case tagThrottled = 1213
    case internalError = 2000
    case networkError = 2201
    case clientError = 4100
    case unknownError = 9999
}

public struct FDBError: Error, CustomStringConvertible {
    public let code: Int32

    public init(code: Int) {
        self.code = Int32(code)
    }

    init(code: Int32) {
        self.code = code
    }

    init(_ errorCode: FDBErrorCode) {
        code = errorCode.rawValue
    }

    public var description: String {
        guard let errorCString = fdb_get_error(code) else {
            return "Unknown FDB error: \(code)"
        }
        return String(cString: errorCString)
    }

    public var isRetryable: Bool {
        switch code {
        case FDBErrorCode.notCommitted.rawValue:
            return true
        case FDBErrorCode.transactionTooOld.rawValue:
            return true
        case FDBErrorCode.futureVersion.rawValue:
            return true
        case FDBErrorCode.transactionCancelled.rawValue:
            return false
        case FDBErrorCode.transactionTimedOut.rawValue:
            return true
        case FDBErrorCode.processBehind.rawValue:
            return true
        case FDBErrorCode.tagThrottled.rawValue:
            return true
        default:
            return false
        }
    }
}
