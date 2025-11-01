/*
 * StringIncrementTests.swift
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

import Testing
@testable import FoundationDB

@Suite("String Increment (strinc) Tests")
struct StringIncrementTests {

    // MARK: - Basic strinc() Tests

    @Test("strinc increments normal byte array")
    func strincNormal() throws {
        let input: FDB.Bytes = [0x01, 0x02, 0x03]
        let result = try input.strinc()
        #expect(result == [0x01, 0x02, 0x04])
    }

    @Test("strinc increments single byte")
    func strincSingleByte() throws {
        let input: FDB.Bytes = [0x42]
        let result = try input.strinc()
        #expect(result == [0x43])
    }

    @Test("strinc strips trailing 0xFF and increments")
    func strincWithTrailing0xFF() throws {
        let input: FDB.Bytes = [0x01, 0x02, 0xFF]
        let result = try input.strinc()
        #expect(result == [0x01, 0x03])
    }

    @Test("strinc strips multiple trailing 0xFF bytes")
    func strincWithMultipleTrailing0xFF() throws {
        let input: FDB.Bytes = [0x01, 0xFF, 0xFF]
        let result = try input.strinc()
        #expect(result == [0x02])
    }

    @Test("strinc handles complex case")
    func strincComplex() throws {
        let input: FDB.Bytes = [0x01, 0x02, 0xFF, 0xFF, 0xFF]
        let result = try input.strinc()
        #expect(result == [0x01, 0x03])
    }

    @Test("strinc handles 0xFE correctly")
    func strinc0xFE() throws {
        let input: FDB.Bytes = [0x01, 0xFE]
        let result = try input.strinc()
        #expect(result == [0x01, 0xFF])
    }

    @Test("strinc handles overflow to 0xFF")
    func strincOverflowTo0xFF() throws {
        let input: FDB.Bytes = [0x00, 0xFE]
        let result = try input.strinc()
        #expect(result == [0x00, 0xFF])
    }

    // MARK: - Error Cases

    @Test("strinc throws error on all 0xFF bytes")
    func strincAllFF() {
        let input: FDB.Bytes = [0xFF, 0xFF]

        do {
            _ = try input.strinc()
            Issue.record("Should throw error for all-0xFF input")
        } catch let error as SubspaceError {
            if case .cannotIncrementKey(let message) = error {
                #expect(message.contains("0xFF"))
            } else {
                Issue.record("Wrong error case")
            }
        } catch {
            Issue.record("Wrong error type: \(error)")
        }
    }

    @Test("strinc throws error on empty array")
    func strincEmpty() {
        let input: FDB.Bytes = []

        do {
            _ = try input.strinc()
            Issue.record("Should throw error for empty input")
        } catch let error as SubspaceError {
            if case .cannotIncrementKey(let message) = error {
                #expect(message.contains("0xFF"))
            } else {
                Issue.record("Wrong error case")
            }
        } catch {
            Issue.record("Wrong error type: \(error)")
        }
    }

    @Test("strinc throws error on single 0xFF")
    func strincSingle0xFF() {
        let input: FDB.Bytes = [0xFF]

        do {
            _ = try input.strinc()
            Issue.record("Should throw error for single 0xFF")
        } catch let error as SubspaceError {
            if case .cannotIncrementKey = error {
                // Expected
            } else {
                Issue.record("Wrong error case")
            }
        } catch {
            Issue.record("Wrong error type: \(error)")
        }
    }

    // MARK: - Cross-Reference with Official Implementations

    @Test("strinc matches Java ByteArrayUtil.strinc behavior")
    func strincJavaCompatibility() throws {
        // Test cases from Java implementation
        let testCases: [(input: FDB.Bytes, expected: FDB.Bytes)] = [
            ([0x01], [0x02]),
            ([0x01, 0x02], [0x01, 0x03]),
            ([0x01, 0xFF], [0x02]),
            ([0xFE], [0xFF]),
            ([0x00, 0xFF], [0x01]),
            ([0x01, 0x02, 0xFF, 0xFF], [0x01, 0x03])
        ]

        for (input, expected) in testCases {
            let result = try input.strinc()
            #expect(result == expected,
                   "strinc(\(input.map { String(format: "%02x", $0) }.joined(separator: " "))) should equal \(expected.map { String(format: "%02x", $0) }.joined(separator: " "))")
        }
    }

    @Test("strinc matches Go fdb.Strinc behavior")
    func strincGoCompatibility() throws {
        // Test cases from Go implementation
        let testCases: [(input: FDB.Bytes, expected: FDB.Bytes)] = [
            ([0x01, 0x00], [0x01, 0x01]),
            ([0x01, 0x00, 0xFF], [0x01, 0x01]),
            ([0xFE, 0xFF, 0xFF], [0xFF])
        ]

        for (input, expected) in testCases {
            let result = try input.strinc()
            #expect(result == expected)
        }
    }

    // MARK: - Edge Cases

    @Test("strinc handles byte overflow correctly")
    func strincByteOverflow() throws {
        // When incrementing 0xFF, it wraps to 0x00 (via &+ operator)
        // But since we increment the LAST non-0xFF byte, this should work
        let input: FDB.Bytes = [0x01, 0xFF, 0xFF]
        let result = try input.strinc()
        #expect(result == [0x02])
    }

    @Test("strinc preserves leading bytes")
    func strincPreservesLeading() throws {
        let input: FDB.Bytes = [0xAA, 0xBB, 0xCC, 0xFF, 0xFF]
        let result = try input.strinc()
        #expect(result == [0xAA, 0xBB, 0xCD])
    }

    @Test("strinc works with maximum non-0xFF value")
    func strincMaxNon0xFF() throws {
        let input: FDB.Bytes = [0xFE]
        let result = try input.strinc()
        #expect(result == [0xFF])
    }
}
