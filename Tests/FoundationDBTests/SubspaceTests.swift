/*
 * SubspaceTests.swift
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

@Suite("Subspace Tests")
struct SubspaceTests {
    @Test("Subspace creation creates non-empty prefix")
    func subspaceCreation() {
        let subspace = Subspace(rootPrefix: "test")
        #expect(!subspace.prefix.isEmpty)
    }

    @Test("Nested subspace prefix includes root prefix")
    func nestedSubspace() {
        let root = Subspace(rootPrefix: "test")
        let nested = root.subspace(Int64(1), "child")

        #expect(nested.prefix.starts(with: root.prefix))
        #expect(nested.prefix.count > root.prefix.count)
    }

    @Test("Pack/unpack preserves subspace prefix")
    func packUnpack() throws {
        let subspace = Subspace(rootPrefix: "test")
        let tuple = Tuple("key", Int64(123))

        let packed = subspace.pack(tuple)
        _ = try subspace.unpack(packed)

        // Verify the packed key has the subspace prefix
        #expect(packed.starts(with: subspace.prefix))
    }

    @Test("Range returns correct begin and end keys")
    func range() {
        let subspace = Subspace(rootPrefix: "test")
        let (begin, end) = subspace.range()

        // Begin should be prefix + 0x00
        #expect(begin == subspace.prefix + [0x00])

        // End should be prefix + 0xFF
        #expect(end == subspace.prefix + [0xFF])

        // Verify range is non-empty (begin < end)
        #expect(begin.lexicographicallyPrecedes(end))
    }

    @Test("Range handles 0xFF suffix correctly")
    func rangeWithTrailing0xFF() {
        let subspace = Subspace(prefix: [0x01, 0xFF])
        let (begin, end) = subspace.range()

        // Correct: append 0x00 and 0xFF
        #expect(begin == [0x01, 0xFF, 0x00])
        #expect(end == [0x01, 0xFF, 0xFF])

        // Verify that a key like [0x01, 0xFF, 0x01] is within the range
        let testKey: FDB.Bytes = [0x01, 0xFF, 0x01]
        #expect(!testKey.lexicographicallyPrecedes(begin))  // testKey >= begin
        #expect(testKey.lexicographicallyPrecedes(end))      // testKey < end
    }

    @Test("Range handles multiple trailing 0xFF bytes")
    func rangeWithMultipleTrailing0xFF() {
        let subspace = Subspace(prefix: [0x01, 0x02, 0xFF, 0xFF])
        let (begin, end) = subspace.range()

        // Correct: append 0x00 and 0xFF
        #expect(begin == [0x01, 0x02, 0xFF, 0xFF, 0x00])
        #expect(end == [0x01, 0x02, 0xFF, 0xFF, 0xFF])
    }

    @Test("Range handles all-0xFF prefix")
    func rangeWithAll0xFF() {
        let subspace = Subspace(prefix: [0xFF, 0xFF])
        let (begin, end) = subspace.range()

        // Correct: append 0x00 and 0xFF even for all-0xFF prefix
        #expect(begin == [0xFF, 0xFF, 0x00])
        #expect(end == [0xFF, 0xFF, 0xFF])

        // Verify range is valid (begin < end)
        #expect(begin.lexicographicallyPrecedes(end))
    }

    @Test("Range handles single 0xFF prefix")
    func rangeWithSingle0xFF() {
        let subspace = Subspace(prefix: [0xFF])
        let (begin, end) = subspace.range()

        // Note: [0xFF] is the start of system key space
        // but range() still follows the pattern
        #expect(begin == [0xFF, 0x00])
        #expect(end == [0xFF, 0xFF])

        // Verify range is valid
        #expect(begin.lexicographicallyPrecedes(end))
    }

    @Test("Range handles special characters")
    func rangeSpecialCharacters() {
        let subspace = Subspace(rootPrefix: "test_special_chars")
        let (begin, end) = subspace.range()

        // begin should be prefix + [0x00]
        #expect(begin == subspace.prefix + [0x00])
        // end should be prefix + [0xFF]
        #expect(end == subspace.prefix + [0xFF])
        #expect(end != begin)
        #expect(end.count > 0)
    }

    @Test("Range handles empty string root prefix")
    func rangeEmptyStringPrefix() {
        // Empty string encodes to [0x02, 0x00] in tuple encoding
        let subspace = Subspace(rootPrefix: "")
        let (begin, end) = subspace.range()

        // Prefix should be tuple-encoded empty string
        let encodedEmpty = Tuple("").encode()
        #expect(begin == encodedEmpty + [0x00])
        #expect(end == encodedEmpty + [0xFF])
    }

    @Test("Range handles truly empty prefix")
    func rangeTrulyEmptyPrefix() {
        // Directly construct subspace with empty byte array
        let subspace = Subspace(prefix: [])
        let (begin, end) = subspace.range()

        // Should cover all user key space
        #expect(begin == [0x00])
        #expect(end == [0xFF])
    }

    @Test("Contains checks if key belongs to subspace")
    func contains() {
        let subspace = Subspace(rootPrefix: "test")
        let tuple = Tuple("key")
        let key = subspace.pack(tuple)

        #expect(subspace.contains(key))

        let otherSubspace = Subspace(rootPrefix: "other")
        #expect(!otherSubspace.contains(key))
    }

    // MARK: - prefixRange() Tests

    @Test("prefixRange returns prefix and strinc as bounds")
    func prefixRange() throws {
        let subspace = Subspace(prefix: [0x01, 0x02])
        let (begin, end) = try subspace.prefixRange()

        // Begin should be the prefix itself
        #expect(begin == [0x01, 0x02])

        // End should be strinc(prefix) = [0x01, 0x03]
        #expect(end == [0x01, 0x03])
    }

    @Test("prefixRange handles trailing 0xFF correctly")
    func prefixRangeWithTrailing0xFF() throws {
        let subspace = Subspace(prefix: [0x01, 0xFF])
        let (begin, end) = try subspace.prefixRange()

        // Begin is the prefix
        #expect(begin == [0x01, 0xFF])

        // End should be strinc([0x01, 0xFF]) = [0x02]
        #expect(end == [0x02])

        // Verify that keys like [0x01, 0xFF, 0xFF, 0x00] are included
        let testKey: FDB.Bytes = [0x01, 0xFF, 0xFF, 0x00]
        #expect(!testKey.lexicographicallyPrecedes(begin))  // testKey >= begin
        #expect(testKey.lexicographicallyPrecedes(end))      // testKey < end
    }

    @Test("prefixRange handles multiple trailing 0xFF bytes")
    func prefixRangeWithMultipleTrailing0xFF() throws {
        let subspace = Subspace(prefix: [0x01, 0x02, 0xFF, 0xFF])
        let (begin, end) = try subspace.prefixRange()

        #expect(begin == [0x01, 0x02, 0xFF, 0xFF])
        #expect(end == [0x01, 0x03])  // strinc strips trailing 0xFF and increments
    }

    @Test("prefixRange throws error for all-0xFF prefix")
    func prefixRangeWithAll0xFF() {
        let subspace = Subspace(prefix: [0xFF, 0xFF])

        do {
            _ = try subspace.prefixRange()
            Issue.record("Should throw error for all-0xFF prefix")
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

    @Test("prefixRange throws error for empty prefix")
    func prefixRangeWithEmptyPrefix() {
        let subspace = Subspace(prefix: [])

        do {
            _ = try subspace.prefixRange()
            Issue.record("Should throw error for empty prefix")
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

    @Test("prefixRange vs range comparison for raw binary prefix")
    func prefixRangeVsRangeComparison() throws {
        // Raw binary prefix ending in 0xFF
        let subspace = Subspace(prefix: [0x01, 0xFF])

        // range() uses prefix + [0x00] / prefix + [0xFF]
        let (rangeBegin, rangeEnd) = subspace.range()
        #expect(rangeBegin == [0x01, 0xFF, 0x00])
        #expect(rangeEnd == [0x01, 0xFF, 0xFF])

        // prefixRange() uses prefix / strinc(prefix)
        let (prefixBegin, prefixEnd) = try subspace.prefixRange()
        #expect(prefixBegin == [0x01, 0xFF])
        #expect(prefixEnd == [0x02])

        // Keys that are included in prefixRange but NOT in range
        let excludedByRange: FDB.Bytes = [0x01, 0xFF, 0xFF, 0x00]

        // Not in range() - excluded because >= rangeEnd
        #expect(!excludedByRange.lexicographicallyPrecedes(rangeEnd))

        // But IS in prefixRange() - included because < prefixEnd
        #expect(!excludedByRange.lexicographicallyPrecedes(prefixBegin))  // >= begin
        #expect(excludedByRange.lexicographicallyPrecedes(prefixEnd))     // < end
    }

    @Test("prefixRange includes the prefix itself as a key")
    func prefixRangeIncludesPrefix() throws {
        let subspace = Subspace(prefix: [0x01, 0x02])
        let (begin, end) = try subspace.prefixRange()

        // The prefix itself is included (begin is inclusive)
        let prefixKey = subspace.prefix
        #expect(!prefixKey.lexicographicallyPrecedes(begin))  // >= begin
        #expect(prefixKey.lexicographicallyPrecedes(end))      // < end
    }

    @Test("prefixRange works with single byte prefix")
    func prefixRangeSingleByte() throws {
        let subspace = Subspace(prefix: [0x42])
        let (begin, end) = try subspace.prefixRange()

        #expect(begin == [0x42])
        #expect(end == [0x43])
    }

    @Test("prefixRange works with 0xFE prefix")
    func prefixRange0xFE() throws {
        let subspace = Subspace(prefix: [0xFE])
        let (begin, end) = try subspace.prefixRange()

        #expect(begin == [0xFE])
        #expect(end == [0xFF])
    }

    @Test("prefixRange for tuple-encoded data")
    func prefixRangeTupleEncoded() throws {
        // Tuple-encoded prefix (no trailing 0xFF possible)
        let subspace = Subspace(rootPrefix: "users")
        let (begin, end) = try subspace.prefixRange()

        // Begin is the tuple-encoded prefix
        #expect(begin == subspace.prefix)

        // End is strinc(prefix) - should work fine
        #expect(end.count >= begin.count)  // Could be shorter or equal length
        #expect(!end.lexicographicallyPrecedes(begin))  // end >= begin
    }
}
