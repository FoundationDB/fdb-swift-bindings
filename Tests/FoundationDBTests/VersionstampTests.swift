/*
 * VersionstampTests.swift
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

import Foundation
import Testing
@testable import FoundationDB

@Suite("Versionstamp Tests")
struct VersionstampTests {

    // MARK: - Basic Versionstamp Tests

    @Test("Versionstamp incomplete creation")
    func testIncompleteCreation() {
        let vs = Versionstamp.incomplete(userVersion: 0)

        #expect(!vs.isComplete)
        #expect(vs.userVersion == 0)

        let bytes = vs.toBytes()
        #expect(bytes.count == 12)
        #expect(bytes.prefix(10).allSatisfy { $0 == 0xFF })
    }

    @Test("Versionstamp incomplete with user version")
    func testIncompleteWithUserVersion() {
        let vs = Versionstamp.incomplete(userVersion: 42)

        #expect(!vs.isComplete)
        #expect(vs.userVersion == 42)

        let bytes = vs.toBytes()
        #expect(bytes.count == 12)
        #expect(bytes.prefix(10).allSatisfy { $0 == 0xFF })

        // User version is big-endian
        #expect(bytes[10] == 0x00)
        #expect(bytes[11] == 0x2A)  // 42 in hex
    }

    @Test("Versionstamp complete creation")
    func testCompleteCreation() {
        let trVersion: [UInt8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A]
        let vs = Versionstamp(transactionVersion: trVersion, userVersion: 100)

        #expect(vs.isComplete)
        #expect(vs.userVersion == 100)

        let bytes = vs.toBytes()
        #expect(bytes.count == 12)
        #expect(Array(bytes.prefix(10)) == trVersion)
    }

    @Test("Versionstamp fromBytes incomplete")
    func testFromBytesIncomplete() throws {
        let bytes: FDB.Bytes = [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,  // incomplete
            0x00, 0x10  // userVersion = 16
        ]

        let vs = try Versionstamp.fromBytes(bytes)

        #expect(!vs.isComplete)
        #expect(vs.userVersion == 16)
    }

    @Test("Versionstamp fromBytes complete")
    func testFromBytesComplete() throws {
        let bytes: FDB.Bytes = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,  // complete
            0x00, 0x20  // userVersion = 32
        ]

        let vs = try Versionstamp.fromBytes(bytes)

        #expect(vs.isComplete)
        #expect(vs.userVersion == 32)
        #expect(vs.transactionVersion == [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A])
    }

    @Test("Versionstamp equality")
    func testEquality() {
        let vs1 = Versionstamp.incomplete(userVersion: 10)
        let vs2 = Versionstamp.incomplete(userVersion: 10)
        let vs3 = Versionstamp.incomplete(userVersion: 20)

        #expect(vs1 == vs2)
        #expect(vs1 != vs3)
    }

    @Test("Versionstamp hashable")
    func testHashable() {
        let vs1 = Versionstamp.incomplete(userVersion: 5)
        let vs2 = Versionstamp.incomplete(userVersion: 5)

        var set: Set<Versionstamp> = []
        set.insert(vs1)
        set.insert(vs2)

        #expect(set.count == 1)
    }

    @Test("Versionstamp description")
    func testDescription() {
        let incompleteVs = Versionstamp.incomplete(userVersion: 100)
        #expect(incompleteVs.description.contains("incomplete"))

        let trVersion: [UInt8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A]
        let completeVs = Versionstamp(transactionVersion: trVersion, userVersion: 200)
        #expect(completeVs.description.contains("0102030405060708090a"))
    }

    // MARK: - TupleElement Tests

    @Test("Versionstamp encodeTuple")
    func testEncodeTuple() {
        let vs = Versionstamp.incomplete(userVersion: 0)
        let encoded = vs.encodeTuple()

        #expect(encoded.count == 13)  // 1 byte type code + 12 bytes versionstamp
        #expect(encoded[0] == 0x33)  // TupleTypeCode.versionstamp
        #expect(encoded.suffix(12) == vs.toBytes())
    }

    @Test("Versionstamp decodeTuple")
    func testDecodeTuple() throws {
        let vs = Versionstamp.incomplete(userVersion: 42)
        let encoded = vs.encodeTuple()

        var offset = 1  // Skip type code
        let decoded = try Versionstamp.decodeTuple(from: encoded, at: &offset)

        #expect(decoded == vs)
        #expect(offset == 13)
    }

    // MARK: - Tuple.packWithVersionstamp() Tests

    @Test("Tuple packWithVersionstamp basic")
    func testPackWithVersionstampBasic() throws {
        let vs = Versionstamp.incomplete(userVersion: 0)
        let tuple = Tuple("prefix", vs)

        let packed = try tuple.packWithVersionstamp()

        // Verify structure:
        // - String "prefix" encoded
        // - Versionstamp 0x33 + 12 bytes
        // - 4-byte offset (little-endian)
        #expect(packed.count > 13 + 4)

        // Last 4 bytes should be the offset
        #expect(packed.count >= 4, "Packed data must have at least 4 bytes for offset")
        let offsetBytes = Array(packed.suffix(4))
        #expect(offsetBytes.count == 4, "Offset must be exactly 4 bytes")

        let offset = offsetBytes.withUnsafeBytes { $0.load(as: UInt32.self).littleEndian }

        // Offset should point to the start of the 10-byte transaction version
        // (after type code 0x33)
        #expect(offset > 0)
        #expect(Int(offset) < packed.count - 4)
    }

    @Test("Tuple packWithVersionstamp with prefix")
    func testPackWithVersionstampWithPrefix() throws {
        let vs = Versionstamp.incomplete(userVersion: 0)
        let tuple = Tuple(vs)
        let prefix: FDB.Bytes = [0x01, 0x02, 0x03]

        let packed = try tuple.packWithVersionstamp(prefix: prefix)

        // Verify prefix is prepended
        #expect(Array(packed.prefix(3)) == prefix)

        // Last 4 bytes should be the offset
        #expect(packed.count >= 4, "Packed data must have at least 4 bytes for offset")
        let offsetBytes = Array(packed.suffix(4))
        #expect(offsetBytes.count == 4, "Offset must be exactly 4 bytes")

        let offset = offsetBytes.withUnsafeBytes { $0.load(as: UInt32.self).littleEndian }

        // Offset should account for prefix length
        #expect(offset == 3 + 1)  // prefix (3) + type code (1)
    }

    @Test("Tuple packWithVersionstamp no incomplete error")
    func testPackWithVersionstampNoIncomplete() {
        let trVersion: [UInt8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A]
        let completeVs = Versionstamp(transactionVersion: trVersion, userVersion: 0)
        let tuple = Tuple("prefix", completeVs)

        do {
            _ = try tuple.packWithVersionstamp()
            Issue.record("Should throw error when no incomplete versionstamp")
        } catch {
            #expect(error is TupleError)
        }
    }

    @Test("Tuple packWithVersionstamp multiple incomplete error")
    func testPackWithVersionstampMultipleIncomplete() {
        let vs1 = Versionstamp.incomplete(userVersion: 0)
        let vs2 = Versionstamp.incomplete(userVersion: 1)
        let tuple = Tuple("prefix", vs1, vs2)

        do {
            _ = try tuple.packWithVersionstamp()
            Issue.record("Should throw error when multiple incomplete versionstamps")
        } catch {
            #expect(error is TupleError)
        }
    }

    @Test("Tuple hasIncompleteVersionstamp")
    func testHasIncompleteVersionstamp() {
        let incompleteVs = Versionstamp.incomplete(userVersion: 0)
        let tuple1 = Tuple("test", incompleteVs)
        #expect(tuple1.hasIncompleteVersionstamp())

        let trVersion: [UInt8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A]
        let completeVs = Versionstamp(transactionVersion: trVersion, userVersion: 0)
        let tuple2 = Tuple("test", completeVs)
        #expect(!tuple2.hasIncompleteVersionstamp())

        let tuple3 = Tuple("test", "no versionstamp")
        #expect(!tuple3.hasIncompleteVersionstamp())
    }

    @Test("Tuple countIncompleteVersionstamps")
    func testCountIncompleteVersionstamps() {
        let vs1 = Versionstamp.incomplete(userVersion: 0)
        let vs2 = Versionstamp.incomplete(userVersion: 1)

        let tuple1 = Tuple(vs1)
        #expect(tuple1.countIncompleteVersionstamps() == 1)

        let tuple2 = Tuple(vs1, "middle", vs2)
        #expect(tuple2.countIncompleteVersionstamps() == 2)

        let tuple3 = Tuple("no versionstamp")
        #expect(tuple3.countIncompleteVersionstamps() == 0)
    }

    @Test("Tuple validateForVersionstamp")
    func testValidateForVersionstamp() throws {
        let vs = Versionstamp.incomplete(userVersion: 0)
        let tuple1 = Tuple(vs)
        try tuple1.validateForVersionstamp()  // Should not throw

        let tuple2 = Tuple("no versionstamp")
        do {
            try tuple2.validateForVersionstamp()
            Issue.record("Should throw when no versionstamp")
        } catch {
            #expect(error is TupleError)
        }

        let vs2 = Versionstamp.incomplete(userVersion: 1)
        let tuple3 = Tuple(vs, vs2)
        do {
            try tuple3.validateForVersionstamp()
            Issue.record("Should throw when multiple versionstamps")
        } catch {
            #expect(error is TupleError)
        }
    }

    // MARK: - Roundtrip Tests (Encode → Decode)

    @Test("Versionstamp roundtrip with complete versionstamp")
    func testVersionstampRoundtripComplete() throws {
        let original = Versionstamp(
            transactionVersion: [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A],
            userVersion: 42
        )
        let tuple = Tuple("prefix", original, "suffix")

        // Encode
        let encoded = tuple.encode()

        // Decode through Tuple.decode()
        let decoded = try Tuple.decode(from: encoded)

        #expect(decoded.count == 3)
        #expect((decoded[0] as? String) == "prefix")
        #expect((decoded[1] as? Versionstamp) == original)
        #expect((decoded[2] as? String) == "suffix")
    }

    @Test("Versionstamp roundtrip with incomplete versionstamp")
    func testVersionstampRoundtripIncomplete() throws {
        let original = Versionstamp.incomplete(userVersion: 123)
        let tuple = Tuple(original)

        // Encode
        let encoded = tuple.encode()

        // Decode
        let decoded = try Tuple.decode(from: encoded)

        #expect(decoded.count == 1)
        let decodedVS = decoded[0] as? Versionstamp
        #expect(decodedVS == original)
        #expect(decodedVS?.isComplete == false)
        #expect(decodedVS?.userVersion == 123)
    }

    @Test("Versionstamp roundtrip mixed tuple")
    func testVersionstampRoundtripMixed() throws {
        let vs = Versionstamp(
            transactionVersion: [0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66],
            userVersion: 999
        )
        let tuple = Tuple(
            "string",
            Int64(12345),
            vs,
            true,
            [UInt8]([0x01, 0x02, 0x03])
        )

        let encoded = tuple.encode()
        let decoded = try Tuple.decode(from: encoded)

        #expect(decoded.count == 5)
        #expect((decoded[0] as? String) == "string")
        #expect((decoded[1] as? Int64) == 12345)
        #expect((decoded[2] as? Versionstamp) == vs)
        #expect((decoded[3] as? Bool) == true)
        #expect((decoded[4] as? FDB.Bytes) == [0x01, 0x02, 0x03])
    }

    @Test("Decode versionstamp with insufficient bytes throws error")
    func testDecodeVersionstampInsufficientBytes() {
        let encoded: FDB.Bytes = [
            TupleTypeCode.versionstamp.rawValue,
            0x01, 0x02, 0x03  // Need 12 bytes but only 3
        ]

        do {
            _ = try Tuple.decode(from: encoded)
            Issue.record("Should throw error for insufficient bytes")
        } catch {
            // Expected - should throw TupleError.invalidEncoding
            #expect(error is TupleError)
        }
    }

    @Test("Multiple versionstamps roundtrip")
    func testMultipleVersionstampsRoundtrip() throws {
        let vs1 = Versionstamp.incomplete(userVersion: 1)
        let vs2 = Versionstamp(
            transactionVersion: [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A],
            userVersion: 2
        )
        let tuple = Tuple(vs1, "middle", vs2)

        let encoded = tuple.encode()
        let decoded = try Tuple.decode(from: encoded)

        #expect(decoded.count == 3)
        #expect((decoded[0] as? Versionstamp) == vs1)
        #expect((decoded[1] as? String) == "middle")
        #expect((decoded[2] as? Versionstamp) == vs2)
    }

    // MARK: - Integration Test Structure
    // Note: These tests require a running FDB cluster
    // Uncomment and adapt when ready for integration testing

    /*
    @Test("Integration: Write and read versionstamped key")
    func testIntegrationWriteReadVersionstampedKey() async throws {
        try await FDBClient.initialize()
        let database = try FDBClient.openDatabase()

        let result = try await database.withTransaction { transaction in
            let vs = Versionstamp.incomplete(userVersion: 0)
            let tuple = Tuple("test_prefix", vs)
            let key = try tuple.packWithVersionstamp()

            // Write versionstamped key
            transaction.atomicOp(
                key: key,
                param: [],
                mutationType: .setVersionstampedKey
            )

            // Get committed versionstamp
            return try await transaction.getVersionstamp()
        }

        // Verify versionstamp was returned
        #expect(result != nil)
        #expect(result!.count == 10)
    }
    */
}
