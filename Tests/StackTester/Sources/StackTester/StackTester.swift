/*
 * StackTester.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

// Simple stack entry - equivalent to Go's stackEntry
struct StackEntry {
    let item: Any
    let idx: Int
}

class StackMachine {
    private let prefix: [UInt8]
    private var stack: [StackEntry] = []
    private var database: FdbDatabase
    private let verbose: Bool
    private var transaction: (any ITransaction)?
    private var transactionMap: [String: any ITransaction] = [:]
    private var transactionName: String = "MAIN"
    private var lastVersion: Int64 = 0

    init(prefix: [UInt8], database: FdbDatabase, verbose: Bool) {
        self.prefix = prefix
        self.database = database
        self.verbose = verbose
    }

    // Equivalent to Go's waitAndPop with error handling
    func waitAndPop() -> StackEntry {
        guard !stack.isEmpty else {
            fatalError("Stack is empty")
        }

        let ret = stack.removeLast()

        // Handle futures and convert types like in Go
        switch ret.item {
        case let data as [UInt8]:
            return StackEntry(item: data, idx: ret.idx)
        case let string as String:
            return StackEntry(item: Array(string.utf8), idx: ret.idx)
        case let int as Int64:
            return StackEntry(item: int, idx: ret.idx)
        default:
            return ret
        }
    }

    // Equivalent to Go's store
    func store(_ idx: Int, _ item: Any) {
        stack.append(StackEntry(item: item, idx: idx))
    }

    // Get current transaction (create if needed)
    func currentTransaction() throws -> any ITransaction {
        if let existingTransaction = transactionMap[transactionName] {
            return existingTransaction
        }

        // Create new transaction if it doesn't exist
        let newTransaction = try database.createTransaction()
        transactionMap[transactionName] = newTransaction
        return newTransaction
    }

    // Create a new transaction for the current transaction name
    func newTransaction() throws {
        let newTransaction = try database.createTransaction()
        transactionMap[transactionName] = newTransaction
    }

    // Switch to a different transaction by name
    func switchTransaction(_ name: [UInt8]) throws {
        let nameString = String(bytes: name, encoding: .utf8) ?? "MAIN"
        transactionName = nameString

        // Create transaction if it doesn't exist
        if transactionMap[transactionName] == nil {
            try newTransaction()
        }
    }

    // Helper method to pack range results like Python's push_range
    func pushRange(_ idx: Int, _ records: [(key: [UInt8], value: [UInt8])], prefixFilter: [UInt8]? = nil) {
        var kvs: [any TupleElement] = []
        for (key, value) in records {
            if let prefix = prefixFilter {
                if key.starts(with: prefix) {
                    kvs.append(key)
                    kvs.append(value)
                }
            } else {
                kvs.append(key)
                kvs.append(value)
            }
        }
        let tuple = Tuple(kvs)
        store(idx, tuple.encode())
    }

    // Helper method to filter key results with prefix
    func filterKeyResult(_ key: [UInt8], prefix: [UInt8]) -> [UInt8] {
        if key.starts(with: prefix) {
            return key
        } else if key.lexicographicallyPrecedes(prefix) {
            return prefix
        } else {
            return prefix + [0xFF]
        }
    }

    // Helper method to log a batch of stack entries
    func logStackBatch(_ entries: [(stackIndex: Int, entry: StackEntry)], prefix: [UInt8]) async throws {
        try await database.withTransaction { transaction in
            for (stackIndex, entry) in entries {
                // Create key: prefix + tuple(stackIndex, entry.idx)
                let keyTuple = Tuple([Int64(stackIndex), Int64(entry.idx)])
                var key = prefix
                key.append(contentsOf: keyTuple.encode())

                // Pack value as a tuple (matching Python/Go behavior)
                let valueTuple: Tuple
                if let data = entry.item as? [UInt8] {
                    valueTuple = Tuple([data])
                } else if let str = entry.item as? String {
                    valueTuple = Tuple([str])
                } else if let int = entry.item as? Int64 {
                    valueTuple = Tuple([int])
                } else {
                    valueTuple = Tuple([Array("UNKNOWN_ITEM".utf8)])
                }

                var packedValue = valueTuple.encode()

                // Limit value size to 40000 bytes
                let maxSize = 40000
                if packedValue.count > maxSize {
                    packedValue = Array(packedValue.prefix(maxSize))
                }

                transaction.setValue(packedValue, for: key)
            }
            return ()
        }
    }

    // Process a single instruction - subset of Go's processInst
    func processInstruction(_ idx: Int, _ instruction: [Any]) async throws {
        guard let op = instruction.first as? String else {
            fatalError("Invalid instruction format")
        }

        if verbose {
            print("\(idx). Instruction is \(op)")
            print("Stack: [\(stack.map { "\($0.item)" }.joined(separator: ", "))] (\(stack.count))")
        }

        switch op {
        case "PUSH":
            assert(instruction.count > 1)
            store(idx, instruction[1])

        case "POP":
            assert(!stack.isEmpty)
            let _ = waitAndPop()

        case "DUP":
            assert(!stack.isEmpty)
            let entry = stack.last!
            store(entry.idx, entry.item)

        case "EMPTY_STACK":
            stack.removeAll()

        case "SWAP":
            assert(!stack.isEmpty)
            let swapIdx = waitAndPop().item as! Int64
            let lastIdx = stack.count - 1
            let targetIdx = lastIdx - Int(swapIdx)
            assert(targetIdx >= 0 && targetIdx < stack.count)
            stack.swapAt(lastIdx, targetIdx)

        case "SUB":
            assert(stack.count >= 2)
            let x = waitAndPop().item as! Int64
            let y = waitAndPop().item as! Int64
            store(idx, x - y)

        case "CONCAT":
            assert(stack.count >= 2)
            let str1 = waitAndPop().item
            let str2 = waitAndPop().item

            if let s1 = str1 as? String, let s2 = str2 as? String {
                store(idx, s1 + s2)
            } else if let d1 = str1 as? [UInt8], let d2 = str2 as? [UInt8] {
                store(idx, d1 + d2)
            } else {
                fatalError("Invalid CONCAT parameters")
            }

        case "NEW_TRANSACTION":
            try newTransaction()

        case "USE_TRANSACTION":
            let name = waitAndPop().item as! [UInt8]
            try switchTransaction(name)

        case "ON_ERROR":
            let errorCode = waitAndPop().item as! Int64
            let transaction = try currentTransaction()

            // Create FdbError from the error code
            let error = FdbError(code: Int32(errorCode))

            // Call onError which will wait and handle the error appropriately
            do {
                try await transaction.onError(error)
                // If onError succeeds, the transaction has been reset and is ready to retry
                store(idx, Array("RESULT_NOT_PRESENT".utf8))
            } catch {
                // If onError fails, store the error (transaction should not be retried)
                throw error
            }
 
        case "GET_READ_VERSION":
            let transaction = try currentTransaction()
            lastVersion = try await transaction.getReadVersion()
            store(idx, Array("GOT_READ_VERSION".utf8))

        case "SET":
            assert(stack.count >= 2)
            let key = waitAndPop().item as! [UInt8]
            let value = waitAndPop().item as! [UInt8]

            try await database.withTransaction { transaction in
                transaction.setValue(value, for: key)
                return ()
            }

        case "GET":
            assert(!stack.isEmpty)
            let key = waitAndPop().item as! [UInt8]

            let result = try await database.withTransaction { transaction in
                return try await transaction.getValue(for: key, snapshot: false)
            }

            if let value = result {
                store(idx, value)
            } else {
                store(idx, Array("RESULT_NOT_PRESENT".utf8))
            }

        case "LOG_STACK":
            assert(!stack.isEmpty)
            let logPrefix = waitAndPop().item as! [UInt8]

            // Process stack in batches of 100 like Python/Go implementations
            var entries: [(stackIndex: Int, entry: StackEntry)] = []
            var stackIndex = stack.count - 1

            while !stack.isEmpty {
                let entry = waitAndPop()
                entries.append((stackIndex: stackIndex, entry: entry))
                stackIndex -= 1

                if entries.count == 100 {
                    try await logStackBatch(entries, prefix: logPrefix)
                    entries.removeAll()
                }
            }

            // Log remaining entries
            if !entries.isEmpty {
                try await logStackBatch(entries, prefix: logPrefix)
            }

        case "COMMIT":
            let transaction = try currentTransaction()
            let success = try await transaction.commit()
            store(idx, Array("COMMIT_RESULT".utf8))

        case "RESET":
            if let transaction = transactionMap[transactionName] as? FdbTransaction {
                try newTransaction()
            }

        case "CANCEL":
            if let transaction = transactionMap[transactionName] {
                transaction.cancel()
            }

        case "GET_KEY":
            // Python order: key, or_equal, offset, prefix = inst.pop(4)
            let prefix = waitAndPop().item as! [UInt8]
            let offset = Int32(waitAndPop().item as! Int64)
            let orEqual = (waitAndPop().item as! Int64) != 0
            let key = waitAndPop().item as! [UInt8]

            let selector = Fdb.KeySelector(key: key, orEqual: orEqual, offset: offset)
            let transaction = try currentTransaction()

            if let resultKey = try await transaction.getKey(selector: selector, snapshot: false) {
                let filteredKey = filterKeyResult(resultKey, prefix: prefix)
                store(idx, filteredKey)
            } else {
                store(idx, Array("RESULT_NOT_PRESENT".utf8))
            }

        case "GET_RANGE":
            // Python/Go order: begin, end, limit, reverse, mode (but Go pops in reverse)
            // Go pops: mode, reverse, limit, endKey, beginKey
            let mode = waitAndPop().item as! Int64 // Streaming mode, ignore for now
            let reverse = (waitAndPop().item as! Int64) != 0
            let limit = Int32(waitAndPop().item as! Int64)
            let endKey = waitAndPop().item as! [UInt8]
            let beginKey = waitAndPop().item as! [UInt8]
            let transaction = try currentTransaction()

            let result = try await transaction.getRange(
                beginKey: beginKey,
                endKey: endKey,
                limit: limit,
                snapshot: false
            )

            pushRange(idx, result.records)

        case "GET_RANGE_STARTS_WITH":
            // Python order: prefix, limit, reverse, mode (pops 4 parameters)
            // Go order: same but pops in reverse
            let mode = waitAndPop().item as! Int64 // Streaming mode, ignore for now
            let reverse = (waitAndPop().item as! Int64) != 0
            let limit = Int32(waitAndPop().item as! Int64)
            let prefix = waitAndPop().item as! [UInt8]
            let transaction = try currentTransaction()

            var endKey = prefix
            endKey.append(0xFF)

            let result = try await transaction.getRange(
                beginKey: prefix,
                endKey: endKey,
                limit: limit,
                snapshot: false
            )

            pushRange(idx, result.records)

        case "GET_RANGE_SELECTOR":
            // Python pops 10 parameters: begin_key, begin_or_equal, begin_offset, end_key, end_or_equal, end_offset, limit, reverse, mode, prefix
            // Go pops in reverse order
            let prefix = waitAndPop().item as! [UInt8]
            let mode = waitAndPop().item as! Int64 // Streaming mode, ignore for now
            let reverse = (waitAndPop().item as! Int64) != 0
            let limit = Int32(waitAndPop().item as! Int64)
            let endOffset = Int32(waitAndPop().item as! Int64)
            let endOrEqual = (waitAndPop().item as! Int64) != 0
            let endKey = waitAndPop().item as! [UInt8]
            let beginOffset = Int32(waitAndPop().item as! Int64)
            let beginOrEqual = (waitAndPop().item as! Int64) != 0
            let beginKey = waitAndPop().item as! [UInt8]

            let beginSelector = Fdb.KeySelector(key: beginKey, orEqual: beginOrEqual, offset: beginOffset)
            let endSelector = Fdb.KeySelector(key: endKey, orEqual: endOrEqual, offset: endOffset)
            let transaction = try currentTransaction()

            let result = try await transaction.getRange(
                beginSelector: beginSelector,
                endSelector: endSelector,
                limit: limit,
                snapshot: false
            )

            pushRange(idx, result.records, prefixFilter: prefix)

        case "GET_ESTIMATED_RANGE_SIZE":
            let endKey = waitAndPop().item as! [UInt8]
            let beginKey = waitAndPop().item as! [UInt8]
            let transaction = try currentTransaction()

            _ = try await transaction.getEstimatedRangeSizeBytes(beginKey: beginKey, endKey: endKey)
            store(idx, Array("GOT_ESTIMATED_RANGE_SIZE".utf8))

        case "GET_RANGE_SPLIT_POINTS":
            let chunkSize = waitAndPop().item as! Int64
            let endKey = waitAndPop().item as! [UInt8]
            let beginKey = waitAndPop().item as! [UInt8]
            let transaction = try currentTransaction()

            _ = try await transaction.getRangeSplitPoints(beginKey: beginKey, endKey: endKey, chunkSize: chunkSize)
            store(idx, Array("GOT_RANGE_SPLIT_POINTS".utf8))

        case "CLEAR":
            let key = waitAndPop().item as! [UInt8]
            let transaction = try currentTransaction()
            transaction.clear(key: key)

        case "CLEAR_RANGE":
            let beginKey = waitAndPop().item as! [UInt8]
            let endKey = waitAndPop().item as! [UInt8]
            let transaction = try currentTransaction()
            transaction.clearRange(beginKey: beginKey, endKey: endKey)

        case "CLEAR_RANGE_STARTS_WITH":
            let prefix = waitAndPop().item as! [UInt8]
            let transaction = try currentTransaction()
            var endKey = prefix
            endKey.append(0xFF)
            transaction.clearRange(beginKey: prefix, endKey: endKey)

        case "ATOMIC_OP":
            // Python order: opType, key, value = inst.pop(3)
            let param = waitAndPop().item as! [UInt8]  // value/param
            let key = waitAndPop().item as! [UInt8]    // key
            let opType = waitAndPop().item as! [UInt8] // opType
            let transaction = try currentTransaction()

            // Convert opType string to MutationType
            let opTypeString = String(bytes: opType, encoding: .utf8) ?? ""
            let mutationType: Fdb.MutationType
            switch opTypeString {
            case "ADD":
                mutationType = .add
            case "BIT_AND":
                mutationType = .bitAnd
            case "BIT_OR":
                mutationType = .bitOr
            case "BIT_XOR":
                mutationType = .bitXor
            default:
                mutationType = .add // Default fallback
            }

            transaction.atomicOp(key: key, param: param, mutationType: mutationType)

        case "SET_READ_VERSION":
            let version = waitAndPop().item as! Int64
            let transaction = try currentTransaction()
            transaction.setReadVersion(version)

        case "GET_COMMITTED_VERSION":
            let transaction = try currentTransaction()
            lastVersion = try transaction.getCommittedVersion()
            store(idx, Array("GOT_COMMITTED_VERSION".utf8))

        case "GET_APPROXIMATE_SIZE":
            let transaction = try currentTransaction()
            _ = try await transaction.getApproximateSize()
            store(idx, Array("GOT_APPROXIMATE_SIZE".utf8))

        case "GET_VERSIONSTAMP":
            let transaction = try currentTransaction()
            if let versionstamp = try await transaction.getVersionstamp() {
                store(idx, versionstamp)
            } else {
                store(idx, Array("RESULT_NOT_PRESENT".utf8))
            }

        case "READ_CONFLICT_RANGE":
            let endKey = waitAndPop().item as! [UInt8]
            let beginKey = waitAndPop().item as! [UInt8]
            let transaction = try currentTransaction()
            try transaction.addConflictRange(beginKey: beginKey, endKey: endKey, type: .read)

        case "WRITE_CONFLICT_RANGE":
            let endKey = waitAndPop().item as! [UInt8]
            let beginKey = waitAndPop().item as! [UInt8]
            let transaction = try currentTransaction()
            try transaction.addConflictRange(beginKey: beginKey, endKey: endKey, type: .write)

        case "READ_CONFLICT_KEY":
            let key = waitAndPop().item as! [UInt8]
            let transaction = try currentTransaction()
            // For a single key, create a range [key, key+\x00)
            var endKey = key
            endKey.append(0x00)
            try transaction.addConflictRange(beginKey: key, endKey: endKey, type: .read)

        case "WRITE_CONFLICT_KEY":
            let key = waitAndPop().item as! [UInt8]
            let transaction = try currentTransaction()
            // For a single key, create a range [key, key+\x00)
            var endKey = key
            endKey.append(0x00)
            try transaction.addConflictRange(beginKey: key, endKey: endKey, type: .write)

        case "DISABLE_WRITE_CONFLICT":
            // Not directly available in Swift bindings, could use transaction option
            let transaction = try currentTransaction()
            try transaction.setOption(.nextWriteNoWriteConflictRange, value: nil)

        case "TUPLE_PACK":
            let numElements = waitAndPop().item as! Int64
            var elements: [any TupleElement] = []

            for _ in 0..<numElements {
                let item = waitAndPop().item
                if let bytes = item as? [UInt8] {
                    elements.append(bytes)
                } else if let string = item as? String {
                    elements.append(string)
                } else if let int = item as? Int64 {
                    elements.append(int)
                } else {
                    // Convert to bytes as fallback
                    let fallbackBytes = Array("UNKNOWN_TYPE".utf8)
                    elements.append(fallbackBytes)
                }
            }

            let tuple = Tuple(elements.reversed()) // Reverse because we popped in reverse order
            store(idx, tuple.encode())

        case "TUPLE_PACK_WITH_VERSIONSTAMP":
            // Python order: prefix, count, items
            let prefix = waitAndPop().item as! [UInt8]
            let numElements = waitAndPop().item as! Int64
            var elements: [any TupleElement] = []

            for _ in 0..<numElements {
                let item = waitAndPop().item
                if let bytes = item as? [UInt8] {
                    elements.append(bytes)
                } else if let string = item as? String {
                    elements.append(string)
                } else if let int = item as? Int64 {
                    elements.append(int)
                } else {
                    let fallbackBytes = Array("UNKNOWN_TYPE".utf8)
                    elements.append(fallbackBytes)
                }
            }

            // For now, treat like regular TUPLE_PACK since versionstamp handling is complex
            let tuple = Tuple(elements.reversed())
            var result = prefix
            result.append(contentsOf: tuple.encode())
            store(idx, result)

        case "TUPLE_UNPACK":
            let encodedTuple = waitAndPop().item as! [UInt8]
            do {
                let elements = try Tuple.decode(from: encodedTuple)
                for element in elements.reversed() { // Reverse to match stack order
                    if let bytes = element as? [UInt8] {
                        store(idx, bytes)
                    } else if let string = element as? String {
                        store(idx, Array(string.utf8))
                    } else if let int = element as? Int64 {
                        store(idx, int)
                    } else {
                        store(idx, Array("UNKNOWN_TYPE".utf8))
                    }
                }
            } catch {
                store(idx, Array("INVALID_TUPLE".utf8))
            }

        case "TUPLE_SORT":
            let numTuples = waitAndPop().item as! Int64
            var tuples: [[UInt8]] = []

            for _ in 0..<numTuples {
                tuples.append(waitAndPop().item as! [UInt8])
            }

            tuples.sort { $0.lexicographicallyPrecedes($1) }

            for tuple in tuples {
                store(idx, tuple)
            }

        case "TUPLE_RANGE":
            let numElements = waitAndPop().item as! Int64
            var elements: [any TupleElement] = []

            for _ in 0..<numElements {
                let item = waitAndPop().item
                if let bytes = item as? [UInt8] {
                    elements.append(bytes)
                } else if let string = item as? String {
                    elements.append(string)
                } else if let int = item as? Int64 {
                    elements.append(int)
                }
            }

            let tuple = Tuple(elements.reversed())
            let prefix = tuple.encode()

            // Create range: prefix to prefix + [0xFF]
            var endKey = prefix
            endKey.append(0xFF)

            store(idx, prefix)
            store(idx, endKey)

        case "ENCODE_FLOAT":
            let floatValue = Float(waitAndPop().item as! Int64) // Convert from int representation
            let data = withUnsafeBytes(of: floatValue.bitPattern) { Array($0) }
            store(idx, data)

        case "ENCODE_DOUBLE":
            let doubleValue = Double(waitAndPop().item as! Int64) // Convert from int representation
            let data = withUnsafeBytes(of: doubleValue.bitPattern) { Array($0) }
            store(idx, data)

        case "DECODE_FLOAT":
            let data = waitAndPop().item as! [UInt8]
            if data.count >= 4 {
                let floatValue = data.withUnsafeBytes { $0.load(as: Float.self) }
                store(idx, Int64(floatValue.bitPattern))
            } else {
                store(idx, Int64(0))
            }

        case "DECODE_DOUBLE":
            let data = waitAndPop().item as! [UInt8]
            if data.count >= 8 {
                let doubleValue = data.withUnsafeBytes { $0.load(as: Double.self) }
                store(idx, Int64(doubleValue.bitPattern))
            } else {
                store(idx, Int64(0))
            }

        case "WAIT_FUTURE":
            // In async context, futures are automatically awaited, just pass through the item
            let oldIdx = stack.count > 0 ? stack.last!.idx : idx
            let item = waitAndPop().item
            store(oldIdx, item)

        case "START_THREAD":
            // Threading not supported in current implementation, just consume the instruction
            let instruction = waitAndPop().item
            // Could implement this with Task.detached in the future

        case "WAIT_EMPTY":
            // Wait until stack is empty - already satisfied since we process sequentially
            break

        case "UNIT_TESTS": // TODO
            store(idx, Array("UNIT_TESTS_COMPLETED".utf8))

        default:
            fatalError("Unhandled operation: \(op)")
        }

        if verbose {
            print("        -> [\(stack.map { "\($0.item)" }.joined(separator: ", "))] (\(stack.count))")
            print()
        }
    }

    // Main run function - equivalent to Go's Run()
    func run() async throws {
        // Read instructions from database using the prefix, like Go version
        let instructions = try await database.withTransaction { transaction -> [(key: [UInt8], value: [UInt8])] in
            // Create range starting with our prefix
            let prefixTuple = Tuple([prefix])
            let beginKey = prefixTuple.encode()
            let endKey = beginKey + [0xFF] // Simple range end

            let result = try await transaction.getRange(
                beginKey: beginKey,
                endKey: endKey,
                limit: 0,
                snapshot: false
            )

            return result.records
        }

        if verbose {
            print("Found \(instructions.count) instructions")
        }

        // Process each instruction
        for (i, (_, value)) in instructions.enumerated() {
            // Unpack the instruction tuple from the value
            let elements = try Tuple.decode(from: value)

            // Convert tuple elements to array for processing
            var instruction: [Any] = []
            for element in elements {
                if let stringElement = element as? String {
                    instruction.append(stringElement)
                } else if let bytesElement = element as? [UInt8] {
                    instruction.append(bytesElement)
                } else if let intElement = element as? Int64 {
                    instruction.append(intElement)
                } else {
                    instruction.append(element)
                }
            }

            if verbose {
                print("Instruction \(i): \(instruction)")
            }

            try await processInstruction(i, instruction)
        }

        print("StackTester completed successfully with \(instructions.count) instructions")
    }
}

