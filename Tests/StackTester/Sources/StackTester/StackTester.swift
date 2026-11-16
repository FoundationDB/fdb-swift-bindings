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
@testable import FoundationDB


/// Data type that can be pushed onto the stack. It either represents an immediate value pushed onto the stack
/// (in the `.element` case) or a future that will complete with a value later (in the `.future`) case.
enum StackItem {
    case element(TupleElement)
    case future(PendingFuture)
}

/// Simple stack entry. See `StackItem` for more details on the stack data elements.
struct StackEntry {
    let item: StackItem
    let idx: Int
}

func toBytes(_ str: String) -> FDB.Bytes {
    FDB.Bytes(str.utf8)
}


/// Isolation of a given operation. This can be one of three levels:
///  * `.database` indicates that this runs in its own transaction that is retried and committed independently
///  * `.snapshot` indicates that this runs in the current transaction but with `SNAPSHOT` read isolation
///  * `.serializable` is the default and indicates that this runs in the current transaction with default (serializable) isolation
/// Each instruction in the stack machine can specify this via their suffix. For instance, `GET_RANGE` runs at `.serializable`,
///  `GET_RANGE_SNAPSHOT` runs at `.snapshot`, and `GET_RANGE_DATABASE` runs at `.database`.
enum OperationIsolation {
    case database
    case snapshot
    case serializable

    static private func withoutSuffixIfPresent(_ op: String, suffix: String) -> String? {
        if op.hasSuffix(suffix) {
            String(op.dropLast(suffix.count))
        } else {
            nil
        }
    }

    /// Split an instruction name such as `GET_RANGE_SNAPSHOT` into `("GET_RANGE", .snapshot)`.
    static func split(_ op: String) -> (op: String, opIsolation: OperationIsolation) {
        if let dbOp = withoutSuffixIfPresent(op, suffix: "_DATABASE") {
            (dbOp, .database)
        } else if let snapshotOp = withoutSuffixIfPresent(op, suffix: "_SNAPSHOT") {
            (snapshotOp, .snapshot)
        } else {
            (op, .serializable)
        }
    }
}

/// Start `operation` running right away, on the current executor, until its first suspension point.
/// A plain `Task` would only be scheduled to start later, and may not have begun when the next
/// instruction runs. Falls back to a plain `Task` on OS versions without `Task.immediate`.
func startImmediately<T: Sendable>(
    _ operation: sending @escaping @isolated(any) () async throws -> T
) -> Task<T, any Error> {
    if #available(macOS 26, iOS 26, tvOS 26, watchOS 26, visionOS 26, *) {
        return Task.immediate(operation: operation)
    } else {
        return Task(operation: operation)
    }
}

/// A stack item whose value is not available yet, such as the versionstamp of an uncommitted
/// transaction. It is resolved when it is popped, like the futures other bindings push.
struct PendingFuture: Sendable {
    let wait: @Sendable () async throws -> FDB.Bytes?
}

class StackMachine {
    private let prefix: FDB.Bytes
    private var stack: [StackEntry] = []
    private var database: FDBDatabase
    private let verbose: Bool
    private var transaction: FDBTransaction?
    private var transactionMap: [FDB.Bytes: any TransactionProtocol] = [:]
    private var transactionName: FDB.Bytes
    private var lastVersion: Int64 = 0

    init(prefix: FDB.Bytes, database: FDBDatabase, verbose: Bool) {
        self.prefix = prefix
        self.database = database
        self.verbose = verbose
        // Like the other bindings, each stack machine starts out using a transaction named by its prefix.
        self.transactionName = prefix
    }

    // Equivalent to Go's waitAndPop with error handling
    func waitAndPop() async -> (item: TupleElement, idx: Int) {
        guard !stack.isEmpty else {
            fatalError("Stack is empty")
        }

        let entry = stack.removeLast()
        if case let .element(element) = entry.item {
            return (item: element, idx: entry.idx)
        }

        // Items keep their types: unicode strings and byte strings are distinct tuple types.
        guard case let .future(pending) = entry.item else {
            fatalError("Unexpected pending entry: \(entry.item)")
        }
        // Resolve pending futures. A void or missing result is RESULT_NOT_PRESENT and an error
        // becomes the packed ("ERROR", <code>) tuple.
        do {
            let result = try await pending.wait()
            return (item: .bytes(result ?? toBytes("RESULT_NOT_PRESENT")), idx: entry.idx)
        } catch let error as FDBError {
            return (item: .bytes(errorTuple(error)), idx: entry.idx)
        } catch {
            fatalError("Unexpected error resolving future: \(error)")
        }
    }

    /// The packed tuple `("ERROR", <code>)` pushed in place of a failed operation's result.
    func errorTuple(_ error: FDBError) -> FDB.Bytes {
        Tuple(toBytes("ERROR"), toBytes(String(error.code))).encode()
    }

    /// Pop an item that is used as a byte string. Unicode strings are accepted and sent as UTF-8,
    /// as the other bindings do.
    func popBytes() async -> FDB.Bytes {
        let item = await waitAndPop().item
        switch item {
        case let .bytes(bytes):
            return bytes
        case let .string(string):
            return toBytes(string)
        default:
            fatalError("Expected a byte string on the stack, found \(item)")
        }
    }

    /// Pop an item that is used as a byte string. Unicode strings are accepted and sent as UTF-8,
    /// as the other bindings do.
    func popInt() async -> Int64 {
        let item = await waitAndPop().item
        switch item {
        case let .int(int):
            return int
        default:
            fatalError("Expected an integer on the stack, found \(item)")
        }
    }

    /// Pop `count` items, returned in the order popped (top of the stack first).
    func popTupleElements(_ count: Int) async -> [TupleElement] {
        var elements: [TupleElement] = []
        for _ in 0 ..< count {
            elements.append(await waitAndPop().item)
        }
        return elements
    }

    /// Store an item on the stack. It appends the entry to the end of the list representing storage on the stack.
    /// A `StackItem` can either be some `Tuple`element or a `PendingFuture` representing work that
    /// has not yet completed.
    func store(_ idx: Int, _ item: StackItem) {
        stack.append(StackEntry(item: item, idx: idx))
    }

    func store(_ idx: Int, _ item: some TupleElementConvertible) {
        store(idx, .element(item.tupleElement()))
    }

    func store(_ idx: Int, _ item: PendingFuture) {
        store(idx, .future(item))
    }

    // Get current transaction (create if needed)
    func currentTransaction() throws -> any TransactionProtocol {
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
    func switchTransaction(_ name: FDB.Bytes) throws {
        transactionName = name

        // Create transaction if it doesn't exist
        if transactionMap[transactionName] == nil {
            try newTransaction()
        }
    }

    // Helper method to pack range results like Python's push_range
    func pushRange(_ idx: Int, _ records: [(key: FDB.Bytes, value: FDB.Bytes)], prefixFilter: FDB.Bytes? = nil) {
        var kvs: [any TupleElementConvertible] = []
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
    func filterKeyResult(_ key: FDB.Bytes, prefix: FDB.Bytes) throws -> FDB.Bytes {
        if key.starts(with: prefix) {
            return key
        } else if key.lexicographicallyPrecedes(prefix) {
            return prefix
        } else {
            return try FDB.strinc(prefix)
        }
    }

    // Helper method to log a batch of stack entries
    func logStackBatch(_ entries: [(stackIndex: Int, entry: (item: TupleElement, idx: Int))], prefix: FDB.Bytes) async throws {
        try await database.withTransaction { transaction in
            for (stackIndex, entry) in entries {
                // Create key: prefix + tuple(stackIndex, entry.idx)
                let keyTuple = Tuple(stackIndex, entry.idx)
                var key = prefix
                key.append(contentsOf: keyTuple.encode())

                // Pack value as a one-element tuple (matching Python/Go behavior)
                let valueTuple = Tuple(entry.item)

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

    /// Run a read according to `opIsolation`: against a fresh retried transaction for
    /// `_DATABASE`, as a snapshot read on the current transaction for `_SNAPSHOT`, or as a
    /// regular read on the current transaction otherwise.
    func read<T>(
        _ opIsolation: OperationIsolation,
        _ body: (any TransactionProtocol, _ snapshot: Bool) async throws -> sending T
    ) async throws -> sending T {
        switch opIsolation {
        case .database:
            return try await database.withTransaction { transaction in
                try await body(transaction, false)
            }
        case .snapshot:
            return try await body(try currentTransaction(), true)
        case .serializable:
            return try await body(try currentTransaction(), false)
        }
    }

    /// Apply a mutation according to `opIsolation`. `_DATABASE` mutations run in their own retried,
    /// committed transaction and push `RESULT_NOT_PRESENT` (the result of a void future).
    func mutate(
        _ idx: Int,
        _ opIsolation: OperationIsolation,
        _ body: (any TransactionProtocol) throws -> Void
    ) async throws {
        if opIsolation == .database {
            try await database.withTransaction { transaction in
                try body(transaction)
                return Void()
            }
            store(idx, toBytes("RESULT_NOT_PRESENT"))
        } else {
            try body(try currentTransaction())
        }
    }

    /// Read a range, following `more` until `limit` is reached (0 means no limit).
    ///
    /// The bindings' range reads do not support reverse reads or streaming modes yet, so reverse
    /// reads are faked by reading the whole range and reversing it, and the streaming mode is
    /// ignored. TODO: use the real thing once `getRange` supports them.
    func readRange(
        _ transaction: any TransactionProtocol,
        begin: FDB.KeySelector,
        end: FDB.KeySelector,
        limit: Int,
        reverse: Bool = false,
        snapshot: Bool
    ) async throws -> [(key: FDB.Bytes, value: FDB.Bytes)] {
        let forwardLimit = reverse ? 0 : limit
        var results: [(key: FDB.Bytes, value: FDB.Bytes)] = []
        // TODO: Include limit and reverse in direct call once this is available
        // See: https://github.com/FoundationDB/fdb-swift-bindings/issues/33
        for try await kv in transaction.getRange(beginSelector: begin, endSelector: end, snapshot: snapshot) {
            results.append(kv)
            if forwardLimit != 0 && results.count == forwardLimit {
                break
            }
        }
        if reverse {
            results.reverse()
            if limit != 0 && results.count > limit {
                results.removeLast(results.count - limit)
            }
        }
        return results
    }

    // Process a single instruction - subset of Go's processInst
    func processInstruction(
        _ idx: Int,
        _ instruction: Tuple,
        threads: inout ThrowingTaskGroup<Void, any Error>
    ) async throws {
        guard case let .string(rawOp) = instruction[0] else {
            fatalError("Invalid instruction format")
        }
        let (op, opIsolation) = OperationIsolation.split(rawOp)

        if verbose {
            print("[\(String(decoding: prefix, as: UTF8.self))] \(idx). \(rawOp) (stack size \(stack.count))")
        }

        do {
            try await execute(idx, op, opIsolation, instruction, threads: &threads)
        } catch let error as FDBError {
            // Errors from FDB operations are pushed as the packed tuple ("ERROR", <code>).
            store(idx, errorTuple(error))
        }

    }

    private func execute(
        _ idx: Int,
        _ op: String,
        _ opIsolation: OperationIsolation,
        _ instruction: Tuple,
        threads: inout ThrowingTaskGroup<Void, any Error>
    ) async throws {
        switch op {
        case "PUSH":
            assert(instruction.count > 1)
            store(idx, instruction[1])

        case "POP":
            assert(!stack.isEmpty)
            _ = await waitAndPop()

        case "DUP":
            assert(!stack.isEmpty)
            let entry = stack.last!
            store(entry.idx, entry.item)

        case "EMPTY_STACK":
            stack.removeAll()

        case "SWAP":
            assert(!stack.isEmpty)
            let swapIdx = await popInt()
            let lastIdx = stack.count - 1
            let targetIdx = lastIdx - Int(swapIdx)
            assert(targetIdx >= 0 && targetIdx < stack.count)
            stack.swapAt(lastIdx, targetIdx)

        case "SUB":
            assert(stack.count >= 2)
            let x = await popInt()
            let y = await popInt()
            store(idx, x - y)

        case "CONCAT":
            assert(stack.count >= 2)
            let str1 = await waitAndPop().item
            let str2 = await waitAndPop().item

            if case let .string(s1) = str1, case let .string(s2) = str2 {
                store(idx, s1 + s2)
            } else if case let .bytes(d1) = str1, case let .bytes(d2) = str2 {
                store(idx, d1 + d2)
            } else {
                fatalError("Invalid CONCAT parameters")
            }

        case "NEW_TRANSACTION":
            try newTransaction()

        case "USE_TRANSACTION":
            let name = await popBytes()
            try switchTransaction(name)

        case "ON_ERROR":
            let errorCode = await popInt()
            let transaction = try currentTransaction()

            // Create FDBError from the error code
            let error = FDBError(code: Int(errorCode))

            // Call onError which will wait and handle the error appropriately
            do {
                try await transaction.onError(error)
                // If onError succeeds, the transaction has been reset and is ready to retry
                store(idx, toBytes("RESULT_NOT_PRESENT"))
            } catch {
                // If onError fails, store the error (transaction should not be retried)
                throw error
            }

        case "GET_READ_VERSION":
            let transaction = try currentTransaction()
            lastVersion = try await transaction.getReadVersion()
            store(idx, toBytes("GOT_READ_VERSION"))

        case "SET":
            assert(stack.count >= 2)
            let key = await popBytes()
            let value = await popBytes()
            try await mutate(idx, opIsolation) { $0.setValue(value, for: key) }

        case "GET":
            assert(!stack.isEmpty)
            let key = await popBytes()
            let result = try await read(opIsolation) { transaction, snapshot in
                try await transaction.getValue(for: key, snapshot: snapshot)
            }
            store(idx, result ?? toBytes("RESULT_NOT_PRESENT"))

        case "LOG_STACK":
            assert(!stack.isEmpty)
            let logPrefix = await popBytes()

            // Process stack in batches of 100 like Python/Go implementations
            var entries: [(stackIndex: Int, entry: (item: TupleElement, idx: Int))] = []
            var stackIndex = stack.count - 1

            while !stack.isEmpty {
                let entry = await waitAndPop()
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
            _ = try await transaction.commit()
            // The (optional) future pushed by COMMIT is void, so it resolves to RESULT_NOT_PRESENT.
            store(idx, toBytes("RESULT_NOT_PRESENT"))

        case "RESET":
            if transactionMap[transactionName] as? FDBTransaction != nil {
                try newTransaction()
            }

        case "CANCEL":
            if let transaction = transactionMap[transactionName] {
                transaction.cancel()
            }

        case "GET_KEY":
            // Top of stack first: KEY, OR_EQUAL, OFFSET, PREFIX
            let key = await popBytes()
            let orEqual = (await popInt()) != 0
            let offset = Int(await popInt())
            let prefix = await popBytes()

            let selector = FDB.KeySelector(key: key, orEqual: orEqual, offset: offset)
            let resultKey = try await read(opIsolation) { transaction, snapshot in
                try await transaction.getKey(selector: selector, snapshot: snapshot)
            }
            store(idx, try filterKeyResult(resultKey ?? [], prefix: prefix))

        case "GET_RANGE":
            // Top of stack first: BEGIN_KEY, END_KEY, LIMIT, REVERSE, STREAMING_MODE
            let beginKey = await popBytes()
            let endKey = await popBytes()
            let limit = Int(await popInt())
            let reverse = await popInt() != 0
            _ = await waitAndPop() // STREAMING_MODE: not supported by the bindings yet

            let records = try await read(opIsolation) { transaction, snapshot in
                try await readRange(
                    transaction,
                    begin: .firstGreaterOrEqual(beginKey),
                    end: .firstGreaterOrEqual(endKey),
                    limit: limit,
                    reverse: reverse,
                    snapshot: snapshot
                )
            }
            pushRange(idx, records)

        case "GET_RANGE_STARTS_WITH":
            // Top of stack first: PREFIX, LIMIT, REVERSE, STREAMING_MODE
            let prefix = await popBytes()
            let limit = Int(await popInt())
            let reverse = await popInt() != 0
            _ = await waitAndPop() // STREAMING_MODE: not supported by the bindings yet

            let records = try await read(opIsolation) { transaction, snapshot in
                try await readRange(
                    transaction,
                    begin: .firstGreaterOrEqual(prefix),
                    end: .firstGreaterOrEqual(try FDB.strinc(prefix)),
                    limit: limit,
                    reverse: reverse,
                    snapshot: snapshot
                )
            }
            pushRange(idx, records)

        case "GET_RANGE_SELECTOR":
            // Top of stack first: BEGIN_KEY, BEGIN_OR_EQUAL, BEGIN_OFFSET, END_KEY, END_OR_EQUAL,
            // END_OFFSET, LIMIT, REVERSE, STREAMING_MODE, PREFIX
            let beginKey = await popBytes()
            let beginOrEqual = (await popInt()) != 0
            let beginOffset = Int(await popInt())
            let endKey = await popBytes()
            let endOrEqual = (await popInt()) != 0
            let endOffset = Int(await popInt())
            let limit = Int(await popInt())
            let reverse = (await popInt()) != 0
            _ = await waitAndPop() // STREAMING_MODE: not supported by the bindings yet
            let prefix = await popBytes()

            let beginSelector = FDB.KeySelector(key: beginKey, orEqual: beginOrEqual, offset: beginOffset)
            let endSelector = FDB.KeySelector(key: endKey, orEqual: endOrEqual, offset: endOffset)
            let records = try await read(opIsolation) { transaction, snapshot in
                try await readRange(
                    transaction,
                    begin: beginSelector,
                    end: endSelector,
                    limit: limit,
                    reverse: reverse,
                    snapshot: snapshot
                )
            }
            pushRange(idx, records, prefixFilter: prefix)

        case "GET_ESTIMATED_RANGE_SIZE":
            let beginKey = await popBytes()
            let endKey = await popBytes()
            let transaction = try currentTransaction()

            _ = try await transaction.getEstimatedRangeSizeBytes(beginKey: beginKey, endKey: endKey)
            store(idx, toBytes("GOT_ESTIMATED_RANGE_SIZE"))

        case "GET_RANGE_SPLIT_POINTS":
            let beginKey = await popBytes()
            let endKey = await popBytes()
            let chunkSize = Int(await popInt())
            let transaction = try currentTransaction()

            _ = try await transaction.getRangeSplitPoints(beginKey: beginKey, endKey: endKey, chunkSize: chunkSize)
            store(idx, toBytes("GOT_RANGE_SPLIT_POINTS"))

        case "CLEAR":
            let key = await popBytes()
            try await mutate(idx, opIsolation) { $0.clear(key: key) }

        case "CLEAR_RANGE":
            let beginKey = await popBytes()
            let endKey = await popBytes()
            try await mutate(idx, opIsolation) { $0.clearRange(beginKey: beginKey, endKey: endKey) }

        case "CLEAR_RANGE_STARTS_WITH":
            let prefix = await popBytes()
            try await mutate(idx, opIsolation) { $0.clearRange(beginKey: prefix, endKey: try FDB.strinc(prefix)) }

        case "ATOMIC_OP":
            // Top of stack first: OPTYPE, KEY, VALUE
            let opType = String(bytes: await popBytes(), encoding: .utf8) ?? ""
            let key = await popBytes()
            let param = await popBytes()

            let mutationType: FDB.MutationType
            switch opType {
            case "ADD": mutationType = .add
            case "BIT_AND": mutationType = .bitAnd
            case "BIT_OR": mutationType = .bitOr
            case "BIT_XOR": mutationType = .bitXor
            case "APPEND_IF_FITS": mutationType = .appendIfFits
            case "MAX": mutationType = .max
            case "MIN": mutationType = .min
            case "SET_VERSIONSTAMPED_KEY": mutationType = .setVersionstampedKey
            case "SET_VERSIONSTAMPED_VALUE": mutationType = .setVersionstampedValue
            case "BYTE_MIN": mutationType = .byteMin
            case "BYTE_MAX": mutationType = .byteMax
            case "COMPARE_AND_CLEAR": mutationType = .compareAndClear
            default: fatalError("Unknown atomic operation type: \(opType)")
            }
            try await mutate(idx, opIsolation) { $0.atomicOp(key: key, param: param, mutationType: mutationType) }

        case "SET_READ_VERSION":
            let transaction = try currentTransaction()
            transaction.setReadVersion(lastVersion)

        case "GET_COMMITTED_VERSION":
            let transaction = try currentTransaction()
            lastVersion = try transaction.getCommittedVersion()
            store(idx, toBytes("GOT_COMMITTED_VERSION"))

        case "GET_APPROXIMATE_SIZE":
            let transaction = try currentTransaction()
            _ = try await transaction.getApproximateSize()
            store(idx, toBytes("GOT_APPROXIMATE_SIZE"))

        case "GET_VERSIONSTAMP":
            // `getVersionstamp()` requests the versionstamp when it starts running, but it only
            // returns once the transaction has committed, which happens in a later instruction.
            // So the call has to be started now (running synchronously up to the point where it
            // first suspends) and only waited on when the item is popped.
            // See: https://github.com/FoundationDB/fdb-swift-bindings/issues/34
            let transaction = try currentTransaction()
            let versionstamp = startImmediately { try await transaction.getVersionstamp() }
            store(idx, PendingFuture(wait: { try await versionstamp.value }))

        case "READ_CONFLICT_RANGE", "WRITE_CONFLICT_RANGE":
            let beginKey = await popBytes()
            let endKey = await popBytes()
            let transaction = try currentTransaction()
            try transaction.addConflictRange(
                beginKey: beginKey, endKey: endKey, type: op == "READ_CONFLICT_RANGE" ? .read : .write
            )
            store(idx, toBytes("SET_CONFLICT_RANGE"))

        case "READ_CONFLICT_KEY", "WRITE_CONFLICT_KEY":
            let key = await popBytes()
            let transaction = try currentTransaction()
            // For a single key, the conflict range is [key, key + \x00)
            // Replace with helper methods once we have them.
            // See: https://github.com/FoundationDB/fdb-swift-bindings/issues/35
            try transaction.addConflictRange(
                beginKey: key, endKey: key + [0x00], type: op == "READ_CONFLICT_KEY" ? .read : .write
            )
            store(idx, toBytes("SET_CONFLICT_KEY"))

        case "DISABLE_WRITE_CONFLICT":
            // Not directly available in Swift bindings, could use transaction option
            let transaction = try currentTransaction()
            try transaction.setOption(to: nil, forOption: .nextWriteNoWriteConflictRange)

        case "TUPLE_PACK":
            let count = Int(await popInt())
            store(idx, Tuple(await popTupleElements(count)).encode())

        case "TUPLE_PACK_WITH_VERSIONSTAMP":
            // Top of stack first: PREFIX, N, then N items
            let prefix = await popBytes()
            let count = Int(await popInt())
            let tuple = Tuple(await popTupleElements(count))
            switch tuple.countIncompleteVersionstamps() {
            case 0:
                store(idx, toBytes("ERROR: NONE"))
            case 1:
                store(idx, toBytes("OK"))
                store(idx, try tuple.encodeWithVersionstamp(prefix: prefix))
            default:
                store(idx, toBytes("ERROR: MULTIPLE"))
            }

        case "TUPLE_UNPACK":
            // Each element is pushed as its own packed one-element tuple.
            for element in try Tuple.decode(from: await popBytes()).elements {
                store(idx, Tuple([element]).encode())
            }

        case "TUPLE_SORT":
            let count = Int(await popInt())
            var tuples: [FDB.Bytes] = []
            for _ in 0 ..< count {
                tuples.append(await popBytes())
            }
            tuples.sort { $0.lexicographicallyPrecedes($1) }
            for tuple in tuples {
                store(idx, tuple)
            }

        case "TUPLE_RANGE":
            let count = Int(await popInt())
            let packed = Tuple(await popTupleElements(count)).encode()
            store(idx, packed + [0x00])
            store(idx, packed + [0xFF])

        case "ENCODE_FLOAT":
            // The argument is the big-endian IEEE-754 representation; push the float element.
            let bits = await popBytes().reduce(UInt32(0)) { ($0 << 8) | UInt32($1) }
            store(idx, TupleElement.float(Float(bitPattern: bits)))

        case "ENCODE_DOUBLE":
            let bits = await popBytes().reduce(UInt64(0)) { ($0 << 8) | UInt64($1) }
            store(idx, TupleElement.double(Double(bitPattern: bits)))

        case "DECODE_FLOAT":
            guard case let .float(value) = await waitAndPop().item else {
                fatalError("DECODE_FLOAT expects a float")
            }
            store(idx, withUnsafeBytes(of: value.bitPattern.bigEndian) { Array($0) })

        case "DECODE_DOUBLE":
            guard case let .double(value) = await waitAndPop().item else {
                fatalError("DECODE_DOUBLE expects a double")
            }
            store(idx, withUnsafeBytes(of: value.bitPattern.bigEndian) { Array($0) })

        case "WAIT_FUTURE":
            // Waits on the top item (if it is a future) and pushes it back with its instruction number.
            let entry = await waitAndPop()
            store(entry.idx, entry.item)

        case "START_THREAD":
            // A new stack machine on the same database with independent state, run concurrently.
            let threadPrefix = await popBytes()
            let database = self.database
            let verbose = self.verbose
            threads.addTask {
                let machine = StackMachine(prefix: threadPrefix, database: database, verbose: verbose)
                try await machine.run()
            }

        case "WAIT_EMPTY":
            // Poll until no keys with the given prefix remain.
            let waitPrefix = await popBytes()
            while true {
                let isEmpty = try await database.withTransaction { transaction in
                    for try await _ in transaction.getRange(begin: waitPrefix, end: try FDB.strinc(waitPrefix)) {
                        return false
                    }
                    return true
                }
                if isEmpty { break }
                try Task.checkCancellation()
                try await Task.sleep(for: .milliseconds(50))
            }
            store(idx, toBytes("WAITED_FOR_EMPTY"))

        case "UNIT_TESTS":
            // Nothing to run yet. Like the other bindings, this pushes nothing onto the stack.
            break

        default:
            fatalError("Unhandled operation: \(op)")
        }
    }

    // Main run function - equivalent to Go's Run()
    func run() async throws {
        // Read instructions from database using the prefix, like Go version
        // The instructions live in the tuple range of the prefix: (prefix, ...)
        let prefixKey = Tuple(prefix).encode()
        let instructions = try await database.withTransaction { transaction in
            try await readRange(
                transaction,
                begin: .firstGreaterOrEqual(prefixKey + [0x00]),
                end: .firstGreaterOrEqual(prefixKey + [0xFF]),
                limit: 0,
                reverse: false,
                snapshot: false
            )
        }

        if verbose {
            print("Found \(instructions.count) instructions")
        }

        // Stack machines started by START_THREAD run as children of this group, so they are
        // awaited (and cancelled if this machine fails) when the group ends.
        try await withThrowingTaskGroup(of: Void.self) { threads in
            for (i, (_, value)) in instructions.enumerated() {
                // Unpack the instruction tuple from the value. Strings, byte strings and integers are
                // kept as native values; everything else stays a TupleElement.
                let instruction: Tuple = try Tuple.decode(from: value)
                try await processInstruction(i, instruction, threads: &threads)
            }

            try await threads.waitForAll()
        }

        print("StackTester completed successfully with \(instructions.count) instructions")
    }
}
