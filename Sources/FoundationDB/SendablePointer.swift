/*
 * SendablePointer.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2016-2026 Apple Inc. and the FoundationDB project authors
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

/// Owns a native object allocated by the FDB C client, and makes it usable from Swift concurrency.
///
/// The type is `Sendable` even though `OpaquePointer` is not. That is an assertion about how the FDB
/// C client behaves, not something the compiler checks, so it rests on the following:
///
///  1. *FDB serializes access to these objects itself.* For transactions and databases, the C client
///  hands out `ThreadSafeTransaction` and `ThreadSafeDatabase` objects (see
///  `fdbclient/ThreadSafeTransaction.cpp`). Each operation on them is marshalled onto the client's
///  network thread, so concurrent calls from different tasks are serialized by FDB rather than
///  running at the same time. (With the multi-threaded client, a database is bound to one of
///  several client threads, but access to any one object is still serialized.) The order of
///  concurrent calls is not deterministic. Futures work differently: they guard their own state with
///  an internal spin lock (`ThreadSingleAssignmentVarBase`) rather than moving work to the network
///  thread. This is how the C client is implemented, and the Java binding relies on it too, but the
///  public C API documentation does not promise it.
///
///  2. *The native object is destroyed exactly once, after its last use.* The wrapper is
///  `~Copyable`, so there is never more than one owner, and the destructor runs when that owner is
///  deinitialized. That rules out destroying the object twice. It does *not* by itself stop a raw
///  `OpaquePointer` from outliving the wrapper, because a pointer is trivially copyable, and
///  nothing here enforces that. Instead, **users of ``pointer`` are required to only do something
///  safe with it**: hand it straight to a native function as part of a call, and never keep a copy
///  (do not store it, capture it in a closure that outlives the call, or return it). The wrapper is
///  stored in a class, and a class instance stays alive for the duration of any method call or
///  `await` made on it, so a pointer used that way cannot be destroyed underneath the call.
struct SendablePointer: Sendable, ~Copyable {
    /// The native pointer. Only pass it directly to native functions. Do not keep a copy of it.
    nonisolated(unsafe) let pointer: OpaquePointer
    private let destructor: @Sendable (OpaquePointer) -> Void

    /// Takes ownership of `pointer`.
    ///
    /// - Parameters:
    ///   - pointer: The native object. This wrapper becomes its only owner.
    ///   - destructor: The native function that destroys the object, such as `fdb_transaction_destroy`.
    ///     It runs exactly once, when this value is deinitialized.
    init(pointer: OpaquePointer, destructor: @escaping @Sendable (OpaquePointer) -> Void) {
        self.pointer = pointer
        self.destructor = destructor
    }

    deinit {
        destructor(pointer)
    }
}
