/*
 * main.swift
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

// Entry point - using RunLoop to keep the process alive
// Line-buffer stdout so output is not lost if the process is killed (e.g. on timeout).
setvbuf(stdout, nil, _IOLBF, 0)

guard CommandLine.arguments.count >= 3 else {
    print("Usage: stacktester <prefix> <api_version> [cluster_file]")
    exit(1)
}

let prefix = toBytes(CommandLine.arguments[1])
let apiVersionString = CommandLine.arguments[2]
let clusterFile = CommandLine.arguments.count > 3 ? CommandLine.arguments[3] : nil

guard let apiVersion = Int(apiVersionString) else {
    print("Invalid API version: \(apiVersionString)")
    exit(1)
}

// Use a serial queue for thread-safe communication
let syncQueue = DispatchQueue(label: "stacktester.sync")
var finished = false
var finalError: Error?

Task {
    do {
        try await FDBClient.initialize(version: apiVersion)
        let database = try FDBClient.openDatabase(clusterFilePath: clusterFile)
        let stackMachine = StackMachine(prefix: prefix, database: database, verbose: ProcessInfo.processInfo.environment["STACKTESTER_VERBOSE"] != nil)
        try await stackMachine.run()
        print("StackMachine completed successfully")

        syncQueue.sync {
            finished = true
        }
    } catch {
        print("Error occurred: \(error)")
        syncQueue.sync {
            finalError = error
            finished = true
        }
    }
}

while true {
    let isFinished = syncQueue.sync { finished }
    if isFinished { break }
    let _ = RunLoop.current.run(mode: .default, before: Date(timeIntervalSinceNow: 0.1))
}

let error = syncQueue.sync { finalError }

// Use _exit rather than exit: the database is torn down on the network thread as it is released, and
// running C++ static destructors via exit() at the same time can crash libfdb_c.
// See: https://github.com/FoundationDB/fdb-swift-bindings/issues/32
if let error = error {
    print("Final error: \(error)")
    fflush(stdout)
    _exit(1)
}

fflush(stdout)
_exit(0)
