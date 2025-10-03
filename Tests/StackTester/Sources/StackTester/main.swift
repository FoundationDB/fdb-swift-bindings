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
guard CommandLine.arguments.count >= 3 else {
    print("Usage: stacktester <prefix> <api_version> [cluster_file]")
    exit(1)
}

let prefix = Array(CommandLine.arguments[1].utf8)
let apiVersionString = CommandLine.arguments[2]
let clusterFile = CommandLine.arguments.count > 3 ? CommandLine.arguments[3] : nil

guard let apiVersion = Int32(apiVersionString) else {
    print("Invalid API version: \(apiVersionString)")
    exit(1)
}

// Use a serial queue for thread-safe communication
let syncQueue = DispatchQueue(label: "stacktester.sync")
var finished = false
var finalError: Error? = nil

Task {
    do {
        try await FdbClient.initialize(version: apiVersion)
        let database = try FdbClient.openDatabase(clusterFilePath: clusterFile)
        let stackMachine = StackMachine(prefix: prefix, database: database, verbose: false)
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
    RunLoop.current.run(mode: .default, before: Date(timeIntervalSinceNow: 0.1))
}

let error = syncQueue.sync { finalError }

if let error = error {
    print("Final error: \(error)")
    exit(1)
}

exit(0)
