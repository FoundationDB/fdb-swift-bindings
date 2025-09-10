# FoundationDB Swift Bindings

Swift bindings for FoundationDB, providing a native Swift API for interacting with FoundationDB clusters.

## Features

- **Native Swift API** - Idiomatic Swift interfaces for FoundationDB operations
- **Async/Await Support** - Modern Swift concurrency with async sequences
- **High Performance** - Optimized range iteration with background pre-fetching
- **Type Safety** - Swift's type system for safer database operations

## Quick Start

### Initialize the Client

```swift
import FoundationDB

// Initialize FoundationDB
try await FdbClient.initialize()
let database = try FdbClient.openDatabase()
```

### Basic Operations

```swift
// Simple key-value operations
try await database.withTransaction { transaction in
    // Set a value
    transaction.setValue("world", for: "hello")
    
    // Get a value
    if let value = try await transaction.getValue(for: "hello") {
        print(String(bytes: value)) // "world"
    }
    
    // Delete a key
    transaction.clear(key: "hello")
}
```

### Range Queries

```swift
// Efficient streaming over large result sets
let sequence = transaction.readRange(
    beginSelector: .firstGreaterOrEqual("user:"),
    endSelector: .firstGreaterOrEqual("user;")
)

for try await (key, value) in sequence {
    let userId = String(bytes: key)
    let userData = String(bytes: value)
    // Process each key-value pair as it streams
}
```

### Atomic Operations

```swift
try await database.withTransaction { transaction in
    // Atomic increment
    let counterKey = "counter"
    let increment = withUnsafeBytes(of: Int64(1).littleEndian) { Array($0) }
    transaction.atomicOp(key: counterKey, param: increment, mutationType: .add)
}
```

## Key Components

- **Transaction Management** - Automatic retry logic and conflict resolution
- **AsyncKVSequence** - Memory-efficient streaming iteration with background pre-fetching
- **Key Selectors** - Flexible key positioning for range queries
- **Atomic Operations** - Built-in atomic mutations (ADD, AND, OR, etc.)
- **Network Options** - Configurable client behavior and performance tuning

## Performance

The bindings include several performance optimizations:

- **Background Pre-fetching** - Range queries pre-fetch next batch while processing current data
- **Streaming Results** - Large result sets don't require full buffering in memory
- **Connection Pooling** - Efficient connection management to FoundationDB clusters
- **Configurable Batching** - Tunable batch sizes for optimal throughput

## Requirements

- Swift 5.7+
- FoundationDB 7.1+
- macOS 12+ / Linux

## Installation

Add the package to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/apple/fdb-swift-bindings", from: "1.0.0")
]
```

## Documentation

For detailed API documentation and advanced usage patterns, see the inline documentation in the source files.

## License

Licensed under the Apache License, Version 2.0. See LICENSE for details.