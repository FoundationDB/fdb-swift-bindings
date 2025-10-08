# FoundationDB Swift Bindings

Swift bindings for FoundationDB, providing a native Swift API for interacting with FoundationDB clusters.

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

## Requirements

- Swift 6.0+
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
