# FoundationDB Swift Bindings

Swift bindings for FoundationDB, providing a native Swift API for interacting with FoundationDB clusters.

## Quick Start

### Initialize the Client

```swift
import FoundationDB

// Initialize FoundationDB
try await FDBClient.initialize()
let database = try FDBClient.openDatabase()
```

### Basic Operations

```swift
// Simple key-value operations
try await database.withTransaction { transaction in
    // Set a value
    let key = "hello"
    let value = "world"
    transaction.setValue([UInt8](value.utf8), for: [UInt8](key.utf8))

    // Get a value
    if let valueBytes = try await transaction.getValue(for: [UInt8](key.utf8)) {
        print(String(decoding: valueBytes, as: UTF8.self)) // "world"
    }

    // Delete a key
    transaction.clear(key: [UInt8](key.utf8))
}
```

### Range Queries

```swift
// Efficient streaming over large result sets
let sequence = transaction.getRange(
    beginSelector: .firstGreaterOrEqual([UInt8]("user:".utf8)),
    endSelector: .firstGreaterOrEqual([UInt8]("user;".utf8))
)

for try await (key, value) in sequence {
    let userId = String(decoding: key, as: UTF8.self)
    let userData = String(decoding: value, as: UTF8.self)
    // Process each key-value pair as it streams
}
```

### Atomic Operations

```swift
try await database.withTransaction { transaction in
    // Atomic increment
    let counterKey = "counter"
    let increment = withUnsafeBytes(of: Int64(1).littleEndian) { Array($0) }
    transaction.atomicOp(key: [UInt8](counterKey.utf8), param: increment, mutationType: .add)
}
```

## Requirements

- Swift 6.1+
- FoundationDB 7.1+ client library and headers
- `pkg-config`
- macOS 12+ / Linux

## Finding the FoundationDB C bindings

Whether you are building this repository directly or adding it as a Swift
package dependency, the system needs the FoundationDB C client library and a
`pkg-config` file so that the Swift Package Manager can locate it. Follow the
steps below once on any machine that will compile the package.

### 1. Install the FoundationDB client library

Download and install the FoundationDB client package for your platform from the
[FoundationDB releases page](https://github.com/apple/foundationdb/releases).
This provides:

- The shared library (`libfdb_c.dylib` on macOS, `libfdb_c.so` on Linux)
- The C headers at `<your-install-prefix>/include/foundationdb/`

### 2. Install `pkg-config`

```bash
brew install pkg-config   # macOS
# or: apt install pkg-config / yum install pkgconfig
```

### 3. Create the `libfdb.pc` pkg-config file

The Swift Package Manager uses `pkg-config` to find the library at build
time. A template is included in the repo at
`Sources/CFoundationDB/include/CFoundationDB_mac.pc` (and `..._linux.pc`).
Copy it to a directory on your `PKG_CONFIG_PATH`. Note that the file
_must_ be named `libfdb.pc`:

```bash
# Common locations — use whichever exists on your system:
#   /usr/local/lib/pkgconfig/    (default on macOS/Linux)
#   /opt/homebrew/lib/pkgconfig/ (Homebrew)

sudo cp Sources/CFoundationDB/include/CFoundationDB_mac.pc \
        /usr/local/lib/pkgconfig/libfdb.pc
```

Open the copy and set `prefix` to the root of your FoundationDB installation:

```ini
prefix=/usr/local          # adjust to your actual install prefix
exec_prefix=${prefix}
includedir=${prefix}/include
libdir=${exec_prefix}/lib

Name: CFoundationDB
Description: The foundationdb C library
Version: 7.3.77            # adjust to match your installed version
Cflags: -I${includedir}
Libs: -L${libdir} -lfdb_c
```

> **Important:** `Cflags` must point to the *parent* of the `foundationdb/`
> directory (i.e., `${includedir}`, **not** `${includedir}/foundationdb`).
> The header is included as `#include <foundationdb/fdb_c.h>`, so the compiler
> needs to search from the directory that *contains* the `foundationdb/` folder.

Verify the configuration before building:

```bash
pkg-config --cflags --libs libfdb
# Expected output (paths will match your prefix):
# -I/usr/local/include -L/usr/local/lib -lfdb_c
```

## Adding as a Swift package dependency

Add the package to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/FoundationDB/fdb-swift-bindings", branch: "main")
]
```

## Building locally

After completing the setup above to make the FoundationDB C client visible, build with:

```bash
swift build
```

### Running the tests

To run the tests, there must be a locally accessible FoundationDB cluster.
The standard FoundationDB installer registers a launch daemon (macOS) or
systemd service (Linux) that starts an FDB server automatically, with a
cluster file at:

- macOS: `/usr/local/etc/foundationdb/fdb.cluster`
- Linux: `/etc/foundationdb/fdb.cluster`

If you are using a custom or minimal install the server may not start
automatically.  Consult your installation's documentation.

To use a non-default cluster file, set the `FDB_CLUSTER_FILE` environment variable
when running tests:

```bash
FDB_CLUSTER_FILE=path/to/fdb.cluster swift test
```

The `libfdb_c` dynamic library must also be on the dynamic library search path
at test run time. This is automatic with a standard install. For a custom
install prefix, add the library directory to the path before running tests by adjusting
your system path or (on Linux) using the `LD_LIBRARY_PATH` environment variable.

## Documentation

For detailed API documentation and advanced usage patterns, see the inline documentation in the source files.

## License

Licensed under the Apache License, Version 2.0. See LICENSE for details.
