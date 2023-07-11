// swift-tools-version:5.2
import PackageDescription

let package = Package(
  name: "FoundationDB",
  products: [
    .library(name: "FoundationDB", targets: ["FoundationDB"]),
    .executable(name: "FoundationDBBindingTestRunner", targets: ["FoundationDBBindingTestRunner"]),
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-nio", from: "2.44.0"),
    .package(url: "git@github.com:FoundationDB/fdb-swift-c-packaging.git", .branch("main"))
  ],
  targets: [
    .target(name: "FoundationDB", dependencies: [.product(name: "NIO", package: "swift-nio"), "CFoundationDB"]),
    .target(name: "CFoundationDB"),
    .target(name: "FoundationDBBindingTest", dependencies: ["FoundationDB"]),
    .target(name: "FoundationDBBindingTestRunner", dependencies: ["FoundationDBBindingTest"]),
    .testTarget(name: "FoundationDBTests", dependencies: ["FoundationDB"]),
    .testTarget(name: "FoundationDBBindingTestTests", dependencies: ["FoundationDBBindingTest"]),
  ]
)
