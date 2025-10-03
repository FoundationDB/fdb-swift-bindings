## TODO: Find SDK directory instead of looking at hardcoded directory.
## TODO: -enable-testing only on debug builds.

if(NOT TARGET fdb_c)
    message(FATAL_ERROR "Swift bindings require C bindings to be built first. Make sure fdb_c target exists.")
endif()

if(CMAKE_Swift_COMPILER_VERSION VERSION_LESS "6.0")
    message(FATAL_ERROR "Swift 6.0 or later is required for Swift Testing support")
endif()

set(FDB_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/bindings/c)
set(SWIFT_SDK_DIR "/usr/lib/swift/linux/")
set(SWIFT_SDK_STATIC_DIR "/usr/lib/swift_static/linux")

file(GLOB_RECURSE SWIFT_BINDING_SOURCES "Sources/**/*.swift")
add_library(FoundationDB-Swift ${SWIFT_BINDING_SOURCES})

add_dependencies(FoundationDB-Swift fdb_c)

target_include_directories(FoundationDB-Swift PUBLIC
    ${FDB_INCLUDE_DIR}
    ${CMAKE_BINARY_DIR}/bindings/c/foundationdb
)

set(STATIC_SWIFT_LIBS
    ${SWIFT_SDK_STATIC_DIR}/libswiftCore.a
    ${SWIFT_SDK_STATIC_DIR}/libswiftDispatch.a
    ${SWIFT_SDK_STATIC_DIR}/libswiftSynchronization.a
    ${SWIFT_SDK_STATIC_DIR}/libswift_Concurrency.a

    ${SWIFT_SDK_STATIC_DIR}/libFoundation.a
    ${SWIFT_SDK_STATIC_DIR}/libCoreFoundation.a
    ${SWIFT_SDK_STATIC_DIR}/libFoundationInternationalization.a
    ${SWIFT_SDK_STATIC_DIR}/libswiftCore.a
    ${SWIFT_SDK_STATIC_DIR}/libswift_RegexParser.a
    ${SWIFT_SDK_STATIC_DIR}/libswift_StringProcessing.a
)

target_link_directories(FoundationDB-Swift PUBLIC
    "${SWIFT_SDK_DIR}"
)

target_link_libraries(FoundationDB-Swift
  PUBLIC
    fdb_c
    ${STATIC_SWIFT_LIBS}
)
  
target_compile_options(FoundationDB-Swift PUBLIC
    "SHELL:-I ${FDB_INCLUDE_DIR}"
    "SHELL:-I ${CMAKE_CURRENT_SOURCE_DIR}/Sources"
    "SHELL:-I ${SWIFT_SDK_DIR}"
    "SHELL:-enable-testing"
)

set_target_properties(FoundationDB-Swift PROPERTIES
    Swift_MODULE_NAME "FoundationDB"
)

# Tests - Configure for Swift Testing
file(GLOB_RECURSE SWIFT_BINDING_TEST_SOURCES "Tests/**/*.swift")
add_executable(FoundationDB-Swift-Tests
    ${SWIFT_BINDING_TEST_SOURCES}
)

add_dependencies(FoundationDB-Swift-Tests FoundationDB-Swift fdb_c)
target_link_libraries(FoundationDB-Swift-Tests PRIVATE FoundationDB-Swift Testing)

set_target_properties(FoundationDB-Swift-Tests PROPERTIES
    Swift_MODULE_NAME "FoundationDBTests"
)

add_executable(stacktester_swift
    Tests/StackTester/Sources/StackTester/StackTester.swift
    Tests/StackTester/Sources/StackTester/main.swift
)
add_dependencies(stacktester_swift FoundationDB-Swift)
target_link_libraries(stacktester_swift PRIVATE FoundationDB-Swift)

set_target_properties(stacktester_swift PROPERTIES
    Swift_MODULE_NAME "StackTester"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bindings/swift/bin"
)

install(TARGETS FoundationDB-Swift stacktester_swift
    RUNTIME DESTINATION bindings/swift/bin
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)

add_test(NAME FoundationDB-Swift-Tests COMMAND FoundationDB-Swift-Tests)
