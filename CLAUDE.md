# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

Whenever working with this codebase (or any codebase), ALWAYS ask me first about any changes or modifications you are trying to make, and do NOT make those changes without me explicitly allowing so.

ALWAYS show me the code/implementation without me having to type `/plan` myself.

## Architecture

This is a **Kafka broker implementation in C++23**. The server listens on port 9092 (standard Kafka broker port) for TCP connections.

**Current state**: Early development - single-file implementation in `src/main.cpp`. Handles ApiVersions requests (API key 18) only. Topic management, consumer groups, and other Kafka features are not yet implemented.

**Connection model**: Single-threaded, sequential client handling. The main loop accepts one connection at a time, processes requests in a loop until the client disconnects, then accepts the next.

**Request processing flow** (`handle_client`):
1. Read 4-byte big-endian message size prefix
2. Read the full message body
3. Peek at `api_key` and `api_version` to select header parser:
   - ApiVersions v0-2: header v1 (`parse_request_header_v1`) - client_id is NULLABLE_STRING (int16 length prefix)
   - ApiVersions v3+: header v2/flexible (`parse_request_header_v2`) - client_id is COMPACT_NULLABLE_STRING (unsigned varint length), followed by TAG_BUFFER
4. Dispatch to API handler (currently only ApiVersions)
5. Build response with header v0 (just correlation_id) + body

**Kafka Protocol Notes**:
- All integers are big-endian encoded on the wire
- Messages: 4-byte size prefix + request/response body
- COMPACT_ARRAY: length encoded as N+1 (where 0 means null)
- COMPACT_STRING/COMPACT_NULLABLE_STRING: unsigned varint length (actual_length = encoded_length - 1)
- Unsigned varints: little-endian, 7 data bits per byte, MSB is continuation flag
- TAG_BUFFER: unsigned varint count of tagged fields, each with tag + size + data

**Note**: CURL is linked as a dependency but not yet used in the code.

## Build Commands

```bash
# Enter Nix development environment (drops into zsh via shellHook)
nix develop

# Build with CMake
cmake . -B build
cmake --build build

# Run the server
./build/src/main

# Run tests (GTest) - tests directory currently commented out in CMakeLists.txt
ctest --test-dir build

# Run a single test
ctest --test-dir build -R <test_name>

# Format code
clang-format -i src/*.cpp
```

## Development Environment

- **Language**: C++23 (required standard)
- **Package Manager**: Nix Flakes
- **Test Framework**: Google Test (GTest), Catch2 v3 available
- **Code Style**: Google style, 4-space indent (see `.clang-format`)

## Compiler Configuration

- Compiler: GCC
- Flags: `-Wall -Wfatal-errors -Wextra -Werror -g -O1`
