# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

Whenever working with this codebase (or any codebase), ALWAYS ask me first about any changes or modifications you are trying to make, and do NOT make those changes without me explicitly allowing so.

ALWAYS show me the code/implementation without me having to type `/plan` myself.

## Architecture

This is a **Kafka broker implementation in C++23**. The server listens on port 9092 (standard Kafka broker port) for TCP connections.

**Current state**: Early development - basic TCP socket server skeleton is implemented. The Kafka protocol layer (message parsing, topic management, consumer groups, etc.) is not yet implemented.

**Main components**:
- `src/main.cpp` - TCP server that binds to port 9092, accepts connections, and handles socket lifecycle

## Build Commands

```bash
# Enter Nix development environment
nix develop

# Build with CMake
cmake . -B build
cmake --build build

# Run the server
./build/src/main

# Run tests (GTest) - Note: tests directory currently commented out in CMakeLists.txt
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
