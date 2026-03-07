# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Context

Whenever working with this codebase (or any codebase), ALWAYS ask me first about any changes or modifications you are trying to make, and do NOT make those changes without me explicitly allowing so.

ALWAYS show me the code/implementation without me having to type `/plan` myself.

_NEVER_ directly make changes to the source files under `src/`.

Save all plans to the `.claude/` directory under local project's root.
Do _NOT_ save plans to the home directory `~/`.

## Architecture

This is a **Kafka broker implementation in C++23**. The server listens on port 9092 (standard Kafka broker port) for TCP connections.

**Current state**: Early development — single-file implementation in `src/main.cpp` (~2070 lines). Supported APIs:

- **Produce** (API key 0, v0-11) — parses requests, writes RecordBatch data to partition log files under `/tmp/kraft-combined-logs/<topic>-<partition>/`, assigns baseOffset by walking existing batches; returns error for unknown topics
- **Fetch** (API key 1, v0-16)
- **ApiVersions** (API key 18, v0-4)
- **DescribeTopicPartitions** (API key 75, v0)

Reads KRaft cluster metadata from `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log` to resolve topic/partition information; returns UNKNOWN_TOPIC_OR_PARTITION for topics not found in that log.

**Connection model**: Multi-threaded — the main loop accepts connections and spawns a detached `std::thread` per client. Each thread runs `handle_client` in a loop until the client disconnects.

**Request processing flow** (`handle_client`):

1. Read 4-byte big-endian message size prefix
2. Read the full message body
3. Peek at `api_key` and `api_version` to select header parser via `uses_flexible_header()`:
   - Non-flexible (header v1, `parse_request_header_v1`): ApiVersions v0-3, Produce v0-8, Fetch v0-11. Fields: api_key, api_version, correlation_id, client_id (NULLABLE_STRING with int16 length prefix)
   - Flexible (header v2, `parse_request_header_v2`): ApiVersions v4, DescribeTopicPartitions (always), Fetch v12+, Produce v9+. Same fields as v1, plus TAG_BUFFER after client_id
4. Dispatch to API handler based on api_key
5. Build response:
   - ApiVersions: response header v0 (`build_response` — just correlation_id) + body
   - DescribeTopicPartitions / Fetch: response header v1 (`build_response_v1` — correlation_id + TAG_BUFFER) + body
   - Produce: response header v0 for v0-8, response header v1 for v9+

**Quirk**: ApiVersions v3 uses non-flexible header (v1) but has the v3+ body format (with COMPACT_STRING fields). The `uses_flexible_header()` check is `api_version > 3`, not `>= 3`. The body parser switches at `api_version >= 3`.

**Metadata log parsing** (`load_cluster_metadata`):

- Reads KRaft RecordBatch log file at a hardcoded path
- Parses RecordBatch frames: 12-byte header (including batchLength at offset 8), records start at offset 61
- Individual records use zigzag-encoded varints for length, timestamps, offsets, key/value lengths
- Record values contain metadata records prefixed with frame_version, api_key, version (all unsigned varints)
- Recognizes TopicRecord (api_key=2) and PartitionRecord (api_key=3), builds a name->TopicInfo map
- Partitions are associated with topics via UUID matching

**Partition log I/O** (`read_partition_log` / `write_partition_log`):

- Reads/writes partition data at `/tmp/kraft-combined-logs/<topic>-<partition>/00000000000000000000.log`
- `write_partition_log` creates the directory if missing, walks existing batches to determine next baseOffset, patches baseOffset in the RecordBatch copy, then appends
- `read_partition_log` returns raw bytes of the entire log file (used by Fetch responses)
- Separate from KRaft cluster metadata — these are the actual message data logs

**Kafka Protocol Notes**:

- All integers are big-endian encoded on the wire
- Messages: 4-byte size prefix + request/response body
- COMPACT_ARRAY: length encoded as N+1 (where 0 means null)
- COMPACT_STRING/COMPACT_NULLABLE_STRING: unsigned varint length (actual_length = encoded_length - 1)
- Unsigned varints: little-endian, 7 data bits per byte, MSB is continuation flag
- TAG_BUFFER: unsigned varint count of tagged fields, each with tag + size + data
- Produce v0-8 uses non-flexible encoding (ARRAY with INT32 count, STRING with INT16 length); v9+ uses flexible (COMPACT_ARRAY, COMPACT_STRING, TAG_BUFFERs)

**Note**: CURL is linked as a dependency but not yet used in the code.

## Build Commands

```bash
# Enter Nix development environment (drops into zsh via shellHook)
nix develop

# Build with CMake (in-source builds are forbidden by CMakeLists.txt)
cmake . -B build
cmake --build build

# Run the server
./build/src/main

# Run tests (GTest) - no tests/ directory exists yet; add_subdirectory(tests) is commented out in root CMakeLists.txt
ctest --test-dir build

# Run a single test
ctest --test-dir build -R <test_name>

# Format code
clang-format -i src/*.cpp
```

## Development Environment

- **Language**: C++23 (required standard)
- **Package Manager**: Nix Flakes
- **Test Framework**: Google Test (GTest), Catch2 v3 available — no `tests/` directory exists yet, and `add_subdirectory(tests)` is commented out in root CMakeLists.txt
- **Code Style**: Google style, 4-space indent (see `.clang-format`)

## Compiler Configuration

- Compiler: GCC
- Flags: `-Wall -Wfatal-errors -Wextra -Werror -g -O1`
