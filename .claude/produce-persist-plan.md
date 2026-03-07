# Plan: Persist Produce records to disk

## Context

The Produce handler currently parses requests but discards the raw RecordBatch data (`offset += actual_len`). We need to capture those bytes, write them to the partition log file, and return correct offsets in the response.

Log path: `/tmp/kraft-combined-logs/<topic>-<partition>/00000000000000000000.log`

## File: `src/main.cpp`

---

### Change 1: Add `#include <filesystem>` (line 7 area)

After `#include <unistd.h>`:

```cpp
#include <filesystem>
```

Needed for `std::filesystem::create_directories()`.

---

### Change 2: Add `records` field to `ProduceRequestPartition` (line 91-93)

```cpp
// before:
struct ProduceRequestPartition {
    int32_t partition_index;
};

// after:
struct ProduceRequestPartition {
    int32_t partition_index;
    std::vector<uint8_t> records;
};
```

---

### Change 3: Capture record bytes in v9+ parse path (lines 1058-1069)

```cpp
// before:
                        topic.partitions.push_back({part_idx});

                        // records (COMPACT_RECORDS): length as unsigned varint
                        uint32_t records_len;
                        vb = read_unsigned_varint(data, len, offset,
                                                  records_len);
                        if (vb == 0) return false;
                        offset += vb;
                        if (records_len > 0) {
                            uint32_t actual_len = records_len - 1;
                            if (offset + actual_len > len) return false;
                            offset += actual_len;  // skip record data
                        }

// after:
                        // records (COMPACT_RECORDS): length as unsigned varint
                        uint32_t records_len;
                        vb = read_unsigned_varint(data, len, offset,
                                                  records_len);
                        if (vb == 0) return false;
                        offset += vb;
                        std::vector<uint8_t> rec_data;
                        if (records_len > 0) {
                            uint32_t actual_len = records_len - 1;
                            if (offset + actual_len > len) return false;
                            rec_data.assign(data + offset,
                                            data + offset + actual_len);
                            offset += actual_len;
                        }
                        topic.partitions.push_back(
                            {part_idx, std::move(rec_data)});
```

Note: the old `topic.partitions.push_back({part_idx});` is removed and replaced by the new push_back after capturing the record data.

---

### Change 4: Capture record bytes in v0-8 parse path (lines 1149-1161)

```cpp
// before:
                topic.partitions.push_back({part_idx});

                // records: RECORDS (INT32 size prefix + data)
                if (offset + 4 > len) return false;
                int32_t records_size =
                    (data[offset] << 24) | (data[offset + 1] << 16) |
                    (data[offset + 2] << 8) | data[offset + 3];
                offset += 4;
                if (records_size > 0) {
                    if (offset + static_cast<size_t>(records_size) > len) {
                        return false;
                    }
                    offset += records_size;  // skip record data
                }

// after:
                // records: RECORDS (INT32 size prefix + data)
                if (offset + 4 > len) return false;
                int32_t records_size =
                    (data[offset] << 24) | (data[offset + 1] << 16) |
                    (data[offset + 2] << 8) | data[offset + 3];
                offset += 4;
                std::vector<uint8_t> rec_data;
                if (records_size > 0) {
                    if (offset + static_cast<size_t>(records_size) > len) {
                        return false;
                    }
                    rec_data.assign(data + offset,
                                    data + offset + records_size);
                    offset += records_size;
                }
                topic.partitions.push_back(
                    {part_idx, std::move(rec_data)});
```

Note: the old `topic.partitions.push_back({part_idx});` is removed and replaced by the new push_back after capturing the record data.

---

### Change 5: Add `write_partition_log()` function (after `read_partition_log`, insert after line ~835)

Insert right after the closing `}` of `read_partition_log()`:

```cpp
/**
 * Append a RecordBatch to the partition log file, updating baseOffset.
 * CRC (offset 17) covers bytes 21+, so modifying baseOffset (0-7) is safe.
 * Returns the assigned baseOffset, or -1 on error.
 */
int64_t write_partition_log(const std::string& topic_name,
                            int32_t partition_id,
                            const std::vector<uint8_t>& record_batch) {
    if (record_batch.size() < 61) {
        std::cerr << "RecordBatch too small: " << record_batch.size()
                  << " bytes" << std::endl;
        return -1;
    }

    std::string dir = "/tmp/kraft-combined-logs/" + topic_name + "-" +
                      std::to_string(partition_id);
    std::string path = dir + "/00000000000000000000.log";

    // Create directory if it doesn't exist
    std::filesystem::create_directories(dir);

    // Determine next offset by walking existing batches
    int64_t next_offset = 0;
    auto existing = read_partition_log(topic_name, partition_id);
    if (!existing.empty()) {
        size_t pos = 0;
        while (pos + 61 <= existing.size()) {
            const uint8_t* b = existing.data() + pos;
            int64_t batch_base =
                ((int64_t)b[0] << 56) | ((int64_t)b[1] << 48) |
                ((int64_t)b[2] << 40) | ((int64_t)b[3] << 32) |
                ((int64_t)b[4] << 24) | ((int64_t)b[5] << 16) |
                ((int64_t)b[6] << 8) | (int64_t)b[7];
            int32_t batch_length =
                (b[8] << 24) | (b[9] << 16) | (b[10] << 8) | b[11];
            if (batch_length <= 0) break;
            size_t total_batch_size = 12 + static_cast<size_t>(batch_length);
            if (pos + total_batch_size > existing.size()) break;

            int32_t last_offset_delta =
                (b[23] << 24) | (b[24] << 16) | (b[25] << 8) | b[26];
            int64_t batch_end = batch_base + last_offset_delta + 1;
            if (batch_end > next_offset) {
                next_offset = batch_end;
            }
            pos += total_batch_size;
        }
    }

    // Patch baseOffset (bytes 0-7) in a copy
    std::vector<uint8_t> batch_copy = record_batch;
    for (int i = 0; i < 8; ++i) {
        batch_copy[i] = (next_offset >> (56 - i * 8)) & 0xFF;
    }

    // Append to log file
    std::ofstream file(path, std::ios::binary | std::ios::app);
    if (!file) {
        std::cerr << "Failed to open log file: " << path << std::endl;
        return -1;
    }
    file.write(reinterpret_cast<const char*>(batch_copy.data()),
               static_cast<std::streamsize>(batch_copy.size()));
    file.flush();
    if (!file.good()) {
        std::cerr << "Failed to write log file: " << path << std::endl;
        return -1;
    }

    return next_offset;
}
```

**Key design decisions:**

1. **Walking all batches** to find the next offset — the log may contain multiple RecordBatch frames from prior produces.

2. **Patching only bytes 0-7 (baseOffset)** — The CRC at offset 17 covers bytes 21 through end of batch. Bytes 0-7 are outside CRC range, so modifying baseOffset does NOT invalidate the CRC.

3. **`std::ios::app`** — appends without truncating; creates the file if it doesn't exist.

4. **`std::filesystem::create_directories()`** — idempotent; creates the directory tree if needed.

---

### Change 6: Write records to disk in `handle_client()` BEFORE building response (lines 1861-1862)

Insert between `auto metadata = load_cluster_metadata();` (line 1861) and `std::vector<uint8_t> body = build_produce_response(...)` (line 1862):

```cpp
            // Write record batches to disk before building response
            for (const auto& topic : req.topics) {
                auto it = metadata.find(topic.name);
                if (it == metadata.end()) continue;

                const TopicInfo& info = it->second;
                for (const auto& part : topic.partitions) {
                    if (part.records.empty()) continue;

                    bool partition_exists = false;
                    for (const auto& p : info.partitions) {
                        if (p.partition_id == part.partition_index) {
                            partition_exists = true;
                            break;
                        }
                    }
                    if (!partition_exists) continue;

                    int64_t assigned = write_partition_log(
                        topic.name, part.partition_index, part.records);
                    if (assigned < 0) {
                        std::cerr << "Failed to write records for "
                                  << topic.name << "-" << part.partition_index
                                  << std::endl;
                    }
                }
            }
```

**Why before the response:** `build_produce_response()` calls `read_partition_log()` internally to compute `base_offset` and `log_start_offset`. Writing first ensures those values reflect the newly appended data.

---

### Change 7: Fix `build_produce_response()` offset computation to walk all batches (lines 1250-1273)

The current code only reads the **first** batch to compute offsets. After appending to multi-batch logs, `base_offset` must come from the **last** batch (the one just written).

```cpp
// before (lines 1250-1273):
            int64_t base_offset = -1;
            int64_t log_start_offset = -1;
            if (error == ERROR_NONE) {
                auto log_data =
                    read_partition_log(topic.name, part.partition_index);
                if (!log_data.empty() && log_data.size() >= 61) {
                    const uint8_t* b = log_data.data();
                    // log_start_offset = baseOffset of first batch
                    log_start_offset =
                        ((int64_t)b[0] << 56) | ((int64_t)b[1] << 48) |
                        ((int64_t)b[2] << 40) | ((int64_t)b[3] << 32) |
                        ((int64_t)b[4] << 24) | ((int64_t)b[5] << 16) |
                        ((int64_t)b[6] << 8) | (int64_t)b[7];
                    int32_t last_offset_data =
                        (b[23] << 24) | (b[24] << 16) | (b[25] << 8) | b[26];
                    // base_offset = next available offset (high watermark)
                    base_offset = log_start_offset + last_offset_data + 1;
                } else {
                    // empty or missing log - first record goes at offset 0
                    base_offset = 0;
                    log_start_offset = 0;
                }
            }

// after:
            int64_t base_offset = -1;
            int64_t log_start_offset = -1;
            if (error == ERROR_NONE) {
                auto log_data =
                    read_partition_log(topic.name, part.partition_index);
                if (!log_data.empty() && log_data.size() >= 61) {
                    const uint8_t* b = log_data.data();
                    // log_start_offset = baseOffset of first batch
                    log_start_offset =
                        ((int64_t)b[0] << 56) | ((int64_t)b[1] << 48) |
                        ((int64_t)b[2] << 40) | ((int64_t)b[3] << 32) |
                        ((int64_t)b[4] << 24) | ((int64_t)b[5] << 16) |
                        ((int64_t)b[6] << 8) | (int64_t)b[7];

                    // Walk to last batch to get base_offset of just-written batch
                    size_t pos = 0;
                    int64_t last_batch_base = log_start_offset;
                    while (pos + 61 <= log_data.size()) {
                        const uint8_t* hdr = log_data.data() + pos;
                        last_batch_base =
                            ((int64_t)hdr[0] << 56) | ((int64_t)hdr[1] << 48) |
                            ((int64_t)hdr[2] << 40) | ((int64_t)hdr[3] << 32) |
                            ((int64_t)hdr[4] << 24) | ((int64_t)hdr[5] << 16) |
                            ((int64_t)hdr[6] << 8) | (int64_t)hdr[7];
                        int32_t bl =
                            (hdr[8] << 24) | (hdr[9] << 16) |
                            (hdr[10] << 8) | hdr[11];
                        if (bl <= 0) break;
                        size_t total = 12 + static_cast<size_t>(bl);
                        if (pos + total > log_data.size()) break;
                        pos += total;
                    }
                    // base_offset = baseOffset of the last batch (just written)
                    base_offset = last_batch_base;
                } else {
                    base_offset = 0;
                    log_start_offset = 0;
                }
            }
```

---

## Summary of all changes

| # | Location | Description |
|---|----------|-------------|
| 1 | Line ~7 (includes) | Add `#include <filesystem>` |
| 2 | Lines 91-93 | Add `std::vector<uint8_t> records` to `ProduceRequestPartition` |
| 3 | Lines 1058-1069 | Capture record bytes in v9+ parse path |
| 4 | Lines 1149-1161 | Capture record bytes in v0-8 parse path |
| 5 | After line ~835 | New `write_partition_log()` function |
| 6 | Lines 1861-1862 | Write to disk in `handle_client()` before response |
| 7 | Lines 1250-1273 | Walk all batches for correct `base_offset` |

## Verification

1. `cmake . -B build && cmake --build build` — must compile cleanly with `-Werror`
2. Run the server, produce a message to a known topic/partition, verify:
   - The log file is created/appended at the expected path
   - The response `base_offset` reflects the assigned offset
   - Subsequent Fetch requests return the written data
3. Produce to an unknown topic — verify `ERROR_UNKNOWN_TOPIC_OR_PARTITION` with no file written
4. Produce to a partition that already has data — verify `base_offset` increments correctly
