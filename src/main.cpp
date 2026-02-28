#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <ios>
#include <iostream>
#include <iterator>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// the ApiVersions request
constexpr int16_t API_VERSIONS = 18;
// DescribeTopicPartitions
constexpr int16_t DESCRIBE_TOPIC_PARTITIONS = 75;
constexpr int16_t ERROR_UNKNOWN_TOPIC_OR_PARTITION = 3;

constexpr int16_t ERROR_NONE = 0;
constexpr int16_t ERROR_UNSUPPORTED_VERSION = 35;

const std::string METADATA_LOG_PATH =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
constexpr uint32_t METADATA_TOPIC_RECORD = 2;
constexpr uint32_t METADATA_PARTITION_RECORD = 3;

/**
 * Full wire format for ApiVersions Request v4:
 * - Message size (4 bytes, big endian)
 * - Request Header v2
 *   - request_api_key
 *   - request_api_version
 *   - correlation_id
 *   - client_id
 *   - TAG_BUFFER
 * - Request body
 *   - client_software_name (COMPACT_STRING)
 *   - client_software_version (COMPACT_STRING)
 *   - TAG_BUFFER
 */

struct RequestHeader {
    int16_t request_api_key;
    int16_t request_api_version;
    int32_t correlation_id;
    std::string client_id;
};

struct ApiVersionsRequestBody {
    std::string client_software_name;
    std::string client_softare_version;
};

struct ApiVersionRange {
    int16_t api_key;
    int16_t min_version;
    int16_t max_version;
};

const std::vector<ApiVersionRange> SUPPORTED_API_VERSIONS = {
    {API_VERSIONS, 0, 4},
    {DESCRIBE_TOPIC_PARTITIONS, 0, 0},
    // add more here
};

/**
 DescribeTopicPartitions Request (API key 75, version 0), using flexible header
 v2

  Message size (4 bytes, big-endian)
  Request Header v2:
    request_api_key (INT16)
    request_api_version (INT16)
    correlation_id (INT32)
    client_id (NULLABLE_STRING - int16 length prefix)
    TAG_BUFFER
  Request Body:
    topics (COMPACT_ARRAY):
      for each topic:
        name (COMPACT_STRING)
        TAG_BUFFER
    response_partition_limit (INT32)
    cursor (nullable struct):
      0xff = null, otherwise:
        topic_name (COMPACT_STRING)
        partition_index (INT32)
        TAG_BUFFER
    TAG_BUFFER
*/
struct DescribeTopicPartitionsRequest {
    std::vector<std::string> topic_names;
    int32_t response_partition_limit;
};

struct PartitionInfo {
    int32_t partition_id;
    int32_t leader_id;
    int32_t leader_epoch;
    int32_t partition_epoch;
    std::vector<int32_t> replicas;
    std::vector<int32_t> isr;
};
struct TopicInfo {
    std::string name;
    std::array<uint8_t, 16> topic_id;
    std::vector<PartitionInfo> partitions;
};

bool is_version_supported(int16_t api_key, int16_t version) {
    for (const auto& api : SUPPORTED_API_VERSIONS) {
        if (api.api_key == api_key) {
            return version >= api.min_version && version <= api.max_version;
        }
    }
    return false;
}

/**
 * Read exactly n bytes from socket _into_ buffer
 */
bool read_exact(int fd, u_int8_t* buffer, size_t n) {
    size_t total_read = 0;
    while (total_read < n) {
        ssize_t bytes_read = read(fd, buffer + total_read, n - total_read);
        if (bytes_read <= 0) {
            return false;
        }
        total_read += bytes_read;
    }
    return true;
}

/**
 * Write exactly n bytes _from_ buffer _to_ socket
 */
bool write_exact(int fd, const uint8_t* buffer, size_t n) {
    size_t total_written = 0;
    while (total_written < n) {
        ssize_t bytes_written =
            write(fd, buffer + total_written, n - total_written);
        if (bytes_written <= 0) {
            return false;
        }
        total_written += bytes_written;
    }
    return true;
}

/**
 * Used for ApiVersion v0-2
 */
bool parse_request_header_v1(const uint8_t* data, size_t len,
                             RequestHeader& header, size_t& offset) {
    if (len < 8) {
        // api_key (2) + api_version (2) + correlation_id (4)
        return false;
    }
    header.request_api_key = (data[0] << 8) | data[1];
    header.request_api_version = (data[2] << 8) | data[3];
    header.correlation_id =
        (data[4] << 24) | (data[5] << 16) | (data[6] << 8) | data[7];
    offset = 8;

    // for v2
    if (len < offset + 2) {
        // need at least 2 more bytes for client_id length
        // in kafka's protocol, a NULLABLE_STRING is encoded as:
        //   - int16 (2 bytes) - length of the string
        //   - N bytes
        return false;
    }
    int16_t client_id_len = (data[offset] << 8) | data[offset + 1];
    offset += 2;

    if (client_id_len == -1) {
        header.client_id = "";  // null string
    } else if (client_id_len >= 0) {
        if (len < offset + static_cast<size_t>(client_id_len)) {
            return false;
        }
        header.client_id = std::string(
            reinterpret_cast<const char*>(data + offset), client_id_len);
        offset += client_id_len;
    } else {
        return false;
    }
    return true;
}

/**
 * Read unsigned varint from buffer
 * Returns number of bytes consumed, 0 on error
 * Unsigned varint:
 *   - space-efficient encoding
 *   - smaller numbers use fewer bytes
 *   - 7 bits for data
 *   - 1 bit (MSB/high bit) as a continuation flag
 *     - 1 = more bytes follow
 *     - 0 = last byte
 * varints are LITTLE ENDIAN!
 */
size_t read_unsigned_varint(const uint8_t* data, size_t len, size_t offset,
                            uint32_t& value) {
    value = 0;
    int shift = 0;
    size_t bytes_read = 0;
    while (offset + bytes_read < len) {
        uint8_t b = data[offset + bytes_read];
        bytes_read++;
        // shifting to left since the bytes arrive in little-endian order
        // here, least significant bits come first
        value |= static_cast<uint32_t>(b & 0x7F) << shift;
        if ((b & 0x80) == 0) {  // highest bit is 0
            return bytes_read;
        }
        shift += 7;
        if (shift > 28) {
            return 0;  // overflow
        }
    }
    return 0;  // incomplete
}

/**
 * Read zigzag-encoded signed varint from buffer.
 * Used in RecordBatch individual records.
 * Zigzag:
 *   - decode: (encoded >>> 1) ^ -(encoded & 1)
 *   - encode: (n << 1) ^ (n >> 31)
 *   - Encoding: signed -> encoded:
 *     - 0 -> 0
 *     - -1 -> 1
 *     - 1 -> 2
 *     - -2 -> 3
 *     - 2 -> 4
 * Small negative (and positive) numbers to unsigned value.
 * the sign bit is store in the lower 0 bit in the final result
 * `encoded >>> 1` is a logical (unsigned) right shift — recovers the magnitude
 * `encoded & 1` extracts the sign bit (0 = positive, 1 = negative)
 * `-(encoded & 1)` produces `0x00000000` or `0xFFFFFFFF`
 * XOR flips all bits back for negatives, does nothing for positives
 */
size_t read_zigzag_varint(const uint8_t* data, size_t len, size_t offset,
                          int32_t& value) {
    uint32_t raw;
    size_t bytes_read = read_unsigned_varint(data, len, offset, raw);
    if (bytes_read == 0) {
        return 0;
    }
    value = static_cast<int32_t>((raw >> 1) ^ -(raw & 1));
    return bytes_read;
}

/**
 * Read COMPACT_NULLABLE_STRING from buffer
 * Encoding: unsigned varint length (0=null, N=N-1 chars) + data
 */
bool read_compact_nullable_string(const uint8_t* data, size_t len,
                                  size_t& offset,
                                  std::optional<std::string>& out) {
    uint32_t length;
    size_t varint_bytes = read_unsigned_varint(data, len, offset, length);
    if (varint_bytes == 0) {
        return false;
    }
    offset += varint_bytes;

    if (length == 0) {
        out = std::nullopt;  // null string
        return true;
    }

    // COMPACT encoding: actual_length = encoded_length - 1
    // convetion of Kafka
    // encoded_length = 0 means null (no string at all)
    // encoded_length = 1 means string of length 0 (empty string "")
    length--;
    if (offset + length > len) {
        return false;
    }

    out = std::string(reinterpret_cast<const char*>(data + offset), length);
    offset += length;
    return true;
}

/**
 * Skip TAG_BUFFER in buffer
 * Encoding: unsigned varint num_fields, then for each field:
 *   - tag (uvarint)
 *   - size (uvarint)
 *   - data (size bytes)
 */
bool skip_tag_buffer(const uint8_t* data, size_t len, size_t& offset) {
    uint32_t num_tags;
    size_t varint_bytes = read_unsigned_varint(data, len, offset, num_tags);
    if (varint_bytes == 0) {
        return false;
    }
    offset += varint_bytes;

    for (uint32_t i = 0; i < num_tags; ++i) {
        uint32_t tag, size;
        varint_bytes = read_unsigned_varint(data, len, offset, tag);
        if (varint_bytes == 0) {
            return false;
        }
        offset += varint_bytes;

        varint_bytes = read_unsigned_varint(data, len, offset, size);
        if (varint_bytes == 0) {
            return false;
        }
        offset += varint_bytes;

        if (offset + size > len) {
            return false;
        }
        offset += size;
    }
    return true;
}

/**
 * parse request header v2 (flexible)
 * used for ApiVersions v3
 */
bool parse_request_header_v2(const uint8_t* data, size_t len,
                             RequestHeader& header, size_t& offset) {
    if (len < 8) {
        // api_key (2) + api_version (2) + correlation_id (4)
        return false;
    }
    header.request_api_key = (data[0] << 8) | data[1];
    header.request_api_version = (data[2] << 8) | data[3];
    header.correlation_id =
        (data[4] << 24) | (data[5] << 16) | (data[6] << 8) | data[7];
    offset = 8;

    // regular NULLABLE_STRING for client_id
    if (len < offset + 2) return false;
    int16_t client_id_len = (data[offset] << 8) | data[offset + 1];
    offset += 2;
    if (client_id_len == -1) {
        header.client_id = "";  // null string
    } else if (client_id_len >= 0) {
        if (len < offset + static_cast<size_t>(client_id_len)) {
            return false;
        }
        header.client_id = std::string(
            reinterpret_cast<const char*>(data + offset), client_id_len);
        offset += client_id_len;
    } else {
        return false;
    }

    // TAG_BUFFER in header
    if (!skip_tag_buffer(data, len, offset)) {
        return false;
    }

    return true;
}

/**
 * Parse ApiVersions request body v3+
 */
bool parse_api_versions_request_body_v3(const uint8_t* data, size_t len,
                                        size_t offset,
                                        ApiVersionsRequestBody& body) {
    // client_software_name (COMPACT_STRING)
    std::optional<std::string> name;
    if (!read_compact_nullable_string(data, len, offset, name)) {
        return false;
    }
    body.client_software_name = name.value_or("");
    // client_software_version (COMPACT_STRING)
    std::optional<std::string> version;
    if (!read_compact_nullable_string(data, len, offset, version)) {
        return false;
    }
    body.client_softare_version = version.value_or("");

    // TAG_BUFFER
    if (!skip_tag_buffer(data, len, offset)) {
        return false;
    }

    return true;
}

/**
topics: COMPACT_ARRAY of { name: COMPACT_STRING, TAG_BUFFER }
response_partition_limit: INT32
cursor: nullable struct (0xff = null, otherwise topic_name + partition_index +
TAG_BUFFER) TAG_BUFFER
*/
bool parse_describe_topic_partitions_request(
    const uint8_t* data, size_t len, size_t offset,
    DescribeTopicPartitionsRequest& req) {
    // topics: COMPACT_ARRAY
    uint32_t num_topics_raw;
    size_t varint_bytes =
        read_unsigned_varint(data, len, offset, num_topics_raw);
    if (varint_bytes == 0) {
        return false;
    }
    offset += varint_bytes;

    if (num_topics_raw > 0) {
        uint32_t num_topics = num_topics_raw - 1;
        for (uint32_t i = 0; i < num_topics; ++i) {
            std::optional<std::string> topic_name;
            if (!read_compact_nullable_string(data, len, offset, topic_name)) {
                return false;
            }
            if (!topic_name.has_value()) {
                return false;  // null topic name is invalid
            }
            req.topic_names.push_back(std::move(*topic_name));

            // TAG_BUFFER per topic entry
            if (!skip_tag_buffer(data, len, offset)) {
                return false;
            }
        }
    }

    // response_partition_limit (INT32)
    // tells the broker the max number of partitions the client wants back
    // across all topics
    if (offset + 4 > len) {
        return false;
    }
    req.response_partition_limit = (data[offset] << 24) |
                                   (data[offset + 1] << 16) |
                                   (data[offset + 2] << 8) | data[offset + 3];
    offset += 4;

    // cursor (nullable struct)
    // a nullable struct for pagination
    // the first byte indicates presense: 0xff = null
    // anything else = curour is present, meaning the client is continuing a
    // previous paginated response
    if (offset >= len) {
        return false;
    }
    uint8_t cursor_tag = data[offset];
    offset++;
    if (cursor_tag != 0xff) {
        // cursor present - skip topic_name + partition_index + TAG_BUFFER
        std::optional<std::string> cursor_topic;
        if (!read_compact_nullable_string(data, len, offset, cursor_topic)) {
            return false;
        }
        if (offset + 4 > len) {
            return false;
        }
        offset += 4;
        if (!skip_tag_buffer(data, len, offset)) {
            return false;
        }
    }

    // TAG_BUFFER
    if (!skip_tag_buffer(data, len, offset)) {
        return false;
    }

    return true;
}

/**
 DescribeTopicPartitions response, using response header v1:

  Message size (4 bytes, big-endian)
  Response Header v1:
    correlation_id (INT32)
    TAG_BUFFER
  Response Body:
    throttle_time_ms (INT32)
    topics (COMPACT_ARRAY):
      for each topic:
        error_code (INT16)
        name (COMPACT_NULLABLE_STRING)
        topic_id (UUID - 16 bytes)
        is_internal (BOOLEAN)
        partitions (COMPACT_ARRAY):
          for each partition:
            error_code (INT16)
            partition_index (INT32)
            leader_id (INT32)
            leader_epoch (INT32)
            replica_nodes (COMPACT_ARRAY of INT32)
            isr_nodes (COMPACT_ARRAY of INT32)
            eligible_leader_replicas (COMPACT_ARRAY of INT32)
            last_known_elr (COMPACT_ARRAY of INT32)
            offline_replicas (COMPACT_ARRAY of INT32)
            TAG_BUFFER
        topic_authorized_operations (INT32)
        TAG_BUFFER
    next_cursor (nullable struct):
      0xff = null, otherwise:
        topic_name (COMPACT_STRING)
        partition_index (INT32)
        TAG_BUFFER
    TAG_BUFFER
*/
std::vector<uint8_t> build_describe_topic_partitions_response(
    const DescribeTopicPartitionsRequest& req,
    const std::map<std::string, TopicInfo>& metadata) {
    std::vector<uint8_t> body;

    // throttle_time_ms (INT32)
    // NO throttling
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);

    // topics: COMPACT_ARRAY (length = N - 1)
    body.push_back(static_cast<uint8_t>(req.topic_names.size() + 1));

    for (const auto& topic_name : req.topic_names) {
        auto it = metadata.find(topic_name);
        if (it == metadata.end()) {
            // Topic not found - return ERROR_UNKNOWN_TOPIC_OR_PARTITION
            body.push_back((ERROR_UNKNOWN_TOPIC_OR_PARTITION >> 8) & 0xFF);
            body.push_back(ERROR_UNKNOWN_TOPIC_OR_PARTITION & 0xFF);

            // name: COMPACT_NULLABLE_STRING
            body.push_back(static_cast<uint8_t>(topic_name.size() + 1));
            body.insert(body.end(), topic_name.begin(), topic_name.end());

            // topic_id: UUID (16 bytes of zeros)
            for (int i = 0; i < 16; ++i) body.push_back(0x00);

            // is_internal: BOOLEAN (false)
            body.push_back(0x00);

            // partition: COMPACT_ARRAY (empty = 1)
            body.push_back(0x01);

            // topic_authorized_operations: INT32
            body.push_back(0x00);
            body.push_back(0x00);
            body.push_back(0x00);
            body.push_back(0x00);

            // TAG_BUFFER
            body.push_back(0x00);
        } else {
            const TopicInfo& info = it->second;

            // error_code: ERROR_NONE
            body.push_back(0x00);
            body.push_back(0x00);

            // name: COMPACT_NULLABLE_STRING
            // actual len is encoded as len + 1, as 0 is reserved for null
            body.push_back(static_cast<uint8_t>(topic_name.size() + 1));
            body.insert(body.end(), topic_name.begin(), topic_name.end());

            // topic_id: UUID (16 bytes)
            body.insert(body.end(), info.topic_id.begin(), info.topic_id.end());

            // is_internal: BOOLEAN (false)
            body.push_back(0x00);

            // partitions: COMPACT_ARRAY (empty = 1, meaning 0 elements)
            // length = N + 1
            body.push_back(static_cast<uint8_t>(info.partitions.size() + 1));

            for (const auto& p : info.partitions) {
                // error_code (INT16): ERROR_NONE
                body.push_back(0x00);
                body.push_back(0x00);

                // partition_index (INT32)
                body.push_back((p.partition_id >> 24) & 0xFF);
                body.push_back((p.partition_id >> 16) & 0xFF);
                body.push_back((p.partition_id >> 8) & 0xFF);
                body.push_back(p.partition_id & 0xFF);

                // leader_id (INT32)
                body.push_back((p.leader_id >> 24) & 0xFF);
                body.push_back((p.leader_id >> 16) & 0xFF);
                body.push_back((p.leader_id >> 8) & 0xFF);
                body.push_back(p.leader_id & 0xFF);

                // leader_epoch (INT32)
                body.push_back((p.leader_epoch >> 24) & 0xFF);
                body.push_back((p.leader_epoch >> 16) & 0xFF);
                body.push_back((p.leader_epoch >> 8) & 0xFF);
                body.push_back(p.leader_epoch & 0xFF);

                // replica_nodes: COMPACT_ARRAY of INT32
                body.push_back(static_cast<uint8_t>(p.replicas.size() + 1));

                for (int32_t r : p.replicas) {
                    body.push_back((r >> 24) & 0xFF);
                    body.push_back((r >> 16) & 0xFF);
                    body.push_back((r >> 8) & 0xFF);
                    body.push_back(r & 0xFF);
                }

                // isr_nodes: COMPACT_ARRAY of INT32
                body.push_back(static_cast<uint8_t>(p.isr.size() + 1));
                for (int32_t i : p.isr) {
                    body.push_back((i >> 24) & 0xFF);
                    body.push_back((i >> 16) & 0xFF);
                    body.push_back((i >> 8) & 0xFF);
                    body.push_back(i & 0xFF);
                }

                // eligible_leader_replicas: COMPACT_ARRAY (empty)
                body.push_back(0x01);

                // last_known_elr: COMPACT_ARRAY (empty)
                body.push_back(0x01);

                // offline_replicas: COMPACT_ARRAY (empty)
                body.push_back(0x01);

                // TAG_BUFFER
                body.push_back(0x00);
            }

            // topic_authorized_operations: INT32
            body.push_back(0x00);
            body.push_back(0x00);
            body.push_back(0x00);
            body.push_back(0x00);

            // TAG_BUFFER
            body.push_back(0.00);
        }
    }

    // next_cursor: null
    body.push_back(0xff);

    // TAG_BUFFER
    body.push_back(0.00);

    return body;
}

/**
 * Build response with header v0 (just correlation_id) and optional body
 */
std::vector<uint8_t> build_response(int32_t correlation_id,
                                    const std::vector<uint8_t>& body = {}) {
    // response: message_size (4 bytes) + correlation_id (4) + body
    int32_t message_size = 4 + body.size();

    std::vector<uint8_t> res;
    res.reserve(4 + message_size);

    // message size (big endian)
    res.push_back((message_size >> 24) & 0xFF);
    res.push_back((message_size >> 16) & 0xFF);
    res.push_back((message_size >> 8) & 0xFF);
    res.push_back(message_size & 0xFF);

    // correlation_id
    res.push_back((correlation_id >> 24) & 0xFF);
    res.push_back((correlation_id >> 16) & 0xFF);
    res.push_back((correlation_id >> 8) & 0xFF);
    res.push_back(correlation_id & 0xFF);

    // body
    res.insert(res.end(), body.begin(), body.end());

    return res;
}

std::vector<uint8_t> build_api_versions_response_v4(int16_t error_code) {
    std::vector<uint8_t> body;
    // error_code (int16)
    body.push_back((error_code >> 8) & 0xFF);
    body.push_back(error_code & 0xFF);

    // api_keys COMPACT_ARRAY: length = N + 1
    body.push_back(static_cast<uint8_t>(SUPPORTED_API_VERSIONS.size() + 1));

    for (const auto& api : SUPPORTED_API_VERSIONS) {
        // api_key (int16)
        body.push_back((api.api_key >> 8) & 0xFF);
        body.push_back(api.api_key & 0xFF);
        // min_version (int16)
        body.push_back((api.min_version >> 8) & 0xFF);
        body.push_back(api.min_version & 0xFF);
        // max_version (int16)
        body.push_back((api.max_version >> 8) & 0xFF);
        body.push_back(api.max_version & 0xFF);
        // TAG_BUFFER (empty)
        body.push_back(0x00);  // TAG_BUFFER
    }

    // throttle_time_ms (int32)
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);

    // TAG_BUFFER
    body.push_back(0x00);

    return body;
}

bool uses_flexible_header(int16_t api_key, int16_t api_version) {
    if (api_key == API_VERSIONS) return api_version > 3;
    if (api_key == DESCRIBE_TOPIC_PARTITIONS) return true;
    return false;
}

std::vector<uint8_t> build_response_v1(int32_t correlation_id,
                                       const std::vector<uint8_t>& body = {}) {
    // message_size + correlation_id + TAG_BUFFER + body
    // extra 1 byte is TAG_BUFFER
    int32_t message_size = 4 + 1 + body.size();

    std::vector<uint8_t> res;
    res.reserve(4 +
                message_size);  // 4 bytes for the actual message_size itself

    // message size
    res.push_back((message_size >> 24) & 0xFF);
    res.push_back((message_size >> 16) & 0xFF);
    res.push_back((message_size >> 8) & 0xFF);
    res.push_back(message_size & 0xFF);

    // correlation_id
    res.push_back((correlation_id >> 24) & 0xFF);
    res.push_back((correlation_id >> 16) & 0xFF);
    res.push_back((correlation_id >> 8) & 0xFF);
    res.push_back(correlation_id & 0xFF);

    // TAG_BUFFER
    res.push_back(0x00);

    // body
    res.insert(res.end(), body.begin(), body.end());

    return res;
}

/**
 * Helper to read a COMPACT_ARRAY of INT32 (after `skip_tag_buffer`)
 */
bool read_compact_array_int32(const uint8_t* data, size_t len, size_t& offset,
                              std::vector<int32_t>& out) {
    uint32_t num_raw;
    size_t varint_bytes = read_unsigned_varint(data, len, offset, num_raw);
    if (varint_bytes == 0) return false;
    offset += varint_bytes;

    if (num_raw == 0) return true;  // null array
    uint32_t count = num_raw - 1;
    for (uint32_t i = 0; i < count; ++i) {
        if (offset + 4 > len) return false;
        int32_t val = (data[offset] << 24) | (data[offset + 1] << 16) |
                      (data[offset + 2] << 8) | data[offset + 3];
        offset += 4;
        out.push_back(val);
    }
    return true;
}

/**
 * Parse a TopicRecord value body (after frame_version, api_key, version).
 * Fields:
 *   - name (COMPACT_STRING)
 *   - topic_id (UUID, 16 bytes)
 *   - TAG_BUFFER
 */
bool parse_topic_record(const uint8_t* data, size_t len, size_t offset,
                        TopicInfo& info) {
    std::optional<std::string> name;
    if (!read_compact_nullable_string(data, len, offset, name)) {
        return false;
    }
    info.name = name.value_or("");
    if (offset + 16 > len) {
        return false;
    }
    std::copy(data + offset, data + offset + 16, info.topic_id.begin());
    offset += 16;
    return skip_tag_buffer(data, len, offset);
}

/**
 * Parse a PartitionRecord value body (after frame_version, api_key, version).
 * Fields:
 *   - partition_id (INT32)
 *   - topic_id (UUID)
 *   - replicas (COMPACT_ARRAY)
 *   - isr (COMPACT_ARRAY)
 *   - removing_replicas (COMPACT_ARRAY)
 *   - adding_replicas (COMPACT_ARRAY)
 *   - leader (INT32)
 *   - leader_epoch (INT32)
 *   - partition_epoch (INT32)
 *   - TAG_BUFFER
 */
bool parse_partition_record(const uint8_t* data, size_t len, size_t offset,
                            std::array<uint8_t, 16>& topic_id,
                            PartitionInfo& info) {
    if (offset + 4 > len) {
        return false;
    }
    info.partition_id = (data[offset] << 24) | (data[offset + 1] << 16) |
                        (data[offset + 2] << 8) | data[offset + 3];
    offset += 4;

    if (offset + 16 > len) return false;
    std::copy(data + offset, data + offset + 16, topic_id.begin());
    offset += 16;

    // replicas
    if (!read_compact_array_int32(data, len, offset, info.replicas)) {
        return false;
    }
    // isr
    if (!read_compact_array_int32(data, len, offset, info.isr)) {
        return false;
    }
    // removing_replicas (skip)
    std::vector<int32_t> removing;
    if (!read_compact_array_int32(data, len, offset, removing)) {
        return false;
    }
    // adding_replicas (skip)
    std::vector<int32_t> adding;
    if (!read_compact_array_int32(data, len, offset, adding)) {
        return false;
    }

    if (offset + 12 > len) return false;
    info.leader_id = (data[offset] << 24) | (data[offset + 1] << 16) |
                     (data[offset + 2] << 8) | data[offset + 3];
    offset += 4;
    info.leader_epoch = (data[offset] << 24) | (data[offset + 1] << 16) |
                        (data[offset + 2] << 8) | data[offset + 3];
    offset += 4;
    info.partition_epoch = (data[offset] << 24) | (data[offset + 1] << 16) |
                           (data[offset + 2] << 8) | data[offset + 3];
    offset += 4;

    return skip_tag_buffer(data, len, offset);
}

/*
**RecordBatch header (61 bytes, all big-endian):**

| Offset | Field                  | Size   |
|--------|------------------------|--------|
| 0      | baseOffset             | int64  |
| 8      | batchLength            | int32  |
| 12     | partitionLeaderEpoch   | int32  |
| 16     | magic                  | int8   |
| 17     | crc                    | uint32 |
| 21     | attributes             | int16  |
| 23     | lastOffsetDelta        | int32  |
| 27     | baseTimestamp           | int64  |
| 35     | maxTimestamp            | int64  |
| 43     | producerId             | int64  |
| 51     | producerEpoch          | int16  |
| 53     | baseSequence           | int32  |
| 57     | recordsCount           | int32  |

Total batch size on disk: `12 + batchLength`.
*/

/**
 * Load cluster metadata from the `__cluster_metadata` log file,
 * which is a sequence of **RecordBatch** frames.
 * Each **RecordBatch** frame:
 * offset:  0  — baseOffset (8 bytes, not used)
 * offset:  8  — batchLength (4 bytes, big-endian)
 * offset: 12  — partitionLeaderEpoch, magic, crc, attributes,
 *                lastOffsetDelta, timestamps, producerId, etc.
 * offset: 57  — recordsCount (4 bytes, big-endian)
 * offset: 61  — records start here
 */
std::map<std::string, TopicInfo> load_cluster_metadata() {
    std::map<std::string, TopicInfo> res;

    std::ifstream file(METADATA_LOG_PATH, std::ios::binary);
    if (!file) {
        std::cerr << "Could not open metadata log: " << METADATA_LOG_PATH
                  << std::endl;
        return res;
    }

    // read entire file stream into memory
    // use the vector range constructor: vector(begin_iterator, end_iter)
    std::vector<uint8_t> file_data{
        // iterator pointing to beginning of the file's stream buffer
        std::istreambuf_iterator<char>(file),
        // default-constructed: the "end" sentinel (EOF)
        std::istreambuf_iterator<char>()};
    file.close();

    // uuid -> TopicInfo
    std::map<std::array<uint8_t, 16>, TopicInfo> by_uuid;

    size_t pos = 0;
    while (pos + 61 <= file_data.size()) {
        const uint8_t* batch = file_data.data() + pos;

        // batchLength at offset 8 (covers from partitionLeaderEpoch through
        // end)
        int32_t batch_length =
            (batch[8] << 24) | (batch[9] << 16) | (batch[10] << 8) | batch[11];
        if (batch_length <= 0) break;

        size_t total_batch_size = 12 + static_cast<size_t>(batch_length);
        if (pos + total_batch_size > file_data.size()) break;

        // recordsCount at offset 57
        int32_t records_count = (batch[57] << 24) | (batch[58] << 16) |
                                (batch[59] << 8) | batch[60];

        // records start at offset 61 within the batch
        size_t rec_offset = pos + 61;
        for (int32_t r = 0; r < records_count; ++r) {
            // Record: length (zigzag varint)
            int32_t rec_len;
            size_t vb = read_zigzag_varint(file_data.data(), file_data.size(),
                                           rec_offset, rec_len);
            if (vb == 0 || rec_len < 0) {
                break;
            }
            rec_offset += vb;  // skip `length` field

            size_t rec_start = rec_offset;
            size_t rec_end = rec_start + static_cast<size_t>(rec_len);
            if (rec_end > file_data.size()) break;

            // attributes (1 byte)
            rec_offset += 1;

            // timestampDelta (zigzag varint)
            int32_t ts_delta;
            vb = read_zigzag_varint(file_data.data(), file_data.size(),
                                    rec_offset, ts_delta);
            if (vb == 0) break;
            rec_offset += vb;

            // offsetDelta (zigzag varint)
            int32_t off_delta;
            vb = read_zigzag_varint(file_data.data(), file_data.size(),
                                    rec_offset, off_delta);
            if (vb == 0) break;
            rec_offset += vb;

            // keyLength (zigzag varint)
            int32_t key_length;
            vb = read_zigzag_varint(file_data.data(), file_data.size(),
                                    rec_offset, key_length);
            if (vb == 0) break;
            rec_offset += vb;

            // skip key bytes
            if (key_length > 0) {
                rec_offset += static_cast<size_t>(key_length);
            }

            // valueLength (zigzag varint)
            int32_t value_length;
            vb = read_zigzag_varint(file_data.data(), file_data.size(),
                                    rec_offset, value_length);
            if (vb == 0) break;
            rec_offset += vb;

            if (value_length > 0) {
                const uint8_t* value_data = file_data.data() + rec_offset;
                size_t value_len = static_cast<size_t>(value_length);

                // parse metadta record value
                // frame_version (uvarint), api_key (uvarint), version
                // (uvarint), body
                size_t v_offset = 0;
                uint32_t frame_version, api_key, version;
                size_t fvb = read_unsigned_varint(value_data, value_len,
                                                  v_offset, frame_version);
                if (fvb == 0) {
                    rec_offset = rec_end;
                    continue;
                }
                v_offset += fvb;

                fvb = read_unsigned_varint(value_data, value_len, v_offset,
                                           api_key);
                if (fvb == 0) {
                    rec_offset = rec_end;
                    continue;
                }
                v_offset += fvb;

                fvb = read_unsigned_varint(value_data, value_len, v_offset,
                                           version);
                if (fvb == 0) {
                    rec_offset = rec_end;
                    continue;
                }
                v_offset += fvb;

                if (api_key == METADATA_TOPIC_RECORD) {
                    TopicInfo info;
                    if (parse_topic_record(value_data, value_len, v_offset,
                                           info)) {
                        by_uuid[info.topic_id] = info;
                    }
                } else if (api_key == METADATA_PARTITION_RECORD) {
                    std::array<uint8_t, 16> topic_uuid;
                    PartitionInfo pinfo;
                    if (parse_partition_record(value_data, value_len, v_offset,
                                               topic_uuid, pinfo)) {
                        by_uuid[topic_uuid].partitions.push_back(pinfo);
                    }
                }

                rec_offset = rec_end;
            } else {
                rec_offset = rec_end;
            }
        }

        pos += total_batch_size;
    }

    // build name -> TopicInfo map
    for (auto& [uuid, info] : by_uuid) {
        if (!info.name.empty()) {
            info.topic_id = uuid;
            res[info.name] = info;
        }
    }

    return res;
}

void handle_client(int client_fd) {
    while (true) {
        // read message size (4 bytes, big-endian)
        uint8_t size_buf[4];
        if (!read_exact(client_fd, size_buf, 4)) {
            break;  // client disconnected or error
        }

        int32_t message_size = (size_buf[0] << 24) | (size_buf[1] << 16) |
                               (size_buf[2] << 8) | size_buf[3];

        if (message_size <= 0 || message_size > 1024 * 1024) {
            std::cerr << "Invalid message size: " << message_size << std::endl;
            break;
        }

        // read request body
        std::vector<uint8_t> request(message_size);
        if (!read_exact(client_fd, request.data(), message_size)) {
            std::cerr << "Failed to request body!" << std::endl;
            break;
        }

        // parse request header
        // need to peek at api_version to decide header format
        // first read fixed header fields to get api_version
        if (request.size() < 4) {
            // peek at least api_key and api_version
            std::cerr << "Request too small!" << std::endl;
            break;
        }

        int16_t api_key = (request[0] << 8) | request[1];
        int16_t api_version = (request[2] << 8) | request[3];
        RequestHeader header;
        size_t header_end_offset = 0;

        // ApiVersions v3+ uses flexible header (v2), v0-2 uses non-flexible
        // (v1)
        bool use_flexible_header = uses_flexible_header(api_key, api_version);
        if (use_flexible_header) {
            if (!parse_request_header_v2(request.data(), request.size(), header,
                                         header_end_offset)) {
                std::cerr << "Failed to parse request header v2!" << std::endl;
                break;
            }

        } else {
            if (!parse_request_header_v1(request.data(), request.size(), header,
                                         header_end_offset)) {
                std::cerr << "Failed to parse request header v1!" << std::endl;
                break;
            }
        }

        std::cerr << "Received request: api_key=" << header.request_api_key
                  << " api_version=" << header.request_api_version
                  << " correlation_id=" << header.correlation_id
                  << " client_id=" << header.client_id << "\n";

        // build and send response with header v0 (empty body for now)
        if (header.request_api_key == API_VERSIONS) {
            // parse request body for v3+
            if (header.request_api_version >= 3) {
                ApiVersionsRequestBody req_body;
                if (!parse_api_versions_request_body_v3(
                        request.data(), request.size(), header_end_offset,
                        req_body)) {
                    std::cerr << "Failed to parse ApiVersions request body!"
                              << std::endl;
                    break;
                }

                std::cerr << "  client_software_name="
                          << req_body.client_software_name
                          << " client_software_version="
                          << req_body.client_softare_version << std::endl;
            }
            int16_t error_code =
                is_version_supported(header.request_api_key,
                                     header.request_api_version)
                    ? ERROR_NONE
                    : ERROR_UNSUPPORTED_VERSION;
            std::vector<uint8_t> body =
                build_api_versions_response_v4(error_code);
            std::vector<uint8_t> res =
                build_response(header.correlation_id, body);
            if (!write_exact(client_fd, res.data(), res.size())) {
                std::cerr << "Failed to send response" << std::endl;
                break;
            }
        } else if (header.request_api_key == DESCRIBE_TOPIC_PARTITIONS) {
            // request body parsing
            DescribeTopicPartitionsRequest req;
            if (!parse_describe_topic_partitions_request(
                    request.data(), request.size(), header_end_offset, req)) {
                std::cerr << "Failed to parse DescribeTopicPartitions request!"
                          << std::endl;
                break;
            }
            std::cerr << "  topics requested:";
            for (const auto& t : req.topic_names) {
                std::cerr << " " << t;
            }
            std::cerr << std::endl;

            // load what topics/partitions actually exist
            auto metadata = load_cluster_metadata();

            std::vector<uint8_t> body =
                build_describe_topic_partitions_response(req, metadata);
            std::vector<uint8_t> res =
                build_response_v1(header.correlation_id, body);
            if (!write(client_fd, res.data(), res.size())) {
                std::cerr << "Failed to send response!" << std::endl;
                break;
            }
        }
    }
}

int main(/* int argc, char* argv[] */) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
        0) {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr),
             sizeof(server_addr)) != 0) {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    std::cerr << "Logs from your program will appear here!\n";

    while (true) {
        struct sockaddr_in client_addr{};
        socklen_t client_addr_len = sizeof(client_addr);

        int client_fd =
            accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr),
                   &client_addr_len);
        if (client_fd < 0) {
            std::cerr << "accept failed!\n";
            continue;
        }
        std::cout << "Client connected\n";
        std::thread([client_fd]() {
            handle_client(client_fd);
            close(client_fd);
            std::cerr << "Client disconnected\n";
        }).detach();
    }

    close(server_fd);
    return 0;
}
