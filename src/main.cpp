#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

constexpr int16_t API_VERSIONS = 18;
constexpr int16_t ERROR_NONE = 0;
constexpr int16_t ERROR_UNSUPPORTED_VERSION = 35;

struct RequestHeader {
    int16_t request_api_key;
    int16_t request_api_version;
    int32_t correlation_id;
    std::string client_id;
};

struct ApiVersionRange {
    int16_t api_key;
    int16_t min_version;
    int16_t max_version;
};

const std::vector<ApiVersionRange> SUPPORTED_API_VERSIONS = {
    {API_VERSIONS, 0, 4},
    // add more here
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

bool parse_request_header(const uint8_t* data, size_t len,
                          RequestHeader& header) {
    if (len < 8) {
        // api_key (2) + api_version (2) + correlation_id (4)
        return false;
    }
    header.request_api_key = (data[0] << 8) | data[1];
    header.request_api_version = (data[2] << 8) | data[3];
    header.correlation_id =
        (data[4] << 24) | (data[5] << 16) | (data[6] << 8) | data[7];

    // for v2
    if (len < 10) {
        // need at least 2 more bytes for client_id length
        // in kafka's protocol, a NULLABLE_STRING is encoded as:
        //   - int16 (2 bytes) - length of the string
        //   - N bytes
        return false;
    }
    int16_t client_id_len = (data[8] << 8) | data[9];
    if (client_id_len == -1) {
        header.client_id = "";  // null string
    } else if (client_id_len >= 0) {
        if (len < 10 + static_cast<size_t>(client_id_len)) {
            return false;
        }
        header.client_id = std::string(reinterpret_cast<const char*>(data + 10),
                                       client_id_len);
    } else {
        return false;
    }
    return true;
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

    // COMPACT_ARRAY
    body.push_back(static_cast<uint8_t>(SUPPORTED_API_VERSIONS.size() + 1));

    for (const auto& api : SUPPORTED_API_VERSIONS) {
        body.push_back((api.api_key >> 8) & 0xFF);
        body.push_back(api.api_key >> 8 & 0xFF);
        body.push_back((api.min_version >> 8) & 0xFF);
        body.push_back(api.min_version & 0xFF);
        body.push_back((api.max_version >> 8) & 0xFF);
        body.push_back(api.max_version & 0xFF);
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
        RequestHeader header;
        if (!parse_request_header(request.data(), request.size(), header)) {
            std::cerr << "Failed to parse request header!" << std::endl;
            break;
        }

        std::cerr << "Received request: api_key=" << header.request_api_key
                  << " api_version=" << header.request_api_version
                  << " correlation_id=" << header.correlation_id
                  << " client_id=" << header.client_id << "\n";

        // build and send response with header v0 (empty body for now)
        if (header.request_api_key == API_VERSIONS) {
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
        handle_client(client_fd);
        close(client_fd);
        std::cerr << "Client disconnected\n";
    }

    close(server_fd);
    return 0;
}
