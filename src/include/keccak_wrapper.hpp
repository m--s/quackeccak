#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <cstdio>

// Keccak C interface (from vendor/keccak.c)
extern "C" {
    void Keccak(unsigned int rate, unsigned int capacity,
                const unsigned char *input, unsigned long long inputByteLen,
                unsigned char delimitedSuffix,
                unsigned char *output, unsigned long long outputByteLen);
}

namespace duckdb {

class KeccakWrapper {
public:
    static constexpr size_t HASH_SIZE = 32;
    static constexpr unsigned int RATE = 1088;     // (1600 - 512) for Keccak-256
    static constexpr unsigned int CAPACITY = 512;
    static constexpr uint8_t ETHEREUM_DELIMITER = 0x01;  // CRITICAL: NOT 0x06!

    // Hash raw bytes to output buffer
    static void Hash256(const uint8_t* input, size_t len, uint8_t output[32]) {
        Keccak(RATE, CAPACITY, input, len, ETHEREUM_DELIMITER, output, 32);
    }

    // Hash string to hex string
    static std::string HashToHex(const uint8_t* input, size_t len) {
        uint8_t hash[32];
        Hash256(input, len, hash);
        return BytesToHex(hash, 32);
    }

    // Utility: Convert bytes to hex string with 0x prefix
    static std::string BytesToHex(const uint8_t* bytes, size_t len) {
        std::string hex;
        hex.reserve(len * 2 + 3);
        hex += "0x";
        
        for (size_t i = 0; i < len; i++) {
            char byte_hex[3];
            std::snprintf(byte_hex, sizeof(byte_hex), "%02x", bytes[i]);
            hex += byte_hex;
        }
        return hex;
    }

    // Utility: Convert hex character to byte value
    static uint8_t HexCharToByte(char c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return 0;
    }

    // Utility: Convert hex string to bytes (without 0x prefix)
    static bool HexToBytes(const std::string &hex, uint8_t* output, size_t expected_len) {
        if (hex.length() != expected_len * 2) {
            return false;
        }
        
        for (size_t i = 0; i < expected_len; i++) {
            if (!std::isxdigit(hex[i * 2]) || !std::isxdigit(hex[i * 2 + 1])) {
                return false;
            }
            output[i] = (HexCharToByte(hex[i * 2]) << 4) | HexCharToByte(hex[i * 2 + 1]);
        }
        return true;
    }
};

}