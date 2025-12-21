#include "er/Flags4096.hpp"

#include <stdexcept>
#include <sstream>
#include <cctype>

namespace er {

Flags4096::Flags4096() : value_(0) {}

static void check_bit(std::size_t bit) {
    if (bit >= 4096)
        throw std::out_of_range("Flags4096: bit index out of range");
}

void Flags4096::set(std::size_t bit) {
    check_bit(bit);
    value_ |= (uint4096(1) << bit);
}

void Flags4096::reset(std::size_t bit) {
    check_bit(bit);
    value_ &= ~(uint4096(1) << bit);
}

bool Flags4096::test(std::size_t bit) const {
    check_bit(bit);
    return (value_ & (uint4096(1) << bit)) != 0;
}

void Flags4096::clear() {
    value_ = 0;
}

Flags4096 Flags4096::operator|(const Flags4096& other) const {
    Flags4096 r;
    r.value_ = value_ | other.value_;
    return r;
}

Flags4096 Flags4096::operator&(const Flags4096& other) const {
    Flags4096 r;
    r.value_ = value_ & other.value_;
    return r;
}

Flags4096 Flags4096::operator^(const Flags4096& other) const {
    Flags4096 r;
    r.value_ = value_ ^ other.value_;
    return r;
}

// ---- hex ----

std::string Flags4096::to_hex() const {
    std::stringstream ss;
    ss << std::hex << value_;
    return ss.str();
}

static int hex_val(char c) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    return -1;
}

Flags4096 Flags4096::from_hex(const std::string& hex) {
    Flags4096 out;
    out.value_ = 0;

    std::size_t i = 0;
    if (hex.size() >= 2 && hex[0] == '0' &&
        (hex[1] == 'x' || hex[1] == 'X')) {
        i = 2;
    }

    for (; i < hex.size(); ++i) {
        if (std::isspace(static_cast<unsigned char>(hex[i]))) continue;
        int v = hex_val(hex[i]);
        if (v < 0)
            throw std::invalid_argument("Flags4096::from_hex: invalid hex");
        out.value_ <<= 4;
        out.value_ += uint4096(v);
    }
    return out;
}

// ---- binary 512B BE ----

std::array<std::uint8_t, 512> Flags4096::to_bytes_be() const {
    std::array<std::uint8_t, 512> out{};
    uint4096 tmp = value_;

    for (int i = 511; i >= 0; --i) {
        out[static_cast<std::size_t>(i)] =
            static_cast<std::uint8_t>(tmp & 0xFF);
        tmp >>= 8;
    }
    return out;
}

Flags4096 Flags4096::from_bytes_be(const std::uint8_t* data, std::size_t len) {
    if (!data)
        throw std::invalid_argument("from_bytes_be: null data");
    if (len != 512)
        throw std::invalid_argument("from_bytes_be: len must be 512");

    Flags4096 out;
    out.value_ = 0;
    for (std::size_t i = 0; i < 512; ++i) {
        out.value_ <<= 8;
        out.value_ += uint4096(data[i]);
    }
    return out;
}

// ---- index helper ----

std::vector<std::size_t> Flags4096::set_bits() const {
    std::vector<std::size_t> bits;
    bits.reserve(64); // tipično mali broj

    // jednostavno i sigurno: 4096 provjera
    for (std::size_t b = 0; b < 4096; ++b) {
        if (test(b)) bits.push_back(b);
    }
    return bits;
}

} // namespace er

