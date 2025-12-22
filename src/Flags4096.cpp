#include "er/Flags4096.hpp"

#include <sstream>
#include <cctype>

namespace er {

Flags4096::Flags4096() : value_(0) {}

static Result<Unit> check_bit(std::size_t bit) noexcept {
    if (bit >= 4096) return Result<Unit>::err(Errc::kInvalidArg, "Flags4096: bit out of range (0..4095)");
    return Result<Unit>::ok();
}

Result<Unit> Flags4096::set(std::size_t bit) noexcept {
    if (auto ok = check_bit(bit); !ok) return ok;
    value_ |= (uint4096(1) << bit);
    return Result<Unit>::ok();
}

Result<Unit> Flags4096::reset(std::size_t bit) noexcept {
    if (auto ok = check_bit(bit); !ok) return ok;
    value_ &= ~(uint4096(1) << bit);
    return Result<Unit>::ok();
}

Result<bool> Flags4096::test(std::size_t bit) const noexcept {
    if (auto ok = check_bit(bit); !ok) return Result<bool>::err(ok.error().code, ok.error().msg);
    return Result<bool>::ok((value_ & (uint4096(1) << bit)) != 0);
}

void Flags4096::clear() noexcept {
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

Result<Flags4096> Flags4096::from_hex(std::string_view hex) noexcept {
    Flags4096 out{};
    out.value_ = 0;

    std::size_t i = 0;
    if (hex.size() >= 2 && hex[0] == '0' &&
        (hex[1] == 'x' || hex[1] == 'X')) {
        i = 2;
    }

    for (; i < hex.size(); ++i) {
        if (std::isspace(static_cast<unsigned char>(hex[i]))) continue;
        int v = hex_val(hex[i]);
        if (v < 0) return Result<Flags4096>::err(Errc::kInvalidArg, "Flags4096::from_hex: invalid hex");
        out.value_ <<= 4;
        out.value_ += uint4096(v);
    }
    return Result<Flags4096>::ok(std::move(out));
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

Result<Flags4096> Flags4096::from_bytes_be(const std::uint8_t* data, std::size_t len) noexcept {
    if (!data) return Result<Flags4096>::err(Errc::kInvalidArg, "Flags4096::from_bytes_be: null data");
    if (len != 512) return Result<Flags4096>::err(Errc::kInvalidArg, "Flags4096::from_bytes_be: len must be 512");

    Flags4096 out{};
    out.value_ = 0;
    for (std::size_t i = 0; i < 512; ++i) {
        out.value_ <<= 8;
        out.value_ += uint4096(data[i]);
    }
    return Result<Flags4096>::ok(std::move(out));
}

// ---- index helper ----

std::vector<std::size_t> Flags4096::set_bits() const {
    std::vector<std::size_t> bits;
    bits.reserve(64);

    // Faster than 4096 big-int tests: scan bytes and extract bit positions.
    const auto bytes = to_bytes_be(); // 512 bytes, BE; out[511] is least-significant byte.
    for (int i = 511; i >= 0; --i) {
        const std::uint8_t byte = bytes[static_cast<std::size_t>(i)];
        if (byte == 0) continue;
        const std::size_t base = static_cast<std::size_t>((511 - i) * 8);
        for (std::size_t b = 0; b < 8; ++b) {
            if (byte & (static_cast<std::uint8_t>(1u) << b)) bits.push_back(base + b);
        }
    }
    return bits;
}

} // namespace er
