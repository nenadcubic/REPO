#pragma once

#include <boost/multiprecision/cpp_int.hpp>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <array>
#include <vector>

#include "er/result.hpp"

namespace er {

using uint4096 = boost::multiprecision::number<
    boost::multiprecision::cpp_int_backend<
        4096, 4096,
        boost::multiprecision::unsigned_magnitude,
        boost::multiprecision::unchecked,
        void>>;

class Flags4096 {
public:
    Flags4096();

    [[nodiscard]] Result<Unit> set(std::size_t bit) noexcept;
    [[nodiscard]] Result<Unit> reset(std::size_t bit) noexcept;
    [[nodiscard]] Result<bool> test(std::size_t bit) const noexcept;
    void clear() noexcept;

    Flags4096 operator|(const Flags4096& other) const;
    Flags4096 operator&(const Flags4096& other) const;
    Flags4096 operator^(const Flags4096& other) const;

    std::string to_hex() const;
    static Result<Flags4096> from_hex(std::string_view hex) noexcept;

    std::array<std::uint8_t, 512> to_bytes_be() const;
    static Result<Flags4096> from_bytes_be(const std::uint8_t* data, std::size_t len) noexcept;

    // index support
    [[nodiscard]] std::vector<std::size_t> set_bits() const;

private:
    uint4096 value_;
};

} // namespace er
