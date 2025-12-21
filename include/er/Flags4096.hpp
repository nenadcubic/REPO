#pragma once
#pragma once

#include <boost/multiprecision/cpp_int.hpp>
#include <cstdint>
#include <string>
#include <array>
#include <vector>

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

    void set(std::size_t bit);
    void reset(std::size_t bit);
    bool test(std::size_t bit) const;
    void clear();

    Flags4096 operator|(const Flags4096& other) const;
    Flags4096 operator&(const Flags4096& other) const;
    Flags4096 operator^(const Flags4096& other) const;

    std::string to_hex() const;
    static Flags4096 from_hex(const std::string& hex);

    std::array<std::uint8_t, 512> to_bytes_be() const;
    static Flags4096 from_bytes_be(const std::uint8_t* data, std::size_t len);

    // index support
    std::vector<std::size_t> set_bits() const;

private:
    uint4096 value_;
};

} // namespace er


