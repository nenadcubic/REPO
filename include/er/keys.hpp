#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <string_view>

namespace er::keys {

inline constexpr std::string_view kPrefixDefault{"er"};

inline std::string universe(std::string_view prefix = kPrefixDefault) {
    std::string k(prefix);
    k.append(":all");
    return k;
}

inline std::string element(std::string_view name, std::string_view prefix = kPrefixDefault) {
    std::string k(prefix);
    k.append(":element:");
    k.append(name);
    return k;
}

inline std::string idx_bit(std::size_t bit, std::string_view prefix = kPrefixDefault) {
    std::string k(prefix);
    k.append(":idx:bit:");
    k.append(std::to_string(bit));
    return k;
}

inline std::string tmp(std::string_view tag, std::string_view prefix = kPrefixDefault) {
    using namespace std::chrono;
    const auto ns = duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
    std::string k(prefix);
    k.append(":tmp:");
    k.append(tag);
    k.push_back(':');
    k.append(std::to_string(ns));
    return k;
}

} // namespace er::keys

