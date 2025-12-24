#pragma once

#include <optional>
#include <string>
#include <utility>

namespace er {

enum class Errc {
    kOk = 0,
    kInvalidArg,
    kRedisIo,
    kRedisProtocol,
    kRedisReplyType,
    kNotFound,
    kTimeout,
    kInternal,
};

struct Error {
    Errc code{Errc::kOk};
    std::string msg{};
};

struct Unit {};

template <class T>
class [[nodiscard]] Result {
public:
    static Result ok(T v) { return Result(std::move(v)); }
    static Result err(Errc c, std::string m) { return Result(Error{c, std::move(m)}); }

    bool is_ok() const noexcept { return value_.has_value(); }
    explicit operator bool() const noexcept { return is_ok(); }

    const T& value() const& { return *value_; }
    T&& value() && { return std::move(*value_); }

    const Error& error() const noexcept { return error_; }

private:
    std::optional<T> value_{};
    Error error_{};

    explicit Result(T v) : value_(std::move(v)) {}
    explicit Result(Error e) : value_(std::nullopt), error_(std::move(e)) {}
};

template <>
class [[nodiscard]] Result<Unit> {
public:
    static Result ok() { return Result(true, Error{}); }
    static Result err(Errc c, std::string m) { return Result(false, Error{c, std::move(m)}); }

    bool is_ok() const noexcept { return ok_; }
    explicit operator bool() const noexcept { return ok_; }

    const Error& error() const noexcept { return error_; }

private:
    bool ok_{false};
    Error error_{};

    explicit Result(bool ok, Error e) : ok_(ok), error_(std::move(e)) {}
};

} // namespace er

