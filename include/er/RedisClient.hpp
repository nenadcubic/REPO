#pragma once

#include <memory>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>
#include <hiredis/hiredis.h>

#include "er/result.hpp"

namespace er {

class RedisClient {
public:
    ~RedisClient();

    RedisClient(RedisClient&&) noexcept = default;
    RedisClient& operator=(RedisClient&&) noexcept = default;

    RedisClient(const RedisClient&) = delete;
    RedisClient& operator=(const RedisClient&) = delete;

    static Result<RedisClient> connect(std::string host = "localhost", int port = 6379, int timeout_ms = 2000) noexcept;

    [[nodiscard]] Result<Unit> ping() noexcept;

    // HASH
    [[nodiscard]] Result<long long> hset(std::string_view key, std::string_view field, std::string_view value) noexcept;
    [[nodiscard]] Result<std::string> hget(std::string_view key, std::string_view field) noexcept;

    [[nodiscard]] Result<long long> hset_bin(std::string_view key,
                                             std::string_view field,
                                             const void* data,
                                             std::size_t len) noexcept;
    [[nodiscard]] Result<std::string> hget_bin(std::string_view key, std::string_view field) noexcept;

    // SET basic
    [[nodiscard]] Result<long long> sadd(std::string_view key, std::string_view member) noexcept;
    [[nodiscard]] Result<long long> srem(std::string_view key, std::string_view member) noexcept;
    [[nodiscard]] Result<std::vector<std::string>> smembers(std::string_view key) noexcept;

    // SET composite (no-store)
    [[nodiscard]] Result<std::vector<std::string>> sinter(const std::vector<std::string>& keys) noexcept;
    [[nodiscard]] Result<std::vector<std::string>> sunion(const std::vector<std::string>& keys) noexcept;
    [[nodiscard]] Result<std::vector<std::string>> sdiff(const std::vector<std::string>& keys) noexcept;

    // STORE + EXPIRE
    [[nodiscard]] Result<Unit> expire_seconds(std::string_view key, int ttl_seconds) noexcept;

    [[nodiscard]] Result<long long> sinterstore(std::string_view dst, const std::vector<std::string>& keys) noexcept;
    [[nodiscard]] Result<long long> sunionstore(std::string_view dst, const std::vector<std::string>& keys) noexcept;
    [[nodiscard]] Result<long long> sdiffstore(std::string_view dst, const std::vector<std::string>& keys) noexcept;
    
    [[nodiscard]] Result<long long> store_expire_lua(std::string_view op,
                                                     std::string_view dst,
                                                     int ttl_seconds,
                                                     const std::vector<std::string>& keys) noexcept;

    [[nodiscard]] Result<long long> store_all_expire_lua(int ttl_seconds,
                                                         const std::vector<std::string>& set_keys,
                                                         std::string_view out_key) noexcept;

    [[nodiscard]] Result<long long> store_any_expire_lua(int ttl_seconds,
                                                         const std::vector<std::string>& set_keys,
                                                         std::string_view out_key) noexcept;

    [[nodiscard]] Result<long long> store_not_expire_lua(int ttl_seconds,
                                                         std::string_view universe_key,
                                                         const std::vector<std::string>& set_keys,
                                                         std::string_view out_key) noexcept;

    [[nodiscard]] Result<long long> store_all_not_expire_lua(int ttl_seconds,
                                                             std::string_view include_key,
                                                             std::string_view universe_key,
                                                             const std::vector<std::string>& exclude_keys,
                                                             std::string_view out_key) noexcept;

    [[nodiscard]] Result<long long> del_key(std::string_view key) noexcept;

private:
    struct CtxDeleter {
        void operator()(redisContext* c) const noexcept {
            if (c) redisFree(c);
        }
    };

    explicit RedisClient(redisContext* c) : ctx_(c) {}

    std::unique_ptr<redisContext, CtxDeleter> ctx_;
};

} // namespace er
