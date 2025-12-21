#pragma once

#include <string>
#include <memory>
#include <vector>
#include <hiredis/hiredis.h>

namespace er {

class RedisClient {
public:
    RedisClient(std::string host = "redis", int port = 6379);
    ~RedisClient();

    RedisClient(const RedisClient&) = delete;
    RedisClient& operator=(const RedisClient&) = delete;

    bool ping();

    // HASH
    bool hset(const std::string& key,
              const std::string& field,
              const std::string& value);

    bool hget(const std::string& key,
              const std::string& field,
              std::string& out_value);

    bool hset_bin(const std::string& key,
                  const std::string& field,
                  const void* data,
                  std::size_t len);

    bool hget_bin(const std::string& key,
                  const std::string& field,
                  std::string& out_blob);

    // SET basic
    bool sadd(const std::string& key, const std::string& member);
    bool srem(const std::string& key, const std::string& member);
    bool smembers(const std::string& key, std::vector<std::string>& out_members);

    // SET composite (no-store)
    bool sinter(const std::vector<std::string>& keys, std::vector<std::string>& out_members);
    bool sunion(const std::vector<std::string>& keys, std::vector<std::string>& out_members);
    bool sdiff (const std::vector<std::string>& keys, std::vector<std::string>& out_members);

    // STORE + EXPIRE
    bool expire_seconds(const std::string& key, int ttl_seconds);

    bool sinterstore(const std::string& dst, const std::vector<std::string>& keys);
    bool sunionstore(const std::string& dst, const std::vector<std::string>& keys);
    bool sdiffstore (const std::string& dst, const std::vector<std::string>& keys);
    
    bool store_expire_lua(const std::string& op,
                          const std::string& dst,
                          int ttl_seconds,
                          const std::vector<std::string>& keys,
                          long long* out_cardinality = nullptr);
    static std::string make_tmp_key(const std::string& tag);

    bool store_all_expire_lua(int ttl_seconds,
                              const std::vector<std::string>& set_keys,
                              const std::string& out_key);

    bool store_any_expire_lua(int ttl_seconds,
                              const std::vector<std::string>& set_keys,
                              const std::string& out_key);

    bool store_not_expire_lua(int ttl_seconds,
                              const std::string& universe_key,
                              const std::vector<std::string>& set_keys,
                              const std::string& out_key);

    bool store_all_not_expire_lua(int ttl_seconds,
                                  const std::string& include_key,
                                  const std::string& universe_key,
                                  const std::vector<std::string>& exclude_keys,
                                  const std::string& out_key);

    bool del_key(const std::string& key);

private:
    struct CtxDeleter {
        void operator()(redisContext* c) const noexcept {
            if (c) redisFree(c);
        }
    };

    std::unique_ptr<redisContext, CtxDeleter> ctx_;
};

} // namespace er
