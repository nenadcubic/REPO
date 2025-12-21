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

    // HASH (string)
    bool hset(const std::string& key,
              const std::string& field,
              const std::string& value);

    bool hget(const std::string& key,
              const std::string& field,
              std::string& out_value);

    // HASH (binary-safe)
    bool hset_bin(const std::string& key,
                  const std::string& field,
                  const void* data,
                  std::size_t len);

    bool hget_bin(const std::string& key,
                  const std::string& field,
                  std::string& out_blob);

    // SET index ops
    bool sadd(const std::string& key, const std::string& member);
    bool srem(const std::string& key, const std::string& member);
    bool smembers(const std::string& key, std::vector<std::string>& out_members);
    bool sinter(const std::vector<std::string>& keys, std::vector<std::string>& out_members);
    bool sunion(const std::vector<std::string>& keys, std::vector<std::string>& out_members);
    bool sdiff(const std::vector<std::string>& keys, std::vector<std::string>& out_members);


private:
    struct CtxDeleter {
        void operator()(redisContext* c) const noexcept {
            if (c) redisFree(c);
        }
    };

    std::unique_ptr<redisContext, CtxDeleter> ctx_;
};

} // namespace er

