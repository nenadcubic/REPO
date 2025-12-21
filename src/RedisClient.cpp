#include "er/RedisClient.hpp"
#include <vector>
#include <stdexcept>
#include <cstdarg>

namespace er {

static redisReply* cmd(redisContext* c, const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    void* r = redisvCommand(c, fmt, ap);
    va_end(ap);
    return static_cast<redisReply*>(r);
}

RedisClient::RedisClient(std::string host, int port) {
    timeval tv{};
    tv.tv_sec = 2;
    tv.tv_usec = 0;

    redisContext* raw =
        redisConnectWithTimeout(host.c_str(), port, tv);
    ctx_.reset(raw);

    if (!ctx_ || ctx_->err) {
        std::string msg =
            ctx_ ? ctx_->errstr : "null context";
        throw std::runtime_error("Redis connect failed: " + msg);
    }
}

RedisClient::~RedisClient() = default;

bool RedisClient::ping() {
    auto* r = cmd(ctx_.get(), "PING");
    if (!r) return false;
    bool ok = (r->type == REDIS_REPLY_STATUS &&
               r->str && std::string(r->str) == "PONG");
    freeReplyObject(r);
    return ok;
}

bool RedisClient::hset(const std::string& key,
                       const std::string& field,
                       const std::string& value) {
    auto* r = cmd(ctx_.get(),
                  "HSET %s %s %s",
                  key.c_str(),
                  field.c_str(),
                  value.c_str());
    if (!r) return false;
    bool ok = (r->type == REDIS_REPLY_INTEGER);
    freeReplyObject(r);
    return ok;
}

bool RedisClient::hget(const std::string& key,
                       const std::string& field,
                       std::string& out_value) {
    auto* r = cmd(ctx_.get(),
                  "HGET %s %s",
                  key.c_str(),
                  field.c_str());
    if (!r) return false;

    bool ok = false;
    if (r->type == REDIS_REPLY_STRING && r->str) {
        out_value.assign(r->str, r->len);
        ok = true;
    }
    freeReplyObject(r);
    return ok;
}

bool RedisClient::hset_bin(const std::string& key,
                           const std::string& field,
                           const void* data,
                           std::size_t len) {
    auto* r = cmd(ctx_.get(),
                  "HSET %s %s %b",
                  key.c_str(),
                  field.c_str(),
                  data, len);
    if (!r) return false;
    bool ok = (r->type == REDIS_REPLY_INTEGER);
    freeReplyObject(r);
    return ok;
}

bool RedisClient::hget_bin(const std::string& key,
                           const std::string& field,
                           std::string& out_blob) {
    auto* r = cmd(ctx_.get(),
                  "HGET %s %s",
                  key.c_str(),
                  field.c_str());
    if (!r) return false;

    bool ok = false;
    if (r->type == REDIS_REPLY_STRING && r->str) {
        out_blob.assign(r->str, r->len);
        ok = true;
    }
    freeReplyObject(r);
    return ok;
}

// ---- SET ops ----

bool RedisClient::sadd(const std::string& key, const std::string& member) {
    auto* r = cmd(ctx_.get(),
                  "SADD %s %s",
                  key.c_str(),
                  member.c_str());
    if (!r) return false;
    bool ok = (r->type == REDIS_REPLY_INTEGER);
    freeReplyObject(r);
    return ok;
}

bool RedisClient::srem(const std::string& key, const std::string& member) {
    auto* r = cmd(ctx_.get(),
                  "SREM %s %s",
                  key.c_str(),
                  member.c_str());
    if (!r) return false;
    bool ok = (r->type == REDIS_REPLY_INTEGER);
    freeReplyObject(r);
    return ok;
}

bool RedisClient::smembers(const std::string& key, std::vector<std::string>& out_members) {
    out_members.clear();
    auto* r = cmd(ctx_.get(), "SMEMBERS %s", key.c_str());
    if (!r) return false;

    bool ok = false;
    if (r->type == REDIS_REPLY_ARRAY) {
        out_members.reserve(static_cast<std::size_t>(r->elements));
        for (std::size_t i = 0; i < r->elements; ++i) {
            redisReply* e = r->element[i];
            if (e && e->type == REDIS_REPLY_STRING && e->str) {
                out_members.emplace_back(e->str, e->len);
            }
        }
        ok = true;
    }
    freeReplyObject(r);
    return ok;
}

bool RedisClient::sinter(const std::vector<std::string>& keys,
                         std::vector<std::string>& out_members) {
    out_members.clear();

    // SINTER key1 key2 ...
    std::vector<const char*> argv;
    std::vector<size_t> argvlen;

    argv.reserve(keys.size() + 1);
    argvlen.reserve(keys.size() + 1);

    argv.push_back("SINTER");
    argvlen.push_back(6);

    for (const auto& k : keys) {
        argv.push_back(k.c_str());
        argvlen.push_back(k.size());
    }

    redisReply* r = static_cast<redisReply*>(
        redisCommandArgv(ctx_.get(),
                         static_cast<int>(argv.size()),
                         argv.data(),
                         argvlen.data())
    );

    if (!r) return false;

    // Ako Redis vrati error, hoću da ti to vidiš odmah
    if (r->type == REDIS_REPLY_ERROR) {
        std::string msg = r->str ? std::string(r->str, r->len) : "unknown redis error";
        freeReplyObject(r);
        throw std::runtime_error("SINTER error: " + msg);
    }

    bool ok = false;
    if (r->type == REDIS_REPLY_ARRAY) {
        out_members.reserve(static_cast<std::size_t>(r->elements));
        for (std::size_t i = 0; i < r->elements; ++i) {
            redisReply* e = r->element[i];
            if (e && e->type == REDIS_REPLY_STRING && e->str) {
                out_members.emplace_back(e->str, e->len);
            }
        }
        ok = true;
    }

    freeReplyObject(r);
    return ok;
  }
  
  bool RedisClient::sunion(const std::vector<std::string>& keys,
                         std::vector<std::string>& out_members) {
    out_members.clear();
    if (keys.empty()) return true;

    std::vector<const char*> argv;
    std::vector<size_t> argvlen;
    argv.reserve(keys.size() + 1);
    argvlen.reserve(keys.size() + 1);

    argv.push_back("SUNION");
    argvlen.push_back(6);

    for (const auto& k : keys) {
        argv.push_back(k.c_str());
        argvlen.push_back(k.size());
    }

    redisReply* r = static_cast<redisReply*>(
        redisCommandArgv(ctx_.get(),
                         static_cast<int>(argv.size()),
                         argv.data(),
                         argvlen.data())
    );

    if (!r) return false;

    if (r->type == REDIS_REPLY_ERROR) {
        std::string msg = r->str ? std::string(r->str, r->len) : "unknown redis error";
        freeReplyObject(r);
        throw std::runtime_error("SUNION error: " + msg);
    }

    bool ok = false;
    if (r->type == REDIS_REPLY_ARRAY) {
        out_members.reserve(static_cast<std::size_t>(r->elements));
        for (std::size_t i = 0; i < r->elements; ++i) {
            redisReply* e = r->element[i];
            if (e && e->type == REDIS_REPLY_STRING && e->str) {
                out_members.emplace_back(e->str, e->len);
            }
        }
        ok = true;
    }

    freeReplyObject(r);
    return ok;
}

bool RedisClient::sdiff(const std::vector<std::string>& keys,
                        std::vector<std::string>& out_members) {
    out_members.clear();
    if (keys.empty()) return true;

    // SDIFF key1 key2 ...  (key1 minus all others)
    std::vector<const char*> argv;
    std::vector<size_t> argvlen;
    argv.reserve(keys.size() + 1);
    argvlen.reserve(keys.size() + 1);

    argv.push_back("SDIFF");
    argvlen.push_back(5);

    for (const auto& k : keys) {
        argv.push_back(k.c_str());
        argvlen.push_back(k.size());
    }

    redisReply* r = static_cast<redisReply*>(
        redisCommandArgv(ctx_.get(),
                         static_cast<int>(argv.size()),
                         argv.data(),
                         argvlen.data())
    );

    if (!r) return false;

    if (r->type == REDIS_REPLY_ERROR) {
        std::string msg = r->str ? std::string(r->str, r->len) : "unknown redis error";
        freeReplyObject(r);
        throw std::runtime_error("SDIFF error: " + msg);
    }

    bool ok = false;
    if (r->type == REDIS_REPLY_ARRAY) {
        out_members.reserve(static_cast<std::size_t>(r->elements));
        for (std::size_t i = 0; i < r->elements; ++i) {
            redisReply* e = r->element[i];
            if (e && e->type == REDIS_REPLY_STRING && e->str) {
                out_members.emplace_back(e->str, e->len);
            }
        }
        ok = true;
    }

    freeReplyObject(r);
    return ok;
}



} // namespace er

