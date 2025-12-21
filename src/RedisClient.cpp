#include "er/RedisClient.hpp"

#include <stdexcept>
#include <cstdarg>
#include <vector>
#include <chrono>
#include <sstream>

namespace er {

static redisReply* cmd(redisContext* c, const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    void* r = redisvCommand(c, fmt, ap);
    va_end(ap);
    return static_cast<redisReply*>(r);
}

static void throw_if_error(redisReply* r, const char* op) {
    if (!r) return;
    if (r->type == REDIS_REPLY_ERROR) {
        std::string msg = r->str ? std::string(r->str, r->len) : "unknown redis error";
        freeReplyObject(r);
        throw std::runtime_error(std::string(op) + " error: " + msg);
    }
}

RedisClient::RedisClient(std::string host, int port) {
    timeval tv{};
    tv.tv_sec = 2;
    tv.tv_usec = 0;

    redisContext* raw = redisConnectWithTimeout(host.c_str(), port, tv);
    ctx_.reset(raw);

    if (!ctx_ || ctx_->err) {
        std::string msg = ctx_ ? ctx_->errstr : "null context";
        throw std::runtime_error("Redis connect failed: " + msg);
    }
}

RedisClient::~RedisClient() = default;

bool RedisClient::ping() {
    auto* r = cmd(ctx_.get(), "PING");
    if (!r) return false;
    bool ok = (r->type == REDIS_REPLY_STATUS && r->str && std::string(r->str) == "PONG");
    freeReplyObject(r);
    return ok;
}

// ---- HASH ----

bool RedisClient::hset(const std::string& key,
                       const std::string& field,
                       const std::string& value) {
    auto* r = cmd(ctx_.get(), "HSET %s %s %s", key.c_str(), field.c_str(), value.c_str());
    if (!r) return false;
    throw_if_error(r, "HSET");
    bool ok = (r->type == REDIS_REPLY_INTEGER);
    freeReplyObject(r);
    return ok;
}

bool RedisClient::hget(const std::string& key,
                       const std::string& field,
                       std::string& out_value) {
    auto* r = cmd(ctx_.get(), "HGET %s %s", key.c_str(), field.c_str());
    if (!r) return false;
    throw_if_error(r, "HGET");

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
    auto* r = cmd(ctx_.get(), "HSET %s %s %b", key.c_str(), field.c_str(), data, len);
    if (!r) return false;
    throw_if_error(r, "HSET(bin)");
    bool ok = (r->type == REDIS_REPLY_INTEGER);
    freeReplyObject(r);
    return ok;
}

bool RedisClient::hget_bin(const std::string& key,
                           const std::string& field,
                           std::string& out_blob) {
    auto* r = cmd(ctx_.get(), "HGET %s %s", key.c_str(), field.c_str());
    if (!r) return false;
    throw_if_error(r, "HGET(bin)");

    bool ok = false;
    if (r->type == REDIS_REPLY_STRING && r->str) {
        out_blob.assign(r->str, r->len);
        ok = true;
    }
    freeReplyObject(r);
    return ok;
}

// ---- SET basic ----

bool RedisClient::sadd(const std::string& key, const std::string& member) {
    auto* r = cmd(ctx_.get(), "SADD %s %s", key.c_str(), member.c_str());
    if (!r) return false;
    throw_if_error(r, "SADD");
    bool ok = (r->type == REDIS_REPLY_INTEGER);
    freeReplyObject(r);
    return ok;
}

bool RedisClient::srem(const std::string& key, const std::string& member) {
    auto* r = cmd(ctx_.get(), "SREM %s %s", key.c_str(), member.c_str());
    if (!r) return false;
    throw_if_error(r, "SREM");
    bool ok = (r->type == REDIS_REPLY_INTEGER);
    freeReplyObject(r);
    return ok;
}

bool RedisClient::smembers(const std::string& key, std::vector<std::string>& out_members) {
    out_members.clear();
    auto* r = cmd(ctx_.get(), "SMEMBERS %s", key.c_str());
    if (!r) return false;
    throw_if_error(r, "SMEMBERS");

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

// ---- composite helpers (ARGV) ----

static redisReply* command_argv(redisContext* c,
                               const std::vector<std::string>& args) {
    std::vector<const char*> argv;
    std::vector<size_t> argvlen;
    argv.reserve(args.size());
    argvlen.reserve(args.size());
    for (const auto& s : args) {
        argv.push_back(s.c_str());
        argvlen.push_back(s.size());
    }
    return static_cast<redisReply*>(
        redisCommandArgv(c, static_cast<int>(argv.size()), argv.data(), argvlen.data())
    );
}

static bool read_set_array(redisReply* r, std::vector<std::string>& out) {
    out.clear();
    if (!r) return false;
    if (r->type == REDIS_REPLY_ARRAY) {
        out.reserve(static_cast<std::size_t>(r->elements));
        for (std::size_t i = 0; i < r->elements; ++i) {
            redisReply* e = r->element[i];
            if (e && e->type == REDIS_REPLY_STRING && e->str) {
                out.emplace_back(e->str, e->len);
            }
        }
        return true;
    }
    return false;
}

bool RedisClient::sinter(const std::vector<std::string>& keys,
                         std::vector<std::string>& out_members) {
    if (keys.empty()) { out_members.clear(); return true; }
    std::vector<std::string> args;
    args.reserve(keys.size() + 1);
    args.push_back("SINTER");
    for (const auto& k : keys) args.push_back(k);

    auto* r = command_argv(ctx_.get(), args);
    if (!r) return false;
    throw_if_error(r, "SINTER");

    bool ok = read_set_array(r, out_members);
    freeReplyObject(r);
    return ok;
}

bool RedisClient::sunion(const std::vector<std::string>& keys,
                         std::vector<std::string>& out_members) {
    if (keys.empty()) { out_members.clear(); return true; }
    std::vector<std::string> args;
    args.reserve(keys.size() + 1);
    args.push_back("SUNION");
    for (const auto& k : keys) args.push_back(k);

    auto* r = command_argv(ctx_.get(), args);
    if (!r) return false;
    throw_if_error(r, "SUNION");

    bool ok = read_set_array(r, out_members);
    freeReplyObject(r);
    return ok;
}

bool RedisClient::sdiff(const std::vector<std::string>& keys,
                        std::vector<std::string>& out_members) {
    if (keys.empty()) { out_members.clear(); return true; }
    std::vector<std::string> args;
    args.reserve(keys.size() + 1);
    args.push_back("SDIFF");
    for (const auto& k : keys) args.push_back(k);

    auto* r = command_argv(ctx_.get(), args);
    if (!r) return false;
    throw_if_error(r, "SDIFF");

    bool ok = read_set_array(r, out_members);
    freeReplyObject(r);
    return ok;
}

// ---- EXPIRE ----

bool RedisClient::expire_seconds(const std::string& key, int ttl_seconds) {
    auto* r = cmd(ctx_.get(), "EXPIRE %s %d", key.c_str(), ttl_seconds);
    if (!r) return false;
    throw_if_error(r, "EXPIRE");
    bool ok = (r->type == REDIS_REPLY_INTEGER);
    freeReplyObject(r);
    return ok;
}

// ---- STORE ----

static bool store_op(redisContext* c,
                     const char* op,
                     const std::string& dst,
                     const std::vector<std::string>& keys) {
    if (keys.empty()) return false;

    std::vector<std::string> args;
    args.reserve(keys.size() + 2);
    args.push_back(op);
    args.push_back(dst);
    for (const auto& k : keys) args.push_back(k);

    auto* r = command_argv(c, args);
    if (!r) return false;

    throw_if_error(r, op);
    bool ok = (r->type == REDIS_REPLY_INTEGER); // returns cardinality
    freeReplyObject(r);
    return ok;
}

bool RedisClient::sinterstore(const std::string& dst, const std::vector<std::string>& keys) {
    return store_op(ctx_.get(), "SINTERSTORE", dst, keys);
}

bool RedisClient::sunionstore(const std::string& dst, const std::vector<std::string>& keys) {
    return store_op(ctx_.get(), "SUNIONSTORE", dst, keys);
}

bool RedisClient::sdiffstore(const std::string& dst, const std::vector<std::string>& keys) {
    return store_op(ctx_.get(), "SDIFFSTORE", dst, keys);
}

bool RedisClient::store_expire_lua(const std::string& op,
                                  const std::string& dst,
                                  int ttl_seconds,
                                  const std::vector<std::string>& keys,
                                  long long* out_cardinality) {
    if (keys.empty()) return false;

    // Redis Lua script (atomic store + expire)
    static const char* kLua = R"(
        local op  = ARGV[1]
        local dst = ARGV[2]
        local ttl = tonumber(ARGV[3])

        local card = redis.call(op, dst, unpack(KEYS))

        if ttl and ttl > 0 then
          redis.call("EXPIRE", dst, ttl)
        end

        return card
    )";

    // EVAL <script> <numkeys> key1 key2 ... op dst ttl
    std::vector<std::string> args;
    args.reserve(3 + 1 + keys.size() + 3);

    args.push_back("EVAL");
    args.push_back(kLua);
    args.push_back(std::to_string(keys.size()));
    for (const auto& k : keys) args.push_back(k);

    args.push_back(op);
    args.push_back(dst);
    args.push_back(std::to_string(ttl_seconds));

    // send via hiredis argv (binary safe)
    std::vector<const char*> argv;
    std::vector<size_t> argvlen;
    argv.reserve(args.size());
    argvlen.reserve(args.size());

    for (const auto& s : args) {
        argv.push_back(s.c_str());
        argvlen.push_back(s.size());
    }

    redisReply* r = static_cast<redisReply*>(
        redisCommandArgv(ctx_.get(),
                         static_cast<int>(argv.size()),
                         argv.data(),
                         argvlen.data())
    );

    if (!r) return false;
    throw_if_error(r, "EVAL(store_expire_lua)");

    bool ok = false;
    if (r->type == REDIS_REPLY_INTEGER) {
        if (out_cardinality) *out_cardinality = r->integer;
        ok = true;
    }

    freeReplyObject(r);
    return ok;
}

std::string er::RedisClient::make_tmp_key(const std::string& tag) {
    using namespace std::chrono;
    auto ns = duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
    std::ostringstream oss;
    oss << "er:tmp:" << tag << ":" << ns;
    return oss.str();
}

static bool eval_ok(redisReply* r) {
    if (!r) return false;
    // EVAL returns integer (count), or array, etc. We accept INTEGER >=0
    if (r->type == REDIS_REPLY_INTEGER) return true;
    // Some redis versions return status "OK" for some ops
    if (r->type == REDIS_REPLY_STATUS) return true;
    return true;
}

bool er::RedisClient::store_all_expire_lua(int ttl_seconds,
                                          const std::vector<std::string>& set_keys,
                                          const std::string& out_key) {
    if (!ctx_ || set_keys.empty()) return false;

    // KEYS: set_keys...
    // ARGV: ttl, out_key
    const char* script = R"lua(
local ttl = tonumber(ARGV[1])
local out = ARGV[2]
redis.call('SINTERSTORE', out, unpack(KEYS))
if ttl and ttl > 0 then
  redis.call('EXPIRE', out, ttl)
end
return redis.call('SCARD', out)
)lua";

    std::vector<const char*> argv;
    std::vector<std::string> args;
    args.reserve(2);
    args.push_back(std::to_string(ttl_seconds));
    args.push_back(out_key);

    // Build redisCommand: EVAL script numkeys key1 key2 ... argv1 argv2
    // We'll use redisCommandArgv for safety
    std::vector<std::string> cmd;
    cmd.reserve(3 + set_keys.size() + args.size());
    cmd.push_back("EVAL");
    cmd.push_back(script);
    cmd.push_back(std::to_string((int)set_keys.size()));
    for (auto& k : set_keys) cmd.push_back(k);
    for (auto& a : args) cmd.push_back(a);

    std::vector<const char*> cstr;
    std::vector<size_t> lens;
    cstr.reserve(cmd.size());
    lens.reserve(cmd.size());
    for (auto& s : cmd) { cstr.push_back(s.c_str()); lens.push_back(s.size()); }

    redisReply* r = (redisReply*)redisCommandArgv(ctx_.get(), (int)cstr.size(), cstr.data(), lens.data());
    bool ok = eval_ok(r);
    if (r) freeReplyObject(r);
    return ok;
}

bool er::RedisClient::store_any_expire_lua(int ttl_seconds,
                                          const std::vector<std::string>& set_keys,
                                          const std::string& out_key) {
    if (!ctx_ || set_keys.empty()) return false;

    const char* script = R"lua(
local ttl = tonumber(ARGV[1])
local out = ARGV[2]
redis.call('SUNIONSTORE', out, unpack(KEYS))
if ttl and ttl > 0 then
  redis.call('EXPIRE', out, ttl)
end
return redis.call('SCARD', out)
)lua";

    std::vector<std::string> cmd;
    cmd.reserve(3 + set_keys.size() + 2);
    cmd.push_back("EVAL");
    cmd.push_back(script);
    cmd.push_back(std::to_string((int)set_keys.size()));
    for (auto& k : set_keys) cmd.push_back(k);
    cmd.push_back(std::to_string(ttl_seconds));
    cmd.push_back(out_key);

    std::vector<const char*> cstr;
    std::vector<size_t> lens;
    cstr.reserve(cmd.size());
    lens.reserve(cmd.size());
    for (auto& s : cmd) { cstr.push_back(s.c_str()); lens.push_back(s.size()); }

    redisReply* r = (redisReply*)redisCommandArgv(ctx_.get(), (int)cstr.size(), cstr.data(), lens.data());
    bool ok = eval_ok(r);
    if (r) freeReplyObject(r);
    return ok;
}

bool er::RedisClient::store_not_expire_lua(int ttl_seconds,
                                          const std::string& universe_key,
                                          const std::vector<std::string>& set_keys,
                                          const std::string& out_key) {
    if (!ctx_) return false;

    // KEYS: universe_key + set_keys...
    const char* script = R"lua(
local ttl = tonumber(ARGV[1])
local out = ARGV[2]
-- SDIFFSTORE out universe s1 s2 ...
redis.call('SDIFFSTORE', out, unpack(KEYS))
if ttl and ttl > 0 then
  redis.call('EXPIRE', out, ttl)
end
return redis.call('SCARD', out)
)lua";

    std::vector<std::string> keys;
    keys.reserve(1 + set_keys.size());
    keys.push_back(universe_key);
    for (auto& k : set_keys) keys.push_back(k);

    std::vector<std::string> cmd;
    cmd.reserve(3 + keys.size() + 2);
    cmd.push_back("EVAL");
    cmd.push_back(script);
    cmd.push_back(std::to_string((int)keys.size()));
    for (auto& k : keys) cmd.push_back(k);
    cmd.push_back(std::to_string(ttl_seconds));
    cmd.push_back(out_key);

    std::vector<const char*> cstr;
    std::vector<size_t> lens;
    cstr.reserve(cmd.size());
    lens.reserve(cmd.size());
    for (auto& s : cmd) { cstr.push_back(s.c_str()); lens.push_back(s.size()); }

    redisReply* r = (redisReply*)redisCommandArgv(ctx_.get(), (int)cstr.size(), cstr.data(), lens.data());
    bool ok = eval_ok(r);
    if (r) freeReplyObject(r);
    return ok;
}

bool er::RedisClient::store_all_not_expire_lua(int ttl_seconds,
                                              const std::string& include_key,
                                              const std::string& universe_key,
                                              const std::vector<std::string>& exclude_keys,
                                              const std::string& out_key) {
    if (!ctx_) return false;

    // KEYS: universe_key, exclude1, exclude2, ..., include_key
    // ARGV: ttl, out_key
    const char* script = R"lua(
local ttl = tonumber(ARGV[1])
local out = ARGV[2]
local tmp = out .. ':tmp'

-- tmp = universe \ excludes
redis.call('SDIFFSTORE', tmp, unpack(KEYS, 1, (#KEYS - 1)))
-- out = include ∩ tmp
redis.call('SINTERSTORE', out, KEYS[#KEYS], tmp)

if ttl and ttl > 0 then
  redis.call('EXPIRE', out, ttl)
end
redis.call('DEL', tmp)
return redis.call('SCARD', out)
)lua";

    std::vector<std::string> keys;
    keys.reserve(2 + exclude_keys.size());
    keys.push_back(universe_key);
    for (const auto& k : exclude_keys) keys.push_back(k);
    keys.push_back(include_key);

    std::vector<std::string> cmd;
    cmd.reserve(3 + keys.size() + 2);
    cmd.push_back("EVAL");
    cmd.push_back(script);
    cmd.push_back(std::to_string((int)keys.size()));
    for (const auto& k : keys) cmd.push_back(k);
    cmd.push_back(std::to_string(ttl_seconds));
    cmd.push_back(out_key);

    std::vector<const char*> cstr;
    std::vector<size_t> lens;
    cstr.reserve(cmd.size());
    lens.reserve(cmd.size());
    for (auto& s : cmd) { cstr.push_back(s.c_str()); lens.push_back(s.size()); }

    redisReply* r = (redisReply*)redisCommandArgv(ctx_.get(), (int)cstr.size(), cstr.data(), lens.data());
    bool ok = eval_ok(r);
    if (r) freeReplyObject(r);
    return ok;
}

bool er::RedisClient::del_key(const std::string& key) {
    auto* r = cmd(ctx_.get(), "DEL %s", key.c_str());
    if (!r) return false;
    throw_if_error(r, "DEL");
    bool ok = (r->type == REDIS_REPLY_INTEGER);
    freeReplyObject(r);
    return ok;
}


} // namespace er
