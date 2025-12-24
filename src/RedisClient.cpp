#include "er/RedisClient.hpp"

#include <cstring>
#include <memory>
#include <vector>
#include <chrono>
#include <sstream>

namespace er {

namespace {

struct ReplyDeleter {
    void operator()(redisReply* r) const noexcept {
        if (r) freeReplyObject(r);
    }
};

using ReplyPtr = std::unique_ptr<redisReply, ReplyDeleter>;

static Result<Unit> reply_no_error(const redisReply& r, std::string_view op) noexcept {
    if (r.type != REDIS_REPLY_ERROR) return Result<Unit>::ok();
    std::string msg = r.str ? std::string(r.str, static_cast<std::size_t>(r.len)) : "unknown redis error";
    std::string full(op);
    full.append(": ");
    full.append(msg);
    return Result<Unit>::err(Errc::kRedisProtocol, std::move(full));
}

class ArgvBuilder {
public:
    explicit ArgvBuilder(std::size_t reserve_n = 0) {
        argv_.reserve(reserve_n);
        argvlen_.reserve(reserve_n);
    }

    void push(std::string_view s) {
        argv_.push_back(s.data());
        argvlen_.push_back(s.size());
    }

    void push_bytes(const void* data, std::size_t len) {
        argv_.push_back(static_cast<const char*>(data));
        argvlen_.push_back(len);
    }

    [[nodiscard]] int argc() const noexcept { return static_cast<int>(argv_.size()); }
    // hiredis takes `const char**` (not `const char* const*`), even though it doesn't mutate argv.
    [[nodiscard]] const char** argv() const noexcept { return const_cast<const char**>(argv_.data()); }
    [[nodiscard]] const size_t* argvlen() const noexcept { return argvlen_.data(); }

private:
    std::vector<const char*> argv_{};
    std::vector<size_t> argvlen_{};
};

static Result<ReplyPtr> command_argv(redisContext* c, const ArgvBuilder& args) noexcept {
    if (!c) return Result<ReplyPtr>::err(Errc::kInternal, "redis context is null");
    void* r = redisCommandArgv(c, args.argc(), args.argv(), args.argvlen());
    if (!r) {
        if (c->err) return Result<ReplyPtr>::err(Errc::kRedisIo, c->errstr ? c->errstr : "redis I/O error");
        return Result<ReplyPtr>::err(Errc::kRedisIo, "redis command failed (null reply)");
    }
    return Result<ReplyPtr>::ok(ReplyPtr(static_cast<redisReply*>(r)));
}

static Result<std::vector<std::string>> read_set_array(const redisReply& r) noexcept {
    if (r.type != REDIS_REPLY_ARRAY) return Result<std::vector<std::string>>::err(Errc::kRedisReplyType, "expected array reply");
    std::vector<std::string> out;
    out.reserve(static_cast<std::size_t>(r.elements));
    for (std::size_t i = 0; i < r.elements; ++i) {
        const redisReply* e = r.element[i];
        if (e && e->type == REDIS_REPLY_STRING && e->str) {
            out.emplace_back(e->str, static_cast<std::size_t>(e->len));
        }
    }
    return Result<std::vector<std::string>>::ok(std::move(out));
}

} // namespace

Result<RedisClient> RedisClient::connect(std::string host, int port, int timeout_ms) noexcept {
    if (host.empty()) return Result<RedisClient>::err(Errc::kInvalidArg, "redis host is empty");
    if (port <= 0) return Result<RedisClient>::err(Errc::kInvalidArg, "redis port must be > 0");
    if (timeout_ms <= 0) return Result<RedisClient>::err(Errc::kInvalidArg, "timeout_ms must be > 0");

    timeval tv{};
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    redisContext* raw = redisConnectWithTimeout(host.c_str(), port, tv);
    if (!raw) return Result<RedisClient>::err(Errc::kRedisIo, "redisConnectWithTimeout returned null");
    if (raw->err) {
        std::string msg = raw->errstr ? raw->errstr : "redis connect error";
        redisFree(raw);
        return Result<RedisClient>::err(Errc::kRedisIo, std::move(msg));
    }

    return Result<RedisClient>::ok(RedisClient(raw));
}

RedisClient::~RedisClient() = default;

Result<Unit> RedisClient::ping() noexcept {
    ArgvBuilder args(1);
    args.push("PING");
    auto r = command_argv(ctx_.get(), args);
    if (!r) return Result<Unit>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "PING"); !ok) return ok;
    if (r.value()->type == REDIS_REPLY_STATUS && r.value()->str &&
        std::string_view(r.value()->str, static_cast<std::size_t>(r.value()->len)) == "PONG") {
        return Result<Unit>::ok();
    }
    return Result<Unit>::err(Errc::kRedisReplyType, "PING: expected PONG");
}

// ---- HASH ----

Result<long long> RedisClient::hset(std::string_view key, std::string_view field, std::string_view value) noexcept {
    ArgvBuilder args(4);
    args.push("HSET");
    args.push(key);
    args.push(field);
    args.push(value);
    auto r = command_argv(ctx_.get(), args);
    if (!r) return Result<long long>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "HSET"); !ok) return Result<long long>::err(ok.error().code, ok.error().msg);
    if (r.value()->type != REDIS_REPLY_INTEGER) return Result<long long>::err(Errc::kRedisReplyType, "HSET: expected integer reply");
    return Result<long long>::ok(r.value()->integer);
}

Result<std::string> RedisClient::hget(std::string_view key, std::string_view field) noexcept {
    ArgvBuilder args(3);
    args.push("HGET");
    args.push(key);
    args.push(field);
    auto r = command_argv(ctx_.get(), args);
    if (!r) return Result<std::string>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "HGET"); !ok) return Result<std::string>::err(ok.error().code, ok.error().msg);
    if (r.value()->type == REDIS_REPLY_NIL) return Result<std::string>::err(Errc::kNotFound, "HGET: not found");
    if (r.value()->type != REDIS_REPLY_STRING || !r.value()->str)
        return Result<std::string>::err(Errc::kRedisReplyType, "HGET: expected string reply");
    return Result<std::string>::ok(std::string(r.value()->str, static_cast<std::size_t>(r.value()->len)));
}

Result<long long> RedisClient::hset_bin(std::string_view key,
                                       std::string_view field,
                                       const void* data,
                                       std::size_t len) noexcept {
    ArgvBuilder args(4);
    args.push("HSET");
    args.push(key);
    args.push(field);
    args.push_bytes(data, len);
    auto r = command_argv(ctx_.get(), args);
    if (!r) return Result<long long>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "HSET(bin)"); !ok) return Result<long long>::err(ok.error().code, ok.error().msg);
    if (r.value()->type != REDIS_REPLY_INTEGER) return Result<long long>::err(Errc::kRedisReplyType, "HSET(bin): expected integer reply");
    return Result<long long>::ok(r.value()->integer);
}

Result<std::string> RedisClient::hget_bin(std::string_view key, std::string_view field) noexcept {
    // Same as HGET, but exposed separately for intent.
    return hget(key, field);
}

// ---- SET basic ----

Result<long long> RedisClient::sadd(std::string_view key, std::string_view member) noexcept {
    ArgvBuilder args(3);
    args.push("SADD");
    args.push(key);
    args.push(member);
    auto r = command_argv(ctx_.get(), args);
    if (!r) return Result<long long>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "SADD"); !ok) return Result<long long>::err(ok.error().code, ok.error().msg);
    if (r.value()->type != REDIS_REPLY_INTEGER) return Result<long long>::err(Errc::kRedisReplyType, "SADD: expected integer reply");
    return Result<long long>::ok(r.value()->integer);
}

Result<long long> RedisClient::srem(std::string_view key, std::string_view member) noexcept {
    ArgvBuilder args(3);
    args.push("SREM");
    args.push(key);
    args.push(member);
    auto r = command_argv(ctx_.get(), args);
    if (!r) return Result<long long>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "SREM"); !ok) return Result<long long>::err(ok.error().code, ok.error().msg);
    if (r.value()->type != REDIS_REPLY_INTEGER) return Result<long long>::err(Errc::kRedisReplyType, "SREM: expected integer reply");
    return Result<long long>::ok(r.value()->integer);
}

Result<std::vector<std::string>> RedisClient::smembers(std::string_view key) noexcept {
    ArgvBuilder args(2);
    args.push("SMEMBERS");
    args.push(key);
    auto r = command_argv(ctx_.get(), args);
    if (!r) return Result<std::vector<std::string>>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "SMEMBERS"); !ok)
        return Result<std::vector<std::string>>::err(ok.error().code, ok.error().msg);
    return read_set_array(*r.value());
}

Result<std::vector<std::string>> RedisClient::sinter(const std::vector<std::string>& keys) noexcept {
    if (keys.empty()) return Result<std::vector<std::string>>::ok({});
    ArgvBuilder args(keys.size() + 1);
    args.push("SINTER");
    for (const auto& k : keys) args.push(k);
    auto r = command_argv(ctx_.get(), args);
    if (!r) return Result<std::vector<std::string>>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "SINTER"); !ok)
        return Result<std::vector<std::string>>::err(ok.error().code, ok.error().msg);
    return read_set_array(*r.value());
}

Result<std::vector<std::string>> RedisClient::sunion(const std::vector<std::string>& keys) noexcept {
    if (keys.empty()) return Result<std::vector<std::string>>::ok({});
    ArgvBuilder args(keys.size() + 1);
    args.push("SUNION");
    for (const auto& k : keys) args.push(k);
    auto r = command_argv(ctx_.get(), args);
    if (!r) return Result<std::vector<std::string>>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "SUNION"); !ok)
        return Result<std::vector<std::string>>::err(ok.error().code, ok.error().msg);
    return read_set_array(*r.value());
}

Result<std::vector<std::string>> RedisClient::sdiff(const std::vector<std::string>& keys) noexcept {
    if (keys.empty()) return Result<std::vector<std::string>>::ok({});
    ArgvBuilder args(keys.size() + 1);
    args.push("SDIFF");
    for (const auto& k : keys) args.push(k);
    auto r = command_argv(ctx_.get(), args);
    if (!r) return Result<std::vector<std::string>>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "SDIFF"); !ok)
        return Result<std::vector<std::string>>::err(ok.error().code, ok.error().msg);
    return read_set_array(*r.value());
}

// ---- EXPIRE ----

Result<Unit> RedisClient::expire_seconds(std::string_view key, int ttl_seconds) noexcept {
    if (ttl_seconds <= 0) return Result<Unit>::err(Errc::kInvalidArg, "EXPIRE ttl_seconds must be > 0");
    const std::string ttl_str = std::to_string(ttl_seconds);
    ArgvBuilder args(3);
    args.push("EXPIRE");
    args.push(key);
    args.push(ttl_str);
    auto r = command_argv(ctx_.get(), args);
    if (!r) return Result<Unit>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "EXPIRE"); !ok) return ok;
    if (r.value()->type != REDIS_REPLY_INTEGER) return Result<Unit>::err(Errc::kRedisReplyType, "EXPIRE: expected integer reply");
    if (r.value()->integer == 0) return Result<Unit>::err(Errc::kNotFound, "EXPIRE: key not found");
    return Result<Unit>::ok();
}

// ---- STORE ----

static Result<long long> store_op(redisContext* c,
                                 const char* op,
                                 std::string_view dst,
                                 const std::vector<std::string>& keys) noexcept {
    if (keys.empty()) return Result<long long>::err(Errc::kInvalidArg, "store op requires at least one key");
    ArgvBuilder args(keys.size() + 2);
    args.push(op);
    args.push(dst);
    for (const auto& k : keys) args.push(k);
    auto r = command_argv(c, args);
    if (!r) return Result<long long>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), op); !ok) return Result<long long>::err(ok.error().code, ok.error().msg);
    if (r.value()->type != REDIS_REPLY_INTEGER) return Result<long long>::err(Errc::kRedisReplyType, "store op: expected integer reply");
    return Result<long long>::ok(r.value()->integer);
}

Result<long long> RedisClient::sinterstore(std::string_view dst, const std::vector<std::string>& keys) noexcept {
    return store_op(ctx_.get(), "SINTERSTORE", dst, keys);
}

Result<long long> RedisClient::sunionstore(std::string_view dst, const std::vector<std::string>& keys) noexcept {
    return store_op(ctx_.get(), "SUNIONSTORE", dst, keys);
}

Result<long long> RedisClient::sdiffstore(std::string_view dst, const std::vector<std::string>& keys) noexcept {
    return store_op(ctx_.get(), "SDIFFSTORE", dst, keys);
}

Result<long long> RedisClient::store_expire_lua(std::string_view op,
                                               std::string_view dst,
                                               int ttl_seconds,
                                               const std::vector<std::string>& keys) noexcept {
    if (ttl_seconds <= 0) return Result<long long>::err(Errc::kInvalidArg, "ttl_seconds must be > 0");
    if (keys.empty()) return Result<long long>::err(Errc::kInvalidArg, "store_expire_lua requires at least one key");

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
    const std::string numkeys_str = std::to_string(keys.size());
    const std::string ttl_str = std::to_string(ttl_seconds);

    ArgvBuilder args(keys.size() + 6);
    args.push("EVAL");
    args.push(std::string_view(kLua, std::strlen(kLua)));
    args.push(numkeys_str);
    for (const auto& k : keys) args.push(k);
    args.push(op);
    args.push(dst);
    args.push(ttl_str);

    auto r = command_argv(ctx_.get(), args);
    if (!r) return Result<long long>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "EVAL(store_expire_lua)"); !ok)
        return Result<long long>::err(ok.error().code, ok.error().msg);
    if (r.value()->type != REDIS_REPLY_INTEGER)
        return Result<long long>::err(Errc::kRedisReplyType, "store_expire_lua: expected integer reply");
    return Result<long long>::ok(r.value()->integer);
}

static Result<long long> eval_lua(redisContext* c,
                                 std::string_view script,
                                 const std::vector<std::string>& keys,
                                 const std::vector<std::string>& argv) noexcept {
    if (!c || script.empty()) return Result<long long>::err(Errc::kInternal, "eval_lua: null context/script");
    const std::string numkeys_str = std::to_string(static_cast<int>(keys.size()));
    ArgvBuilder cmd(3 + keys.size() + argv.size());
    cmd.push("EVAL");
    cmd.push(script);
    cmd.push(numkeys_str);
    for (const auto& k : keys) cmd.push(k);
    for (const auto& a : argv) cmd.push(a);

    auto r = command_argv(c, cmd);
    if (!r) return Result<long long>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "EVAL"); !ok) return Result<long long>::err(ok.error().code, ok.error().msg);
    if (r.value()->type != REDIS_REPLY_INTEGER) return Result<long long>::err(Errc::kRedisReplyType, "EVAL: expected integer reply");
    return Result<long long>::ok(r.value()->integer);
}

Result<long long> er::RedisClient::store_all_expire_lua(int ttl_seconds,
                                                       const std::vector<std::string>& set_keys,
                                                       std::string_view out_key) noexcept {
    if (!ctx_) return Result<long long>::err(Errc::kInternal, "redis context is null");
    if (ttl_seconds <= 0) return Result<long long>::err(Errc::kInvalidArg, "ttl_seconds must be > 0");
    if (set_keys.empty()) return Result<long long>::err(Errc::kInvalidArg, "store_all_expire_lua requires at least one key");

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

    const std::vector<std::string> argv{
        std::to_string(ttl_seconds),
        std::string(out_key),
    };
    return eval_lua(ctx_.get(), std::string_view(script, std::strlen(script)), set_keys, argv);
}

Result<long long> er::RedisClient::store_any_expire_lua(int ttl_seconds,
                                                       const std::vector<std::string>& set_keys,
                                                       std::string_view out_key) noexcept {
    if (!ctx_) return Result<long long>::err(Errc::kInternal, "redis context is null");
    if (ttl_seconds <= 0) return Result<long long>::err(Errc::kInvalidArg, "ttl_seconds must be > 0");
    if (set_keys.empty()) return Result<long long>::err(Errc::kInvalidArg, "store_any_expire_lua requires at least one key");

    const char* script = R"lua(
local ttl = tonumber(ARGV[1])
local out = ARGV[2]
redis.call('SUNIONSTORE', out, unpack(KEYS))
if ttl and ttl > 0 then
  redis.call('EXPIRE', out, ttl)
end
return redis.call('SCARD', out)
	)lua";

    const std::vector<std::string> argv{
        std::to_string(ttl_seconds),
        std::string(out_key),
    };
    return eval_lua(ctx_.get(), std::string_view(script, std::strlen(script)), set_keys, argv);
}

Result<long long> er::RedisClient::store_not_expire_lua(int ttl_seconds,
                                                       std::string_view universe_key,
                                                       const std::vector<std::string>& set_keys,
                                                       std::string_view out_key) noexcept {
    if (!ctx_) return Result<long long>::err(Errc::kInternal, "redis context is null");
    if (ttl_seconds <= 0) return Result<long long>::err(Errc::kInvalidArg, "ttl_seconds must be > 0");

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
    keys.push_back(std::string(universe_key));
    for (auto& k : set_keys) keys.push_back(k);
    const std::vector<std::string> argv{
        std::to_string(ttl_seconds),
        std::string(out_key),
    };
    return eval_lua(ctx_.get(), std::string_view(script, std::strlen(script)), keys, argv);
}

Result<long long> er::RedisClient::store_all_not_expire_lua(int ttl_seconds,
                                                           std::string_view include_key,
                                                           std::string_view universe_key,
                                                           const std::vector<std::string>& exclude_keys,
                                                           std::string_view out_key) noexcept {
    if (!ctx_) return Result<long long>::err(Errc::kInternal, "redis context is null");
    if (ttl_seconds <= 0) return Result<long long>::err(Errc::kInvalidArg, "ttl_seconds must be > 0");

    // KEYS: universe_key, exclude1, exclude2, ..., include_key
    // ARGV: ttl, out_key
    const char* script = R"lua(
	local ttl = tonumber(ARGV[1])
	local out = ARGV[2]
	-- Avoid tmp-key collisions across concurrent calls for the same out key.
	-- Use server TIME + a monotonic counter key.
	local t = redis.call('TIME')
	local nonce = redis.call('INCR', 'er:tmp:nonce')
	if redis.call('TTL', 'er:tmp:nonce') < 0 then
	  redis.call('EXPIRE', 'er:tmp:nonce', 86400)
	end
	local tmp = out .. ':tmp:' .. t[1] .. ':' .. t[2] .. ':' .. nonce
	local tmp_ttl = (ttl and ttl > 0) and ttl or 60

	-- tmp = universe \ excludes
	redis.call('SDIFFSTORE', tmp, unpack(KEYS, 1, (#KEYS - 1)))
	redis.call('EXPIRE', tmp, tmp_ttl)
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
    keys.push_back(std::string(universe_key));
    for (const auto& k : exclude_keys) keys.push_back(k);
    keys.push_back(std::string(include_key));
    const std::vector<std::string> argv{
        std::to_string(ttl_seconds),
        std::string(out_key),
    };
    return eval_lua(ctx_.get(), std::string_view(script, std::strlen(script)), keys, argv);
}

Result<long long> er::RedisClient::del_key(std::string_view key) noexcept {
    ArgvBuilder args(2);
    args.push("DEL");
    args.push(key);
    auto r = command_argv(ctx_.get(), args);
    if (!r) return Result<long long>::err(r.error().code, r.error().msg);
    if (auto ok = reply_no_error(*r.value(), "DEL"); !ok) return Result<long long>::err(ok.error().code, ok.error().msg);
    if (r.value()->type != REDIS_REPLY_INTEGER) return Result<long long>::err(Errc::kRedisReplyType, "DEL: expected integer reply");
    return Result<long long>::ok(r.value()->integer);
}


} // namespace er
