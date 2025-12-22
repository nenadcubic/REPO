#include "er_abi/er_abi.h"

#include <string>
#include <vector>
#include <sstream>
#include <memory>
#include <cstring>
#include <chrono>

#include "er/RedisClient.hpp"
#include "er/Flags4096.hpp"
#include "er/keys.hpp"

struct er_handle {
    std::unique_ptr<er::RedisClient> redis;
    std::string last_error;
};

/* helpers */
static int set_err(er_handle_t* h, const std::string& e) {
    if (h) h->last_error = e;
    return ER_ERR;
}

static int set_err(er_handle_t* h, const er::Error& e) {
    return set_err(h, e.msg);
}

static bool load_existing_flags(er::RedisClient& r, const std::string& elem_key, er::Flags4096& out) {
    auto blob = r.hget_bin(elem_key, "flags_bin");
    if (blob && blob.value().size() == 512) {
        auto f = er::Flags4096::from_bytes_be(reinterpret_cast<const std::uint8_t*>(blob.value().data()), blob.value().size());
        if (f) {
            out = std::move(f).value();
            return true;
        }
    }

    auto hex = r.hget(elem_key, "flags_hex");
    if (hex && !hex.value().empty()) {
        auto f = er::Flags4096::from_hex(hex.value());
        if (f) {
            out = std::move(f).value();
            return true;
        }
    }

    out.clear();
    return false;
}

/* update index sets based on delta old->new */
static er::Result<er::Unit> update_index(er::RedisClient& r,
                                        const std::string& name,
                                        const er::Flags4096& oldf,
                                        const er::Flags4096& newf) {
    const auto old_bits = oldf.set_bits();
    const auto new_bits = newf.set_bits();

    // brute-force delta with two-pointer would be faster, but this is fine for now
    // (bit count is usually small)
    for (auto b : old_bits) {
        auto t = newf.test(b);
        if (!t) return er::Result<er::Unit>::err(t.error().code, t.error().msg);
        if (!t.value()) {
            auto ok = r.srem(er::keys::idx_bit(b), name);
            if (!ok) return er::Result<er::Unit>::err(ok.error().code, ok.error().msg);
        }
    }
    for (auto b : new_bits) {
        auto t = oldf.test(b);
        if (!t) return er::Result<er::Unit>::err(t.error().code, t.error().msg);
        if (!t.value()) {
            auto ok = r.sadd(er::keys::idx_bit(b), name);
            if (!ok) return er::Result<er::Unit>::err(ok.error().code, ok.error().msg);
        }
    }
    return er::Result<er::Unit>::ok();
}

static std::string make_tmp_key(const char* op, int ttl_sec) {
    std::string tag(op);
    tag.append(":ttl");
    tag.append(std::to_string(ttl_sec));
    return er::keys::tmp(tag);
}

/* lifecycle */
er_handle_t* er_create(const char* host, int port) {
    if (!host || port <= 0) return nullptr;
    auto client = er::RedisClient::connect(host, port);
    if (!client) return nullptr;

    auto* h = new er_handle();
    h->redis = std::make_unique<er::RedisClient>(std::move(client).value());
    auto ok = h->redis->ping();
    if (!ok) {
        h->last_error = ok.error().msg;
        delete h;
        return nullptr;
    }
    return h;
}

void er_destroy(er_handle_t* h) {
    if (!h) return;
    delete h;
}

int er_ping(er_handle_t* h) {
    if (!h || !h->redis) return ER_BADARG;
    auto ok = h->redis->ping();
    return ok ? ER_OK : set_err(h, ok.error());
}

const char* er_last_error(er_handle_t* h) {
    if (!h) return "null handle";
    return h->last_error.c_str();
}

/* element ops */
int er_put_bits(er_handle_t* h, const char* name,
                const uint16_t* bits, size_t n_bits) {
    if (!h || !h->redis || !name || (!bits && n_bits > 0))
        return ER_BADARG;

    // build new flags
    er::Flags4096 newf;
    for (size_t i = 0; i < n_bits; ++i) {
        if (bits[i] >= 4096) return ER_RANGE;
        auto ok = newf.set(bits[i]);
        if (!ok) return set_err(h, ok.error());
    }

    const std::string sname(name);
    const std::string elem_key = er::keys::element(sname);

    // load old flags for index delta
    er::Flags4096 oldf;
    load_existing_flags(*h->redis, elem_key, oldf);

    // update index sets
    if (auto ok = update_index(*h->redis, sname, oldf, newf); !ok) return set_err(h, ok.error());

    // store element hash
    if (auto ok = h->redis->hset(elem_key, "name", sname); !ok) return set_err(h, ok.error());

    const auto bytes = newf.to_bytes_be(); // 512 bytes
    if (auto ok = h->redis->hset_bin(elem_key, "flags_bin", bytes.data(), bytes.size()); !ok) return set_err(h, ok.error());

    return ER_OK;
}

/* composite store: find_all + TTL (Lua, atomic) */
int er_find_all_store(er_handle_t* h, int ttl_sec,
                      const uint16_t* bits, size_t n_bits,
                      char* out_tmp_key, size_t key_cap) {
    if (!h || !h->redis || !bits || n_bits == 0 || !out_tmp_key || key_cap == 0)
        return ER_BADARG;
    if (ttl_sec <= 0) return ER_BADARG;

    std::vector<std::string> idx_keys;
    idx_keys.reserve(n_bits);
    for (size_t i = 0; i < n_bits; ++i) {
        if (bits[i] >= 4096) return ER_RANGE;
        idx_keys.push_back(er::keys::idx_bit(bits[i]));
    }

    std::string tmp_key = make_tmp_key("all", ttl_sec);

    auto ok = h->redis->store_expire_lua("SINTERSTORE", tmp_key, ttl_sec, idx_keys);
    if (!ok) return set_err(h, ok.error());

    std::snprintf(out_tmp_key, key_cap, "%s", tmp_key.c_str());
    return ER_OK;
}

/* read set members */
int er_show_set(er_handle_t* h, const char* set_key,
                char* out, size_t out_cap) {
    if (!h || !h->redis || !set_key || !out || out_cap == 0)
        return ER_BADARG;

    auto members = h->redis->smembers(set_key);
    if (!members) return set_err(h, members.error());

    std::ostringstream oss;
    for (auto& m : members.value()) oss << m << "\n";

    const std::string s = oss.str();
    if (s.size() + 1 > out_cap) return ER_RANGE;

    std::memcpy(out, s.c_str(), s.size() + 1);
    return ER_OK;
}
    
int er_find_any_store(er_handle_t* h, int ttl_seconds,
                      const uint16_t* bits, size_t n_bits,
                      char* out_tmp_key, size_t key_cap) {
    if (!h || !h->redis || !bits || n_bits == 0 || !out_tmp_key || key_cap == 0)
        return ER_BADARG;

    std::vector<std::string> keys;
    keys.reserve(n_bits);
    for (size_t i = 0; i < n_bits; ++i) {
        if (bits[i] >= 4096) return ER_RANGE;
        keys.push_back(er::keys::idx_bit(bits[i]));
    }

    std::string tmp_key = er::keys::tmp(std::string("any:ttl") + std::to_string(ttl_seconds));
    auto ok = h->redis->store_any_expire_lua(ttl_seconds, keys, tmp_key);
    if (!ok) return set_err(h, ok.error());

    std::snprintf(out_tmp_key, key_cap, "%s", tmp_key.c_str());
    return ER_OK;
}

int er_find_not_store(er_handle_t* h, int ttl_seconds,
                      const uint16_t* bits, size_t n_bits,
                      char* out_tmp_key, size_t key_cap) {
    if (!h || !h->redis || !bits || n_bits == 0 || !out_tmp_key || key_cap == 0)
        return ER_BADARG;

    std::vector<std::string> keys;
    keys.reserve(n_bits);
    for (size_t i = 0; i < n_bits; ++i) {
        if (bits[i] >= 4096) return ER_RANGE;
        keys.push_back(er::keys::idx_bit(bits[i]));
    }

    std::string tmp_key = er::keys::tmp(std::string("not:ttl") + std::to_string(ttl_seconds));
    auto ok = h->redis->store_not_expire_lua(ttl_seconds, er::keys::universe(), keys, tmp_key);
    if (!ok) return set_err(h, ok.error());

    std::snprintf(out_tmp_key, key_cap, "%s", tmp_key.c_str());
    return ER_OK;
}
