#include "er_abi/er_abi.h"

#include <string>
#include <vector>
#include <sstream>
#include <memory>
#include <cstring>
#include <chrono>

#include "er/RedisClient.hpp"
#include "er/Flags4096.hpp"

struct er_handle {
    std::unique_ptr<er::RedisClient> redis;
    std::string last_error;
};

/* helpers */
static int set_err(er_handle_t* h, const std::string& e) {
    if (h) h->last_error = e;
    return ER_ERR;
}

static std::string key_for_element(const std::string& name) {
    return "er:element:" + name;
}
static std::string key_for_bit_index(std::size_t bit) {
    return "er:idx:bit:" + std::to_string(bit);
}

static bool load_existing_flags(er::RedisClient& r, const std::string& elem_key, er::Flags4096& out) {
    std::string blob;
    if (r.hget_bin(elem_key, "flags_bin", blob) && blob.size() == 512) {
        out = er::Flags4096::from_bytes_be(
            reinterpret_cast<const std::uint8_t*>(blob.data()),
            blob.size()
        );
        return true;
    }

    std::string hex;
    if (r.hget(elem_key, "flags_hex", hex) && !hex.empty()) {
        out = er::Flags4096::from_hex(hex);
        return true;
    }

    out.clear();
    return false;
}

/* update index sets based on delta old->new */
static void update_index(er::RedisClient& r,
                         const std::string& name,
                         const er::Flags4096& oldf,
                         const er::Flags4096& newf) {
    const auto old_bits = oldf.set_bits();
    const auto new_bits = newf.set_bits();

    // brute-force delta with two-pointer would be faster, but this is fine for now
    // (bit count is usually small)
    for (auto b : old_bits) {
        if (!newf.test(b)) {
            r.srem(key_for_bit_index(b), name);
        }
    }
    for (auto b : new_bits) {
        if (!oldf.test(b)) {
            r.sadd(key_for_bit_index(b), name);
        }
    }
}

static std::string make_tmp_key(const char* op, int ttl_sec) {
    // unique-enough tmp key: time-based
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(now).count();

    std::ostringstream oss;
    oss << "er:tmp:" << op << ":ttl" << ttl_sec << ":t" << us;
    return oss.str();
}

/* lifecycle */
er_handle_t* er_create(const char* host, int port) {
    if (!host || port <= 0) return nullptr;
    try {
        auto* h = new er_handle();
        h->redis = std::make_unique<er::RedisClient>(host, port);
        if (!h->redis->ping()) {
            h->last_error = "redis ping failed";
            delete h;
            return nullptr;
        }
        return h;
    } catch (...) {
        return nullptr;
    }
}

void er_destroy(er_handle_t* h) {
    if (!h) return;
    delete h;
}

int er_ping(er_handle_t* h) {
    if (!h || !h->redis) return ER_BADARG;
    return h->redis->ping() ? ER_OK : set_err(h, "ping failed");
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

    try {
        // build new flags
        er::Flags4096 newf;
        for (size_t i = 0; i < n_bits; ++i) {
            if (bits[i] >= 4096) return ER_RANGE;
            newf.set(bits[i]);
        }

        const std::string sname(name);
        const std::string elem_key = key_for_element(sname);

        // load old flags for index delta
        er::Flags4096 oldf;
        load_existing_flags(*h->redis, elem_key, oldf);

        // update index sets
        update_index(*h->redis, sname, oldf, newf);

        // store element hash
        if (!h->redis->hset(elem_key, "name", sname))
            return set_err(h, "HSET name failed");

        const auto bytes = newf.to_bytes_be(); // 512 bytes
        if (!h->redis->hset_bin(elem_key, "flags_bin", bytes.data(), bytes.size()))
            return set_err(h, "HSET flags_bin failed");

        return ER_OK;
    } catch (const std::exception& e) {
        return set_err(h, e.what());
    }
}

/* composite store: find_all + TTL (Lua, atomic) */
int er_find_all_store(er_handle_t* h, int ttl_sec,
                      const uint16_t* bits, size_t n_bits,
                      char* out_tmp_key, size_t key_cap) {
    if (!h || !h->redis || !bits || n_bits == 0 || !out_tmp_key || key_cap == 0)
        return ER_BADARG;
    if (ttl_sec <= 0) return ER_BADARG;

    try {
        std::vector<std::string> idx_keys;
        idx_keys.reserve(n_bits);
        for (size_t i = 0; i < n_bits; ++i) {
            if (bits[i] >= 4096) return ER_RANGE;
            idx_keys.push_back(key_for_bit_index(bits[i]));
        }

        std::string tmp_key = make_tmp_key("all", ttl_sec);

        long long card = 0;
        bool ok = h->redis->store_expire_lua(
            "SINTERSTORE", tmp_key, ttl_sec, idx_keys, &card
        );
        if (!ok) return set_err(h, "store_expire_lua failed");

        std::snprintf(out_tmp_key, key_cap, "%s", tmp_key.c_str());
        return ER_OK;
    } catch (const std::exception& e) {
        return set_err(h, e.what());
    }
}

/* read set members */
int er_show_set(er_handle_t* h, const char* set_key,
                char* out, size_t out_cap) {
    if (!h || !h->redis || !set_key || !out || out_cap == 0)
        return ER_BADARG;

    try {
        std::vector<std::string> members;
        if (!h->redis->smembers(set_key, members))
            return set_err(h, "SMEMBERS failed");

        std::ostringstream oss;
        for (auto& m : members) oss << m << "\n";

        const std::string s = oss.str();
        if (s.size() + 1 > out_cap) return ER_RANGE;

        std::memcpy(out, s.c_str(), s.size() + 1);
        return ER_OK;
    } catch (const std::exception& e) {
        return set_err(h, e.what());
    }
}
    
int er_find_any_store(er_handle_t* h, int ttl_seconds,
                      const uint16_t* bits, size_t n_bits,
                      char* out_tmp_key, size_t key_cap) {
    if (!h || !h->redis || !bits || n_bits == 0 || !out_tmp_key || key_cap == 0)
        return ER_BADARG;

    try {
        std::vector<std::string> keys;
        keys.reserve(n_bits);
        for (size_t i = 0; i < n_bits; ++i) {
            keys.push_back("er:idx:bit:" + std::to_string(bits[i]));
        }

        std::string tmp_key = er::RedisClient::make_tmp_key("any");
        bool ok = h->redis->store_any_expire_lua(ttl_seconds, keys, tmp_key);
        if (!ok) return set_err(h, "ANY store+expire failed");

        std::snprintf(out_tmp_key, key_cap, "%s", tmp_key.c_str());
        return ER_OK;
    } catch (...) {
        return set_err(h, "exception in er_find_any_store");
    }
}

int er_find_not_store(er_handle_t* h, int ttl_seconds,
                      const uint16_t* bits, size_t n_bits,
                      char* out_tmp_key, size_t key_cap) {
    if (!h || !h->redis || !bits || n_bits == 0 || !out_tmp_key || key_cap == 0)
        return ER_BADARG;

    try {
        std::vector<std::string> keys;
        keys.reserve(n_bits);
        for (size_t i = 0; i < n_bits; ++i) {
            keys.push_back("er:idx:bit:" + std::to_string(bits[i]));
        }

        std::string tmp_key = er::RedisClient::make_tmp_key("not");
        bool ok = h->redis->store_not_expire_lua(ttl_seconds, "er:all", keys, tmp_key);
        if (!ok) return set_err(h, "NOT store+expire failed");

        std::snprintf(out_tmp_key, key_cap, "%s", tmp_key.c_str());
        return ER_OK;
    } catch (...) {
        return set_err(h, "exception in er_find_not_store");
    }
}
