#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>
#include <cstdint>
#include <unordered_set>
#include <sstream>

#include "er/Element.hpp"
#include "er/RedisClient.hpp"
#include "er/Flags4096.hpp"

static void usage() {
    std::cout <<
      "Usage:\n"
      "  er_cli put <name> <bit> [bit2 bit3 ...]\n"
      "  er_cli get <name>\n"
      "  er_cli find <bit>\n"
      "  er_cli find_all <bit1> <bit2> [bit3 ...]\n"
      "  er_cli find_any <bit1> <bit2> [bit3 ...]\n"
      "  er_cli find_not <include_bit> <exclude_bit1> [exclude_bit2 ...]\n"
      "\n"
      "Store+TTL:\n"
      "  er_cli find_all_store <ttl_sec> <bit1> <bit2> [bit3 ...]\n"
      "  er_cli find_any_store <ttl_sec> <bit1> <bit2> [bit3 ...]\n"
      "  er_cli find_not_store <ttl_sec> <include_bit> <exclude_bit1> [exclude_bit2 ...]\n"
      "  er_cli show <redis_set_key>\n";
}

static std::string key_for(const std::string& name) {
    return "er:element:" + name;
}

static std::string idx_key_for_bit(std::size_t bit) {
    return "er:idx:bit:" + std::to_string(bit);
}

static bool load_existing_flags(er::RedisClient& r, const std::string& key, er::Flags4096& out_flags) {
    std::string blob;
    if (r.hget_bin(key, "flags_bin", blob) && blob.size() == 512) {
        out_flags = er::Flags4096::from_bytes_be(
            reinterpret_cast<const std::uint8_t*>(blob.data()),
            blob.size()
        );
        return true;
    }

    std::string hex;
    if (r.hget(key, "flags_hex", hex) && !hex.empty()) {
        out_flags = er::Flags4096::from_hex(hex);
        return true;
    }

    out_flags.clear();
    return false;
}

static void update_index_for_put(er::RedisClient& r,
                                 const std::string& name,
                                 const er::Flags4096& oldf,
                                 const er::Flags4096& newf) {
    const auto old_bits = oldf.set_bits();
    const auto new_bits = newf.set_bits();

    std::unordered_set<std::size_t> old_set(old_bits.begin(), old_bits.end());
    std::unordered_set<std::size_t> new_set(new_bits.begin(), new_bits.end());

    for (auto b : old_set) {
        if (new_set.find(b) == new_set.end()) {
            r.srem(idx_key_for_bit(b), name);
        }
    }
    for (auto b : new_set) {
        if (old_set.find(b) == old_set.end()) {
            r.sadd(idx_key_for_bit(b), name);
        }
    }
}

static std::vector<std::string> build_idx_keys_from_bits(int argc, char** argv, int start_i) {
    std::vector<std::string> idx_keys;
    idx_keys.reserve(static_cast<std::size_t>(argc - start_i));
    for (int i = start_i; i < argc; ++i) {
        const std::size_t bit = static_cast<std::size_t>(std::stoul(argv[i]));
        if (bit >= 4096) throw std::runtime_error("bit out of range (0..4095): " + std::to_string(bit));
        idx_keys.push_back(idx_key_for_bit(bit));
    }
    return idx_keys;
}

static std::string make_tmp_key(const std::string& op, int ttl, const std::vector<std::string>& idx_keys) {
    // deterministički ključ (isti upit => isti tmp key), u praksi dovoljno:
    // er:tmp:<op>:ttl<ttl>:k<hash>
    std::uint64_t h = 1469598103934665603ull; // FNV-1a
    auto mix = [&](const std::string& s) {
        for (unsigned char c : s) {
            h ^= c;
            h *= 1099511628211ull;
        }
    };
    mix(op);
    mix(std::to_string(ttl));
    for (const auto& k : idx_keys) mix(k);

    std::ostringstream oss;
    oss << "er:tmp:" << op << ":ttl" << ttl << ":h" << std::hex << h;
    return oss.str();
}

static void print_members(const std::string& label, const std::vector<std::string>& members) {
    std::cout << label << "\n";
    std::cout << "Count: " << members.size() << "\n";
    for (const auto& m : members) std::cout << " - " << m << "\n";
}

int main(int argc, char** argv) {
    if (argc < 2) { usage(); return 1; }

    const std::string op = argv[1];

    try {
        er::RedisClient r("redis", 6379);
        if (!r.ping()) {
            std::cerr << "Redis PING failed\n";
            return 2;
        }

        // ---- PUT ----
        if (op == "put") {
            if (argc < 4) { usage(); return 1; }

            const std::string name = argv[2];
            const std::string key  = key_for(name);

            er::Flags4096 oldf;
            load_existing_flags(r, key, oldf);

            er::Element e(name);
            for (int i = 3; i < argc; ++i) {
                const std::size_t bit = static_cast<std::size_t>(std::stoul(argv[i]));
                e.flags().set(bit);
            }

            update_index_for_put(r, name, oldf, e.flags());

            if (!r.hset(key, "name", e.name())) {
                std::cerr << "HSET name failed\n";
                return 3;
            }

            const auto bytes = e.flags().to_bytes_be();
            if (!r.hset_bin(key, "flags_bin", bytes.data(), bytes.size())) {
                std::cerr << "HSET flags_bin failed\n";
                return 3;
            }

            std::cout << "OK: stored " << key << " and updated index\n";
            return 0;
        }

        // ---- GET ----
        if (op == "get") {
            if (argc < 3) { usage(); return 1; }

            const std::string name = argv[2];
            const std::string key  = key_for(name);

            er::Flags4096 f;
            if (!load_existing_flags(r, key, f)) {
                std::cerr << "Missing element (no flags_bin/flags_hex)\n";
                return 4;
            }

            std::cout << "Key: " << key << "\n";
            std::cout << "bit42: " << f.test(42) << "\n";
            std::cout << "bit4095: " << f.test(4095) << "\n";
            return 0;
        }

        // ---- FIND single ----
        if (op == "find") {
            if (argc < 3) { usage(); return 1; }
            const std::size_t bit = static_cast<std::size_t>(std::stoul(argv[2]));
            if (bit >= 4096) { std::cerr << "bit out of range (0..4095)\n"; return 1; }

            std::vector<std::string> members;
            const std::string idx = idx_key_for_bit(bit);
            if (!r.smembers(idx, members)) { std::cerr << "SMEMBERS failed\n"; return 6; }
            print_members("Index: " + idx, members);
            return 0;
        }

        // ---- FIND_ALL (no store) ----
        if (op == "find_all") {
            if (argc < 4) { usage(); return 1; }
            auto idx_keys = build_idx_keys_from_bits(argc, argv, 2);

            std::vector<std::string> members;
            if (!r.sinter(idx_keys, members)) { std::cerr << "SINTER failed\n"; return 7; }
            print_members("Query AND (SINTER)", members);
            return 0;
        }

        // ---- FIND_ANY (no store) ----
        if (op == "find_any") {
            if (argc < 4) { usage(); return 1; }
            auto idx_keys = build_idx_keys_from_bits(argc, argv, 2);

            std::vector<std::string> members;
            if (!r.sunion(idx_keys, members)) { std::cerr << "SUNION failed\n"; return 8; }
            print_members("Query OR (SUNION)", members);
            return 0;
        }

        // ---- FIND_NOT (no store) ----
        if (op == "find_not") {
            if (argc < 4) { usage(); return 1; }
            const std::size_t include_bit = static_cast<std::size_t>(std::stoul(argv[2]));
            if (include_bit >= 4096) { std::cerr << "include bit out of range\n"; return 1; }

            std::vector<std::string> idx_keys;
            idx_keys.push_back(idx_key_for_bit(include_bit));
            for (int i = 3; i < argc; ++i) {
                const std::size_t b = static_cast<std::size_t>(std::stoul(argv[i]));
                if (b >= 4096) { std::cerr << "exclude bit out of range\n"; return 1; }
                idx_keys.push_back(idx_key_for_bit(b));
            }

            std::vector<std::string> members;
            if (!r.sdiff(idx_keys, members)) { std::cerr << "SDIFF failed\n"; return 9; }
            print_members("Query NOT (SDIFF)", members);
            return 0;
        }

        // ---- STORE variants ----
        if (op == "find_all_store" || op == "find_any_store" || op == "find_not_store") {
            if (argc < 5) { usage(); return 1; }

            const int ttl = std::stoi(argv[2]);
            if (ttl <= 0) { std::cerr << "ttl_sec must be > 0\n"; return 1; }

            std::string tmp_key;
            bool ok_store = false;

            if (op == "find_all_store") {
                // args: ttl bit1 bit2 ...
                auto idx_keys = build_idx_keys_from_bits(argc, argv, 3);
                if (idx_keys.size() < 2) { usage(); return 1; }
                tmp_key = make_tmp_key("and", ttl, idx_keys);
                ok_store = r.sinterstore(tmp_key, idx_keys) && r.expire_seconds(tmp_key, ttl);
            } else if (op == "find_any_store") {
                auto idx_keys = build_idx_keys_from_bits(argc, argv, 3);
                if (idx_keys.size() < 2) { usage(); return 1; }
                tmp_key = make_tmp_key("or", ttl, idx_keys);
                ok_store = r.sunionstore(tmp_key, idx_keys) && r.expire_seconds(tmp_key, ttl);
            } else { // find_not_store
                // args: ttl include exclude1 ...
                const std::size_t include_bit = static_cast<std::size_t>(std::stoul(argv[3]));
                if (include_bit >= 4096) { std::cerr << "include bit out of range\n"; return 1; }

                std::vector<std::string> idx_keys;
                idx_keys.push_back(idx_key_for_bit(include_bit));
                for (int i = 4; i < argc; ++i) {
                    const std::size_t b = static_cast<std::size_t>(std::stoul(argv[i]));
                    if (b >= 4096) { std::cerr << "exclude bit out of range\n"; return 1; }
                    idx_keys.push_back(idx_key_for_bit(b));
                }
                if (idx_keys.size() < 2) { usage(); return 1; }
                tmp_key = make_tmp_key("not", ttl, idx_keys);
                ok_store = r.sdiffstore(tmp_key, idx_keys) && r.expire_seconds(tmp_key, ttl);
            }

            if (!ok_store) {
                std::cerr << "STORE+EXPIRE failed\n";
                return 11;
            }

            // pokaži ključ + rezultate (možeš kasnije prebaciti da samo printa ključ)
            std::vector<std::string> members;
            if (!r.smembers(tmp_key, members)) {
                std::cerr << "SMEMBERS tmp_key failed\n";
                return 12;
            }

            std::cout << "TMP_KEY: " << tmp_key << " (ttl=" << ttl << "s)\n";
            print_members("Result:", members);
            return 0;
        }

        // ---- SHOW tmp set ----
        if (op == "show") {
            if (argc < 3) { usage(); return 1; }
            const std::string k = argv[2];
            std::vector<std::string> members;
            if (!r.smembers(k, members)) { std::cerr << "SMEMBERS failed\n"; return 13; }
            print_members("SHOW: " + k, members);
            return 0;
        }

        usage();
        return 1;

    } catch (const std::exception& ex) {
        std::cerr << "ERROR: " << ex.what() << "\n";
        return 10;
    }
}

