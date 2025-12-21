#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>
#include <cstdint>
#include <unordered_set>

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
      "  er_cli find_not <include_bit> <exclude_bit1> [exclude_bit2 ...]\n";
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

    // fallback: hex (ako ima starih upisa)
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
    // setovi bitova
    const auto old_bits = oldf.set_bits();
    const auto new_bits = newf.set_bits();

    std::unordered_set<std::size_t> old_set(old_bits.begin(), old_bits.end());
    std::unordered_set<std::size_t> new_set(new_bits.begin(), new_bits.end());

    // remove: in old but not in new
    for (auto b : old_set) {
        if (new_set.find(b) == new_set.end()) {
            r.srem(idx_key_for_bit(b), name);
        }
    }

    // add: in new but not in old
    for (auto b : new_set) {
        if (old_set.find(b) == old_set.end()) {
            r.sadd(idx_key_for_bit(b), name);
        }
    }
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

        if (op == "put") {
            if (argc < 4) { usage(); return 1; }

            const std::string name = argv[2];
            const std::string key  = key_for(name);

            // load old flags (ako postoji)
            er::Flags4096 oldf;
            load_existing_flags(r, key, oldf);

            // build new element
            er::Element e(name);
            for (int i = 3; i < argc; ++i) {
                const std::size_t bit = static_cast<std::size_t>(std::stoul(argv[i]));
                e.flags().set(bit);
            }

            // 1) update index (diff)
            update_index_for_put(r, name, oldf, e.flags());

            // 2) store element hash
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

        if (op == "find") {
            if (argc < 3) { usage(); return 1; }
            const std::size_t bit = static_cast<std::size_t>(std::stoul(argv[2]));
            if (bit >= 4096) {
                std::cerr << "bit out of range (0..4095)\n";
                return 1;
            }

            std::vector<std::string> members;
            const std::string idx = idx_key_for_bit(bit);
            if (!r.smembers(idx, members)) {
                std::cerr << "SMEMBERS failed\n";
                return 6;
            }

            std::cout << "Index: " << idx << "\n";
            std::cout << "Count: " << members.size() << "\n";
            for (const auto& m : members) {
                std::cout << " - " << m << "\n";
            }
            return 0;
        }
        
        if (op == "find_all") {
            if (argc < 4) { // treba bar 2 bita
                usage();
                return 1;
            }

            std::vector<std::string> idx_keys;
            idx_keys.reserve(static_cast<std::size_t>(argc - 2));

            for (int i = 2; i < argc; ++i) {
                const std::size_t bit = static_cast<std::size_t>(std::stoul(argv[i]));
                if (bit >= 4096) {
                    std::cerr << "bit out of range (0..4095): " << bit << "\n";
                    return 1;
                }
                idx_keys.push_back(idx_key_for_bit(bit));
            }

            std::vector<std::string> members;
            if (!r.sinter(idx_keys, members)) {
                std::cerr << "SINTER failed\n";
                return 7;
            }

            std::cout << "Query: AND of bits ";
            for (int i = 2; i < argc; ++i) {
                std::cout << argv[i] << (i + 1 < argc ? "," : "");
            }
            std::cout << "\nCount: " << members.size() << "\n";
            for (const auto& m : members) {
                std::cout << " - " << m << "\n";
            }
            return 0;
        }
        
        if (op == "find_any") {
            if (argc < 4) { // bar 2 bita
                usage();
                return 1;
            }

            std::vector<std::string> idx_keys;
            idx_keys.reserve(static_cast<std::size_t>(argc - 2));

            for (int i = 2; i < argc; ++i) {
                const std::size_t bit = static_cast<std::size_t>(std::stoul(argv[i]));
                if (bit >= 4096) {
                    std::cerr << "bit out of range (0..4095): " << bit << "\n";
                    return 1;
                }
                idx_keys.push_back(idx_key_for_bit(bit));
            }

            std::vector<std::string> members;
            if (!r.sunion(idx_keys, members)) {
                std::cerr << "SUNION failed\n";
                return 8;
            }

            std::cout << "Query: OR of bits ";
            for (int i = 2; i < argc; ++i) {
                std::cout << argv[i] << (i + 1 < argc ? "," : "");
            }
            std::cout << "\nCount: " << members.size() << "\n";
            for (const auto& m : members) {
                std::cout << " - " << m << "\n";
            }
            return 0;
        }

        if (op == "find_not") {
            if (argc < 4) { // include + bar 1 exclude
                usage();
                return 1;
            }

            const std::size_t include_bit = static_cast<std::size_t>(std::stoul(argv[2]));
            if (include_bit >= 4096) {
                std::cerr << "include bit out of range (0..4095): " << include_bit << "\n";
                return 1;
            }

            std::vector<std::string> idx_keys;
            idx_keys.reserve(static_cast<std::size_t>(argc - 2));

            // key1 = include
            idx_keys.push_back(idx_key_for_bit(include_bit));

            // key2.. = excludes
            for (int i = 3; i < argc; ++i) {
                const std::size_t bit = static_cast<std::size_t>(std::stoul(argv[i]));
                if (bit >= 4096) {
                    std::cerr << "exclude bit out of range (0..4095): " << bit << "\n";
                    return 1;
                }
                idx_keys.push_back(idx_key_for_bit(bit));
            }

            std::vector<std::string> members;
            if (!r.sdiff(idx_keys, members)) {
                std::cerr << "SDIFF failed\n";
                return 9;
            }

            std::cout << "Query: include bit " << include_bit << " NOT bits ";
            for (int i = 3; i < argc; ++i) {
                std::cout << argv[i] << (i + 1 < argc ? "," : "");
            }
            std::cout << "\nCount: " << members.size() << "\n";
            for (const auto& m : members) {
                std::cout << " - " << m << "\n";
            }
            return 0;
        }


        usage();
        return 1;

    } catch (const std::exception& ex) {
        std::cerr << "ERROR: " << ex.what() << "\n";
        return 10;
    }
}

