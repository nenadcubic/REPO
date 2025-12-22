#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>
#include <cstdint>
#include <unordered_set>
#include <sstream>
#include <string_view>

#include "er/Element.hpp"
#include "er/RedisClient.hpp"
#include "er/Flags4096.hpp"
#include "er/keys.hpp"

static void usage() {
    std::cout <<
      "Usage:\n"
      "  er_cli [--keys-only] <command> ...\n"
      "\n"
      "Options:\n"
      "  --keys-only          For *_store commands, print only the tmp key\n"
      "  (or set ER_KEYS_ONLY=1)\n"
      "  (Redis: ER_REDIS_HOST, ER_REDIS_PORT)\n"
      "\n"
      "Commands:\n"
      "  er_cli put <name> <bit> [bit2 bit3 ...]\n"
      "  er_cli get <name>\n"
      "  er_cli del <name> [--force]\n"
      "  er_cli find <bit>\n"
      "  er_cli find_all <bit1> <bit2> [bit3 ...]\n"
      "  er_cli find_any <bit1> <bit2> [bit3 ...]\n"
      "  er_cli find_not <include_bit> <exclude_bit1> [exclude_bit2 ...]\n"
      "  er_cli find_universe_not <exclude_bit1> [exclude_bit2 ...]\n"
      "  er_cli find_all_not <include_bit> <exclude_bit1> [exclude_bit2 ...]\n"
      "\n"
      "Store+TTL:\n"
      "  er_cli find_all_store <ttl_sec> <bit1> <bit2> [bit3 ...]\n"
      "  er_cli find_any_store <ttl_sec> <bit1> <bit2> [bit3 ...]\n"
      "  er_cli find_not_store <ttl_sec> <include_bit> <exclude_bit1> [exclude_bit2 ...]\n"
      "  er_cli show <redis_set_key>\n"
      "  er_cli find_universe_not_store <ttl_sec> <exclude_bit1> [exclude_bit2 ...]\n"
      "  er_cli find_all_not_store <ttl_sec> <include_bit> <exclude_bit1> [exclude_bit2 ...]\n";
}

static std::string key_for(const std::string& name) {
    return er::keys::element(name);
}

static std::string idx_key_for_bit(std::size_t bit) {
    return er::keys::idx_bit(bit);
}

static bool load_existing_flags(er::RedisClient& r, const std::string& key, er::Flags4096& out_flags) {
    auto blob = r.hget_bin(key, "flags_bin");
    if (blob && blob.value().size() == 512) {
        auto f = er::Flags4096::from_bytes_be(
            reinterpret_cast<const std::uint8_t*>(blob.value().data()),
            blob.value().size()
        );
        if (f) {
            out_flags = std::move(f).value();
            return true;
        }
    }

    auto hex = r.hget(key, "flags_hex");
    if (hex && !hex.value().empty()) {
        auto f = er::Flags4096::from_hex(hex.value());
        if (f) {
            out_flags = std::move(f).value();
            return true;
        }
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
            (void)r.srem(idx_key_for_bit(b), name);
        }
    }
    for (auto b : new_set) {
        if (old_set.find(b) == old_set.end()) {
            (void)r.sadd(idx_key_for_bit(b), name);
        }
    }
}

static std::size_t parse_bit_arg(const char* s) {
    const std::size_t bit = static_cast<std::size_t>(std::stoul(s));
    if (bit >= 4096) throw std::runtime_error("bit out of range (0..4095): " + std::to_string(bit));
    return bit;
}

static int parse_ttl_arg(const char* s) {
    const int ttl = std::stoi(s);
    if (ttl <= 0) throw std::runtime_error("ttl_sec must be > 0");
    return ttl;
}

static std::vector<std::string> build_idx_keys_from_bits(int argc, char** argv, int start_i) {
    std::vector<std::string> idx_keys;
    idx_keys.reserve(static_cast<std::size_t>(argc - start_i));
    for (int i = start_i; i < argc; ++i) {
        idx_keys.push_back(idx_key_for_bit(parse_bit_arg(argv[i])));
    }
    return idx_keys;
}

static std::string make_tmp_key(const std::string& tag, int ttl) {
    // unique tmp key per call (no collisions between concurrent runs)
    return er::keys::tmp(tag + ":ttl" + std::to_string(ttl));
}

static void print_members(const std::string& label, const std::vector<std::string>& members) {
    std::cout << label << "\n";
    std::cout << "Count: " << members.size() << "\n";
    for (const auto& m : members) std::cout << " - " << m << "\n";
}

static std::string env_string(const char* name, const std::string& def) {
    const char* v = std::getenv(name);
    if (!v || !*v) return def;
    return std::string(v);
}

static int env_int(const char* name, int def) {
    const char* v = std::getenv(name);
    if (!v || !*v) return def;
    return std::stoi(v);
}

static bool env_truthy(const char* name) {
    const char* v = std::getenv(name);
    if (!v || !*v) return false;
    const std::string s(v);
    return s == "1" || s == "true" || s == "TRUE" || s == "yes" || s == "YES";
}

struct Invocation {
    std::string host;
    int port = 6379;
    bool keys_only = false;
    bool help = false;
    int cmd_index = 1;
};

static Invocation parse_invocation(int argc, char** argv) {
    Invocation inv;
    inv.keys_only = env_truthy("ER_KEYS_ONLY");
    inv.host = env_string("ER_REDIS_HOST", "redis");
    inv.port = env_int("ER_REDIS_PORT", 6379);

    int i = 1;
    for (; i < argc; ++i) {
        const std::string_view arg(argv[i]);
        if (arg == "--keys-only" || arg == "--key-only") {
            inv.keys_only = true;
            continue;
        }
        if (arg == "--help" || arg == "-h") {
            usage();
            inv.help = true;
            inv.cmd_index = argc;
            return inv;
        }
        if (arg.starts_with("--")) {
            throw std::runtime_error("unknown option: " + std::string(arg));
        }
        break;
    }
    inv.cmd_index = i;
    return inv;
}

int main(int argc, char** argv) {
    if (argc < 2) { usage(); return 1; }

    Invocation inv;
    try {
        inv = parse_invocation(argc, argv);
    } catch (const std::exception& ex) {
        std::cerr << "ERROR: " << ex.what() << "\n";
        usage();
        return 1;
    }

    if (inv.help) return 0;
    if (inv.cmd_index >= argc) { usage(); return 1; }

    try {
        const std::string op = argv[inv.cmd_index];
        const int cmd_argc = argc - inv.cmd_index;
        char** cmd_argv = argv + inv.cmd_index;

        auto rc = er::RedisClient::connect(inv.host, inv.port);
        if (!rc) {
            std::cerr << "Redis connect failed: " << rc.error().msg << "\n";
            return 2;
        }
        er::RedisClient r = std::move(rc).value();
        if (auto ok = r.ping(); !ok) {
            std::cerr << "Redis PING failed: " << ok.error().msg << "\n";
            return 2;
        }

        // ---- PUT ----
        if (op == "put") {
            if (cmd_argc < 3) { usage(); return 1; }

            const std::string name = cmd_argv[1];
            const std::string key  = key_for(name);

            er::Flags4096 oldf;
            load_existing_flags(r, key, oldf);

            auto e_res = er::Element::create(name);
            if (!e_res) {
                std::cerr << "ERROR: " << e_res.error().msg << "\n";
                return 1;
            }
            er::Element e = std::move(e_res).value();
            for (int i = 2; i < cmd_argc; ++i) {
                auto ok = e.flags().set(parse_bit_arg(cmd_argv[i]));
                if (!ok) {
                    std::cerr << "ERROR: " << ok.error().msg << "\n";
                    return 1;
                }
            }

            update_index_for_put(r, name, oldf, e.flags());

            if (auto ok = r.hset(key, "name", std::string(e.name())); !ok) {
                std::cerr << "HSET name failed: " << ok.error().msg << "\n";
                return 3;
            }

            const auto bytes = e.flags().to_bytes_be();
            if (auto ok = r.hset_bin(key, "flags_bin", bytes.data(), bytes.size()); !ok) {
                std::cerr << "HSET flags_bin failed: " << ok.error().msg << "\n";
                return 3;
            }
            
            // maintain universe set for NOT queries
            if (auto ok = r.sadd(er::keys::universe(), name); !ok) {
                std::cerr << "SADD er:all failed: " << ok.error().msg << "\n";
                return 3;
            }

            std::cout << "OK: stored " << key << " and updated index\n";
            return 0;
        }

        // ---- GET ----
        if (op == "get") {
            if (cmd_argc < 2) { usage(); return 1; }

            const std::string name = cmd_argv[1];
            const std::string key  = key_for(name);

            er::Flags4096 f;
            if (!load_existing_flags(r, key, f)) {
                std::cerr << "Missing element (no flags_bin/flags_hex)\n";
                return 4;
            }

            std::cout << "Key: " << key << "\n";
            auto t42 = f.test(42);
            auto t4095 = f.test(4095);
            if (!t42 || !t4095) {
                std::cerr << "ERROR: invalid bit test\n";
                return 4;
            }
            std::cout << "bit42: " << t42.value() << "\n";
            std::cout << "bit4095: " << t4095.value() << "\n";
            return 0;
        }

        // ---- DEL ----
        if (op == "del") {
            if (cmd_argc < 2) { usage(); return 1; }
            const std::string name = cmd_argv[1];
            const std::string key  = key_for(name);

            const bool force = (cmd_argc >= 3 && std::string(cmd_argv[2]) == "--force");

            er::Flags4096 f;
            const bool have_flags = load_existing_flags(r, key, f);

            if (have_flags) {
                for (auto b : f.set_bits()) {
                    (void)r.srem(idx_key_for_bit(b), name);
                }
            } else if (force) {
                for (std::size_t b = 0; b < 4096; ++b) {
                    (void)r.srem(idx_key_for_bit(b), name);
                }
            }

            (void)r.srem(er::keys::universe(), name);
            (void)r.del_key(key);

            if (!have_flags && !force) {
                std::cerr << "WARN: element missing; pass --force to scrub all 4096 indexes\n";
            }
            std::cout << "OK: deleted " << name << "\n";
            return 0;
        }

        // ---- FIND single ----
        if (op == "find") {
            if (cmd_argc < 2) { usage(); return 1; }
            const std::size_t bit = parse_bit_arg(cmd_argv[1]);

            const std::string idx = idx_key_for_bit(bit);
            auto members = r.smembers(idx);
            if (!members) { std::cerr << "SMEMBERS failed: " << members.error().msg << "\n"; return 6; }
            print_members("Index: " + idx, members.value());
            return 0;
        }

        // ---- FIND_ALL (no store) ----
        if (op == "find_all") {
            if (cmd_argc < 3) { usage(); return 1; }
            auto idx_keys = build_idx_keys_from_bits(cmd_argc, cmd_argv, 1);

            auto members = r.sinter(idx_keys);
            if (!members) { std::cerr << "SINTER failed: " << members.error().msg << "\n"; return 7; }
            print_members("Query AND (SINTER)", members.value());
            return 0;
        }

        // ---- FIND_ANY (no store) ----
        if (op == "find_any") {
            if (cmd_argc < 3) { usage(); return 1; }
            auto idx_keys = build_idx_keys_from_bits(cmd_argc, cmd_argv, 1);

            auto members = r.sunion(idx_keys);
            if (!members) { std::cerr << "SUNION failed: " << members.error().msg << "\n"; return 8; }
            print_members("Query OR (SUNION)", members.value());
            return 0;
        }

        // ---- FIND_NOT (no store) ----
        if (op == "find_not") {
            if (cmd_argc < 3) { usage(); return 1; }
            const std::size_t include_bit = parse_bit_arg(cmd_argv[1]);

            std::vector<std::string> idx_keys;
            idx_keys.push_back(idx_key_for_bit(include_bit));
            for (int i = 2; i < cmd_argc; ++i) {
                idx_keys.push_back(idx_key_for_bit(parse_bit_arg(cmd_argv[i])));
            }

            auto members = r.sdiff(idx_keys);
            if (!members) { std::cerr << "SDIFF failed: " << members.error().msg << "\n"; return 9; }
            print_members("Query NOT (SDIFF)", members.value());
            return 0;
        }

        // ---- FIND_UNIVERSE_NOT (no store) ----
        if (op == "find_universe_not") {
            if (cmd_argc < 2) { usage(); return 1; }

            std::vector<std::string> keys;
            keys.push_back(er::keys::universe());
            for (int i = 1; i < cmd_argc; ++i) {
                keys.push_back(idx_key_for_bit(parse_bit_arg(cmd_argv[i])));
            }

            auto members = r.sdiff(keys);
            if (!members) { std::cerr << "SDIFF failed: " << members.error().msg << "\n"; return 9; }
            print_members("Query UNIVERSE NOT (er:all \\ excludes)", members.value());
            return 0;
        }

        // ---- FIND_ALL_NOT (no store) ----
        if (op == "find_all_not") {
            if (cmd_argc < 3) { usage(); return 1; }

            const std::size_t include_bit = parse_bit_arg(cmd_argv[1]);

            // tmp = er:all \ excludes
            std::vector<std::string> diff_keys;
            diff_keys.push_back(er::keys::universe());
            for (int i = 2; i < cmd_argc; ++i) {
                diff_keys.push_back(idx_key_for_bit(parse_bit_arg(cmd_argv[i])));
            }

            auto universe_minus = r.sdiff(diff_keys);
            if (!universe_minus) { std::cerr << "SDIFF failed: " << universe_minus.error().msg << "\n"; return 9; }

            // intersect with include
            // NOTE: hiredis/Redis nema SINTER između "virtual list" i seta bez store,
            // pa radimo: members(include_set) ∩ (universe_minus) lokalno.
            auto include_members = r.smembers(idx_key_for_bit(include_bit));
            if (!include_members) { std::cerr << "SMEMBERS failed: " << include_members.error().msg << "\n"; return 6; }

            std::unordered_set<std::string> allow(universe_minus.value().begin(), universe_minus.value().end());
            std::vector<std::string> out;
            out.reserve(include_members.value().size());
            for (auto& m : include_members.value()) {
                if (allow.find(m) != allow.end()) out.push_back(m);
            }

            print_members("Query ALL NOT (include ∩ (er:all \\ excludes))", out);
            return 0;
        }

        // ---- STORE variants ----
        if (op == "find_all_store" || op == "find_any_store" || op == "find_not_store"
         || op == "find_universe_not_store" || op == "find_all_not_store") {
            // Minimalni broj argumenata zavisi od komande:
            // - find_universe_not_store: ttl + 1 exclude => argc >= 4
            // - sve ostale store komande: trebaju bar 2 bita (ili include+exclude) => argc >= 5
            const bool is_universe_not_store = (op == "find_universe_not_store");
            if ((is_universe_not_store && cmd_argc < 3) || (!is_universe_not_store && cmd_argc < 4)) {
                usage();
                return 1;
            }


            const int ttl = parse_ttl_arg(cmd_argv[1]);

            std::string tmp_key;

            if (op == "find_all_store") {
                // args: ttl bit1 bit2 ...
                auto idx_keys = build_idx_keys_from_bits(cmd_argc, cmd_argv, 2);
                if (idx_keys.size() < 2) { usage(); return 1; }
                tmp_key = make_tmp_key("and", ttl);
                auto ok_store = r.store_all_expire_lua(ttl, idx_keys, tmp_key);
                if (!ok_store) { std::cerr << "STORE+EXPIRE failed: " << ok_store.error().msg << "\n"; return 11; }
            } else if (op == "find_any_store") {
                auto idx_keys = build_idx_keys_from_bits(cmd_argc, cmd_argv, 2);
                if (idx_keys.size() < 2) { usage(); return 1; }
                tmp_key = make_tmp_key("or", ttl);
                auto ok_store = r.store_any_expire_lua(ttl, idx_keys, tmp_key);
                if (!ok_store) { std::cerr << "STORE+EXPIRE failed: " << ok_store.error().msg << "\n"; return 11; }
            } else if (op == "find_universe_not_store") {
                // args: ttl exclude1 exclude2 ...
                std::vector<std::string> set_keys;
                set_keys.reserve(static_cast<std::size_t>(cmd_argc - 2));
                for (int i = 2; i < cmd_argc; ++i) {
                    set_keys.push_back(idx_key_for_bit(parse_bit_arg(cmd_argv[i])));
                }
                tmp_key = make_tmp_key("unot", ttl);
                // universe \ excludes
                auto ok_store = r.store_not_expire_lua(ttl, er::keys::universe(), set_keys, tmp_key);
                if (!ok_store) { std::cerr << "STORE+EXPIRE failed: " << ok_store.error().msg << "\n"; return 11; }

            } else if (op == "find_all_not_store") {
                // args: ttl include exclude1 exclude2 ...
                const std::size_t include_bit = parse_bit_arg(cmd_argv[2]);

                std::vector<std::string> excludes;
                excludes.reserve(static_cast<std::size_t>(cmd_argc - 3));
                for (int i = 3; i < cmd_argc; ++i) {
                    excludes.push_back(idx_key_for_bit(parse_bit_arg(cmd_argv[i])));
                }

                tmp_key = make_tmp_key("andnot", ttl);
                auto ok_store = r.store_all_not_expire_lua(
                    ttl,
                    idx_key_for_bit(include_bit),
                    er::keys::universe(),
                    excludes,
                    tmp_key
                );
                if (!ok_store) { std::cerr << "STORE+EXPIRE failed: " << ok_store.error().msg << "\n"; return 11; }
            } else { // find_not_store
                // args: ttl include exclude1 ...
                const std::size_t include_bit = parse_bit_arg(cmd_argv[2]);

                std::vector<std::string> excludes;
                excludes.reserve(static_cast<std::size_t>(cmd_argc - 3));
                for (int i = 3; i < cmd_argc; ++i) {
                    excludes.push_back(idx_key_for_bit(parse_bit_arg(cmd_argv[i])));
                }
                if (excludes.empty()) { usage(); return 1; }
                tmp_key = make_tmp_key("not", ttl);
                auto ok_store = r.store_not_expire_lua(ttl, idx_key_for_bit(include_bit), excludes, tmp_key);
                if (!ok_store) { std::cerr << "STORE+EXPIRE failed: " << ok_store.error().msg << "\n"; return 11; }
            }

            if (inv.keys_only) {
                std::cout << tmp_key << "\n";
                return 0;
            }

            // pokaži ključ + rezultate (možeš kasnije prebaciti da samo printa ključ)
            auto members = r.smembers(tmp_key);
            if (!members) { std::cerr << "SMEMBERS tmp_key failed: " << members.error().msg << "\n"; return 12; }

            std::cout << "TMP_KEY: " << tmp_key << " (ttl=" << ttl << "s)\n";
            print_members("Result:", members.value());
            return 0;
        }

        // ---- SHOW tmp set ----
        if (op == "show") {
            if (cmd_argc < 2) { usage(); return 1; }
            const std::string k = cmd_argv[1];
            auto members = r.smembers(k);
            if (!members) { std::cerr << "SMEMBERS failed: " << members.error().msg << "\n"; return 13; }
            print_members("SHOW: " + k, members.value());
            return 0;
        }

        usage();
        return 1;

    } catch (const std::exception& ex) {
        std::cerr << "ERROR: " << ex.what() << "\n";
        return 10;
    }
}
