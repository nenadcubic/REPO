from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from typing import Any, Iterable, Literal

import redis

from .errors import ApiError

# WordNet bit-profile v1 (4096-bit integer stored as decimal string in Redis)
#
# 0–31   : POS / identification bits
# 32–63  : frequency/root indicators (reserved)
# 128–159: relation-type bits
# 256–319: macro semantic domains
#
# For now we only implement a small subset required by the Associations game.

BIT_POS_NOUN = 0
BIT_POS_VERB = 1
BIT_POS_ADJ = 2
BIT_POS_ADV = 3
BIT_MULTIWORD = 4

BIT_REL_SYNONYM = 128
BIT_REL_HYPERNYM = 129
BIT_REL_HYPONYM = 130
BIT_REL_MERONYM = 131
BIT_REL_HOLONYM = 132
BIT_REL_ANTONYM = 133
BIT_REL_ENTAILMENT = 134
BIT_REL_SIMILAR_TO = 135

DOMAIN_BITS: dict[str, int] = {
    "FOOD": 256,
    "PERSON": 257,
    "PLACE": 258,
    "LIVING": 259,
    "OBJECT": 260,
    "EMOTION": 261,
    "TIME_EVENT": 262,
    "ABSTRACT": 263,
    "BODY": 264,
    "NATURAL": 265,
}

RELATION_BITS: dict[str, int] = {
    "synonym": BIT_REL_SYNONYM,
    "hypernym": BIT_REL_HYPERNYM,
    "hyponym": BIT_REL_HYPONYM,
    "meronym": BIT_REL_MERONYM,
    "holonym": BIT_REL_HOLONYM,
    "antonym": BIT_REL_ANTONYM,
    "entailment": BIT_REL_ENTAILMENT,
    "similar_to": BIT_REL_SIMILAR_TO,
}


def wn_dict_key(synset: str) -> str:
    return f"wn:dict:{synset}"


def wn_meta_key(synset: str) -> str:
    return f"wn:meta:{synset}"


def wn_rels_key(synset: str) -> str:
    return f"wn:rels:{synset}"


def wn_lemma_key(lemma: str) -> str:
    return f"wn:lemma:{lemma}"


def assoc_board_key(board_id: str) -> str:
    return f"assoc:board:{board_id}"


def assoc_explain_key(board_id: str) -> str:
    return f"assoc:explain:{board_id}"


def _b(s: str) -> bytes:
    return s.encode("utf-8")


def _jload(raw: bytes | str | None) -> Any:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(raw)
    except Exception:
        return None


def _jdump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _norm_guess(s: str) -> str:
    out = (s or "").strip().lower()
    out = out.replace("_", " ")
    out = " ".join(out.split())
    return out


def _int_from_bits(bits: Iterable[int]) -> int:
    x = 0
    for b in bits:
        if not isinstance(b, int) or b < 0 or b > 4095:
            raise ApiError("INVALID_BIT", "bit must be 0..4095", status_code=422, details={"bit": b})
        x |= 1 << b
    return x


def _bits_from_int(x: int) -> set[int]:
    # Only used on very small data sets (boards); keep simple.
    bits: set[int] = set()
    if x <= 0:
        return bits
    for b in range(4096):
        if (x >> b) & 1:
            bits.add(b)
    return bits


def _domain_labels_from_bits(bits: set[int]) -> list[str]:
    out = [name for name, bit in DOMAIN_BITS.items() if bit in bits]
    out.sort()
    return out


def _relation_labels_from_bits(bits: set[int]) -> list[str]:
    out = [name for name, bit in RELATION_BITS.items() if bit in bits]
    out.sort()
    return out


@dataclass(frozen=True)
class WnMeta:
    synset: str
    lemma: str
    lemmas: list[str]
    lexname: str | None
    domains: list[str]
    primary_domain: str | None
    pos: str | None


def load_meta(*, r: redis.Redis, synset: str) -> WnMeta | None:
    raw = r.get(wn_meta_key(synset))
    doc = _jload(raw)
    if not isinstance(doc, dict):
        return None
    lemma = str(doc.get("lemma") or "").strip()
    if not lemma:
        return None
    return WnMeta(
        synset=str(doc.get("synset") or synset),
        lemma=lemma,
        lemmas=[str(x) for x in (doc.get("lemmas") if isinstance(doc.get("lemmas"), list) else []) if str(x).strip()],
        lexname=str(doc.get("lexname") or "") or None,
        domains=[str(x) for x in (doc.get("domains") if isinstance(doc.get("domains"), list) else []) if str(x).strip()],
        primary_domain=str(doc.get("primary_domain") or "") or None,
        pos=str(doc.get("pos") or "") or None,
    )


def load_bits_int(*, r: redis.Redis, synset: str) -> int | None:
    raw = r.get(wn_dict_key(synset))
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    s = str(raw).strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def load_rels(*, r: redis.Redis, synset: str) -> dict[str, list[str]]:
    raw = r.get(wn_rels_key(synset))
    doc = _jload(raw)
    if not isinstance(doc, dict):
        return {}
    out: dict[str, list[str]] = {}
    for k, v in doc.items():
        if not isinstance(k, str):
            continue
        if not isinstance(v, list):
            continue
        out[k] = [str(x) for x in v if isinstance(x, str) and x.strip()]
    return out


def _neighbors(rels: dict[str, list[str]]) -> list[str]:
    out: list[str] = []
    for k, lst in rels.items():
        if k in ("hypernyms", "hyponyms", "meronyms", "holonyms", "antonyms", "entailments", "similar_tos", "synonyms"):
            out.extend(lst)
    # de-dupe while preserving order
    seen: set[str] = set()
    uniq: list[str] = []
    for s in out:
        if s in seen:
            continue
        seen.add(s)
        uniq.append(s)
    return uniq


def _pick_primary_domain(meta: WnMeta | None) -> str | None:
    if not meta:
        return None
    if meta.primary_domain:
        return meta.primary_domain
    if meta.domains:
        return meta.domains[0]
    return None


def _popcount_and(a: int, b: int) -> int:
    return int((a & b).bit_count())


def _ensure_wordnet_ready(*, r: redis.Redis) -> None:
    if not r.exists("wn:all"):
        raise ApiError(
            "WORDNET_NOT_INGESTED",
            "WordNet is not ingested into Redis. Run the WordNet ingest tool (or use demo mode).",
            status_code=422,
        )


def _board_cell_map(board: dict[str, Any]) -> dict[str, dict[str, Any]]:
    mp: dict[str, dict[str, Any]] = {}
    cols = board.get("columns") if isinstance(board.get("columns"), list) else []
    for col in cols:
        if not isinstance(col, dict):
            continue
        cid = str(col.get("id") or "").strip().upper()
        if cid not in ("A", "B", "C", "D"):
            continue
        mp[cid] = col
        clues = col.get("clues") if isinstance(col.get("clues"), list) else []
        for i, clue in enumerate(clues):
            if not isinstance(clue, dict):
                continue
            mp[f"{cid}{i+1}"] = clue
    fin = board.get("final") if isinstance(board.get("final"), dict) else None
    if fin:
        mp["final"] = fin
    return mp


def _find_path(
    *,
    r: redis.Redis,
    src: str,
    dst: str,
    max_depth: int = 2,
) -> tuple[str, list[str]]:
    if src == dst:
        return "same", []
    # BFS over directed adjacency, tracking relation types.
    # Return first found path (small graphs only).
    q: list[tuple[str, list[str], list[str]]] = [(src, [], [])]  # node, via, rel_types
    seen: set[str] = {src}
    while q:
        node, via, rels_used = q.pop(0)
        if len(rels_used) >= max_depth:
            continue
        rels = load_rels(r=r, synset=node)
        for rel_type, targets in rels.items():
            for t in targets:
                if not t or t in seen:
                    continue
                if t == dst:
                    return rel_type, via + ([] if node == src else [node])
                seen.add(t)
                q.append((t, via + ([] if node == src else [node]), rels_used + [rel_type]))
    return "related", []


def build_explanation(*, r: redis.Redis, board: dict[str, Any]) -> dict[str, Any]:
    final = board.get("final") if isinstance(board.get("final"), dict) else {}
    fin_syn = str(final.get("synset") or "").strip()
    fin_lemma = str(final.get("lemma") or "").strip()
    if not fin_syn:
        raise ApiError("INVALID_BOARD", "board.final.synset missing", status_code=500)

    fin_bits = load_bits_int(r=r, synset=fin_syn) or 0
    fin_set = _bits_from_int(fin_bits)
    fin_rel_bits = set(_relation_labels_from_bits(fin_set))
    fin_dom_bits = set(_domain_labels_from_bits(fin_set))

    out_cols: list[dict[str, Any]] = []
    for col in board.get("columns") if isinstance(board.get("columns"), list) else []:
        if not isinstance(col, dict):
            continue
        cid = str(col.get("id") or "")
        syn = str(col.get("synset") or "").strip()
        lemma = str(col.get("lemma") or "").strip()
        if not syn:
            continue

        bits = load_bits_int(r=r, synset=syn) or 0
        bs = _bits_from_int(bits)
        shared_rel = sorted(list(fin_rel_bits.intersection(_relation_labels_from_bits(bs))))
        shared_dom = sorted(list(fin_dom_bits.intersection(_domain_labels_from_bits(bs))))
        rel_type, via = _find_path(r=r, src=syn, dst=fin_syn, max_depth=2)

        col_out: dict[str, Any] = {
            "id": cid,
            "lemma": lemma,
            "synset": syn,
            "relation_to_final": {
                "type": rel_type,
                "via": via,
                "shared_bits": {"relation_bits": shared_rel, "domain_bits": shared_dom},
            },
            "clues": [],
        }

        for clue in col.get("clues") if isinstance(col.get("clues"), list) else []:
            if not isinstance(clue, dict):
                continue
            c_syn = str(clue.get("synset") or "").strip()
            c_lemma = str(clue.get("lemma") or "").strip()
            if not c_syn:
                continue
            c_bits = load_bits_int(r=r, synset=c_syn) or 0
            c_set = _bits_from_int(c_bits)
            shared_rel_c = sorted(list(set(_relation_labels_from_bits(c_set)).intersection(_relation_labels_from_bits(bs))))
            shared_dom_c = sorted(list(set(_domain_labels_from_bits(c_set)).intersection(_domain_labels_from_bits(bs))))
            c_rel_type, c_via = _find_path(r=r, src=c_syn, dst=syn, max_depth=2)
            col_out["clues"].append(
                {
                    "lemma": c_lemma,
                    "synset": c_syn,
                    "relation_to_column": {
                        "type": c_rel_type,
                        "via": c_via,
                        "shared_bits": {"relation_bits": shared_rel_c, "domain_bits": shared_dom_c},
                    },
                }
            )
        out_cols.append(col_out)

    return {"id": str(board.get("id") or ""), "final": {"synset": fin_syn, "lemma": fin_lemma}, "columns": out_cols}


def seed_demo(*, r: redis.Redis) -> dict[str, Any]:
    # Minimal self-contained dataset for UI demos/tests (no NLTK required at runtime).
    # The IDs match real WordNet synset naming patterns, but are used here only as stable identifiers.
    synsets: dict[str, dict[str, Any]] = {
        "festival.n.01": {
            "lemma": "festival",
            "domains": ["TIME_EVENT"],
            "pos": "n",
            "rels": {"hyponyms": ["music.n.01", "food.n.01", "city.n.01", "happiness.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["TIME_EVENT"], BIT_REL_HYPONYM],
        },
        "music.n.01": {
            "lemma": "music",
            "domains": ["ABSTRACT"],
            "pos": "n",
            # Ensure outbound neighbors are >= 4 so the random board generator can work even on the demo dataset.
            "rels": {
                "hypernyms": ["festival.n.01"],
                "hyponyms": ["song.n.01"],
                "meronyms": ["guitar.n.01", "singer.n.01"],
                "holonyms": ["concert_hall.n.01"],
            },
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["ABSTRACT"], BIT_REL_HYPERNYM, BIT_REL_HYPONYM, BIT_REL_MERONYM],
        },
        "song.n.01": {
            "lemma": "song",
            "domains": ["ABSTRACT"],
            "pos": "n",
            "rels": {"hypernyms": ["music.n.01"], "meronyms": ["singer.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["ABSTRACT"], BIT_REL_HYPERNYM, BIT_REL_MERONYM],
        },
        "guitar.n.01": {
            "lemma": "guitar",
            "domains": ["OBJECT"],
            "pos": "n",
            "rels": {"holonyms": ["music.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["OBJECT"], BIT_REL_HOLONYM],
        },
        "singer.n.01": {
            "lemma": "singer",
            "domains": ["PERSON"],
            "pos": "n",
            "rels": {"holonyms": ["music.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["PERSON"], BIT_REL_HOLONYM],
        },
        "concert_hall.n.01": {
            "lemma": "concert hall",
            "domains": ["PLACE"],
            "pos": "n",
            "rels": {"holonyms": ["music.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["PLACE"], BIT_REL_HOLONYM],
        },
        "food.n.01": {
            "lemma": "food",
            "domains": ["FOOD"],
            "pos": "n",
            "rels": {
                "hypernyms": ["festival.n.01"],
                "hyponyms": ["bread.n.01"],
                "meronyms": ["recipe.n.01"],
                "holonyms": ["chef.n.01", "restaurant.n.01"],
            },
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["FOOD"], BIT_REL_HYPERNYM, BIT_REL_HYPONYM, BIT_REL_MERONYM],
        },
        "bread.n.01": {
            "lemma": "bread",
            "domains": ["FOOD"],
            "pos": "n",
            "rels": {"hypernyms": ["food.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["FOOD"], BIT_REL_HYPERNYM],
        },
        "chef.n.01": {
            "lemma": "chef",
            "domains": ["PERSON"],
            "pos": "n",
            "rels": {"holonyms": ["food.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["PERSON"], BIT_REL_HOLONYM],
        },
        "restaurant.n.01": {
            "lemma": "restaurant",
            "domains": ["PLACE"],
            "pos": "n",
            "rels": {"holonyms": ["food.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["PLACE"], BIT_REL_HOLONYM],
        },
        "recipe.n.01": {
            "lemma": "recipe",
            "domains": ["ABSTRACT"],
            "pos": "n",
            "rels": {"holonyms": ["food.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["ABSTRACT"], BIT_REL_HOLONYM],
        },
        "city.n.01": {
            "lemma": "city",
            "domains": ["PLACE"],
            "pos": "n",
            "rels": {
                "hypernyms": ["festival.n.01"],
                "hyponyms": ["street.n.01"],
                "meronyms": ["building.n.01"],
                "holonyms": ["mayor.n.01", "pollution.n.01"],
            },
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["PLACE"], BIT_REL_HYPERNYM, BIT_REL_HYPONYM, BIT_REL_MERONYM],
        },
        "street.n.01": {
            "lemma": "street",
            "domains": ["PLACE"],
            "pos": "n",
            "rels": {"hypernyms": ["city.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["PLACE"], BIT_REL_HYPERNYM],
        },
        "mayor.n.01": {
            "lemma": "mayor",
            "domains": ["PERSON"],
            "pos": "n",
            "rels": {"holonyms": ["city.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["PERSON"], BIT_REL_HOLONYM],
        },
        "building.n.01": {
            "lemma": "building",
            "domains": ["OBJECT"],
            "pos": "n",
            "rels": {"holonyms": ["city.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["OBJECT"], BIT_REL_HOLONYM],
        },
        "pollution.n.01": {
            "lemma": "pollution",
            "domains": ["NATURAL"],
            "pos": "n",
            "rels": {"holonyms": ["city.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["NATURAL"], BIT_REL_HOLONYM],
        },
        "happiness.n.01": {
            "lemma": "happiness",
            "domains": ["EMOTION"],
            "pos": "n",
            "rels": {
                "hypernyms": ["festival.n.01"],
                "hyponyms": ["smile.n.01"],
                "holonyms": ["friend.n.01", "celebration.n.01", "sunshine.n.01"],
            },
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["EMOTION"], BIT_REL_HYPERNYM, BIT_REL_HYPONYM],
        },
        "smile.n.01": {
            "lemma": "smile",
            "domains": ["BODY"],
            "pos": "n",
            "rels": {"holonyms": ["happiness.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["BODY"], BIT_REL_HOLONYM],
        },
        "friend.n.01": {
            "lemma": "friend",
            "domains": ["PERSON"],
            "pos": "n",
            "rels": {"holonyms": ["happiness.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["PERSON"], BIT_REL_HOLONYM],
        },
        "celebration.n.01": {
            "lemma": "celebration",
            "domains": ["TIME_EVENT"],
            "pos": "n",
            "rels": {"holonyms": ["happiness.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["TIME_EVENT"], BIT_REL_HOLONYM],
        },
        "sunshine.n.01": {
            "lemma": "sunshine",
            "domains": ["NATURAL"],
            "pos": "n",
            "rels": {"holonyms": ["happiness.n.01"]},
            "bits": [BIT_POS_NOUN, DOMAIN_BITS["NATURAL"], BIT_REL_HOLONYM],
        },
    }

    board_id = "demo_v1"
    board = {
        "id": board_id,
        "final": {"synset": "festival.n.01", "lemma": "festival", "domain": "TIME_EVENT"},
        "columns": [
            {
                "id": "A",
                "synset": "music.n.01",
                "lemma": "music",
                "domain": "ABSTRACT",
                "clues": [
                    {"synset": "song.n.01", "lemma": "song", "domain": "ABSTRACT"},
                    {"synset": "guitar.n.01", "lemma": "guitar", "domain": "OBJECT"},
                    {"synset": "singer.n.01", "lemma": "singer", "domain": "PERSON"},
                    {"synset": "concert_hall.n.01", "lemma": "concert hall", "domain": "PLACE"},
                ],
            },
            {
                "id": "B",
                "synset": "food.n.01",
                "lemma": "food",
                "domain": "FOOD",
                "clues": [
                    {"synset": "restaurant.n.01", "lemma": "restaurant", "domain": "PLACE"},
                    {"synset": "chef.n.01", "lemma": "chef", "domain": "PERSON"},
                    {"synset": "recipe.n.01", "lemma": "recipe", "domain": "ABSTRACT"},
                    {"synset": "bread.n.01", "lemma": "bread", "domain": "FOOD"},
                ],
            },
            {
                "id": "C",
                "synset": "city.n.01",
                "lemma": "city",
                "domain": "PLACE",
                "clues": [
                    {"synset": "street.n.01", "lemma": "street", "domain": "PLACE"},
                    {"synset": "mayor.n.01", "lemma": "mayor", "domain": "PERSON"},
                    {"synset": "building.n.01", "lemma": "building", "domain": "OBJECT"},
                    {"synset": "pollution.n.01", "lemma": "pollution", "domain": "NATURAL"},
                ],
            },
            {
                "id": "D",
                "synset": "happiness.n.01",
                "lemma": "happiness",
                "domain": "EMOTION",
                "clues": [
                    {"synset": "friend.n.01", "lemma": "friend", "domain": "PERSON"},
                    {"synset": "celebration.n.01", "lemma": "celebration", "domain": "TIME_EVENT"},
                    {"synset": "sunshine.n.01", "lemma": "sunshine", "domain": "NATURAL"},
                    {"synset": "smile.n.01", "lemma": "smile", "domain": "BODY"},
                ],
            },
        ],
    }

    pipe = r.pipeline(transaction=False)
    pipe.sadd("wn:all", *list(synsets.keys()))

    for syn, info in synsets.items():
        lemma = str(info.get("lemma") or syn)
        domains = [str(x) for x in (info.get("domains") or []) if str(x).strip()]
        primary = domains[0] if domains else None
        pos = str(info.get("pos") or "")
        rels = info.get("rels") if isinstance(info.get("rels"), dict) else {}
        bits = info.get("bits") if isinstance(info.get("bits"), list) else []
        bits_int = _int_from_bits([int(x) for x in bits])
        pipe.set(wn_dict_key(syn), str(bits_int))
        pipe.set(
            wn_meta_key(syn),
            _jdump({"synset": syn, "lemma": lemma, "lemmas": [lemma], "lexname": None, "domains": domains, "primary_domain": primary, "pos": pos}),
        )
        pipe.set(wn_rels_key(syn), _jdump(rels))

        if pos == "n":
            pipe.sadd("wn:idx:pos:n", syn)
        if primary and primary in DOMAIN_BITS:
            pipe.sadd(f"wn:idx:domain:{primary}", syn)

        for w in _norm_guess(lemma).split(" "):
            if w:
                pipe.sadd(wn_lemma_key(w), syn)

    pipe.set(assoc_board_key(board_id), _jdump(board))
    pipe.execute()

    explain = build_explanation(r=r, board=board)
    r.set(assoc_explain_key(board_id), _jdump(explain))

    return {"seed": "demo_v1", "synsets": len(synsets), "board_id": board_id}


def get_board(*, r: redis.Redis, board_id: str) -> dict[str, Any]:
    raw = r.get(assoc_board_key(board_id))
    board = _jload(raw)
    if not isinstance(board, dict):
        raise ApiError("NOT_FOUND", "board not found", status_code=404, details={"id": board_id})
    return board


def get_or_build_explain(*, r: redis.Redis, board: dict[str, Any]) -> dict[str, Any]:
    board_id = str(board.get("id") or "").strip()
    if board_id:
        raw = r.get(assoc_explain_key(board_id))
        doc = _jload(raw)
        if isinstance(doc, dict):
            return doc
    exp = build_explanation(r=r, board=board)
    if board_id:
        r.set(assoc_explain_key(board_id), _jdump(exp))
    return exp


def generate_board(
    *,
    r: redis.Redis,
    seed: str | None,
) -> dict[str, Any]:
    _ensure_wordnet_ready(r=r)
    rnd = random.Random(seed) if seed is not None else random.Random()

    def rand_member(key: str) -> str | None:
        raw = r.srandmember(key)
        if raw is None:
            return None
        return raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)

    for _ in range(200):
        fin = rand_member("wn:idx:pos:n") or rand_member("wn:all")
        if not fin:
            break
        fin_meta = load_meta(r=r, synset=fin)
        fin_bits = load_bits_int(r=r, synset=fin) or 0
        fin_dom = _pick_primary_domain(fin_meta)
        if not fin_dom:
            continue

        rels = load_rels(r=r, synset=fin)
        neigh = _neighbors(rels)
        if len(neigh) < 4:
            continue

        scored: list[tuple[int, str, str]] = []  # score, synset, domain
        for s in neigh:
            meta = load_meta(r=r, synset=s)
            dom = _pick_primary_domain(meta)
            if not dom:
                continue
            bits = load_bits_int(r=r, synset=s)
            if bits is None:
                continue
            score = _popcount_and(fin_bits, int(bits))
            if score <= 0:
                continue
            scored.append((score, s, dom))
        if len(scored) < 4:
            continue
        scored.sort(reverse=True)

        used_domains: set[str] = set()
        cols: list[dict[str, Any]] = []
        for _, syn, dom in scored:
            if dom in used_domains:
                continue
            meta = load_meta(r=r, synset=syn)
            if not meta:
                continue
            used_domains.add(dom)
            cols.append({"synset": syn, "lemma": meta.lemma, "domain": dom})
            if len(cols) >= 4:
                break
        if len(cols) < 4:
            continue

        col_objs: list[dict[str, Any]] = []
        for i, col in enumerate(cols):
            cid = "ABCD"[i]
            syn = col["synset"]
            meta = load_meta(r=r, synset=syn)
            if not meta:
                break
            bits_col = load_bits_int(r=r, synset=syn) or 0
            c_rels = load_rels(r=r, synset=syn)
            candidates = _neighbors(c_rels)
            if len(candidates) < 4:
                break
            cand_scored: list[tuple[int, str, str, str]] = []  # score, syn, dom, lemma
            for s in candidates:
                m = load_meta(r=r, synset=s)
                d = _pick_primary_domain(m)
                if not d or not m:
                    continue
                b = load_bits_int(r=r, synset=s)
                if b is None:
                    continue
                sc = _popcount_and(bits_col, int(b))
                if sc <= 0:
                    continue
                cand_scored.append((sc, s, d, m.lemma))
            cand_scored.sort(reverse=True)
            used: set[str] = set()
            clues: list[dict[str, Any]] = []
            for _, s, d, lemma in cand_scored:
                if d in used:
                    continue
                used.add(d)
                clues.append({"synset": s, "lemma": lemma, "domain": d})
                if len(clues) >= 4:
                    break
            if len(clues) < 4:
                break
            col_objs.append({"id": cid, "synset": syn, "lemma": meta.lemma, "domain": col["domain"], "clues": clues})

        if len(col_objs) != 4:
            continue

        board_id = f"rnd_{int(time.time())}_{rnd.randrange(1_000_000):06d}"
        board = {
            "id": board_id,
            "final": {"synset": fin, "lemma": fin_meta.lemma if fin_meta else fin, "domain": fin_dom},
            "columns": col_objs,
        }
        r.set(assoc_board_key(board_id), _jdump(board))
        return board

    try:
        wn_count = int(r.scard("wn:all"))
    except Exception:
        wn_count = 0
    raise ApiError(
        "NO_BOARD",
        "could not generate a board from the currently ingested WordNet data (dataset too small or missing relations); ingest full WordNet or use demo mode",
        status_code=422,
        details={"wn_all_count": wn_count},
    )


def check_guess(*, r: redis.Redis, board: dict[str, Any], cell: str, guess: str) -> dict[str, Any]:
    mp = _board_cell_map(board)
    key = str(cell or "").strip()
    if not key:
        raise ApiError("INVALID_INPUT", "cell is required", status_code=422)
    target = mp.get(key)
    if not isinstance(target, dict):
        raise ApiError("INVALID_INPUT", "unknown cell", status_code=422, details={"cell": key})

    target_lemma = _norm_guess(str(target.get("lemma") or ""))
    target_synset = str(target.get("synset") or "")
    norm = _norm_guess(guess)
    if not norm:
        return {"correct": False, "normalized": norm, "target": target_lemma, "synset": target_synset}

    if norm == target_lemma:
        return {"correct": True, "normalized": norm, "target": target_lemma, "synset": target_synset}

    # If lemma index exists, accept any synset match.
    if target_synset:
        members = r.smembers(wn_lemma_key(norm))
        syns = {m.decode("utf-8", errors="replace") if isinstance(m, (bytes, bytearray)) else str(m) for m in members}
        if target_synset in syns:
            return {"correct": True, "normalized": norm, "target": target_lemma, "synset": target_synset}

    return {"correct": False, "normalized": norm, "target": target_lemma, "synset": target_synset}


def hint_for(*, board: dict[str, Any], cell: str, kind: Literal["first_letter", "reveal"] = "first_letter") -> dict[str, Any]:
    mp = _board_cell_map(board)
    key = str(cell or "").strip()
    if not key:
        raise ApiError("INVALID_INPUT", "cell is required", status_code=422)
    target = mp.get(key)
    if not isinstance(target, dict):
        raise ApiError("INVALID_INPUT", "unknown cell", status_code=422, details={"cell": key})
    lemma = _norm_guess(str(target.get("lemma") or ""))
    if kind == "reveal":
        return {"cell": key, "kind": kind, "hint": lemma}
    return {"cell": key, "kind": kind, "hint": (lemma[:1] if lemma else "")}
