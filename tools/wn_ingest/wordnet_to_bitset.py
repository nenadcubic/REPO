#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from typing import Iterable

import redis


def _norm(s: str) -> str:
    out = (s or "").strip().lower()
    out = out.replace("_", " ")
    out = " ".join(out.split())
    return out


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


def wn_dict_key(synset: str) -> str:
    return f"wn:dict:{synset}"


def wn_meta_key(synset: str) -> str:
    return f"wn:meta:{synset}"


def wn_rels_key(synset: str) -> str:
    return f"wn:rels:{synset}"


def wn_lemma_key(lemma: str) -> str:
    return f"wn:lemma:{lemma}"


def int_from_bits(bits: Iterable[int]) -> int:
    x = 0
    for b in bits:
        if b < 0 or b > 4095:
            continue
        x |= 1 << int(b)
    return x


def domain_bits_for_lexname(lexname: str) -> list[str]:
    lx = (lexname or "").strip().lower()
    out: list[str] = []
    if "food" in lx:
        out.append("FOOD")
    if "person" in lx:
        out.append("PERSON")
    if "location" in lx or "place" in lx:
        out.append("PLACE")
    if "animal" in lx or "plant" in lx:
        out.append("LIVING")
    if "artifact" in lx or "tool" in lx or "device" in lx:
        out.append("OBJECT")
    if "feeling" in lx or "emotion" in lx or "state" in lx:
        out.append("EMOTION")
    if "time" in lx or "event" in lx:
        out.append("TIME_EVENT")
    if "cognition" in lx or "attribute" in lx or "communication" in lx:
        out.append("ABSTRACT")
    if "body" in lx:
        out.append("BODY")
    if "phenomenon" in lx or "object" in lx:
        out.append("NATURAL")
    if not out:
        # deterministic fallback: keep it domain-less, or default to ABSTRACT
        out.append("ABSTRACT")
    # de-dupe, stable order by bit index
    uniq = sorted(set(out), key=lambda d: DOMAIN_BITS.get(d, 9999))
    return uniq


def reset_keys(r: redis.Redis) -> None:
    patterns = ["wn:dict:*", "wn:meta:*", "wn:rels:*", "wn:lemma:*", "wn:idx:*", "wn:all"]
    pipe = r.pipeline(transaction=False)
    deleted = 0
    for pat in patterns:
        if "*" in pat:
            for k in r.scan_iter(match=pat, count=2000):
                pipe.delete(k)
                deleted += 1
                if deleted % 5000 == 0:
                    pipe.execute()
        else:
            pipe.delete(pat)
            deleted += 1
    pipe.execute()
    print(f"reset: deleted~={deleted} keys")


def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest NLTK WordNet → Redis (wn:dict:* bitsets + metadata)")
    ap.add_argument("--redis-host", default="127.0.0.1")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--redis-db", type=int, default=0)
    ap.add_argument("--reset", action="store_true", help="Delete existing wn:* keys before ingest")
    ap.add_argument("--limit", type=int, default=0, help="Ingest only first N synsets (debug)")
    ap.add_argument("--batch", type=int, default=2000, help="Pipeline batch size")
    args = ap.parse_args()

    r = redis.Redis(host=args.redis_host, port=args.redis_port, db=args.redis_db, decode_responses=False)
    r.ping()

    if args.reset:
        reset_keys(r)

    import nltk  # type: ignore
    from nltk.corpus import wordnet as wn  # type: ignore

    try:
        wn.synsets("dog")
    except LookupError:
        nltk.download("wordnet")
        nltk.download("omw-1.4")

    t0 = time.perf_counter()
    pipe = r.pipeline(transaction=False)
    queued = 0
    count = 0

    def flush() -> None:
        nonlocal queued, pipe
        if queued:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
            queued = 0

    for syn in wn.all_synsets():
        syn_id = syn.name()
        pos = syn.pos()
        bits: list[int] = []
        if pos == "n":
            bits.append(BIT_POS_NOUN)
        elif pos == "v":
            bits.append(BIT_POS_VERB)
        elif pos in ("a", "s"):
            bits.append(BIT_POS_ADJ)
        elif pos == "r":
            bits.append(BIT_POS_ADV)

        lemma_names = [str(x) for x in syn.lemma_names() if str(x).strip()]
        lemma_norm = [_norm(x) for x in lemma_names]
        lemma_norm = [x for x in lemma_norm if x]
        lemma = lemma_norm[0] if lemma_norm else _norm(syn_id.split(".", 1)[0])

        if any((" " in ln) for ln in lemma_norm):
            bits.append(BIT_MULTIWORD)

        if len(set(lemma_norm)) > 1:
            bits.append(BIT_REL_SYNONYM)

        if syn.hypernyms():
            bits.append(BIT_REL_HYPERNYM)
        if syn.hyponyms():
            bits.append(BIT_REL_HYPONYM)
        if syn.part_meronyms() or syn.substance_meronyms() or syn.member_meronyms():
            bits.append(BIT_REL_MERONYM)
        if syn.part_holonyms() or syn.substance_holonyms() or syn.member_holonyms():
            bits.append(BIT_REL_HOLONYM)
        if syn.entailments():
            bits.append(BIT_REL_ENTAILMENT)
        if syn.similar_tos():
            bits.append(BIT_REL_SIMILAR_TO)

        has_ant = False
        for lem in syn.lemmas():
            if lem.antonyms():
                has_ant = True
                break
        if has_ant:
            bits.append(BIT_REL_ANTONYM)

        lexname = ""
        try:
            lexname = syn.lexname()
        except Exception:
            lexname = ""
        domains = domain_bits_for_lexname(lexname)
        for d in domains:
            b = DOMAIN_BITS.get(d)
            if b is not None:
                bits.append(b)

        rels = {
            "hypernyms": [s.name() for s in syn.hypernyms()],
            "hyponyms": [s.name() for s in syn.hyponyms()],
            "meronyms": [s.name() for s in (syn.part_meronyms() + syn.substance_meronyms() + syn.member_meronyms())],
            "holonyms": [s.name() for s in (syn.part_holonyms() + syn.substance_holonyms() + syn.member_holonyms())],
            "entailments": [s.name() for s in syn.entailments()],
            "similar_tos": [s.name() for s in syn.similar_tos()],
            "antonyms": [],
        }
        ants = set()
        for lem in syn.lemmas():
            for a in lem.antonyms():
                try:
                    ants.add(a.synset().name())
                except Exception:
                    continue
        rels["antonyms"] = sorted(ants)

        bits_int = int_from_bits(bits)

        pipe.set(wn_dict_key(syn_id), str(bits_int))
        pipe.set(
            wn_meta_key(syn_id),
            json.dumps(
                {
                    "synset": syn_id,
                    "lemma": lemma,
                    "lemmas": sorted(set(lemma_norm))[:32],
                    "lexname": lexname,
                    "domains": domains,
                    "primary_domain": domains[0] if domains else None,
                    "pos": pos,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        )
        pipe.set(wn_rels_key(syn_id), json.dumps(rels, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
        pipe.sadd("wn:all", syn_id)
        if pos in ("n", "v", "a", "r"):
            pipe.sadd(f"wn:idx:pos:{pos}", syn_id)
        for d in domains:
            pipe.sadd(f"wn:idx:domain:{d}", syn_id)
        for ln in sorted(set(lemma_norm)):
            pipe.sadd(wn_lemma_key(ln), syn_id)

        count += 1
        queued += 1
        if queued >= args.batch:
            flush()
            if count % 20000 == 0:
                ms = int((time.perf_counter() - t0) * 1000)
                print(f"progress: {count} synsets ({ms} ms)")

        if args.limit and count >= args.limit:
            break

    flush()
    ms = int((time.perf_counter() - t0) * 1000)
    print(f"OK: ingested synsets={count} elapsed_ms={ms}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

