# WordNet ingest → Redis bitsets

This tool ingests the full English WordNet into Redis as **4096-bit integer bitsets** and supporting metadata for the “Associations (WordNet)” game.

## What gets written to Redis

Per synset:
- `wn:dict:<synset>` → decimal string of a 4096-bit integer (example: `wn:dict:music.n.01`)
- `wn:meta:<synset>` → JSON (lemma(s), POS, domains)
- `wn:rels:<synset>` → JSON (local relation adjacency lists)

Indexes:
- `wn:all` → set of all synset IDs
- `wn:idx:pos:<n|v|a|r>` → POS index sets
- `wn:idx:domain:<DOMAIN>` → macro-domain index sets (e.g. `wn:idx:domain:FOOD`)
- `wn:lemma:<normalized lemma>` → set of synset IDs for that lemma (optional but recommended)

## Bit profile (v1)

- **POS bits (0–3)**:
  - `0` noun, `1` verb, `2` adjective, `3` adverb
  - `4` multi-word expression (lemma contains space/underscore)

- **Relation bits (128–135)** (set if the synset has at least one of that relation locally):
  - `128` synonym (multiple lemmas)
  - `129` hypernyms
  - `130` hyponyms
  - `131` meronyms
  - `132` holonyms
  - `133` antonyms
  - `134` entailments
  - `135` similar_to

- **Macro domain bits (256–265)** (derived from `synset.lexname()`):
  - `256` FOOD
  - `257` PERSON
  - `258` PLACE
  - `259` LIVING
  - `260` OBJECT
  - `261` EMOTION
  - `262` TIME_EVENT
  - `263` ABSTRACT
  - `264` BODY
  - `265` NATURAL

## Install deps (host Python)

Requires Python 3.10+ recommended.

```bash
pip install -r tools/wn_ingest/requirements.txt
python -c "import nltk; nltk.download('wordnet'); nltk.download('omw-1.4')"
```

## Run

Defaults connect to Redis at `127.0.0.1:6379` (override via flags).

```bash
python tools/wn_ingest/wordnet_to_bitset.py --redis-host 127.0.0.1 --redis-port 6379
```

Reset + reingest:

```bash
python tools/wn_ingest/wordnet_to_bitset.py --reset
```

## Notes

- This tool is intentionally separate from the GUI backend container (NLTK is not a backend dependency).
- For local GUI sandbox Redis, either expose Redis or run ingest in a separate container on the same Docker network.
- The Associations UI shows WordNet ingest status at `http://localhost:18080/explorer/assoc/` and provides copyable ingest commands.
