TODO (Oracle -> Redis, 1:1 rekonstrukcija)
=========================================

Cilj: iz Oracle instance (jedna instanca) parsirati/izvući sve metapodatke + definicije objekata, upisati u Redis tako da se baza može rekonstruisati 1:1, uz “živu” šemu bitova (šifrarnike) koja se dopunjava tokom rada.

---

1) Identitet i naming (obavezno prije koda)
------------------------------------------

- Definisati kanonski identitet objekta: `owner`, `type`, `name` (+ po potrebi `subname`, npr. partition), i normalizaciju (case, quoting).
- Definisati Redis namespace za instancu (pošto je jedna instanca): npr. prefix `er:ora:`.
- Definisati kako se tretiraju rename/drop (tombstone vs hard delete) da bi rekonstrukcija bila deterministička.

---

2) Redis model podataka (1:1 rekonstrukcija)
--------------------------------------------

Minimalno potrebno da se baza može vratiti 1:1:
- Sačuvati **izvorni DDL** (ili DBMS_METADATA output) za svaki objekat, plus strukturirane metapodatke gdje treba.
- Sačuvati sve atribute koji utiču na DDL i ponašanje: constraints, indexi, kolone, defaulti, grants, comments, triggers, dependencies, storage parametri (gdje relevantno).

TODO:
- Definisati ključeve za:
  - Instance info: verzija, charset, NLS, timestamp ingest-a.
  - Object registry (svi objekti): lista + lookup po `owner/type/name`.
  - Per-object record: meta + DDL + checksums.
  - Kolone (TABLE/VIEW): lista, tipovi, nullability, defaults, identity, virtual columns.
  - Constraints (PK/UK/FK/CK): definicije + referencirani objekti.
  - Indexi: definicije + kolone + tablespace/params.
  - Triggers: definicije + status.
  - Views/materialized views: tekst definicije + dependencies.
  - PL/SQL source (PACKAGE/PROC/FUNC/TRIGGER): source + compile status + referenced objects.
  - Synonyms, sequences, grants, comments, types.
- Definisati “relacione” indekse u Redis-u (SETs) za high-cardinality odnose (npr. `proc -> referenced tables`), ne kao bitove.

---

3) Šifrarnici bitova (živa šema)
-------------------------------

Princip:
- Poseban šifrarnik po tipu objekta (TABLE/VIEW/INDEX/PROC/…): tokeni -> bitovi (append-only).
- Bitset se koristi za **ograničene i često filtrirane** osobine (low/medium cardinality).
- Sve što je “previše vrijednosti” ide u relacije (SET/HASH), ne u bitset.

TODO:
- Definisati `schema_id` po tipu objekta (npr. `TABLE`, `VIEW`, …) + “master schema” za opis samih šema.
- Redis ključevi šifrarnika:
  - `...:schema:<schema_id>` (HASH): `type`, `next_bit`, `frozen?`, `updated_at`
  - `...:schema:<schema_id>:token2bit` (HASH)
  - `...:schema:<schema_id>:bit2token` (HASH, opcionalno)
- Implementirati atomsko dodjeljivanje bita (Lua):
  - `HGET token2bit`; ako nema: `INCR next_bit`; provjera `<4096`; `HSET token2bit` (+ `HSET bit2token`).
- Definisati strategiju kad se približi 4096 (alarm + prelazak na novu šemu/tipizacija tokena).

---

4) Elementi u Redis-u (TABLE_<name>, bitset)
--------------------------------------------

TODO:
- Uvesti standardni key format (primjer):
  - Element hash: `er:ora:element:<schema_id>:<owner>.<name>` (HASH: `name`, `flags_bin`, `ddl`, `meta_json`, `updated_at`)
  - Universe set: `er:ora:all:<schema_id>` (SET)
  - Per-bit index: `er:ora:idx:<schema_id>:bit:<bit>` (SET)
- Upsert element:
  - Izračunati token listu -> postaviti bitset.
  - Update indeks setova delta old->new.
  - Održavati universe set.
  - Sačuvati meta/ddl.
- Drop element:
  - `del` logika (ukloniti iz indeksa + universe + element key).

---

5) Oracle extractor + (opcionalno) PL/SQL parser
------------------------------------------------

TODO:
- Definisati extractor module koji čita:
  - `DBA_OBJECTS/ALL_OBJECTS`, `DBA_TAB_COLUMNS`, `DBA_CONSTRAINTS`, `DBA_INDEXES`, `DBA_IND_COLUMNS`,
    `DBA_VIEWS`, `DBA_TRIGGERS`, `DBA_SOURCE`, `DBA_DEPENDENCIES`, `DBA_SYNONYMS`, `DBA_SEQUENCES`,
    `DBA_TAB_PRIVS`, `DBA_COL_COMMENTS`, `DBA_TAB_COMMENTS`, …
- DDL:
  - `DBMS_METADATA.GET_DDL` per objekat (uz consistent transform params).
- PL/SQL parser:
  - Koristiti parser samo gdje dictionary nije dovoljan (npr. dynamic SQL), ili za dodatne “feature” tokene.

---

6) Rekonstrukcija baze iz Redis-a (1:1)
--------------------------------------

TODO:
- Definisati “exporter” koji iz Redis-a generiše:
  - DDL u pravilnom redoslijedu (types -> tables -> constraints -> indexes -> views -> plsql -> grants -> synonyms…)
  - (Opcionalno) provjere/checksums za verifikaciju 1:1.
- Definisati verifikaciju:
  - uporediti checksums DDL-a / strukturiranih meta podataka sa Oracle-om.

---

7) CLI/ABI/API proširenja
-------------------------

TODO:
- `schema_show <schema_id>` i `schema_alloc <schema_id> <token>` (debug).
- `put_object_meta` / `get_object_meta` (DDL + meta).
- `ingest_oracle` (batch ingest) + `export_ddl`.

---

8) Testovi (Docker)
-------------------

TODO:
- Dodati smoke test za:
  - schema alloc (idempotent, atomic, concurrent-safe).
  - upsert + delta index update + del.
  - `_store` atomic i TTL.
- Kad Oracle bude spreman: integration test ingest->export->(recreate/compare).

