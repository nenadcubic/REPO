# Element-Redis GUI – Installation & Usage Guide

This repository contains a self-contained sandbox for working with bit-encoded elements backed by Redis, with a web-based GUI for inspection and experimentation.

The system is portable, zero-setup, and runs entirely in Docker.

## 1. System Requirements

Only the following are required:

- Docker (version 20+ recommended)
- Docker Compose (v2)

No other dependencies are needed.  
No Python, Node, Redis, or compilers are required on the host system.

Verify installation:

```bash
docker --version
docker compose version
```

## 2. Repository Structure (Relevant Parts)

```text
.
├── gui/
│   ├── docker-compose.yml
│   ├── backend/        # REST API (thin adapter)
│   ├── frontend/       # Web GUI
│   ├── presets/        # Configuration presets + metadata
│   │   ├── default.env
│   │   ├── dev.env
│   │   ├── demo.env
│   │   └── default/
│   │       ├── namespaces.json
│   │       └── bitmaps/
│   │           ├── er.json
│   │           └── or.json
│   └── README.md
└── README.md           # (this file)
```

All GUI-related code lives strictly under `gui/`.

## 3. Installation & Startup (One Command)

From the root of the repository:

```bash
cd gui
docker compose up -d --build
```

What this does:

- builds and starts Redis
- builds and starts the backend API
- builds and starts the web GUI
- runs everything in an isolated sandbox

This may take a few minutes the first time (Docker image build).

## 4. Opening the GUI

Once started, open a browser and go to:

- http://localhost:18080

You should see a dark-themed web interface with a sidebar:

- Status
- Elements
- Queries
- Store + TTL
- Examples
- Logs
- Bit-maps
- Explorer

Use the Namespace selector in the header to switch between allowed Redis prefix families (for example `er:*` vs `or:*`).

## Explorer

The GUI also includes an `Explorer` screen (link in the sidebar) that provides a read-focused overview:

1) Select a namespace (left column)
2) Browse/search elements in that namespace (middle column)
3) For bitset namespaces (`er_layout_v1`), inspect one element’s set bits in `Details` or visualize them in a 64×64 `Matrix`
4) For bitset namespaces, switch to `Namespace bitmap` to render many elements at once; click a row to open that element
5) For object namespaces (`or_layout_v2`), Explorer shows object hashes (Matrix/bitmap are not available)

If the namespace has no elements, seed data via the `Examples` screen (seed-type examples) or create an element in `Elements`.

## Examples

All examples live under `examples/<id>/` and must include:
- `examples/<id>/example.json`
- `examples/<id>/README.md`

Examples are discovered by the backend and shown in the GUI’s `Examples` tab. No scripts from `examples/` are executed by the GUI.

Example types:
- `seed`: loads a small predefined set of elements (useful for `Elements`, `Queries`, and `Matrix`)
- `dataset_compare`: imports a dataset (for example from SQLite) and provides comparison reports

Built-in dataset compare example:
- `northwind_compare` imports from `examples/northwind_compare/assets/northwind.sqlite` into the `or` namespace and compares SQLite vs Redis metrics.

## 5. Verifying the System (Recommended)

### 5.1 Backend health check

Optional but recommended:

```bash
curl http://localhost:18000/api/v1/health
```

Expected response (example):

```json
{
  "ok": true,
  "data": {
    "backend_version": "1.0.0",
    "redis": { "ok": true }
  }
}
```

## 6. Basic Usage Walkthrough

### 6.1 Creating an Element

Open Elements

Enter:

- Name: example
- Bits: 1 2 5 13

Click Save Element

Result: element example is stored with those bits set.

### 6.2 Inspecting an Element

In Elements → Get

Enter name: example

Click Fetch

You will see the sorted list of active bits.

### 6.3 Matrix Visualization

In Elements → Show Element (Matrix View)

Enter name: example

Click Fetch & Render

You will see a 64×64 matrix (4096 bits).

- Colored cells = bit = 1
- Dark cells = bit = 0

Hovering a cell shows:

- BIT_NAME: 0|1

Where BIT_NAME comes from the Bit-maps dictionary.

### 6.4 Bit-maps (Bit Dictionary)

Open Bit-maps tab to see:

- list of all defined bits
- their names
- group membership
- descriptions

This metadata is loaded from:

- gui/presets/default/bitmaps/er.json

It is human-readable and editable via the GUI.

### 6.5 Queries

The Queries tab allows set-based queries:

- Find ALL (AND)
- Find ANY (OR)
- Find NOT
- Universe NOT

Results are returned as element names.

### 6.6 Store + TTL (Temporary Sets)

Queries can be stored temporarily in Redis with a TTL (time-to-live).

This is useful for:

- iterative analysis
- intermediate result inspection

Stored results expire automatically.

## 7. Presets & Configuration

The system supports presets.

Default preset is:

- gui/presets/default.env

To run with another preset:

```bash
GUI_PRESET=default docker compose up -d --build
```

(Other presets can be added under `gui/presets/`.)

You should never edit `docker-compose.yml` directly.

## 8. Stopping the System

To stop all containers:

```bash
cd gui
docker compose down
```

## 9. Deinstallation / Cleanup

### 9.1 Remove containers

```bash
docker compose down
```

### 9.2 Remove built images (optional)

```bash
docker image prune
```

### 9.3 Full cleanup (including volumes)

⚠️ This removes all stored data.

```bash
docker compose down -v
```

## 10. Design Notes (For Technical Readers)

- GUI is thin: no business logic, no Redis logic
- Backend is a pure adapter
- Redis operations are atomic where required
- Bit semantics are explicit and inspectable
- All data structures are finite, enumerable, and visualizable

This system is intended for:

- experimentation with large discrete state spaces
- bit-encoded mathematical or logical objects
- inspection of high-dimensional binary structures
