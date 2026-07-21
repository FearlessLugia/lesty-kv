# Lesty KV

A persistent key-value storage engine written from scratch in C++20, following a Log-Structured Merge-Tree (LSM-Tree)
architecture in the spirit of LevelDB and RocksDB.

Created by: Kiiro Huang

## Highlights

- **LSM-Tree storage engine** with a `Put` / `Get` / `Scan` / `Delete` API, backed by a memtable and levelled SSTables.
- **Static B-Tree SSTables**: each flushed run is organized as a 3-layer B-Tree (root, internal nodes, leaf pages),
  enabling faster point lookups than a flat sorted run.
- **Custom buffer pool**: a chained hash table (MurmurHash3) with an LRU eviction policy behind a clean `EvictionPolicy`
  interface, so alternative policies can be dropped in later.
- **Direct I/O**: all file reads and writes bypass the OS page cache (`O_DIRECT` / `F_NOCACHE` on macOS), with
  page-aligned buffers via `posix_memalign`, so the engine's own buffer pool is the only cache in the read path.
- **Dostoevsky compaction**: tiered compaction for most levels, switching to a levelling-style merge at the last level,
  trading write amplification for read performance where it matters.
- **Sequential flooding defense**: large range scans bypass the buffer pool entirely, so a single big scan cannot evict
  everything else that is currently hot.
- **Dependency injection throughout**: no global singletons. `BufferPool`, `SSTCounter`, and `LsmTree` are owned by
  `Database` and passed down explicitly, which keeps the engine safe to run as multiple isolated instances.

## Architecture

```
                Put / Get / Scan / Delete
                          │
                          ▼
              ┌────────────────────────┐
              │        Memtable          │   in-memory, std::map
              └────────────┬─────────────┘
                            │ flush at kMemtableSize
                            ▼
              ┌────────────────────────┐
              │        LSM-Tree          │   levelled BTreeSSTable runs
              └────────────┬─────────────┘
                            │ every page access (read during
                            │ Get/Scan, write during flush/compaction)
                            ▼
              ┌────────────────────────┐
              │       BufferPool         │
              │   hash table + LRU       │
              └──────┬─────────────┬─────┘
               cache hit      cache miss
                    │               │
                    ▼               ▼
             return page      Direct I/O ──▶ disk
                                    │
                                    └──▶ page is inserted into
                                         BufferPool on the way back
```

Memtable reads and writes never touch the buffer pool at all, they're pure in-memory `std::map` operations. The buffer
pool only enters the picture once the LSM-Tree needs to touch an actual SSTable page, whether that's a lookup during
`Get`/`Scan` or a page being written out during a flush or compaction. Every such page request is routed through the
buffer pool first: a hit returns the cached page directly, and a miss falls through to Direct I/O, with the freshly
read (or written) page inserted into the pool on the way back. So the buffer pool isn't a separate stage after the
LSM-Tree, it's a cache that sits in front of every disk access the LSM-Tree layer makes.

Every layer above was built as its own subsystem with its own concerns, rather than as a thin wrapper over a single
library. The sections below go through each one.

### Memtable

Keys and values are both `int64_t` (16 bytes per pair). The memtable itself is a `std::map`, which gives an in-memory
sorted structure "for free" while keeping the rest of the engine's design (SSTable layout, flush logic, compaction)
unaffected by whatever internal structure backs it. `Memtable::Size()` tracks bytes rather than entry count, so
`kMemtableSize` is an actual byte budget. Once that budget is exceeded, `Database::Put` triggers `FlushFromMemtable`,
which traverses the map in sorted order, hands the resulting run to a new `BTreeSSTable`, appends it as a level-0 run in
the LSM-Tree, and clears the memtable.

Deletes never touch storage directly. `Memtable::Delete` writes `(key, INT64_MIN)` as a tombstone, which then flows
through flush and compaction like any other entry. Every read path (`Get`, `Scan`, `SortMerge`) has to explicitly check
for this sentinel and treat it as "not found," which is easy to get subtly wrong. One such bug (tombstones leaking into
`Scan` results after a flush) is now covered directly by a regression test.

### Buffer pool

The buffer pool is a hash table with separate chaining, not `std::unordered_map`, so that both the collision list and
the eviction queue could be built and reasoned about explicitly:

```
buckets_
┌───────┬───────┬───────┬───────┐
│   0   │   1   │   2   │   3   │
└───┬───┴───┬───┴───┬───┴───┬───┘
    │       │       │       │
    ▼       ▼       ▼       ▼
 Bucket   null   Bucket   null
 Node(A)         Node(D)
    │next_          │next_
    ▼               ▼
 Bucket            null
 Node(B)
    │next_
    ▼
   null

each BucketNode also carries lru_node_, a direct pointer into:

LRU list (dummy_ sentinel, doubly linked)
                 front (evict next)                    rear (just used)
dummy_ ⇄ QueueNode(B) ⇄ QueueNode(D) ⇄ QueueNode(A) ⇄ dummy_
```

Chaining and eviction are deliberately two separate structures connected by one pointer each way, rather than one
combined structure, since they solve different problems: buckets answer "is this page here," the LRU list answers "which
page should go next." Bucket collisions on the same key hash (A and B above) are singly-linked via `BucketNode::next_`
and walked linearly on lookup, since chains are expected to stay short at a reasonable load factor. The LRU list is a
separate doubly-linked list of `QueueNode`s with a dummy sentinel head, so it never needs null checks for empty or
single-element edge cases. The two structures are tied together by `BucketNode::lru_node_`, a raw pointer from a bucket
entry straight to its position in the LRU list, so that a cache hit (`BufferPool::Get`) can promote a page to
most-recently-used by directly unlinking and re-appending that one `QueueNode`, in O(1), instead of scanning the list to
find it.

- **Hashing.** Page IDs (`"<sst name>_<offset>"`) are hashed with MurmurHash3 (x64_128), imported as an external
  dependency and wired into `BufferPool::HashFunction`, to get uniform bucket distribution independent of the sequential
  offsets pages are naturally named with.
- **Eviction.** `EvictionPolicy` is an abstract interface with a single concrete `LRU` implementation, so `Put` (append
  to rear), `Update` (move-to-rear on hit), `Evict` (pop from front), and `Remove` are all O(1).
- **Eviction trigger.** Rather than evicting only when completely full, the pool starts evicting once it crosses
  `kCoeffBufferPool` (80%) of capacity, so a burst of inserts doesn't repeatedly hit the exact capacity boundary and
  thrash.
- **Ownership.** The pool was originally a singleton; it was refactored to be owned by `Database` and passed by pointer
  into `BufferPool`, `SSTable`, and `LsmTree` so that multiple `Database` instances in the same process do not share
  state, and so the engine has no hidden global mutable state at all.

### Direct I/O and page alignment

All SSTable reads and writes go through `pread`/`pwrite` with `O_DIRECT` (`F_NOCACHE` on macOS, since Linux's `O_DIRECT`
flag doesn't exist there), bypassing the OS page cache entirely. This was a deliberate choice: with the OS cache in the
loop, benchmark numbers mostly measure the OS cache, not the engine's own buffer pool. The tradeoff is that Direct I/O
requires every buffer to be aligned to the page size, so every read and write path allocates its buffer with
`posix_memalign(&buffer, kPageSize, kPageSize)` instead of a plain `new`/`vector`. Because pages are also written at
fixed `kPageSize` boundaries but the actual data at the end of a file rarely fills a whole page, `ftruncate` is used
after a flush to trim the trailing zero-padding back to the exact logical file size.

### SSTables as static B-Trees

An SSTable is immutable once written, so `BTreeSSTable` builds its index once, at flush time, rather than maintaining it
incrementally. The layout has three logical layers:

1. **Leaf pages**, written first and streamed straight to disk as they're built, one page at a time, rather than being
   buffered fully in memory. This is what keeps flush and compaction memory-bounded even for very large runs.
2. **Internal nodes**, holding up to `kFanOut` separator keys each (`kFanOut` is bound to `kPagePairs`, 256, so a node's
   key count lines up with what fits in one page).
3. **Root**, built from the internal nodes' separator keys. If the number of internal-node separators itself exceeds
   `kFanOut`, the internal layer is split again and rebuilt one level up (`GenerateBTreeLayers` handles this
   recursively), so the structure stays correct even for very large runs, though only the root and internal layers are
   ever kept resident in memory; leaves are read back from disk on demand.

Reconstructing an SSTable after a restart means figuring out where the leaf layer starts without re-scanning the whole
file: `ReadOffset` reads the file's first page (the root) and counts non-zero entries to recover how many pages the root
and internal layers actually occupy, which is also how `GetDataStartOffset` locates the beginning of the leaf pages for
scans.

Both point lookups and range scans exploit the layering: `BinarySearch` first binary-searches across leaf pages using
each page's first/last key, then binary-searches again inside the matched page, since entries within a page are already
sorted, doing a linear scan there would be strictly worse for no benefit. `BinarySearchUpperbound` does the analogous
two-level search to find where a scan's start key would go, after which `LinearSearchToEndKey` walks forward page by
page, appending qualifying entries, until it passes `end_key`.

Every SSTable also tracks `min_key_`/`max_key_`, computed once at open time in `InitialKeyRange`. Both `Get` and `Scan`
check these before doing any I/O at all, so an SSTable whose range can't possibly contain the query is skipped without a
single disk read. With many small runs (which a tiered LSM-Tree accumulates), this pruning matters more than the search
algorithm inside any individual SSTable.

### LSM-Tree and Dostoevsky compaction

New SSTables always enter at level 0. `LsmTree::OrderLsmTree` is invoked after every flush and decides whether any level
now needs to compact. Below the configured `kLevelToApplyDostoevsky` level, compaction is tiered: a level merges all of
its runs into one new run at the next level once it accumulates `kLsmRatio` runs (level 0 needs 3, level 1 needs 9,
level 2 needs 27, and so on, since the threshold scales with `kLsmRatio^(level+1)`). At `kLevelToApplyDostoevsky`
itself, the strategy switches to pairwise levelling: as soon as two runs exist at that level, they merge into one, which
bounds the number of runs (and therefore the number of files a query has to check) at the level that matters most for
read amplification, at the cost of more frequent, smaller merges there specifically.

The actual merge, `LsmTree::SortMerge`, is an n-way merge over a min-heap (`std::priority_queue` with a custom
`greater<>` comparator), rather than a full sort of all the concatenated data. Each SSTable begins by reading its leaf
offset, pushing its first key-value pair onto the heap tagged with its source SST. Each pop yields the current global
minimum key; ties are broken toward the more recently written SST so that newer values shadow older ones with the same
key. When a value equals the tombstone sentinel and the merge is configured to drop tombstones (true for the final-level
merge, false for intermediate levels, since a tombstone at level 2 must still be able to shadow an older value further
down), it's dropped from the output rather than propagated. After popping, the same SST's next entry is pushed back onto
the heap, either from the already-buffered page or by reading the next page from disk, so at most one page per active
SST is held in memory at any point during a merge.

Recovering the tree from disk (`ReadSSTsFromStorage`) parses each file's name to recover its level and index via regex,
groups files by level, sorts each level's runs oldest-first, and reinitializes `SSTCounter`'s per-level counters from
the highest index seen, so newly created SSTables after a restart don't collide with existing file names. `BuildLsmTree`
then immediately re-runs `OrderLsmTree`, so a database that was closed mid-way through accumulating runs at some level
will resume compaction where it left off rather than requiring a new flush to trigger it.

### Write path

`Put` writes into the memtable. Once the memtable's byte size reaches `kMemtableSize`, it's flushed to a new level-0
SSTable and cleared, and `OrderLsmTree` is invoked in case that flush pushed a level over its compaction threshold.

### Read path

`Get` checks the memtable first, then walks the LSM-Tree from the lowest level to the highest, and within a level from
the newest SSTable to the oldest, stopping at the first match (or tombstone). `Scan` follows the same level and recency
order, querying every SSTable whose key range could overlap the scan, and merges results from all sources while keeping
only the most recently written version of each key, exactly as `SortMerge` does during compaction, since both are really
the same "most recent value wins across multiple sorted sources" problem.

A large scan touches far more pages than a point lookup, and without a safeguard it would flush the entire buffer pool
of genuinely hot pages just to service one query. When a scan's estimated page range exceeds
`kCoeffSequentialFlooding` (25%) of the buffer pool's page capacity, the pages it touches are marked ephemeral and read
directly into a page object that is discarded after use, rather than being inserted into the buffer pool at all.

## Building

Requires CMake 3.29+ and a C++20 compiler.

```bash
mkdir build && cd build
cmake ..
cmake --build .
```

This produces three targets:

- `kv-lib`: the storage engine as a static library
- `kv-test`: unit and integration tests
- `kv-experiment`: throughput benchmarks

## Running tests

```bash
./build/kv-test
```

Covers the buffer pool (bucket placement, LRU ordering and eviction, pointer bookkeeping), B-Tree construction, LSM-Tree
merging and level management, and end-to-end database behavior including overwrites, tombstone scans, key range
boundaries, and sequential flooding.

## Running benchmarks

```bash
./build/kv-experiment
```

Inserts data in exponentially increasing steps up to 1 GB, measuring `Put`, `Get`, and `Scan` throughput at each step.
Results are written to `experiment_Put.csv`, `experiment_Get.csv`, and `experiment_Scan.csv`.

To generate plots from those CSVs:

```bash
python3 plot_generator.py --input-dir <path-to-csvs> --output-dir docs/benchmarks
```

### Results

Measured on a MacBook Pro (Apple M2 Pro, 16 GB RAM) with a 1 MB memtable, 10 MB buffer pool, and 1 GB of inserted data:

- **Put throughput** decreases logarithmically as data size grows, consistent with the expected insertion cost of
  `O((1/B) * log_T(N/P))` for a tiered LSM-Tree.
- **Get throughput** follows the expected binary-search query cost of `O(log_T(N/P) * log2(N/B))` across levels; using
  the B-Tree index instead reduces the second term to `O(log_B N)`.
- **Scan throughput** follows the same shape with an added linear term for the number of returned entries,
  `O(... + S/B)`.

## Configuration

Key tunables live in `utils/constants.h`:

| Constant                   | Meaning                                                                               |
|----------------------------|---------------------------------------------------------------------------------------|
| `kPageSize`                | Page size in bytes (4 KB)                                                             |
| `kPagePairs`               | Key-value pairs per page                                                              |
| `kMemtableSize`            | Memtable flush threshold                                                              |
| `kBufferPoolSize`          | Buffer pool capacity                                                                  |
| `kCoeffBufferPool`         | Load factor at which eviction kicks in                                                |
| `kCoeffSequentialFlooding` | Scan size (as a fraction of the buffer pool) above which pages bypass the buffer pool |
| `kFanOut`                  | B-Tree fan-out (bound to `kPagePairs`)                                                |
| `kLsmRatio`                | Size ratio between adjacent LSM-Tree levels                                           |
| `kLevelToApplyDostoevsky`  | Level at which tiered compaction switches to pairwise merging                         |
