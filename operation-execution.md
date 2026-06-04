# A.P.E.X. Operation and Execution Model

This document explains how A.P.E.X. executes a sort from input records to final
placement. It is the implementation-facing companion to the in-development
mathematical manuscript:

```text
A.P.E.X. Adaptive Parallel Extremal Dispatch
StrmCkr
June 3, 2026
Draft PDF placeholder: [A.P.E.X-math-paper.pdf]
```

The manuscript gives the formal model for bucket descriptors, variable-bit
masks, deterministic dispatch, MSD/LSD refinement, tuple projection, parallel
descriptor reduction, and global correctness. This document keeps those ideas
tied to the repository implementation.

For a focused explanation of record layout, data topology families, and the
descriptor vocabulary, see [Data and Descriptors](data-descriptors.md).

## Data Model

A.P.E.X. sorts fixed-width records:

```text
record = (unsigned 64-bit key, 64-bit value)
record size = 16 bytes
```

Ordering is by unsigned key. The value travels with the key and is used as the
record payload.

## Descriptor Model

Each active bucket is summarized by descriptors:

```text
n_b     = record count in bucket b
OR_b    = bitwise OR across bucket keys
AND_b   = bitwise AND across bucket keys
M_s     = active-domain mask for unresolved positions
VBM_b   = (OR_b ^ AND_b) & M_s
RVB_b   = bitCount(VBM_b)
```

`VBM_b` is the variable-bit mask. A bit is present in `VBM_b` only when keys in
the bucket differ at that position. `RVB_b` is the residual variable-bit count,
the amount of unresolved ordering structure still left in the bucket.

This is the core A.P.E.X. idea: refinement is driven by remaining variable
structure, not by repeatedly paying for all fixed key bits.

## Monotonicity

A.P.E.X. uses two monotonicity scopes:

| Scope | Meaning | Action |
| --- | --- | --- |
| Global ascending | Whole input is already sorted | Accept source as sorted |
| Global descending | Whole input is reversed | Reverse into destination |
| Bucket ascending | One bucket is already sorted | Mark bucket done |
| Bucket descending | One bucket is reversed | Reverse bucket in place |

These paths are terminal for their scope. They do not need tiny sort, tuple
projection, or LSD cycles.

## Execution Pipeline

1. **Read input records**

   Input records are stored in an off-heap memory segment as key/value pairs.

2. **Global order scan**

   The runtime checks whether the whole input is ascending or descending. An
   ascending input returns immediately. A descending input uses the global
   reverse path.

3. **Adaptive MSD planning**

   A.P.E.X. builds an MSD bucket plan. The plan chooses an extraction window,
   counts bucket populations, computes starts, and records per-bucket
   descriptors.

4. **Parallel MSD scatter**

   Worker threads read contiguous blocks of the source array and scatter
   records into their MSD bucket ranges. The bucket starts define the final
   global order of the buckets.

5. **Bucket classification**

   Each bucket is classified as empty, all-equal, ascending, descending, or
   mixed. Empty, all-equal, ascending, and descending buckets can finish without
   normal refinement.

6. **Per-bucket dispatch**

   Mixed buckets are routed by priority:

   ```text
   terminal/order path
   tiny sort
   direct tuple projection
   local MSD repartition
   LSD refinement
   LSD refinement with tuple tail
   ```

   The mathematical dispatch hierarchy is expressed as terminal, tiny, tuple,
   MSD, then LSD. The implementation first uses a top-level MSD scatter to form
   globally ordered buckets, then applies the same priority inside each bucket.

7. **Final placement**

   Every bucket already owns its final range. When a bucket finishes, its
   records are already in the final sorted array.

## Tiny Sort

Tiny sort is a bucket-level fallback controlled by the configured tiny
threshold. The current implementation supports the following size routes:

| Size | Function |
| ---: | --- |
| `1..23` | `insertionSmall` |
| `24..63` | `binaryInsertionSmall` |
| `64..127` | `quickSort` |
| `128..191` | `threeWayQuickSort` |
| `192+` | `MsdRadix8KV` |

The runtime tiny threshold can be auto-tuned or locked. Common locked values are
`32`, `64`, `128`, `512`, and `1024`.

## Tuple Projection

Tuple projection is not a tiny sort. It uses the active variable bits as an
exact compact index:

```text
tupleIndex = Long.compress(key, VBM_b)
tupleRadix = 1 << bitCount(VBM_b)
```

The pass counts tuple indexes, prefix-sums tuple ranges, and places records
directly into those ranges. It is valid when the tuple space fits the configured
cap and the bucket size. The maximum direct tuple cap is `16` bits.

Tuple projection appears in two places:

- **direct tuple projection**, when an entire bucket fits the tuple cap
- **tuple tail**, when LSD cycles consume enough bits that the remaining tail
  fits the tuple cap

Set `tupleBits=0` to prevent direct tuple-space termination. Packed sparse
tuple cycles can be forced with `tuplePacking`; automatic packed cycles can
still be selected when they reduce the cycle count.

## LSD Refinement

When a mixed bucket is too large for tiny sort and too wide for direct tuple
projection, A.P.E.X. builds an LSD cycle plan from the bucket variable mask.

The plan can use:

- contiguous cycles over normal bit windows
- packed sparse cycles over separated variable bits
- tuple-tail termination after a prefix of LSD cycles

LSD refinement is stable within the active bucket range, so prior MSD bucket
ordering is preserved.

## Local MSD Repartition

Large skewed buckets can receive a local MSD repartition before LSD work. This
creates child partitions inside a bucket when a high-order unresolved region is
large enough to be worth splitting. Children then use the same order paths,
tiny route, tuple route, and LSD route.

This keeps one oversized bucket from forcing a single long refinement path when
its descriptor shows useful internal structure.

The local-MSD width may be derived from the active config or overridden with
`localMsdBits`. `localMsdMaxChildren` caps the total child buckets planned from
top-level MSD buckets so adversarial tiny-child explosions do not dominate the
plan phase.

## Dominant-Core Outlier Path

Duplicate-heavy buckets with sparse outliers can take a dominant-core path
before normal LSD refinement. A.P.E.X. samples possible dominant keys, verifies
candidate counts exactly, emits the sorted dominant core when it is a safe
prefix, and then refines the outlier tail. The path is controlled by
`dominantCore`, `dominantCoreSample`, `dominantCoreCandidates`,
`dominantCoreMinShare`, and `dominantKeyMinShareDivisor`.

## Parallel Work Model

A.P.E.X. parallelizes work in stages:

| Stage | Parallel behavior |
| --- | --- |
| Descriptor planning | Per-thread counts and descriptors are reduced into the plan |
| MSD scatter | Threads read source blocks and write into bucket ranges |
| Bucket refinement | Buckets or local child buckets become work items |
| Work stealing | Largest unfinished buckets can be claimed dynamically |
| Large partitions | Off-heap large partition permits limit concurrent scratch use |

With work stealing enabled, buckets needing refinement are ordered by size and
claimed from a shared progress counter. With work stealing disabled, workers
process bucket stripes by thread id.

## Correctness Reference

The mathematical manuscript gives the formal basis for these implementation
rules. The relevant named results include:

| Paper result | Implementation meaning |
| --- | --- |
| Variable-bit characterization | `OR ^ AND` identifies unresolved key bits |
| Deterministic dispatch | Exactly one priority route is selected for a bucket |
| LSD stability | LSD cycles preserve established bucket order |
| Projection ordering equivalence | Tuple indexes match ordering over active bits |
| Descriptor reduction equivalence | Parallel descriptor reduction matches serial descriptors |
| MSD preservation | Top-level bucket order is preserved during refinement |
| Monotonic termination | Ascending and descending paths finish safely |
| Active-domain resolution | Only unresolved active bits need further work |
| A.P.E.X. sorting correctness | Final record order is unsigned sorted order |

## Implementation Map

| Area | File or directory |
| --- | --- |
| Main pipeline | `src/main/Apex.java` |
| Runtime options | `src/config/runoptions.java` |
| Config model | `src/config/configurations.java` |
| MSD planning | `src/MSD/msdbucketplan.java` |
| Parallel scatter | `src/scatter/scattered.java` |
| LSD work planning | `src/LSD/lsdbucketplan.java` |
| Tuple projection | `src/Tuples/tuples.java` |
| Tiny sort routes | `src/tinysorts/tinysort.java` |
| Verification | `src/Tools/verifier.java` |
| Data modes | `src/generator/DataMode.java` |

## Runtime Controls

The command-line controls are intentionally limited:

| Control | Runtime option |
| --- | --- |
| Record count | `records`, `n` |
| Data topology | `mode`, `modes` |
| MSD width | `msd`, `config=MSD,LSD,TINY` |
| LSD width | `lsd`, `config=MSD,LSD,TINY` |
| Thread count | `threads` |
| Tiny threshold | `tiny`, `config=MSD,LSD,TINY` |
| Input order pre-scan | `orderFastPath`, `inputOrderFastPath`, `prescan` |
| Tuple cap | `tupleBits` |
| Tuple packing | `tuplePacking` |
| Staggered tuple cycles | `staggerTuples`, `staggerTupleCycles` |
| Staggered tuple width | `staggerTupleBits` |
| Staggered tuple scoring | `staggerTupleCostModel`, `staggerCostModel` |
| Staggered tuple minimum size | `staggerTupleMinRecords` |
| LSD heap unroll | `lsdHeapUnroll`, `lsdHeapUnrollMinRecords` |
| Heap scratch | `heapScratch`, `heapScratchRecords`, `maxHeapScratch` |
| Large partition permits | `largePermits`, `largePartitionPermits`, `largePartitions` |
| Local MSD repartition width | `localMsdBits`, `secondaryMsdBits`, `subMsdBits` |
| Local MSD child cap | `localMsdMaxChildren`, `localMsdChildren`, `maxLocalMsdChildren` |
| Dominant-core fast path | `dominantCore`, `dominantCoreFastPath` |
| Dominant-core sample size | `dominantCoreSample`, `dominantCoreSampleRecords` |
| Dominant-core candidate slots | `dominantCoreCandidates` |
| Dominant-core minimum share | `dominantCoreMinShare`, `dominantCoreMinSharePercent` |
| Dominant-key minimum share divisor | `dominantKeyMinShareDivisor`, `dominantKeyShareDivisor` |
| Work stealing | `workStealing` |
| Work claim batch size | `workBatch`, `stealBatch`, `workStealBatch` |
| Auto-tune subset size | `tuneRecords`, `tune` |
| Warmup size | `warmupRecords`, `warmup` |
| Sweep mode | `sweep`, `sweepRecords`, `sweepN` |

The browser visualizer mirrors the core execution controls:

| Visualizer control | Values |
| --- | --- |
| `N` | Record count from `0` to `1,000,000`, default `2,048` |
| `Data type` | Visualizer data topology; display names use `EXTREMAL` |
| `MSD` | MSD bit width, `2..13`, default `8` |
| `LSD` | LSD bit width, `2..17`, default `8` |
| `Threads` | `1`, `2`, `4`, `8`, `16`, `32`, or `64`, default `16` |
| `Tiny` | `32`, `64`, `128`, `512`, or `1024`, default `128` |
| `Tuple bits` | Direct tuple cap, `2..16`, default `9` |
| `Tuples` | Enables tuple direct/tail planning and shows the tuple lane |
| `Play/Pause` | Runs or pauses the current execution plan |
| `Step` | Advances one execution step |
| `Reset` | Rebuilds the plan from the current controls |

Changing a visualizer control resets and rebuilds the visual plan. The display
order is source, MSD, tiny, tuples, LSD, reverse, then sorted output.
