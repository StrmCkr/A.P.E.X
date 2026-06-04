# A.P.E.X. Data and Descriptors

This document explains what A.P.E.X. sorts, what descriptors it computes, and
why those descriptors are the basis for adaptive dispatch.

The short version:

```text
data gives records
descriptors summarize unresolved structure
dispatch chooses the cheapest valid route
```

## Why This Exists

Most fixed-width radix pipelines pay for fixed key width. A.P.E.X. instead asks
what is still unresolved inside each active partition. Descriptors are the
answer to that question.

A descriptor is a compact summary of a record range. It does not replace the
data. It tells the sorter which bits vary, whether the range is already ordered,
and which refinement paths are valid.

This keeps the implementation tied to the mathematical model:

```text
records -> bucket descriptors -> variable-bit masks -> deterministic dispatch
```

## Record Data

A.P.E.X. sorts fixed-width key/value records:

```text
record = (key, value)
key    = unsigned 64-bit integer
value  = 64-bit payload
size   = 16 bytes
```

Ordering is by unsigned key. The value travels with the key and is used for
pair-integrity verification after sorting.

The implementation stores records in off-heap memory segments as:

```text
offset + 0  : key
offset + 8  : value
```

## Data Modes

The generator provides data modes so A.P.E.X. can be tested against different
topologies:

| Family | Purpose |
| --- | --- |
| Uniform entropy | Balanced full-width refinement pressure |
| Sorted and reversed | Tests global monotonic fast paths |
| Duplicate-heavy | Tests all-equal, tuple, and dominant-core behavior |
| Low/high-bit entropy | Tests whether fixed bits are skipped |
| Sparse entropy | Tests packed tuple and sparse-cycle planning |
| Skewed buckets | Tests local MSD and work stealing |
| Tiny partition stress | Tests local fanout and tiny-route pressure |
| Pathological/adversarial | Tests worst-case routing and verification |

These modes are benchmark and validation inputs. The sorter should not depend
on mode names. A.P.E.X. routes by descriptors computed from the actual records.

## Bucket Data

After MSD planning and scatter, each bucket owns a final global key range. The
records inside the bucket may still need local refinement, but the bucket itself
is already in the correct global position relative to other MSD buckets.

For each bucket `b`, A.P.E.X. tracks:

```text
start_b = first record index in the bucket's final range
n_b     = number of records in the bucket
```

The start and size descriptors define where work belongs. Refinement should
change order inside the bucket range, not move the bucket to a different global
range.

## Extremal Descriptors

The core descriptors are bitwise extremal summaries:

```text
OR_b  = bitwise OR across all keys in bucket b
AND_b = bitwise AND across all keys in bucket b
```

They identify which key positions still vary:

```text
VBM_b = (OR_b ^ AND_b) & M_s
RVB_b = bitCount(VBM_b)
```

Where:

| Symbol | Meaning |
| --- | --- |
| `M_s` | Active-domain mask for unresolved positions |
| `VBM_b` | Variable-bit mask for bucket `b` |
| `RVB_b` | Residual variable-bit count |

If a bit is `0` in `VBM_b`, every key in the bucket agrees on that bit. A.P.E.X.
does not need to spend a refinement pass on that bit.

## Order Descriptors

A.P.E.X. also tracks monotonicity:

| Descriptor | Meaning | Route |
| --- | --- | --- |
| `ascending` | Records are already sorted by unsigned key | Done |
| `descending` | Records are reverse sorted by unsigned key | Reverse |
| `all-equal` | No key variation remains | Done |
| `mixed` | Refinement is still required | Dispatch |

There are two scopes:

| Scope | Example |
| --- | --- |
| Global | Whole input is ascending or descending |
| Bucket | One bucket or local child is ascending or descending |

Global order descriptors can skip the whole MSD/LSD pipeline. Bucket order
descriptors terminate only that bucket or child range.

## Derived Planning Fields

The implementation stores additional fields derived from the core descriptors:

| Field | Purpose |
| --- | --- |
| MSD shift | Which key window forms the top-level buckets |
| Bucket starts | Final target ranges for each MSD bucket |
| Thread offsets | Per-thread scatter offsets into bucket ranges |
| LSD cycle plan | Which unresolved bit windows to refine |
| Tuple-tail mask | Remaining sparse bits after planned LSD cycles |
| Local MSD shifts | Secondary split window for large skewed buckets |
| Local child descriptors | Per-child sizes, masks, and order states |

These are implementation planning fields. They are useful because they make the
mathematical descriptor decisions executable in parallel.

## What Descriptors Enable

Descriptors decide which paths are valid:

| Descriptor condition | Result |
| --- | --- |
| `n_b == 0` | Empty bucket, no work |
| `VBM_b == 0` | All keys equal over active bits, no refinement |
| `ascending == true` | Already sorted, no refinement |
| `descending == true` | Reverse the range |
| `n_b < tinyThreshold` | Tiny-sort route |
| `bitCount(VBM_b) <= tupleBits` and tuple space fits | Direct tuple projection |
| Large skewed bucket with useful unresolved window | Local MSD repartition |
| Remaining unresolved bits | LSD refinement |
| LSD leaves a small sparse tail | Tuple-tail projection |

The route is deterministic: once a bucket descriptor is known, A.P.E.X. chooses
one priority path for that bucket or child.

## Tuple Descriptors

Tuple projection uses the variable-bit mask as an exact compact key:

```text
tupleIndex = Long.compress(key, VBM_b)
tupleRadix = 1 << bitCount(VBM_b)
```

This is not a tiny sort. It is a counting/scatter projection over the exact
remaining variable bits. It is valid when the tuple radix fits the configured
cap and the bucket size.

Tuple projection can happen in two places:

| Route | Meaning |
| --- | --- |
| Direct tuple | The whole bucket fits the tuple domain |
| Tuple tail | LSD cycles resolve enough bits that the remaining tail fits |

## Local MSD Descriptors

Some datasets create one or a few very large buckets. Local MSD repartition
uses the parent bucket descriptor to decide whether a secondary MSD split is
useful.

A local child receives its own:

```text
start
size
variable mask
ascending / descending state
thread offsets
```

Children then follow the same dispatch rules as top-level buckets. This keeps
large skewed buckets from forcing one long refinement path when the descriptor
shows useful internal structure.

`localMsdMaxChildren` caps total child planning so local MSD remains an
optimization policy, not an uncontrolled fanout.

## Dominant-Core Descriptor Policy

Duplicate-heavy buckets can contain a large repeated-key core with sparse
outliers. The dominant-core route is an implementation policy layered on top of
the descriptor model.

It:

1. Samples candidate dominant keys.
2. Counts candidates exactly.
3. Confirms the combined dominant share.
4. Emits the sorted dominant core only when it is a safe prefix.
5. Refines the remaining tail with normal descriptor-based dispatch.

This route preserves the same correctness requirement: only a range that is
known to be placed correctly can be marked done.

## Data Integrity

A.P.E.X. verifies more than sorted order. Verification checks:

| Check | Purpose |
| --- | --- |
| Order | Keys are unsigned sorted |
| Pair integrity | Values remain attached to their keys |
| Range integrity | Record domain is preserved |
| XOR / sum / hash | Detect loss, duplication, or corruption |
| Min / max key | Confirm expected key range |

This matters because descriptor-driven algorithms may skip large amounts of
work. Verification confirms that skipped work was safe.

## What Descriptors Do Not Claim

Descriptors do not claim that every route is globally optimal. They identify
valid structure and enable bounded dispatch choices.

The formal model supports:

```text
correct descriptor reduction
valid unresolved-bit characterization
bucket-order preservation
stable refinement inside bucket ranges
tuple projection equivalence
deterministic dispatch correctness
```

The implementation adds engineering policy for performance:

```text
work stealing
heap/off-heap scratch thresholds
local-MSD child caps
dominant-core detection thresholds
adaptive unroll paths
```

Those policies should be evaluated empirically, but they are designed to
preserve the descriptor invariants.

## Implementation Map

| Concept | Implementation |
| --- | --- |
| Data modes | `src/generator/DataMode.java` |
| Data initialization | `src/generator/initiatedata.java` |
| MSD descriptors | `src/MSD/msdbucketplan.java` |
| MSD scatter offsets | `src/scatter/scattered.java` |
| LSD cycle planning | `src/LSD/lsdbucketplan.java` |
| Tuple projection | `src/Tuples/tuples.java` |
| Tiny routes | `src/tinysorts/tinysort.java` |
| Verification | `src/Tools/verifier.java` |

