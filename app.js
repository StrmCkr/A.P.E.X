"use strict";

// A.P.E.X visualizer model.
// Mirrors the Java planner path: input order fast path -> adaptive MSD plan
// -> MSD scatter -> per-bucket refinement by tiny sort, LSD cycles, direct
// tuple pass, and optional tuple-tail pass.

const U64_MASK = 0xFFFFFFFFFFFFFFFFn;
const SEED = 0x9E3779B97F4A7C15n;
const BUCKET_EMPTY = 0;
const BUCKET_ALL_EQUAL = 1;
const BUCKET_MIXED = 2;
const BUCKET_ASCENDING = 3;
const BUCKET_DESCENDING = 4;
const SMALL_TUPLE_LOOKUP_BITS = 10;
const PACKED_TUPLE_CYCLES = false;
const MAX_RECORD_COUNT = 1_000_000;
const MAX_SCATTER_STEPS = 420;
const MAX_LANE_RECORDS = 512;
const MAX_DRAW_BARS = 4096;
const MAX_VISIBLE_BUCKETS = 128;
const TINY_THRESHOLDS = [32, 64, 128, 512, 1024];
const ROUTE_RGB = {
  done: [214, 218, 226],
  msd: [240, 228, 66],
  tiny: [255, 176, 0],
  "tuple-direct": [204, 121, 167],
  "lsd-tuple-tail": [86, 180, 233],
  lsd: [0, 114, 178],
  reverse: [120, 94, 240]
};
const TINY_RGB = {
  insertion: [255, 176, 0],
  binary: [254, 97, 0],
  quick: [100, 143, 255],
  threeway: [220, 38, 127],
  msd8: [0, 194, 168]
};
const TINY_PROFILES = [
  { id: "insertion", min: 1, max: 23, label: "insertionSmall", range: "1-23", detail: "classic insertion sort" },
  { id: "binary", min: 24, max: 63, label: "binaryInsertionSmall", range: "24-63", detail: "binary insertion with block moves" },
  { id: "quick", min: 64, max: 127, label: "quickSort", range: "64-127", detail: "median-of-three quicksort" },
  { id: "threeway", min: 128, max: 191, label: "threeWayQuickSort", range: "128-191", detail: "Bentley-McIlroy duplicate collapser" },
  { id: "msd8", min: 192, max: Number.POSITIVE_INFINITY, label: "MsdRadix8KV", range: "192+", detail: "8-bit MSD radix fallback" }
];

const DATA_MODES = [
  "RANDOM",
  "SORTED",
  "REVERSE",
  "NEARLY_SORTED",
  "DUPLICATES",
  "LOW_BITS_ONLY",
  "HIGH_BITS_ONLY",
  "ZIPFIANISH",
  "ALL_EQUAL",
  "EMPTY",
  "SINGLE_ELEMENT",
  "TWO_ELEMENTS_SORTED",
  "TWO_ELEMENTS_REVERSED",
  "SAWTOOTH",
  "ORGAN_PIPE",
  "ROTATED_SORTED",
  "PARTIALLY_SHUFFLED",
  "BLOCK_SORTED",
  "STAGGERED_RUNS",
  "FEW_UNIQUE_VALUES",
  "MANY_DUPLICATES_WITH_OUTLIERS",
  "ALTERNATING_LOW_HIGH",
  "MIN_MAX_ALTERNATING",
  "NEGATIVE_VALUES",
  "EXTREME_VALUES",
  "INTEGER_OVERFLOW_RISK",
  "GAUSSIAN",
  "EXPONENTIAL",
  "BIMODAL",
  "POWER_LAW",
  "DELAYED_ENTROPY",
  "CLUSTERED_KEYS",
  "BIT_REVERSAL",
  "STRIDED",
  "SPIKE_NOISE",
  "DESCENDING_BLOCKS",
  "DENSE_16BIT",
  "ENTROPY_BANDS",
  "TINY_PARTITIONS_STRESS",
  "ALMOST_SORTED_WITH_SPIKES",
  "HIGH_BIT_ONLY_MOVING",
  "ALTERNATING_BUCKET_DESTINATIONS",
  "EXTREME_DUPLICATE_DENSITY",
  "WORST_CASE_MSD_COLLAPSE",
  "ENTROPY_OSCILLATION",
  "SINGLE_BIT_TAIL_ENTROPY",
  "SPARSE_ENTROPY_EXPLOSION",
  "TUPLE_31BIT_OVERFLOW",
  "CROSS_THREAD_BUCKET_SKEW",
  "CACHE_THRASH",
  "SIGN_BIT_BOUNDARY",
  "PREFIX_CONSTANT_RANDOM_TAIL",
  "TUPLE_29BIT",
  "TUPLE_30BIT",
  "PERMUTATION_STRESS",
  "HIGH_ENTROPY_PREFIX_CONSTANT_TAIL",
  "TWO_BUCKET_COLLISION",
  "LARGE_EQUAL_REGION_WITH_RANDOM_END",
  "RADIX_PATHOLOGICAL",
  "BIT_SPARSE_POWERLAW",
  "MICRO_CLUSTERS",
  "INTERLEAVED_SORTED_RANDOM",
  "SAWTOOTH_DESCENDING",
  "SINGLE_HOT_BUCKET",
  "LOW_CARDINALITY_HIGH_VOLUME"
];

const defaultConfig = {
  mode: "RANDOM",
  count: 2048,
  msdBits: 8,
  lsdBits: 8,
  workers: 16,
  workStealing: true,
  tuplesEnabled: true,
  tupleBits: 9,
  tinyThreshold: 128,
  speed: 45
};

const state = {
  cfg: { ...defaultConfig },
  records: [],
  plan: null,
  msdRecords: [],
  finalRecords: [],
  inputOrder: "mixed",
  keyRange: { minKey: 0n, maxKey: 1n },
  steps: [],
  stepIndex: 0,
  playing: false,
  hasStarted: false,
  labels: {},
  canvases: {},
  ctx: {}
};

window.state = state;

function u64(x) {
  return x & U64_MASK;
}

function lowBitsMask(bits) {
  if (bits <= 0) return 0n;
  if (bits >= 64) return U64_MASK;
  return (1n << BigInt(bits)) - 1n;
}

function lowIntMask(bits) {
  if (bits <= 0) return 0;
  if (bits >= 31) return -1;
  return (1 << bits) - 1;
}

function bitCount64(x) {
  let v = u64(x);
  let count = 0;
  while (v !== 0n) {
    v &= v - 1n;
    count++;
  }
  return count;
}

function trailingZeros64(x) {
  if (x === 0n) return 64;
  let v = u64(x);
  let z = 0;
  while ((v & 1n) === 0n) {
    z++;
    v >>= 1n;
  }
  return z;
}

function leadingZeros64(x) {
  if (x === 0n) return 64;
  return 64 - x.toString(2).length;
}

function keyLabel(k) {
  return "0x" + u64(k).toString(16).padStart(16, "0");
}

function displayDataMode(mode) {
  return mode.replaceAll("ENTROPY", "EXTREMAL");
}

function mix64(x) {
  let z = u64(BigInt(x) + SEED);
  z = u64((z ^ (z >> 30n)) * 0xBF58476D1CE4E5B9n);
  z = u64((z ^ (z >> 27n)) * 0x94D049BB133111EBn);
  return u64(z ^ (z >> 31n));
}

function scaleOrderedKey(rank, count) {
  const n = BigInt(count);
  if (n <= 1n) return 0n;
  const bits = n - 1n === 0n ? 0 : (n - 1n).toString(2).length;
  return u64(BigInt(rank) << BigInt(64 - bits));
}

function reverseBits64(x) {
  let v = u64(x);
  v = ((v & 0x5555555555555555n) << 1n) | ((v >> 1n) & 0x5555555555555555n);
  v = ((v & 0x3333333333333333n) << 2n) | ((v >> 2n) & 0x3333333333333333n);
  v = ((v & 0x0F0F0F0F0F0F0F0Fn) << 4n) | ((v >> 4n) & 0x0F0F0F0F0F0F0F0Fn);
  v = ((v & 0x00FF00FF00FF00FFn) << 8n) | ((v >> 8n) & 0x00FF00FF00FF00FFn);
  v = ((v & 0x0000FFFF0000FFFFn) << 16n) | ((v >> 16n) & 0x0000FFFF0000FFFFn);
  return u64((v << 32n) | (v >> 32n));
}

function recordColor(key) {
  const hue = Number((u64(key) >> 48n) & 0xFFFFn) / 65535 * 360;
  const light = 48 + Number((u64(key) >> 8n) & 0xFn);
  return `hsl(${hue.toFixed(1)} 78% ${light}%)`;
}

function makeRecord(key, index) {
  return { key: u64(key), value: BigInt(index), sourceIndex: index, color: recordColor(key) };
}

function keyForMode(mode, iRaw, nRaw) {
  const i = BigInt(iRaw);
  const n = BigInt(nRaw);
  switch (mode) {
    case "RANDOM":
      return mix64(i);
    case "SORTED":
      return scaleOrderedKey(i, n);
    case "REVERSE":
      return scaleOrderedKey(n - 1n - i, n);
    case "NEARLY_SORTED": {
      let j = i ^ (mix64(i) & 1023n);
      if (j >= n) j = i;
      return scaleOrderedKey(j, n);
    }
    case "DUPLICATES": {
      const classes = 1n << 20n;
      return scaleOrderedKey(mix64(i) & (classes - 1n), classes);
    }
    case "LOW_BITS_ONLY":
      return mix64(i) & 0xFFFFFFFFn;
    case "HIGH_BITS_ONLY":
      return mix64(i) & 0xFFFFFFFF00000000n;
    case "ZIPFIANISH": {
      const x = mix64(i);
      const bucket = BigInt(leadingZeros64(x | 1n));
      return u64((bucket << 56n) | (x & 0x00FFFFFFFFFFFFFFn));
    }
    case "ALL_EQUAL":
    case "SINGLE_ELEMENT":
      return 0n;
    case "TWO_ELEMENTS_SORTED":
      return scaleOrderedKey(i, 2n);
    case "TWO_ELEMENTS_REVERSED":
      return scaleOrderedKey(1n - i, 2n);
    case "SAWTOOTH": {
      const period = 1024n;
      return scaleOrderedKey(i % period, period);
    }
    case "ORGAN_PIPE": {
      const mid = n >> 1n;
      const x = i <= mid ? i : n - 1n - i;
      return scaleOrderedKey(x, mid + 1n);
    }
    case "ROTATED_SORTED": {
      const shift = n / 3n;
      return scaleOrderedKey((i + shift) % n, n);
    }
    case "PARTIALLY_SHUFFLED":
      return (mix64(i) & 15n) === 0n ? mix64(i) : scaleOrderedKey(i, n);
    case "BLOCK_SORTED": {
      const blockSize = 1024n;
      return scaleOrderedKey((i / blockSize) * blockSize + (i % blockSize), n);
    }
    case "STAGGERED_RUNS": {
      const run = 64n;
      const group = i / run;
      const pos = i % run;
      return scaleOrderedKey(group * run + ((pos * 7n) % run), n);
    }
    case "FEW_UNIQUE_VALUES":
      return mix64(i) & 15n;
    case "MANY_DUPLICATES_WITH_OUTLIERS":
      return (mix64(i) & 1023n) === 0n ? mix64(i) : (mix64(i) & 15n);
    case "ALTERNATING_LOW_HIGH":
      return (i & 1n) === 0n ? scaleOrderedKey(i >> 1n, n) : scaleOrderedKey(n - 1n - (i >> 1n), n);
    case "MIN_MAX_ALTERNATING":
    case "EXTREME_VALUES":
      return (i & 1n) === 0n ? 0x8000000000000000n : 0x7FFFFFFFFFFFFFFFn;
    case "NEGATIVE_VALUES":
      return mix64(i);
    case "INTEGER_OVERFLOW_RISK":
      return u64(0x7FFFFFFFFFFFFFFFn - i);
    case "GAUSSIAN": {
      const x = mix64(i) & 0xFFFFn;
      const y = mix64(i + 1n) & 0xFFFFn;
      return scaleOrderedKey(x + y, 1n << 17n);
    }
    case "EXPONENTIAL": {
      const x = mix64(i);
      return scaleOrderedKey(BigInt(leadingZeros64(x | 1n)), 64n);
    }
    case "BIMODAL": {
      const x = mix64(i);
      return (x & 1n) === 0n ? scaleOrderedKey(x & 0xFFFFn, 1n << 16n) : scaleOrderedKey((x & 0xFFFFn) + (1n << 16n), 1n << 17n);
    }
    case "POWER_LAW": {
      const x = mix64(i);
      const lz = leadingZeros64(x | 1n);
      return 1n << BigInt(63 - lz);
    }
    case "DELAYED_ENTROPY":
      return 0x123456789ABC0000n | (mix64(i) & 0xFFFFn);
    case "CLUSTERED_KEYS":
      return ((i / 1024n) << 32n) | (mix64(i) & 0x3FFn);
    case "BIT_REVERSAL":
      return reverseBits64(i);
    case "STRIDED":
      return u64(i * SEED);
    case "SPIKE_NOISE":
      return (i & 0xFFFFn) === 0n ? mix64(i) : 42n;
    case "DESCENDING_BLOCKS":
      return ((i / 1024n) << 10n) | (1023n - (i % 1024n));
    case "DENSE_16BIT":
      return mix64(i) & 0xFFFFn;
    case "ENTROPY_BANDS":
      return (((i / 1024n) & 1n) === 0n) ? i : mix64(i);
    case "TINY_PARTITIONS_STRESS":
      return ((i % 128n) << 32n) | (mix64(i) & 0xFFFFFFFFn);
    case "ENTROPY_OSCILLATION":
      return (((i >> 6n) & 1n) === 0n) ? mix64(i) : 0x1234567812345678n;
    case "SINGLE_BIT_TAIL_ENTROPY":
      return 0xCAFEBABE00000000n | (mix64(i) & 1n);
    case "SPARSE_ENTROPY_EXPLOSION": {
      const x = mix64(i);
      let k = 0n;
      if (x & (1n << 0n)) k |= 1n << 0n;
      if (x & (1n << 1n)) k |= 1n << 7n;
      if (x & (1n << 2n)) k |= 1n << 13n;
      if (x & (1n << 3n)) k |= 1n << 24n;
      if (x & (1n << 4n)) k |= 1n << 31n;
      if (x & (1n << 5n)) k |= 1n << 40n;
      if (x & (1n << 6n)) k |= 1n << 52n;
      if (x & (1n << 7n)) k |= 1n << 61n;
      return k;
    }
    case "WORST_CASE_MSD_COLLAPSE":
      return i < n - 1000n ? 0x1000000000000000n : mix64(i);
    case "ALTERNATING_BUCKET_DESTINATIONS":
      return (i & 1n) === 0n ? 0n : 0xFF00000000000000n;
    case "EXTREME_DUPLICATE_DENSITY":
      return [0x1111111111111111n, 0x2222222222222222n, 0x3333333333333333n, 0x4444444444444444n][Number(mix64(i) & 3n)];
    case "HIGH_ENTROPY_PREFIX_CONSTANT_TAIL":
      return (mix64(i) & ~((1n << 40n) - 1n)) | 0x123456789An;
    case "SIGN_BIT_BOUNDARY":
      return u64(0x8000000000000000n + i);
    case "CACHE_THRASH":
      return mix64(i * SEED);
    case "CROSS_THREAD_BUCKET_SKEW":
      return i < (n * 95n) / 100n ? 0n : mix64(i);
    case "TUPLE_29BIT":
      return mix64(i) & ((1n << 29n) - 1n);
    case "TUPLE_30BIT":
      return mix64(i) & ((1n << 30n) - 1n);
    case "TUPLE_31BIT_OVERFLOW":
      return mix64(i) & ((1n << 31n) - 1n);
    case "PERMUTATION_STRESS":
      return mix64(i ^ SEED);
    case "SINGLE_HOT_BUCKET":
      return (mix64(i) & 0xFFFFn) === 0n ? mix64(i) : 0x7777777777777777n;
    case "RADIX_PATHOLOGICAL":
      return (i & 1n) === 0n ? (mix64(i) & 0x00000000FFFFFFFFn) : (mix64(i) & 0xFFFFFFFF00000000n);
    case "PREFIX_CONSTANT_RANDOM_TAIL":
      return 0xABCDEF1234000000n | (mix64(i) & 0xFFFFFFn);
    case "TWO_BUCKET_COLLISION":
      return (mix64(i) & 1n) === 0n ? 0x0100000000000000n : 0x0200000000000000n;
    case "LARGE_EQUAL_REGION_WITH_RANDOM_END":
      return i < n - 8192n ? 0x5555555555555555n : mix64(i);
    case "BIT_SPARSE_POWERLAW": {
      const x = mix64(i);
      const bit = leadingZeros64(x | 1n);
      return 1n << BigInt(63 - bit);
    }
    case "MICRO_CLUSTERS":
      return ((i / 16n) << 8n) | (mix64(i) & 0xFn);
    case "INTERLEAVED_SORTED_RANDOM":
      return (i & 1n) === 0n ? scaleOrderedKey(i >> 1n, n) : mix64(i);
    case "SAWTOOTH_DESCENDING": {
      const period = 1024n;
      return scaleOrderedKey(period - 1n - (i % period), period);
    }
    case "LOW_CARDINALITY_HIGH_VOLUME":
      return mix64(i) & 7n;
    case "HIGH_BIT_ONLY_MOVING":
      return (mix64(i) & 1n) << 63n;
    case "ALMOST_SORTED_WITH_SPIKES":
      return (i & 0xFFFFn) === 0n ? mix64(i) : scaleOrderedKey(i, n);
    default:
      return mix64(i);
  }
}

function generateRecords(cfg) {
  if (cfg.mode === "EMPTY") return [];
  let count = cfg.count;
  if (cfg.mode === "SINGLE_ELEMENT") count = 1;
  if (cfg.mode === "TWO_ELEMENTS_SORTED" || cfg.mode === "TWO_ELEMENTS_REVERSED") count = 2;
  const out = [];
  for (let i = 0; i < count; i++) out.push(makeRecord(keyForMode(cfg.mode, i, count), i));
  return out;
}

function compareRecords(a, b) {
  if (a.key < b.key) return -1;
  if (a.key > b.key) return 1;
  return a.sourceIndex - b.sourceIndex;
}

function detectMonotonicOrder(records) {
  if (records.length <= 1) return "ascending";
  let ascending = true;
  let descending = true;
  for (let i = 1; i < records.length; i++) {
    const prev = records[i - 1].key;
    const key = records[i].key;
    ascending &&= prev <= key;
    descending &&= prev >= key;
  }
  if (ascending) return "ascending";
  if (descending) return "descending";
  return "mixed";
}

function buildMsdHistograms(records, cfg, msdShift) {
  const bucketCount = 1 << cfg.msdBits;
  const bucketMask = bucketCount - 1;
  const hist = new Array(bucketCount).fill(0);
  const orMasks = new Array(bucketCount).fill(0n);
  const andMasks = new Array(bucketCount).fill(U64_MASK);

  for (const rec of records) {
    const b = Number((rec.key >> BigInt(msdShift)) & BigInt(bucketMask));
    hist[b]++;
    orMasks[b] |= rec.key;
    andMasks[b] &= rec.key;
  }

  return { hist, orMasks, andMasks, order: detectMonotonicOrder(records) };
}

function tupleSpaceFitsDirectPass(entropyMask, size = Number.MAX_SAFE_INTEGER, cfg = state.cfg) {
  const bits = bitCount64(entropyMask);
  return bits > 1 && bits <= cfg.tupleBits && (1 << bits) <= size;
}

function tupleRadix(entropyMask) {
  return 1 << bitCount64(entropyMask);
}

function buildSmallTuplePlan(entropyMask) {
  const tupleBits = bitCount64(entropyMask);
  if (tupleBits <= 1 || tupleBits > SMALL_TUPLE_LOOKUP_BITS) return 0n;
  let plan = BigInt(tupleBits);
  let outShift = 4n;
  let mask = entropyMask;
  while (mask !== 0n) {
    const bit = mask & -mask;
    plan |= BigInt(trailingZeros64(bit)) << outShift;
    mask ^= bit;
    outShift += 6n;
  }
  return plan;
}

function contiguousShift(bitMask) {
  const shift = trailingZeros64(bitMask);
  const bits = bitCount64(bitMask);
  return ((bitMask >> BigInt(shift)) === lowBitsMask(bits)) ? shift : -1;
}

function buildPackedTupleLsdCyclePlan(variableMask, cfg) {
  const cycles = [];
  let bitsInCycle = 0;
  let bitMask = 0n;
  let mask = variableMask;

  while (mask !== 0n) {
    const bit = mask & -mask;
    mask ^= bit;
    bitMask |= bit;
    bitsInCycle++;

    if (bitsInCycle === cfg.lsdBits || mask === 0n) {
      const shift = contiguousShift(bitMask);
      cycles.push({
        shift,
        mask: lowIntMask(bitsInCycle),
        bitMask,
        tuplePlan: shift < 0 ? buildSmallTuplePlan(bitMask) : 0n
      });
      bitMask = 0n;
      bitsInCycle = 0;
    }
  }

  return cycles;
}

function buildContiguousLsdCyclePlan(variableMask, cfg, remainingBits) {
  let mask = variableMask & lowBitsMask(remainingBits);
  const cycles = [];

  while (mask !== 0n) {
    const runStart = trailingZeros64(mask);
    const shifted = mask >> BigInt(runStart);
    let runLength = 0;
    while (((shifted >> BigInt(runLength)) & 1n) === 1n) runLength++;
    const runEnd = runStart + runLength;

    for (let shift = runStart; shift < runEnd; shift += cfg.lsdBits) {
      const bitsThisCycle = Math.min(cfg.lsdBits, runEnd - shift);
      cycles.push({
        shift,
        mask: lowIntMask(bitsThisCycle),
        bitMask: lowBitsMask(bitsThisCycle) << BigInt(shift),
        tuplePlan: 0n
      });
    }

    mask &= runEnd >= 64 ? 0n : (U64_MASK << BigInt(runEnd)) & U64_MASK;
  }

  return cycles;
}

function buildLsdCyclePlan(variableMask, cfg, remainingBits) {
  const masked = variableMask & lowBitsMask(remainingBits);
  const contiguous = buildContiguousLsdCyclePlan(masked, cfg, remainingBits);
  const packedCount = bitCount64(masked) === 0 ? 0 : Math.ceil(bitCount64(masked) / cfg.lsdBits);
  if (PACKED_TUPLE_CYCLES || packedCount < contiguous.length) {
    return buildPackedTupleLsdCyclePlan(masked, cfg);
  }
  return contiguous;
}

function plannedCyclePrefixBeforeTupleTail(variableMask, cycleBitMasks, cycles, size, cfg) {
  if (tupleSpaceFitsDirectPass(variableMask, size, cfg)) return 0;
  let consumed = 0n;
  for (let prefix = 1; prefix < cycles; prefix++) {
    consumed |= cycleBitMasks[prefix - 1];
    if (tupleSpaceFitsDirectPass(variableMask & ~consumed, size, cfg)) return prefix;
  }
  return cycles;
}

function tupleTailMaskAfterPrefix(variableMask, cycleBitMasks, prefix, size, cfg) {
  let consumed = 0n;
  for (let i = 0; i < prefix; i++) consumed |= cycleBitMasks[i];
  const tailMask = variableMask & ~consumed;
  return tupleSpaceFitsDirectPass(tailMask, size, cfg) ? tailMask : 0n;
}

function buildMsdBucketPlan(histResult, records, cfg, msdShift) {
  const bucketCount = 1 << cfg.msdBits;
  const plan = {
    starts: new Array(bucketCount).fill(0),
    sizes: new Array(bucketCount).fill(0),
    buckets: new Array(bucketCount),
    bucketFlags: new Array(bucketCount).fill(BUCKET_EMPTY),
    msdShift,
    totalRecords: records.length,
    inputAscending: histResult.order === "ascending",
    inputDescending: histResult.order === "descending",
    variableMasks: new Array(bucketCount).fill(0n),
    cycleCounts: new Array(bucketCount).fill(0),
    cycleShifts: new Array(bucketCount).fill(null),
    cycleMasks: new Array(bucketCount).fill(null),
    cycleBitMasks: new Array(bucketCount).fill(null),
    cycleTuplePlans: new Array(bucketCount).fill(null),
    tupleTailMasks: new Array(bucketCount).fill(0n),
    tupleTailPlans: new Array(bucketCount).fill(0n),
    hasLocalMsd: false
  };

  let pos = 0;
  const lowerKeyMask = lowBitsMask(msdShift);

  for (let b = 0; b < bucketCount; b++) {
    const size = histResult.hist[b];
    const seenAny = size > 0;
    const variableMask = size > 1 ? ((histResult.orMasks[b] ^ histResult.andMasks[b]) & lowerKeyMask) : 0n;

    plan.starts[b] = pos;
    plan.sizes[b] = size;
    plan.variableMasks[b] = variableMask;
    plan.buckets[b] = { id: b, start: pos, size, records: [] };
    pos += size;

    if (!seenAny) {
      plan.bucketFlags[b] = BUCKET_EMPTY;
    } else if (size === 1 || variableMask === 0n) {
      plan.bucketFlags[b] = BUCKET_ALL_EQUAL;
    } else {
      plan.bucketFlags[b] = BUCKET_MIXED;
    }

    if (plan.bucketFlags[b] !== BUCKET_MIXED) continue;
    if (size < cfg.tinyThreshold) continue;

    if (cfg.tuplesEnabled && tupleSpaceFitsDirectPass(variableMask, size, cfg)) {
      plan.tupleTailMasks[b] = variableMask;
      plan.tupleTailPlans[b] = buildSmallTuplePlan(variableMask);
      continue;
    }

    const cycles = buildLsdCyclePlan(variableMask, cfg, msdShift);
    if (cycles.length === 0) {
      plan.bucketFlags[b] = BUCKET_ALL_EQUAL;
      continue;
    }

    const cycleBitMasks = cycles.map(c => c.bitMask);
    const plannedCycles = cfg.tuplesEnabled
      ? plannedCyclePrefixBeforeTupleTail(variableMask, cycleBitMasks, cycles.length, size, cfg)
      : cycles.length;
    const tupleTailMask = cfg.tuplesEnabled
      ? tupleTailMaskAfterPrefix(variableMask, cycleBitMasks, plannedCycles, size, cfg)
      : 0n;
    const planned = cycles.slice(0, plannedCycles);

    plan.cycleCounts[b] = planned.length;
    plan.cycleShifts[b] = planned.map(c => c.shift);
    plan.cycleMasks[b] = planned.map(c => c.mask);
    plan.cycleBitMasks[b] = planned.map(c => c.bitMask);
    plan.cycleTuplePlans[b] = planned.map(c => c.tuplePlan);
    plan.tupleTailMasks[b] = tupleTailMask;
    plan.tupleTailPlans[b] = buildSmallTuplePlan(tupleTailMask);
  }

  if (pos !== records.length) throw new Error(`Histogram mismatch: ${pos} != ${records.length}`);
  return plan;
}

function largestBucketSize(plan) {
  return plan.sizes.reduce((m, s) => Math.max(m, s), 0);
}

function collapsedPlanVariableMask(plan, cfg) {
  let variableMask = 0n;
  for (let b = 0; b < (1 << cfg.msdBits); b++) {
    if (plan.sizes[b] !== 0) variableMask |= plan.variableMasks[b];
  }
  return variableMask;
}

function prefixAboveWindowIsConstant(variableMask, firstVariableBit) {
  if (firstVariableBit >= 64) return true;
  return (variableMask & ~lowBitsMask(firstVariableBit)) === 0n;
}

function windowContainsVariableBits(variableMask, shift, bits) {
  return (variableMask & (lowBitsMask(bits) << BigInt(shift))) !== 0n;
}

function msdShiftCandidates(cfg) {
  const shifts = new Set();
  const topShift = 64 - cfg.msdBits;
  for (let shift = topShift; shift > 0; shift = Math.max(0, shift - cfg.msdBits)) {
    shifts.add(shift);
    if (shift <= cfg.msdBits) {
      shifts.add(0);
      break;
    }
  }
  for (const shift of [32, 16, 8, 0]) {
    if (shift >= 0 && shift <= topShift) shifts.add(shift);
  }
  return [...shifts].sort((a, b) => b - a);
}

function buildAdaptiveMsdBucketPlan(records, cfg) {
  const topShift = 64 - cfg.msdBits;
  const topPlan = buildMsdBucketPlan(buildMsdHistograms(records, cfg, topShift), records, cfg, topShift);

  if (largestBucketSize(topPlan) !== records.length) return topPlan;

  const variableMask = collapsedPlanVariableMask(topPlan, cfg);
  if (variableMask === 0n) return topPlan;

  for (const shift of msdShiftCandidates(cfg)) {
    if (shift === topShift) continue;
    if (!prefixAboveWindowIsConstant(variableMask, shift + cfg.msdBits)) continue;
    if (shift !== 0 && !windowContainsVariableBits(variableMask, shift, cfg.msdBits)) continue;

    const plan = buildMsdBucketPlan(buildMsdHistograms(records, cfg, shift), records, cfg, shift);
    if (largestBucketSize(plan) !== records.length || shift === 0) return plan;
  }

  return topPlan;
}

function scatterIntoMsdBuckets(records, plan, cfg) {
  const bucketCount = 1 << cfg.msdBits;
  const bucketMask = BigInt(bucketCount - 1);
  const buckets = Array.from({ length: bucketCount }, (_, id) => []);

  for (const rec of records) {
    const id = Number((rec.key >> BigInt(plan.msdShift)) & bucketMask);
    buckets[id].push(rec);
  }

  const out = [];
  for (let b = 0; b < bucketCount; b++) {
    plan.buckets[b].records = buckets[b];
    out.push(...buckets[b]);
  }
  applyBucketOrderFlags(plan, cfg);
  return out;
}

function applyBucketOrderFlags(plan, cfg) {
  for (let b = 0; b < (1 << cfg.msdBits); b++) {
    const size = plan.sizes[b];
    const records = plan.buckets[b].records;

    if (size === 0) {
      plan.bucketFlags[b] = BUCKET_EMPTY;
      clearBucketRefinementPlan(plan, b);
      continue;
    }

    if (size === 1 || plan.variableMasks[b] === 0n) {
      plan.bucketFlags[b] = BUCKET_ALL_EQUAL;
      clearBucketRefinementPlan(plan, b);
      continue;
    }

    const order = detectMonotonicOrder(records);
    if (order === "ascending") {
      plan.bucketFlags[b] = BUCKET_ASCENDING;
      clearBucketRefinementPlan(plan, b);
    } else if (order === "descending") {
      plan.bucketFlags[b] = BUCKET_DESCENDING;
      clearBucketRefinementPlan(plan, b);
    } else {
      plan.bucketFlags[b] = BUCKET_MIXED;
    }
  }
}

function clearBucketRefinementPlan(plan, b) {
  plan.cycleCounts[b] = 0;
  plan.cycleShifts[b] = null;
  plan.cycleMasks[b] = null;
  plan.cycleBitMasks[b] = null;
  plan.cycleTuplePlans[b] = null;
  plan.tupleTailMasks[b] = 0n;
  plan.tupleTailPlans[b] = 0n;
}

function bucketHasLsdWork(plan, cfg, b) {
  if (plan.sizes[b] <= 1) return false;
  if (plan.bucketFlags[b] === BUCKET_DESCENDING) return true;
  return plan.bucketFlags[b] === BUCKET_MIXED &&
    plan.variableMasks[b] !== 0n &&
    (plan.sizes[b] < cfg.tinyThreshold || plan.cycleCounts[b] > 0 || plan.tupleTailMasks[b] !== 0n);
}

function buildLsdWorkBucketsByDescendingSize(plan, cfg) {
  const buckets = [];
  for (let b = 0; b < (1 << cfg.msdBits); b++) {
    if (bucketHasLsdWork(plan, cfg, b)) buckets.push(b);
  }
  buckets.sort((a, b) => plan.sizes[b] - plan.sizes[a]);
  return buckets;
}

function digit(key, shift, mask, bitMask) {
  if (shift >= 0) return Number((key >> BigInt(shift)) & BigInt(mask));
  return Number(compressBits(key, bitMask));
}

function compressBits(key, entropyMask) {
  let out = 0n;
  let outBit = 1n;
  let mask = entropyMask;
  while (mask !== 0n) {
    const bit = mask & -mask;
    if ((key & bit) !== 0n) out |= outBit;
    outBit <<= 1n;
    mask ^= bit;
  }
  return out;
}

function countingPass(records, pass) {
  const bins = new Map();
  for (const rec of records) {
    const id = digit(rec.key, pass.shift, pass.mask, pass.bitMask);
    if (!bins.has(id)) bins.set(id, []);
    bins.get(id).push(rec);
  }
  const orderedBins = [...bins.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([id, bucketRecords]) => ({ id, records: bucketRecords }));
  return { records: orderedBins.flatMap(bin => bin.records), bins: orderedBins };
}

function tinySort(records) {
  return [...records].sort(compareRecords);
}

function tupleCountingPass(records, entropyMask) {
  const bins = new Map();
  for (const rec of records) {
    const id = Number(compressBits(rec.key, entropyMask));
    if (!bins.has(id)) bins.set(id, []);
    bins.get(id).push(rec);
  }
  const orderedBins = [...bins.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([id, bucketRecords]) => ({ id, records: bucketRecords }));
  return { records: orderedBins.flatMap(bin => bin.records), bins: orderedBins };
}

function routeForBucket(plan, cfg, b) {
  const size = plan.sizes[b];
  if (plan.bucketFlags[b] === BUCKET_DESCENDING) return "reverse";
  if (!bucketHasLsdWork(plan, cfg, b)) return "done";
  if (size < cfg.tinyThreshold) return "tiny";
  if (plan.cycleCounts[b] === 0 && plan.tupleTailMasks[b] !== 0n) return "tuple-direct";
  if (plan.cycleCounts[b] > 0 && plan.tupleTailMasks[b] !== 0n) return "lsd-tuple-tail";
  if (plan.cycleCounts[b] > 0) return "lsd";
  return "done";
}

function bucketSummary(plan, cfg, b) {
  const size = plan.sizes[b];
  const flag = bucketFlagLabel(plan.bucketFlags[b]);
  const route = routeForBucket(plan, cfg, b);
  return `bucket ${b} | size ${size} | ${flag} | ${route}`;
}

function bucketFlagLabel(flag) {
  if (flag === BUCKET_EMPTY) return "empty";
  if (flag === BUCKET_ALL_EQUAL) return "all equal";
  if (flag === BUCKET_ASCENDING) return "bucket ascending";
  if (flag === BUCKET_DESCENDING) return "bucket descending";
  return "mixed";
}

function buildExecutionSteps(plan, cfg, msdRecords) {
  const steps = [{
    phase: "source",
    title: "Input records loaded",
    status: "Input scan complete",
    active: null,
    records: state.records
  }];

  steps.push(...makeMsdScatterSteps(state.records, plan, cfg));

  steps.push({
    phase: "msd",
    title: `MSD scatter using shift ${plan.msdShift}`,
    status: `MSD buckets built: ${plan.sizes.filter(Boolean).length} non-empty of ${1 << cfg.msdBits}`,
    active: null,
    records: msdRecords
  });

  if (plan.inputAscending) {
    steps.push({
      phase: "done",
      title: "Global ascending fast path",
      status: "Global ascending: MSD/LSD skipped by input order fast path",
      active: null,
      records: state.records
    });
    return steps;
  }

  if (plan.inputDescending) {
    steps.push({
      phase: "done",
      title: "Global reverse",
      status: "Global reverse: whole input reversed",
      active: null,
      records: [...state.records].reverse()
    });
    return steps;
  }

  const bucketResults = new Map();
  const jobsByBucket = new Map();
  for (let b = 0; b < (1 << cfg.msdBits); b++) {
    if (!bucketNeedsRefinementWork(plan, cfg, b)) continue;
    const job = makeBucketJob(plan, cfg, b);
    bucketResults.set(b, job.finalRecords);
    jobsByBucket.set(b, job);
  }

  const visualWorkers = Math.min(cfg.workers, Math.max(1, 1 << cfg.msdBits));
  plan.visualWorkers = visualWorkers;

  const scheduled = cfg.workStealing
    ? makeWorkStealingScheduledSteps(plan, cfg, jobsByBucket, visualWorkers)
    : makeStrideScheduledSteps(plan, cfg, jobsByBucket, visualWorkers);

  steps.push(...scheduled);

  const finalRecords = [];
  for (let b = 0; b < (1 << cfg.msdBits); b++) {
    finalRecords.push(...(bucketResults.get(b) || plan.buckets[b].records));
  }
  state.finalRecords = finalRecords;

  steps.push({
    phase: "done",
    title: "Sorted output complete",
    status: verifySorted(finalRecords) ? "Verified sorted by unsigned 64-bit key" : "Verifier failed: output order mismatch",
    active: null,
    records: finalRecords
  });

  return steps;
}

function makeMsdScatterSteps(records, plan, cfg) {
  const workers = Math.min(cfg.workers, Math.max(1, records.length));
  const chunk = Math.floor(records.length / workers);
  const bucketMask = BigInt((1 << cfg.msdBits) - 1);
  const targetTicks = Math.max(1, Math.min(MAX_SCATTER_STEPS, Math.max(12, Math.ceil(records.length / Math.max(1, workers * 8)))));
  const queues = Array.from({ length: workers }, (_, worker) => {
    const start = worker * chunk;
    const end = worker === workers - 1 ? records.length : start + chunk;
    const queue = [];
    const batchSize = Math.max(1, Math.ceil((end - start) / targetTicks));

    for (let batchStart = start; batchStart < end; batchStart += batchSize) {
      const batchEnd = Math.min(end, batchStart + batchSize);
      const bucketCounts = new Map();
      const samples = [];
      const sampleStep = Math.max(1, Math.ceil((batchEnd - batchStart) / 12));

      for (let i = batchStart; i < batchEnd; i++) {
        const record = records[i];
        const bucket = Number((record.key >> BigInt(plan.msdShift)) & bucketMask);
        bucketCounts.set(bucket, (bucketCounts.get(bucket) || 0) + 1);
        if ((i - batchStart) % sampleStep === 0 || i === batchEnd - 1) {
          samples.push({ index: i, bucket, record });
        }
      }

      const buckets = [...bucketCounts.keys()].sort((a, b) => a - b);
      queue.push({
        phase: "msdScatter",
        worker,
        index: batchStart,
        count: batchEnd - batchStart,
        batchStart,
        batchEnd,
        bucket: buckets[0],
        buckets,
        samples,
        status: `Thread ${worker}: records ${batchStart}-${batchEnd - 1} -> ${buckets.length} MSD bucket${buckets.length === 1 ? "" : "s"}`
      });
    }

    return queue;
  });
  plan.scatterWorkers = workers;
  plan.scatterBatchRecords = Math.max(1, Math.ceil(records.length / Math.max(1, workers * targetTicks)));

  const steps = [];
  let tick = 0;
  while (queues.some(queue => queue.length > 0)) {
    const scatterOps = queues.map((queue, worker) => queue.shift() || {
      phase: "scatterIdle",
      worker,
      status: `Thread ${worker}: MSD scatter idle`
    });
    const active = scatterOps.filter(op => op.phase === "msdScatter").length;
    steps.push({
      phase: "parallelMsd",
      tick,
      scatterOps,
      title: `Parallel MSD scatter tick ${tick + 1}`,
      status: `Parallel MSD scatter tick ${tick + 1}: ${active} thread lanes read batches and dropped records into buckets.`,
      active: null,
      records
    });
    tick++;
  }

  return steps;
}

function makeBucketJob(plan, cfg, b) {
  let working = [...plan.buckets[b].records];
  const base = { bucket: b, start: plan.starts[b], size: plan.sizes[b], summary: bucketSummary(plan, cfg, b) };
  const ops = [];

  if (plan.bucketFlags[b] === BUCKET_DESCENDING && working.length > 1) {
    const reverse = makeReverseVisualOps(working, base);
    working = reverse.records;
    ops.push(...reverse.ops);
    return { bucket: b, cost: estimateBucketCost(plan, cfg, b), ops, finalRecords: working };
  }

  if (!bucketHasLsdWork(plan, cfg, b)) {
    return { bucket: b, cost: 1, ops, finalRecords: working };
  }

  if (plan.sizes[b] < cfg.tinyThreshold) {
    const tiny = makeTinySortVisualOps(working, base, cfg);
    working = tiny.records;
    ops.push(...tiny.ops);
    return { bucket: b, cost: estimateBucketCost(plan, cfg, b), ops, finalRecords: working };
  }

  if (plan.cycleCounts[b] === 0 && plan.tupleTailMasks[b] !== 0n) {
    const tuple = makeTupleVisualOps(working, plan.tupleTailMasks[b], base, "Direct tuple pass");
    working = tuple.records;
    ops.push(...tuple.ops);
    return { bucket: b, cost: estimateBucketCost(plan, cfg, b), ops, finalRecords: working };
  }

  for (let c = 0; c < plan.cycleCounts[b]; c++) {
    const pass = {
      shift: plan.cycleShifts[b][c],
      mask: plan.cycleMasks[b][c],
      bitMask: plan.cycleBitMasks[b][c],
      tuplePlan: plan.cycleTuplePlans[b][c]
    };
    const result = countingPass(working, pass);
    working = result.records;
    ops.push({
      phase: "lsd",
      lanePhase: "lsd",
      bucket: b,
      records: visualRecords(working),
      active: { ...base, cycle: c, pass, bins: visualBins(result.bins) },
      title: `LSD cycle ${c + 1}/${plan.cycleCounts[b]} ${base.summary}`,
      status: pass.shift >= 0
        ? `LSD contiguous pass: bits ${pass.shift}..${pass.shift + Math.log2(pass.mask + 1) - 1}`
        : `LSD sparse tuple cycle: ${bitCount64(pass.bitMask)} compressed bits`
    });
  }

  if (plan.tupleTailMasks[b] !== 0n) {
    const tuple = makeTupleVisualOps(working, plan.tupleTailMasks[b], base, "Tuple-tail pass");
    working = tuple.records;
    ops.push(...tuple.ops);
  }

  markLastOpComplete(ops);
  return { bucket: b, cost: estimateBucketCost(plan, cfg, b), ops, finalRecords: working };
}

function bucketNeedsRefinementWork(plan, cfg, b) {
  if (bucketHasLsdWork(plan, cfg, b)) return true;
  return false;
}

function markLastOpComplete(ops) {
  if (ops.length) ops[ops.length - 1].completesBucket = true;
  return ops;
}

function makeReverseVisualOps(records, base) {
  const out = [...records];
  const ops = [];
  const maxVisualOps = 512;
  const pairs = Math.floor(out.length / 2);
  const step = Math.max(1, Math.ceil(pairs / maxVisualOps));

  for (let left = 0; left < pairs; left++) {
    const right = out.length - 1 - left;
    const tmp = out[left];
    out[left] = out[right];
    out[right] = tmp;

    if (left % step === 0 || left === pairs - 1) {
      ops.push({
        phase: "reverse",
        lanePhase: "reverse",
        bucket: base.bucket,
        records: visualRecords(out),
        active: { ...base, left, right, reverseKind: "bucket" },
        title: `Bucket reverse ${base.summary}`,
        status: `Bucket reverse: bucket ${base.bucket}, swap ${left} <-> ${right}`
      });
    }
  }

  return { ops: markLastOpComplete(ops), records: out };
}

function makeGlobalReverseSteps(records, cfg) {
  const visualWorkers = Math.min(cfg.workers, Math.max(1, records.length));
  const out = [...records];
  const pairs = Math.floor(out.length / 2);
  const queues = Array.from({ length: visualWorkers }, () => []);
  const maxVisualOps = Math.min(MAX_SCATTER_STEPS, Math.max(24, visualWorkers * 24));
  const pairStep = Math.max(1, Math.ceil(pairs / maxVisualOps));

  for (let left = 0; left < pairs; left++) {
    const right = out.length - 1 - left;
    const tmp = out[left];
    out[left] = out[right];
    out[right] = tmp;
    if (left % pairStep !== 0 && left !== pairs - 1) continue;
    const worker = left % visualWorkers;
    queues[worker].push({
      phase: "reverse",
      lanePhase: "reverse",
      worker,
      bucket: "input",
      records: visualRecords(out),
      active: { reverseKind: "global", left, right, size: records.length },
      title: "Global reverse input",
      status: `Global reverse: input sampled swap ${left} <-> ${right}`
    });
  }

  const steps = [];
  let tick = 0;
  while (queues.some(queue => queue.length > 0)) {
    const workerOps = queues.map((queue, worker) => queue.shift() || idleOp(worker));
    steps.push({
      phase: "parallelWork",
      tick,
      workerOps,
      status: `Global reverse tick ${tick + 1}: ${workerOps.filter(op => op.phase === "reverse").length} lanes active.`,
      title: `Global reverse tick ${tick + 1}`,
      active: null,
      records: []
    });
    tick++;
  }

  return steps;
}

function makeTinySortVisualOps(records, base, cfg) {
  const profile = tinySortProfile(records.length);
  if (profile.id === "insertion") return makeTinyInsertionVisualOps(records, base, profile);
  if (profile.id === "binary") return makeTinyBinaryInsertionVisualOps(records, base, profile);
  return makeSampledTinyVisualOps(records, base, profile);
}

function makeTinyInsertionVisualOps(records, base, profile) {
  const arr = [...records];
  const ops = [];
  const maxVisualOps = 512;

  const push = (phase, i, j, key) => {
    if (ops.length >= maxVisualOps) return;
    ops.push({
      phase: "tiny",
      lanePhase: "tiny",
      bucket: base.bucket,
      records: visualRecords(arr),
      active: { ...base, i, j, key, tinyTier: tinySortTier(records.length), tinyProfile: profile },
      title: `${tinySortTier(records.length)} ${base.summary}`,
      status: `${profile.label} (${profile.range}) ${phase}: bucket ${base.bucket}, size ${records.length}, i=${i}, j=${j}`
    });
  };

  push("start", 0, 0, arr[0]?.key ?? 0n);
  for (let i = 1; i < arr.length; i++) {
    let j = i;
    const tmp = arr[i];
    push("compare", i, j, tmp.key);

    while (j > 0 && compareRecords(tmp, arr[j - 1]) < 0) {
      arr[j] = arr[j - 1];
      j--;
      push("shift", i, j, tmp.key);
    }

    arr[j] = tmp;
    push("insert", i, j, tmp.key);
  }

  const sorted = tinySort(arr);
  if (ops.length >= maxVisualOps) {
    ops.push({
      phase: "tiny",
      lanePhase: "tiny",
      bucket: base.bucket,
      records: visualRecords(sorted),
      active: { ...base, tinyTier: tinySortTier(records.length), tinyProfile: profile },
      title: `${tinySortTier(records.length)} ${base.summary}`,
      status: `${profile.label} (${profile.range}) final: bucket ${base.bucket} sorted after sampled work.`
    });
  }

  return { ops: markLastOpComplete(ops), records: sorted };
}

function makeTinyBinaryInsertionVisualOps(records, base, profile) {
  const arr = [...records];
  const ops = [];
  const maxVisualOps = 512;
  const tier = tinySortTier(records.length);

  const push = (phase, details = {}) => {
    if (ops.length >= maxVisualOps) return;
    const suffix = details.text ? `, ${details.text}` : "";
    ops.push({
      phase: "tiny",
      lanePhase: "tiny",
      bucket: base.bucket,
      records: visualRecords(arr),
      active: { ...base, ...details, tinyTier: tier, tinyProfile: profile },
      title: `${tier} ${base.summary}`,
      status: `${profile.label} (${profile.range}) ${phase}: bucket ${base.bucket}, size ${records.length}${suffix}`
    });
  };

  push("start", { i: 0, j: 0 });
  for (let i = 1; i < arr.length; i++) {
    const tmp = arr[i];
    let lo = 0;
    let hi = i;
    push("probe", { i, lo, hi, key: tmp.key, text: `search ${lo}..${hi}` });

    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (compareRecords(arr[mid], tmp) <= 0) lo = mid + 1;
      else hi = mid;
      push("binary search", { i, lo, hi, mid, key: tmp.key, text: `mid=${mid}, target ${lo}` });
    }

    for (let j = i; j > lo; j--) arr[j] = arr[j - 1];
    arr[lo] = tmp;
    push("insert", { i, j: lo, key: tmp.key, text: `slot ${lo}` });
  }

  const sorted = tinySort(arr);
  if (ops.length >= maxVisualOps) {
    ops.push({
      phase: "tiny",
      lanePhase: "tiny",
      bucket: base.bucket,
      records: visualRecords(sorted),
      active: { ...base, tinyTier: tier, tinyProfile: profile },
      title: `${tier} ${base.summary}`,
      status: `${profile.label} (${profile.range}) final: bucket ${base.bucket} sorted after sampled work.`
    });
  }

  return { ops: markLastOpComplete(ops), records: sorted };
}

function makeSampledTinyVisualOps(records, base, profile) {
  const tier = tinySortTier(records.length);
  const sorted = tinySort(records);
  const ops = [{
    phase: "tiny",
    lanePhase: "tiny",
    bucket: base.bucket,
    records: visualRecords(records),
    active: { ...base, tinyTier: tier, tinyProfile: profile },
    title: `${tier} ${base.summary}`,
    status: `${profile.label} (${profile.range}) load: bucket ${base.bucket}, size ${records.length}, ${profile.detail}.`
  }];

  const bins = tinyFunctionBins(records, profile);
  if (bins.length > 1) {
    ops.push({
      phase: "tiny",
      lanePhase: "tiny",
      bucket: base.bucket,
      records: visualRecords(records),
      active: { ...base, tinyTier: tier, tinyProfile: profile, bins: visualBins(bins) },
      title: `${tier} ${base.summary}`,
      status: `${profile.label} (${profile.range}) partition: bucket ${base.bucket}, ${bins.length} visual group${bins.length === 1 ? "" : "s"}.`
    });
  }

  ops.push({
    phase: "tiny",
    lanePhase: "tiny",
    bucket: base.bucket,
    records: visualRecords(sorted),
    active: { ...base, tinyTier: tier, tinyProfile: profile },
    title: `${tier} ${base.summary}`,
    status: `${profile.label} (${profile.range}) final: bucket ${base.bucket} sorted by ${profile.detail}.`
  });

  return { ops: markLastOpComplete(ops), records: sorted };
}

function tinyFunctionBins(records, profile) {
  if (records.length === 0) return [];

  if (profile.id === "quick" || profile.id === "threeway") {
    const pivot = records[records.length >>> 1].key;
    const groups = new Map([
      [0, { id: profile.id === "quick" ? "less" : "less-than-pivot", records: [] }],
      [1, { id: "pivot/equal", records: [] }],
      [2, { id: profile.id === "quick" ? "greater" : "greater-than-pivot", records: [] }]
    ]);
    for (const rec of records) {
      const cmp = rec.key < pivot ? 0 : rec.key > pivot ? 2 : 1;
      groups.get(cmp).records.push(rec);
    }
    return [...groups.values()].filter(group => group.records.length > 0);
  }

  if (profile.id === "msd8") {
    const bins = new Map();
    for (const rec of records) {
      const digit = Number((rec.key >> 56n) & 0xffn);
      if (!bins.has(digit)) bins.set(digit, []);
      bins.get(digit).push(rec);
    }
    return orderedSparseBins(bins);
  }

  return [];
}

function tinySortTier(size) {
  const profile = tinySortProfile(size);
  return `Tiny ${profile.label}`;
}

function tinySortProfile(size) {
  return TINY_PROFILES.find(profile => size >= profile.min && size <= profile.max) || TINY_PROFILES[0];
}

function makeTupleVisualOps(records, entropyMask, base, label) {
  const radix = tupleRadix(entropyMask);
  const counts = new Map();
  for (const rec of records) {
    const bin = Number(compressBits(rec.key, entropyMask));
    counts.set(bin, (counts.get(bin) || 0) + 1);
  }

  const starts = new Map();
  const offsets = new Map();
  let prefix = 0;
  for (const bin of [...counts.keys()].sort((a, b) => a - b)) {
    starts.set(bin, prefix);
    offsets.set(bin, prefix);
    prefix += counts.get(bin);
  }

  const placed = new Array(records.length);
  const placedBins = new Map();
  const ops = [];
  const maxVisualOps = records.length > 100000 ? 96 : 768;
  const step = Math.max(1, Math.ceil(records.length / maxVisualOps));
  const tupleKind = label.toLowerCase().includes("tail") ? "tuple-tail" : "tuple-direct";

  for (let i = 0; i < records.length; i++) {
    const rec = records[i];
    const bin = Number(compressBits(rec.key, entropyMask));
    const targetIndex = offsets.get(bin);
    offsets.set(bin, targetIndex + 1);
    placed[targetIndex] = rec;
    if (!placedBins.has(bin)) placedBins.set(bin, []);
    placedBins.get(bin).push(rec);

    if (i % step === 0 || i === records.length - 1) {
      const orderedBins = orderedSparseBins(placedBins);
      ops.push({
        phase: "tuple",
        lanePhase: "tuple",
        bucket: base.bucket,
        records: visualRecords(placed.filter(Boolean)),
        active: {
          ...base,
          tupleMask: entropyMask,
          tupleKind,
          bins: visualBins(orderedBins),
          bin,
          record: rec,
          targetIndex,
          targetStart: starts.get(bin),
          targetEnd: starts.get(bin) + counts.get(bin)
        },
        title: `${label} ${base.summary}`,
        status: `${label}: OR^AND tuple mask ${keyLabel(entropyMask)} | compress(key)=${bin} -> slot ${base.start + targetIndex} of range ${base.start + starts.get(bin)}..${base.start + starts.get(bin) + counts.get(bin) - 1} | radix ${radix}`
      });
    }
  }

  const finalRecords = placed;
  return { ops: markLastOpComplete(ops), records: finalRecords };
}

function orderedSparseBins(bins) {
  return [...bins.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([id, bucketRecords]) => ({ id, records: bucketRecords }));
}

function estimateBucketCost(plan, cfg, b) {
  const size = plan.sizes[b];
  if (size <= 1) return 1;
  return size * Math.max(1, plan.cycleCounts[b] + (plan.tupleTailMasks[b] !== 0n ? 1 : 0));
}

function idleOp(worker) {
  return {
    phase: "workerIdle",
    lanePhase: "idle",
    worker,
    status: `Thread ${worker}: idle`
  };
}

function workStealOp(worker, job) {
  return {
    phase: "workSteal",
    lanePhase: job.ops[0]?.lanePhase || "lsd",
    worker,
    bucket: job.bucket,
    status: `Thread ${worker}: idle, stole bucket ${job.bucket} (${job.finalRecords.length} records) from thread ${job.from}.`
  };
}

function attachWorker(op, worker) {
  return {
    ...op,
    worker,
    active: op.active ? { ...op.active, worker } : op.active
  };
}

function makeParallelStep(tick, workerOps, cfg) {
  const active = workerOps.filter(op => op.phase !== "workerIdle");
  const steals = workerOps.filter(op => op.phase === "workSteal").length;
  const status = steals
    ? `Dispatch tick ${tick + 1}: ${steals} idle thread lane${steals === 1 ? "" : "s"} claimed remaining bucket work.`
    : `Dispatch tick ${tick + 1}: ${active.length} thread lanes active.`;

  return {
    phase: "parallelWork",
    tick,
    workerOps,
    status,
    title: status,
    active: null,
    records: []
  };
}

function makeStrideScheduledSteps(plan, cfg, jobsByBucket, visualWorkers) {
  const steps = [];
  const workerQueues = Array.from({ length: visualWorkers }, (_, worker) => {
    const ops = [];
    for (let b = worker; b < (1 << cfg.msdBits); b += visualWorkers) {
      const job = jobsByBucket.get(b);
      if (job) ops.push(...job.ops);
    }
    return ops;
  });

  let tick = 0;
  while (workerQueues.some(queue => queue.length > 0)) {
    const workerOps = workerQueues.map((queue, worker) =>
      queue.length ? attachWorker(queue.shift(), worker) : idleOp(worker)
    );
    steps.push(makeParallelStep(tick++, workerOps, cfg));
  }
  return steps;
}

function makeWorkStealingScheduledSteps(plan, cfg, jobsByBucket, visualWorkers) {
  const steps = [];
  const localJobs = Array.from({ length: visualWorkers }, (_, worker) => {
    const jobs = [];
    for (let b = worker; b < (1 << cfg.msdBits); b += visualWorkers) {
      const job = jobsByBucket.get(b);
      if (job) jobs.push(job);
    }
    return jobs;
  });
  const queues = Array.from({ length: visualWorkers }, () => []);

  let tick = 0;
  while (localJobs.some(jobs => jobs.length > 0) || queues.some(queue => queue.length > 0)) {
    const workerOps = queues.map((queue, worker) => {
      if (queue.length === 0) {
        let job = localJobs[worker].shift();
        let stolen = false;

        if (!job) {
          const steal = stealLargestRemainingJob(localJobs, worker);
          job = steal.job;
          stolen = steal.stolen;
        }

        if (job) {
          queue.push(...job.ops);
          if (stolen) return workStealOp(worker, job);
        }
      }

      return queue.length ? attachWorker(queue.shift(), worker) : idleOp(worker);
    });

    steps.push(makeParallelStep(tick++, workerOps, cfg));
  }

  return steps;
}

function stealLargestRemainingJob(localJobs, thief) {
  let bestWorker = -1;
  let bestIndex = -1;
  let bestCost = -1;

  for (let worker = 0; worker < localJobs.length; worker++) {
    if (worker === thief) continue;
    for (let i = 0; i < localJobs[worker].length; i++) {
      const job = localJobs[worker][i];
      if (job.cost > bestCost) {
        bestWorker = worker;
        bestIndex = i;
        bestCost = job.cost;
      }
    }
  }

  if (bestWorker === -1) return { job: null, stolen: false };

  const [job] = localJobs[bestWorker].splice(bestIndex, 1);
  return { job: { ...job, from: bestWorker }, stolen: true };
}

function verifySorted(records) {
  for (let i = 1; i < records.length; i++) {
    if (records[i - 1].key > records[i].key) return false;
  }
  return true;
}

function reset() {
  const cfg = state.cfg;
  cfg.count = clampInt(cfg.count, 0, MAX_RECORD_COUNT);
  cfg.msdBits = clampInt(cfg.msdBits, 2, 13);
  cfg.lsdBits = clampInt(cfg.lsdBits, 2, 17);
  cfg.workers = clampInt(cfg.workers, 1, 64);
  cfg.tinyThreshold = normalizeTinyThreshold(cfg.tinyThreshold);
  cfg.tupleBits = clampInt(cfg.tupleBits, 2, 16);

  state.records = generateRecords(cfg);
  state.keyRange = computeKeyRange(state.records);
  state.finalRecords = [];
  state.inputOrder = "mixed";
  state.stepIndex = 0;
  state.playing = false;
  state.hasStarted = false;
  const playBtn = document.getElementById("playBtn");
  if (playBtn) playBtn.textContent = "Play";

  if (state.records.length === 0) {
    state.plan = null;
    state.msdRecords = [];
    state.inputOrder = "empty";
    state.steps = [{ phase: "done", title: "EMPTY dataset", status: "Nothing to sort", active: null, records: [] }];
    render();
    return;
  }

  const inputOrder = detectMonotonicOrder(state.records);
  state.inputOrder = inputOrder;
  if (inputOrder === "ascending") {
    state.plan = null;
    state.msdRecords = [...state.records];
    state.finalRecords = [...state.records];
    state.steps = [
      {
        phase: "source",
        title: "Global order scan",
        status: "Global ascending: input already sorted",
        active: null,
        records: state.records
      },
      {
        phase: "done",
        title: "Global ascending fast path",
        status: "Global ascending: MSD plan/scatter/LSD skipped",
        active: null,
        records: state.finalRecords
      }
    ];
    render();
    return;
  }

  if (inputOrder === "descending") {
    state.plan = null;
    state.msdRecords = [...state.records].reverse();
    state.finalRecords = state.msdRecords;
    state.steps = [
      {
        phase: "source",
        title: "Global order scan",
        status: "Global reverse: descending input fast path",
        active: null,
        records: state.records
      },
      ...makeGlobalReverseSteps(state.records, cfg),
      {
        phase: "done",
        title: "Global reverse",
        status: "Global reverse: whole input reversed",
        active: null,
        records: state.finalRecords
      }
    ];
    render();
    return;
  }

  state.plan = buildAdaptiveMsdBucketPlan(state.records, cfg);
  state.msdRecords = scatterIntoMsdBuckets(state.records, state.plan, cfg);
  state.steps = buildExecutionSteps(state.plan, cfg, state.msdRecords);
  render();
}

function clampInt(value, min, max) {
  const n = Number.parseInt(value, 10);
  if (Number.isNaN(n)) return min;
  return Math.max(min, Math.min(max, n));
}

function normalizeTinyThreshold(value) {
  const n = clampInt(value, TINY_THRESHOLDS[0], TINY_THRESHOLDS[TINY_THRESHOLDS.length - 1]);
  return TINY_THRESHOLDS.reduce((best, item) =>
    Math.abs(item - n) < Math.abs(best - n) ? item : best,
    TINY_THRESHOLDS[0]
  );
}

function currentStep() {
  return state.steps[Math.min(state.stepIndex, state.steps.length - 1)] || null;
}

function startRunIfNeeded() {
  if (!state.steps.length) reset();
  if (state.hasStarted) return false;
  state.hasStarted = true;
  state.stepIndex = 0;
  return true;
}

function stepForward() {
  if (startRunIfNeeded()) {
    render();
    return;
  }
  state.stepIndex = Math.min(state.stepIndex + 1, state.steps.length - 1);
  render();
}

function advanceSteps(count) {
  if (!state.steps.length) reset();
  state.stepIndex = Math.min(state.stepIndex + count, state.steps.length - 1);
}

function stepBackward() {
  if (!state.hasStarted) return;
  state.stepIndex = Math.max(state.stepIndex - 1, 0);
  render();
}

function runToEnd() {
  startRunIfNeeded();
  state.stepIndex = Math.max(0, state.steps.length - 1);
  render();
}

window.reset = reset;
window.stepForward = stepForward;
window.stepBackward = stepBackward;
window.runToEnd = runToEnd;

function clearCanvas(ctx, canvas) {
  if (!ctx || !canvas) return;
  if (typeof ctx.resetTransform === "function") ctx.resetTransform();
  else if (typeof ctx.setTransform === "function") ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.globalAlpha = 1;
  ctx.lineWidth = 1;
  ctx.textAlign = "left";
  ctx.textBaseline = "alphabetic";
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "#111617";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
}

function visualRecords(records, cap = MAX_LANE_RECORDS) {
  if (!records || records.length <= cap) return records ? [...records] : [];
  const step = Math.max(1, Math.ceil(records.length / cap));
  const out = [];
  for (let i = 0; i < records.length; i += step) out.push(records[i]);
  const last = records[records.length - 1];
  if (out[out.length - 1] !== last) out.push(last);
  return out;
}

function normalizeBins(bins) {
  if (!bins) return [];
  return bins.map((bin, index) =>
    Array.isArray(bin) ? { id: index, records: bin } : { id: bin.id, records: bin.records || [] }
  );
}

function visualBins(bins, cap = MAX_LANE_RECORDS) {
  const normalized = normalizeBins(bins);
  const nonEmpty = normalized.filter(bin => bin.records.length > 0).length || 1;
  const perBin = Math.max(1, Math.floor(cap / nonEmpty));
  return normalized
    .filter(bin => bin.records.length > 0)
    .map(bin => ({ id: bin.id, records: visualRecords(bin.records, perBin) }));
}

function visualRecordsFromBins(bins, cap = MAX_LANE_RECORDS) {
  const normalized = normalizeBins(bins).filter(bin => bin.records.length > 0);
  const perBin = Math.max(1, Math.floor(cap / Math.max(1, normalized.length)));
  return visualRecords(normalized.flatMap(bin => visualRecords(bin.records, perBin)), cap);
}

function drawRecords(ctx, rect, records, opts = {}) {
  if (!records || records.length === 0) return;
  const { x, y, w, h } = rect;
  if (w <= 0 || h <= 0) return;
  const n = records.length;
  const maxBars = opts.maxBars || Math.max(96, Math.min(MAX_DRAW_BARS, Math.ceil(w * 1.25)));
  const stride = Math.max(1, Math.ceil(n / maxBars));
  const visualCount = Math.ceil(n / stride);
  const barWidth = w / visualCount;
  let min = opts.minKey;
  let max = opts.maxKey;
  if (min === undefined || max === undefined) {
    min = records[0].key;
    max = records[0].key;
    for (let i = 0; i < n; i += stride) {
      const rec = records[i];
      if (rec.key < min) min = rec.key;
      if (rec.key > max) max = rec.key;
    }
  }
  const range = max === min ? 1n : max - min;

  ctx.save();
  ctx.beginPath();
  ctx.rect(x, y, w, h);
  ctx.clip();

  let visualIndex = 0;
  for (let i = 0; i < n; i += stride) {
    const rec = records[i];
    const normRaw = Number(((rec.key - min) * 1000n) / range) / 1000;
    const norm = Math.max(0, Math.min(1, normRaw));
    const barH = opts.flat ? h : Math.max(1, norm * h);
    const muted = opts.mutedIndices && opts.mutedIndices.has(rec.sourceIndex);
    ctx.globalAlpha = muted ? 0.25 : 0.95;
    ctx.fillStyle = rec.color;
    const bx = x + visualIndex * barWidth;
    const drawW = Math.max(0.75, barWidth + 0.3);
    ctx.fillRect(bx, opts.flat ? y : y + h - barH, drawW, barH);
    visualIndex++;
  }
  ctx.globalAlpha = 1;
  ctx.restore();
}

function globalKeyRange() {
  return state.keyRange || { minKey: 0n, maxKey: 1n };
}

function computeKeyRange(records) {
  if (!records || records.length === 0) return { minKey: 0n, maxKey: 1n };
  let minKey = records[0].key;
  let maxKey = records[0].key;
  for (const rec of records) {
    const key = rec.key;
    if (key < minKey) minKey = key;
    if (key > maxKey) maxKey = key;
  }
  if (minKey === maxKey) maxKey = minKey + 1n;
  return { minKey, maxKey };
}

function canvasFont(size) {
  return `650 ${size}px Segoe UI, system-ui, sans-serif`;
}

function fitCanvasText(ctx, text, maxWidth, size = 12) {
  const value = String(text ?? "");
  if (!ctx || value.length === 0) return value;
  if (maxWidth <= 8) return "";
  ctx.save();
  ctx.font = canvasFont(size);
  if (ctx.measureText(value).width <= maxWidth) {
    ctx.restore();
    return value;
  }
  let lo = 0;
  let hi = value.length;
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2);
    if (ctx.measureText(`${value.slice(0, mid)}...`).width <= maxWidth) lo = mid;
    else hi = mid - 1;
  }
  const out = lo > 0 ? `${value.slice(0, lo)}...` : "";
  ctx.restore();
  return out;
}

function drawText(ctx, text, x, y, color = "#edf3f1", size = 12) {
  ctx.save();
  ctx.textAlign = "left";
  ctx.textBaseline = "alphabetic";
  ctx.shadowColor = "rgba(0,0,0,0.75)";
  ctx.shadowBlur = 3;
  ctx.shadowOffsetX = 1;
  ctx.shadowOffsetY = 1;
  ctx.fillStyle = color;
  ctx.font = canvasFont(size);
  ctx.fillText(text, x, y);
  ctx.restore();
}

function renderSource() {
  const canvas = state.canvases.sourceCanvas;
  const ctx = state.ctx.sourceCanvas;
  clearCanvas(ctx, canvas);
  if (!ctx || !canvas) return;
  const scatterState = state.plan ? bucketScatterStateForCurrentStep() : { done: true, count: state.records.length, indices: new Set() };
  const keyRange = globalKeyRange();
  drawRecords(ctx, { x: 0, y: 0, w: canvas.width, h: canvas.height - 18 }, state.records, keyRange);

  if (!scatterState.done && state.records.length > 0) {
    drawThreadReadBlocks(ctx, scatterState.currentRanges || [], {
      x: 0,
      y: 0,
      w: canvas.width,
      h: canvas.height - 18
    });
    const progress = scatterState.count / state.records.length;
    ctx.fillStyle = "rgba(49,64,68,0.9)";
    ctx.fillRect(0, canvas.height - 12, canvas.width, 8);
    ctx.fillStyle = routeColor("msd", 0.95);
    ctx.fillRect(0, canvas.height - 12, canvas.width * progress, 8);
    drawText(ctx, `${scatterState.count}/${state.records.length} scattered`, 8, canvas.height - 18, routeColor("msd", 1), 11);
  }
}

function drawThreadReadBlocks(ctx, ranges, rect) {
  if (!ranges.length || state.records.length === 0) return;
  ctx.save();
  ctx.beginPath();
  ctx.rect(rect.x, rect.y, rect.w, rect.h);
  ctx.clip();
  for (const range of ranges) {
    const x = rect.x + (rect.w * range.start) / state.records.length;
    const w = Math.max(2, (rect.w * range.count) / state.records.length);
    ctx.fillStyle = routeColor("msd", 0.22);
    ctx.fillRect(x, rect.y, w, rect.h);
    ctx.strokeStyle = routeColor("msd", 0.95);
    ctx.lineWidth = 2;
    ctx.strokeRect(x, rect.y + 1, Math.min(w, rect.x + rect.w - x), rect.h - 2);
  }
  ctx.restore();
}

function renderBuckets() {
  const canvas = state.canvases.bucketCanvas;
  const ctx = state.ctx.bucketCanvas;
  clearCanvas(ctx, canvas);
  if (!ctx || !canvas || !state.plan) return;

  const step = currentStep();
  const scatterState = bucketScatterStateForCurrentStep();
  const buckets = scatterState.done
    ? state.plan.buckets.map(bucket => ({ id: bucket.id, records: bucket.records, size: bucket.size }))
    : scatterState.buckets.map((records, id) => ({ id, records, size: state.plan.sizes[id] }));
  const liveBuckets = liveBucketRecordsForCurrentStep();
  for (const bucket of buckets) {
    if (liveBuckets.has(bucket.id)) bucket.records = liveBuckets.get(bucket.id);
  }
  const activeBucket = activeBucketsForStep(step);
  const completedBuckets = completedBucketsForCurrentStep(scatterState.done);
  const visible = bucketDisplayListForGrid(buckets, scatterState.done, completedBuckets, activeBucket);

  const pad = 12;
  const headerH = 38;
  const minCellW = 156;
  const cols = Math.max(1, Math.min(visible.length || 1, Math.floor((canvas.width - pad * 2) / minCellW)));
  const rows = Math.max(1, Math.ceil((visible.length || 1) / cols));
  const cellW = (canvas.width - pad * 2) / cols;
  const cellH = Math.max(44, (canvas.height - pad * 2 - headerH) / rows);

  drawText(ctx, scatterState.done
    ? `MSD scatter complete: ${state.plan.scatterWorkers || state.cfg.workers} parallel lanes, showing ${visible.length} populated bucket${visible.length === 1 ? "" : "s"} of ${1 << state.cfg.msdBits}.`
    : `MSD scatter live: ${scatterState.count}/${state.records.length} records processed by ${state.plan.scatterWorkers || state.cfg.workers} lanes, about ${state.plan.scatterBatchRecords || 1} records/lane tick.`,
    pad,
    18,
    "#edf3f1",
    13
  );

  for (let i = 0; i < visible.length; i++) {
    const bucket = visible[i];
    const col = i % cols;
    const row = Math.floor(i / cols);
    const x = pad + col * cellW;
    const y = pad + headerH + row * cellH;
    drawBucketCell(ctx, x + 3, y + 3, cellW - 6, cellH - 6, bucket, activeBucket.has(bucket.id), scatterState.done, completedBuckets.has(bucket.id));
  }
}

function activeBucketsForStep(step) {
  if (step?.phase === "parallelMsd") {
    return new Set(step.scatterOps
      .filter(op => op.phase === "msdScatter")
      .flatMap(op => op.buckets || [op.bucket]));
  }
  if (step?.phase === "parallelWork") {
    return new Set(step.workerOps
      .filter(op => typeof op.bucket === "number" && op.phase !== "workerIdle")
      .map(op => op.bucket));
  }
  return new Set();
}

function liveBucketRecordsForCurrentStep() {
  const live = new Map();
  if (!state.plan) return live;

  for (let i = 0; i <= state.stepIndex; i++) {
    const item = state.steps[i];
    if (!item || item.phase !== "parallelWork") continue;
    for (const op of item.workerOps) {
      if (typeof op.bucket !== "number" || !op.records) continue;
      live.set(op.bucket, op.records);
    }
  }

  return live;
}

function completedBucketsForCurrentStep(scatterDone) {
  const completed = new Set();
  if (!state.plan || !scatterDone) return completed;

  for (let b = 0; b < (1 << state.cfg.msdBits); b++) {
    if (!bucketNeedsRefinementWork(state.plan, state.cfg, b)) completed.add(b);
  }

  const step = currentStep();
  if (step?.phase === "done") {
    for (let b = 0; b < (1 << state.cfg.msdBits); b++) completed.add(b);
    return completed;
  }

  for (let i = 0; i <= state.stepIndex; i++) {
    const item = state.steps[i];
    if (!item || item.phase !== "parallelWork") continue;
    for (const op of item.workerOps) {
      if (op.completesBucket && op.bucket !== undefined) completed.add(op.bucket);
    }
  }

  return completed;
}

function bucketScatterStateForCurrentStep() {
  const lastScatterIndex = state.steps.reduce((last, step, index) =>
    step.phase === "parallelMsd" ? index : last, -1);
  if (lastScatterIndex < 0 || state.stepIndex > lastScatterIndex) {
    return {
      done: true,
      count: state.records.length,
      indices: new Set(),
      buckets: state.plan.buckets.map(bucket => bucket.records)
    };
  }

  const buckets = Array.from({ length: 1 << state.cfg.msdBits }, () => []);
  const indices = new Set();
  const currentRanges = [];
  let count = 0;
  for (let i = 0; i <= state.stepIndex; i++) {
    const step = state.steps[i];
    if (!step || step.phase !== "parallelMsd") continue;
    for (const op of step.scatterOps) {
      if (op.phase !== "msdScatter") continue;
      count += op.count || 1;
      if (i === state.stepIndex) {
        currentRanges.push({
          worker: op.worker,
          start: op.batchStart ?? op.index,
          count: op.count || 1
        });
      }
      if (op.samples) {
        for (const sample of op.samples) {
          buckets[sample.bucket].push(sample.record);
          indices.add(sample.index);
        }
      } else {
        buckets[op.bucket].push(op.record);
        indices.add(op.index);
      }
    }
  }

  return { done: false, count: Math.min(count, state.records.length), buckets, indices, currentRanges };
}

function bucketDisplayListForGrid(buckets, done, completed = new Set(), active = new Set()) {
  const planned = buckets.filter(bucket =>
    bucket.records.length > 0 || (state.plan && state.plan.sizes[bucket.id] > 0)
  );
  if (planned.length === 0) return buckets.slice(0, 1);
  const pending = planned.filter(bucket =>
    state.plan && bucketNeedsRefinementWork(state.plan, state.cfg, bucket.id) && !completed.has(bucket.id)
  );
  const pendingIds = new Set(pending.map(bucket => bucket.id));
  const doneBuckets = planned.filter(bucket => completed.has(bucket.id));
  const fallback = active.size || pending.length ? planned : (doneBuckets.length ? doneBuckets : planned);

  return [...fallback]
    .sort((a, b) => {
      const activeDelta = Number(active.has(b.id)) - Number(active.has(a.id));
      if (activeDelta) return activeDelta;
      const pendingDelta = Number(pendingIds.has(b.id)) - Number(pendingIds.has(a.id));
      if (pendingDelta) return pendingDelta;
      const doneDelta = Number(completed.has(b.id)) - Number(completed.has(a.id));
      if (doneDelta) return doneDelta;
      return b.size - a.size || a.id - b.id;
    })
    .slice(0, MAX_VISIBLE_BUCKETS);
}

function drawBucketCell(ctx, x, y, w, h, bucket, active, done, completed) {
  const route = done ? routeForBucket(state.plan, state.cfg, bucket.id) : active ? "msd" : "done";
  const method = done ? bucketMethodLabel(state.plan, state.cfg, bucket.id, completed) : `planned ${bucket.size}`;
  ctx.fillStyle = completed
    ? routeColor("done", 0.16)
    : active ? routeColor("msd", 0.24) : routeColor(route, bucket.records.length ? 0.16 : 0.05);
  ctx.strokeStyle = completed
    ? routeColor("done", 0.95)
    : active ? routeColor("msd", 0.95) : bucket.records.length ? routeColor(route, 0.75) : "rgba(49,64,68,0.8)";
  ctx.fillRect(x, y, w, h);
  ctx.strokeRect(x, y, w, h);

  ctx.save();
  ctx.beginPath();
  ctx.rect(x, y, w, h);
  ctx.clip();

  if (h >= 18 && w >= 42) {
    const labelH = Math.min(h - 2, done ? 36 : 34);
    ctx.fillStyle = "rgba(9,12,14,0.82)";
    ctx.fillRect(x + 1, y + 1, Math.max(1, w - 2), labelH);
    ctx.strokeStyle = "rgba(237,243,241,0.12)";
    ctx.strokeRect(x + 1, y + 1, Math.max(1, w - 2), labelH);

    const displaySize = done ? bucket.size : bucket.records.length;
    const sizeText = done ? `n=${bucket.size}` : `seen=${displaySize}/${bucket.size}`;
    const title = fitCanvasText(ctx, `B${bucket.id} ${sizeText}`, w - 12, 11);
    drawText(ctx, title, x + 6, y + 15, "#ffffff", 11);
    if (done) {
      drawText(ctx, fitCanvasText(ctx, method, w - 12, 10), x + 6, y + Math.min(h - 6, 31), completed ? "#f8fbff" : routeColor(route, 1), 10);
    } else {
      drawText(ctx, fitCanvasText(ctx, `planned ${bucket.size}`, w - 12, 10), x + 6, y + Math.min(h - 6, 31), "#edf3f1", 10);
    }
  }

  if (bucket.records.length) {
    drawRecords(ctx, {
      x: x + 5,
      y: y + Math.min(36, Math.max(24, h * 0.4)),
      w: Math.max(1, w - 10),
      h: Math.max(1, h - Math.min(40, Math.max(28, h * 0.45)))
    }, bucket.records, { flat: false });
  }

  ctx.restore();
}

function bucketMethodLabel(plan, cfg, b, completed = false) {
  const size = plan.sizes[b];
  const flag = plan.bucketFlags[b];
  if (size === 0) return "done empty";
  if (size === 1) return "done single";
  if (flag === BUCKET_ALL_EQUAL) return "done equal";
  if (flag === BUCKET_ASCENDING) return "bucket ascending";
  if (flag === BUCKET_DESCENDING) return completed ? "bucket reverse done" : "bucket reverse";

  const route = routeForBucket(plan, cfg, b);
  if (route === "tiny") return tinySortTier(size);
  if (route === "tuple-direct") return "tuple-direct";
  if (route === "lsd-tuple-tail") return "LSD -> tuple-tail";
  if (route === "lsd") return "LSD cycles";
  return routeLabel(route);
}

function routeColor(route, alpha = 1) {
  const [r, g, b] = ROUTE_RGB[route] || ROUTE_RGB.done;
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function tinyColor(profileOrId, alpha = 1) {
  const id = typeof profileOrId === "string" ? profileOrId : profileOrId?.id;
  const [r, g, b] = TINY_RGB[id] || TINY_RGB.insertion;
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function routeLabel(route) {
  return {
    msd: "MSD",
    tiny: "tiny",
    "tuple-direct": "tuples",
    "lsd-tuple-tail": "LSD->tuples",
    lsd: "LSD",
    reverse: "reverse",
    done: "done"
  }[route] || route;
}

function renderWorkCanvas(canvasName, phase, label) {
  const canvas = state.canvases[canvasName];
  const ctx = state.ctx[canvasName];
  clearCanvas(ctx, canvas);
  if (!ctx || !canvas) return;

  const step = currentStep();
  const routedBuckets = state.plan ? bucketsScheduledForLane(phase) : bucketsScheduledForLaneWithoutPlan(phase);
  const workerIds = visibleWorkerIdsForPhase(phase, routedBuckets);

  if (workerIds.length === 0) {
    const text = routedBuckets.length === 0
      ? `${label}: no buckets routed here for this plan`
      : `${label}: ${routedBuckets.length} routed bucket${routedBuckets.length === 1 ? "" : "s"} waiting for dispatch`;
    drawText(ctx, text, 14, 24, "#edf3f1", 14);
    return;
  }

  const ops = workerIds.map(worker => {
    const op = step && step.phase === "parallelWork"
      ? step.workerOps.find(item => item.worker === worker)
      : null;
    return op;
  }).filter(Boolean);

  const columns = workLaneColumns(workerIds.length);
  const rows = Math.ceil(workerIds.length / columns);
  const gap = 8;
  const pad = 12;
  const laneW = (canvas.width - pad * 2 - gap * (columns - 1)) / columns;
  const laneH = (canvas.height - pad * 2 - gap * (rows - 1)) / rows;

  drawText(
    ctx,
    `${label}: ${workerIds.length} shown thread lane${workerIds.length === 1 ? "" : "s"} | bucket dispatch | ${routedBuckets.length} routed buckets`,
    pad,
    16,
    "#edf3f1",
    13
  );

  for (let i = 0; i < ops.length; i++) {
    const col = i % columns;
    const row = Math.floor(i / columns);
    const x = pad + col * (laneW + gap);
    const y = pad + 14 + row * (laneH + gap);
    drawWorkerLane(ctx, x, y, laneW, Math.max(42, laneH - 14), ops[i], phase);
  }
}

function visibleWorkerIdsForPhase(phase, routedBuckets) {
  const active = new Set();
  const step = currentStep();
  if (step?.phase === "parallelWork") {
    for (const op of step.workerOps) {
      if (op.lanePhase === phase || (phase === "lsd" && op.lanePhase === "steal")) {
        active.add(op.worker);
      }
    }
  }
  return [...active].sort((a, b) => a - b);
}

function workLaneColumns(workerCount) {
  if (workerCount > 24) return 4;
  if (workerCount > 8) return 2;
  return 1;
}

function bucketMatchesLane(plan, cfg, b, phase) {
  const route = routeForBucket(plan, cfg, b);
  if (phase === "lsd") return route === "lsd" || route === "lsd-tuple-tail";
  if (phase === "tiny") return route === "tiny";
  if (phase === "tuple") return route === "tuple-direct" || route === "lsd-tuple-tail";
  if (phase === "reverse") return plan.bucketFlags[b] === BUCKET_DESCENDING && plan.sizes[b] > 1;
  return false;
}

function bucketsScheduledForLane(phase) {
  if (!state.plan) return bucketsScheduledForLaneWithoutPlan(phase);
  if (!state.steps.length) return [];
  const buckets = new Set();
  for (const step of state.steps) {
    if (step.phase !== "parallelWork") continue;
    for (const op of step.workerOps) {
      if (op.lanePhase === phase && op.bucket !== undefined) buckets.add(op.bucket);
    }
  }

  if (phase === "lsd") {
    for (const b of buildLsdWorkBucketsByDescendingSize(state.plan, state.cfg)) {
      const route = routeForBucket(state.plan, state.cfg, b);
      if (route === "lsd" || route === "lsd-tuple-tail") buckets.add(b);
    }
  } else if (phase === "tiny") {
    for (const b of buildLsdWorkBucketsByDescendingSize(state.plan, state.cfg)) {
      if (routeForBucket(state.plan, state.cfg, b) === "tiny") buckets.add(b);
    }
  } else if (phase === "tuple") {
    for (const b of buildLsdWorkBucketsByDescendingSize(state.plan, state.cfg)) {
      const route = routeForBucket(state.plan, state.cfg, b);
      if (route === "tuple-direct" || route === "lsd-tuple-tail") buckets.add(b);
    }
  } else if (phase === "reverse") {
    for (let b = 0; b < (1 << state.cfg.msdBits); b++) {
      if (state.plan.bucketFlags[b] === BUCKET_DESCENDING && state.plan.sizes[b] > 1) buckets.add(b);
    }
  }

  return [...buckets].sort((a, b) => a - b);
}

function bucketsScheduledForLaneWithoutPlan(phase) {
  const buckets = new Set();
  for (const step of state.steps) {
    if (step.phase !== "parallelWork") continue;
    for (const op of step.workerOps) {
      if (op.lanePhase === phase && op.bucket !== undefined) buckets.add(op.bucket);
    }
  }
  return [...buckets];
}

function drawWorkerLane(ctx, x, y, w, h, op, lanePhase) {
  const active = op.phase !== "workerIdle";
  const steal = op.phase === "workSteal";
  const route = op.lanePhase === "reverse"
    ? "reverse"
    : op.lanePhase === "done"
      ? "done"
      : op.bucket !== undefined && state.plan ? routeForBucket(state.plan, state.cfg, op.bucket) : "done";
  const laneAccent = op.active?.tinyProfile ? tinyColor(op.active.tinyProfile, 1) : routeColor(route, 1);

  ctx.fillStyle = steal
    ? routeColor("msd", 0.16)
    : active && op.active?.tinyProfile ? tinyColor(op.active.tinyProfile, 0.22)
      : active ? routeColor(route, 0.24)
        : "rgba(255,255,255,0.035)";
  ctx.strokeStyle = steal ? routeColor("msd", 0.95) : active ? laneAccent : "rgba(49,64,68,0.95)";
  ctx.lineWidth = 1;
  ctx.fillRect(x, y, w, h);
  ctx.strokeRect(x, y, w, h);

  ctx.save();
  ctx.beginPath();
  ctx.rect(x, y, w, h);
  ctx.clip();

  const title = active
    ? `Thread ${op.worker} | bucket ${op.bucket} | ${op.phase}`
    : `Thread ${op.worker} | idle`;
  drawText(ctx, fitCanvasText(ctx, title, w - 16, 11), x + 8, y + 14, active ? "#edf3f1" : "#9fb0ac", 11);
  drawText(ctx, fitCanvasText(ctx, op.status || "", w - 16, 10), x + 8, y + 29, steal ? routeColor("msd", 1) : "#edf3f1", 10);
  if (active && op.active?.tinyTier) {
    drawText(ctx, fitCanvasText(ctx, `${op.active.tinyTier} | ${op.active.tinyProfile?.range || ""}`, w - 16, 10), x + 8, y + 43, laneAccent, 10);
  }
  if (active && op.active?.tupleKind) {
    drawText(ctx, fitCanvasText(ctx, op.active.tupleKind, w - 16, 10), x + 8, y + 43, routeColor("tuple-direct", 1), 10);
  }
  if (active && op.active?.reverseKind) {
    drawText(ctx, fitCanvasText(ctx, `${op.active.reverseKind} reverse`, w - 16, 10), x + 8, y + 43, routeColor("reverse", 1), 10);
  }

  if (!active || steal) {
    ctx.restore();
    return;
  }

  const records = op.records || [];
  const hasBins = Boolean(op.active?.bins);
  const hasDetailLine = op.active?.tinyTier || op.active?.tupleKind || op.active?.reverseKind;
  const preferredChartY = hasDetailLine ? y + 50 : y + 38;
  const chartBottom = y + h - 8;
  const chartY = Math.min(preferredChartY, Math.max(y + 34, chartBottom - 36));
  const chartH = Math.max(1, chartBottom - chartY);

  if (!hasBins) {
    drawRecords(ctx, { x: x + 8, y: chartY, w: Math.max(1, w - 16), h: chartH }, records, { flat: false });
    ctx.restore();
    return;
  }

  drawRecords(ctx, { x: x + 8, y: chartY, w: Math.max(1, w * 0.34), h: chartH }, records, { flat: false });

  const bins = compactBins(op.active.bins, Math.max(3, Math.floor((w * 0.56) / 16)));
  const binX = x + w * 0.4;
  const binY = chartY + 2;
  const binW = (w * 0.58) / Math.max(1, bins.length);
  const binH = Math.max(1, chartH - 2);
  for (let i = 0; i < bins.length; i++) {
    const bx = binX + i * binW;
    ctx.strokeStyle = op.active?.tinyProfile
      ? tinyColor(op.active.tinyProfile, 0.8)
      : lanePhase === "tuple" ? routeColor("tuple-direct", 0.8) : routeColor("lsd-tuple-tail", 0.75);
    ctx.strokeRect(bx, binY, Math.max(2, binW - 2), binH);
    drawRecords(ctx, { x: bx + 1, y: binY + 1, w: Math.max(1, binW - 4), h: Math.max(1, binH - 2) }, bins[i].records, { flat: false });
  }
  ctx.restore();
}

function compactBins(bins, maxBins) {
  const nonEmpty = normalizeBins(bins)
    .filter(bin => bin.records.length > 0);
  if (nonEmpty.length <= maxBins) return nonEmpty;
  const out = [];
  for (let i = 0; i < maxBins; i++) {
    const start = Math.floor((i * nonEmpty.length) / maxBins);
    const end = Math.max(start + 1, Math.floor(((i + 1) * nonEmpty.length) / maxBins));
    out.push({
      id: nonEmpty[start].id,
      records: nonEmpty.slice(start, end).flatMap(bin => bin.records)
    });
  }
  return out;
}

function renderSorted() {
  const canvas = state.canvases.sortedCanvas;
  const ctx = state.ctx.sortedCanvas;
  clearCanvas(ctx, canvas);
  if (!ctx || !canvas) return;
  const step = currentStep();

  if (!state.plan) {
    const records = step && step.phase === "done" ? step.records : [];
    drawRecords(ctx, { x: 0, y: 0, w: canvas.width, h: canvas.height - 18 }, records, globalKeyRange());
    return;
  }

  const keyRange = globalKeyRange();
  const total = Math.max(1, state.records.length);
  const rail = { x: 0, y: 8, w: canvas.width, h: canvas.height - 28 };
  ctx.strokeStyle = "rgba(49,64,68,0.95)";
  ctx.strokeRect(rail.x, rail.y, rail.w, rail.h);

  const scatterState = bucketScatterStateForCurrentStep();
  const completed = completedBucketsForCurrentStep(scatterState.done);
  const recent = recentlyCompletedBucketsForCurrentStep();

  let placed = 0;
  ctx.save();
  ctx.beginPath();
  ctx.rect(rail.x, rail.y, rail.w, rail.h);
  ctx.clip();
  for (let b = 0; b < (1 << state.cfg.msdBits); b++) {
    if (!completed.has(b) || state.plan.sizes[b] === 0) continue;

    const start = state.plan.starts[b];
    const size = state.plan.sizes[b];
    const x = rail.x + (rail.w * start) / total;
    const w = Math.max(0.5, Math.min((rail.w * size) / total, rail.x + rail.w - x));
    const records = finalBucketRecords(b);
    const isRecent = recent.has(b);

    ctx.fillStyle = isRecent ? routeColor("done", 0.2) : routeColor("done", 0.08);
    ctx.fillRect(x, rail.y, w, rail.h);
    drawRecords(ctx, { x, y: rail.y, w, h: rail.h }, records, keyRange);
    ctx.strokeStyle = isRecent ? routeColor("done", 1) : routeColor("done", 0.42);
    ctx.lineWidth = isRecent ? 3 : 1;
    ctx.strokeRect(x, rail.y, w, rail.h);
    ctx.lineWidth = 1;
    placed += size;
  }
  ctx.restore();

  if (step && step.phase === "done" && state.plan) {
    const summary = bucketMethodSummary();
    drawText(ctx, summary, 8, canvas.height - 5, "#edf3f1", 12);
  } else {
    drawText(ctx, `${placed}/${state.records.length} records landed in final array`, 8, canvas.height - 5, "#edf3f1", 12);
  }
}

function recentlyCompletedBucketsForCurrentStep() {
  const recent = new Set();
  const step = currentStep();
  if (!step || step.phase !== "parallelWork") return recent;
  for (const op of step.workerOps) {
    if (op.completesBucket && typeof op.bucket === "number") recent.add(op.bucket);
  }
  return recent;
}

function finalBucketRecords(b) {
  const start = state.plan.starts[b];
  const size = state.plan.sizes[b];
  if (state.finalRecords.length >= start + size) {
    return state.finalRecords.slice(start, start + size);
  }
  return state.plan.buckets[b].records;
}

function bucketMethodSummary() {
  if (!state.plan) return "";
  const counts = new Map();
  for (let b = 0; b < (1 << state.cfg.msdBits); b++) {
    const label = bucketMethodLabel(state.plan, state.cfg, b, true);
    counts.set(label, (counts.get(label) || 0) + 1);
  }
  return [...counts.entries()]
    .filter(([, count]) => count > 0)
    .map(([label, count]) => `${label}: ${count}`)
    .join(" | ");
}

function tinyBucketProfileSummary(plan, cfg) {
  if (!plan) return "skipped";
  const counts = new Map();
  let total = 0;
  for (const b of buildLsdWorkBucketsByDescendingSize(plan, cfg)) {
    if (routeForBucket(plan, cfg, b) !== "tiny") continue;
    const profile = tinySortProfile(plan.sizes[b]);
    counts.set(profile.label, (counts.get(profile.label) || 0) + 1);
    total++;
  }
  if (total === 0) return "0 tiny buckets";
  const parts = TINY_PROFILES
    .map(profile => [profile.label, counts.get(profile.label) || 0])
    .filter(([, count]) => count > 0)
    .map(([label, count]) => `${label}: ${count}`);
  return `${total} tiny buckets | ${parts.join(" | ")}`;
}

function bucketOrderSummary(plan, cfg) {
  if (!plan) return "";
  let ascending = 0;
  let descending = 0;
  for (let b = 0; b < (1 << cfg.msdBits); b++) {
    if (plan.bucketFlags[b] === BUCKET_ASCENDING && plan.sizes[b] > 1) ascending++;
    if (plan.bucketFlags[b] === BUCKET_DESCENDING && plan.sizes[b] > 1) descending++;
  }
  return `${ascending} bucket ascending skip${ascending === 1 ? "" : "s"} | ${descending} bucket reverse trigger${descending === 1 ? "" : "s"}`;
}

function reverseLaneSummary(plan, cfg) {
  if (!plan) {
    if (state.inputOrder === "descending") return "global reverse: whole input";
    if (state.inputOrder === "ascending") return "global ascending: no reverse work";
    if (state.inputOrder === "empty") return "global order: empty input";
    return "global order fast path";
  }
  const reverseBuckets = bucketsScheduledForLane("reverse").length;
  const bucketOrder = bucketOrderSummary(plan, cfg);
  return `${reverseBuckets} bucket reverse work item${reverseBuckets === 1 ? "" : "s"} | ${bucketOrder}`;
}

function renderLabels() {
  const step = currentStep();
  const plan = state.plan;
  const cfg = state.cfg;
  const set = (id, text) => {
    if (state.labels[id]) state.labels[id].textContent = text;
  };

  if (!state.hasStarted) {
    set("status", "Ready");
    set("msdLabel", "");
    set("lsdLabel", "");
    set("tinyLabel", "");
    set("reverseLabel", "");
    set("tuplesLabel", "");
    set("sortedLabel", "Ready");
    return;
  }

  set("status", step ? `${state.stepIndex + 1}/${state.steps.length} - ${step.status}` : "Ready");
  set("msdLabel", plan
    ? `shift ${plan.msdShift}, ${plan.sizes.filter(Boolean).length}/${1 << cfg.msdBits} non-empty, ${plan.scatterWorkers || cfg.workers} parallel read lanes | ${bucketOrderSummary(plan, cfg)}`
    : `skipped by global ${state.inputOrder} fast path`);
  set("lsdLabel", plan ? `${buildLsdWorkBucketsByDescendingSize(plan, cfg).filter(b => plan.cycleCounts[b] > 0).length} cycle buckets` : "skipped");
  set("tinyLabel", tinyBucketProfileSummary(plan, cfg));
  set("reverseLabel", reverseLaneSummary(plan, cfg));
  set("tuplesLabel", cfg.tuplesEnabled && plan
    ? `${buildLsdWorkBucketsByDescendingSize(plan, cfg).filter(b => plan.tupleTailMasks[b] !== 0n).length} tuple/direct-tail buckets, ${cfg.tupleBits} bit range`
    : "disabled");
  set("sortedLabel", step && step.phase === "done" ? step.status : "Appears when the final bucket is complete.");
}

function render() {
  updateOptionalDisplays();
  renderLabels();
  updateDynamicCanvasLayout();
  setupCanvasResolution();
  if (!state.hasStarted) {
    renderReady();
    return;
  }
  renderSource();
  renderBuckets();
  renderWorkCanvas("tinyCanvas", "tiny", "Tiny");
  if (state.cfg.tuplesEnabled) renderWorkCanvas("tuplesCanvas", "tuple", "Tuples");
  renderWorkCanvas("lsdCanvas", "lsd", "LSD");
  renderWorkCanvas("reverseCanvas", "reverse", "Reverse");
  renderSorted();
}

function updateOptionalDisplays() {
  const tuplesHidden = !state.cfg.tuplesEnabled;
  setElementHidden("tuplesStage", tuplesHidden);
  setElementHidden("tupleKeyItem", tuplesHidden);
  setElementHidden("tupleTailKeyItem", tuplesHidden);

  const tupleBits = document.getElementById("tupleBits");
  if (tupleBits) tupleBits.disabled = tuplesHidden;
}

function setElementHidden(id, hidden) {
  const el = document.getElementById(id);
  if (el) el.classList.toggle("is-hidden", hidden);
}

function renderReady() {
  for (const [name, canvas] of Object.entries(state.canvases)) {
    const ctx = state.ctx[name];
    clearCanvas(ctx, canvas);
    if (ctx && canvas) drawText(ctx, "Ready", 14, 30, "#edf3f1", 18);
  }
}

function updateDynamicCanvasLayout() {
  setCanvasHeight("sourceCanvas", state.hasStarted ? 168 : 92);
  setCanvasHeight("sortedCanvas", state.hasStarted ? 168 : 92);

  if (!state.hasStarted) {
    for (const name of ["bucketCanvas", "lsdCanvas", "tinyCanvas", "reverseCanvas", "tuplesCanvas"]) {
      setCanvasHeight(name, 92);
    }
    return;
  }

  if (!state.plan) {
    setCanvasHeight("bucketCanvas", 86);
    setCanvasHeight("tinyCanvas", 86);
    if (state.cfg.tuplesEnabled) setCanvasHeight("tuplesCanvas", 86);
    setCanvasHeight("lsdCanvas", 86);
    setWorkCanvasHeight("reverseCanvas", "reverse");
    return;
  }

  const scatterState = bucketScatterStateForCurrentStep();
  const bucketVisible = Math.max(1, bucketDisplayListForGrid(
    state.plan.buckets.map(bucket => ({ id: bucket.id, records: bucket.records, size: bucket.size })),
    scatterState.done,
    completedBucketsForCurrentStep(scatterState.done),
    activeBucketsForStep(currentStep())
  ).length);
  const bucketCanvas = state.canvases.bucketCanvas;
  const bucketWidth = bucketCanvas?.clientWidth || 1400;
  const bucketCols = Math.max(1, Math.floor((bucketWidth - 24) / 156));
  const bucketRows = Math.ceil(bucketVisible / bucketCols);
  setCanvasHeight("bucketCanvas", clampNumber(78 + bucketRows * 64, 150, 920));

  setWorkCanvasHeight("tinyCanvas", "tiny");
  if (state.cfg.tuplesEnabled) setWorkCanvasHeight("tuplesCanvas", "tuple");
  setWorkCanvasHeight("lsdCanvas", "lsd");
  setWorkCanvasHeight("reverseCanvas", "reverse");
}

function setWorkCanvasHeight(canvasName, phase) {
  const routedBuckets = bucketsScheduledForLane(phase);
  const workerCount = visibleWorkerIdsForPhase(phase, routedBuckets).length;
  if (workerCount === 0) {
    setCanvasHeight(canvasName, 86);
    return;
  }

  const columns = workLaneColumns(workerCount);
  const rows = Math.max(1, Math.ceil(workerCount / columns));
  const rowHeight = workerCount > 24 ? 104 : 132;
  setCanvasHeight(canvasName, clampNumber(58 + rows * rowHeight, 150, 920));
}

function setCanvasHeight(canvasName, height) {
  const canvas = state.canvases[canvasName];
  if (!canvas) return;
  canvas.style.height = `${Math.round(height)}px`;
}

function clampNumber(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function setupCanvasResolution() {
  for (const canvas of Object.values(state.canvases)) {
    if (!canvas) continue;
    const rect = canvas.getBoundingClientRect();
    const width = Math.max(1, Math.floor(rect.width));
    const height = Math.max(1, Math.floor(rect.height));
    if (canvas.width !== width || canvas.height !== height) {
      canvas.width = width;
      canvas.height = height;
    }
  }
}

function playLoop() {
  if (!state.playing) return;
  if (state.stepIndex >= state.steps.length - 1) {
    state.playing = false;
    const playBtn = document.getElementById("playBtn");
    if (playBtn) playBtn.textContent = "Play";
    return;
  }
  const speed = clampInt(state.cfg.speed, 1, 100);
  const actionsPerTick = speed >= 80 ? 3 : speed >= 55 ? 2 : 1;
  advanceSteps(actionsPerTick);
  render();
  const delay = speed < 70
    ? Math.max(8, Math.round(220 - speed * 2.6))
    : Math.max(1, Math.round(18 - (speed - 70) * 0.55));
  window.setTimeout(() => requestAnimationFrame(playLoop), delay);
}

function wireDom() {
  state.canvases.sourceCanvas = document.getElementById("sourceCanvas");
  state.canvases.bucketCanvas = document.getElementById("bucketCanvas");
  state.canvases.lsdCanvas = document.getElementById("lsdCanvas");
  state.canvases.tinyCanvas = document.getElementById("tinyCanvas");
  state.canvases.reverseCanvas = document.getElementById("reverseCanvas");
  state.canvases.tuplesCanvas = document.getElementById("tuplesCanvas");
  state.canvases.sortedCanvas = document.getElementById("sortedCanvas");

  for (const [key, canvas] of Object.entries(state.canvases)) {
    if (canvas) state.ctx[key] = canvas.getContext("2d");
  }

  for (const id of ["status", "msdLabel", "lsdLabel", "tinyLabel", "reverseLabel", "tuplesLabel", "sortedLabel"]) {
    state.labels[id] = document.getElementById(id);
  }

  setupCanvasResolution();
  window.addEventListener("resize", () => {
    setupCanvasResolution();
    render();
  });

  const modeEl = document.getElementById("dataMode");
  if (modeEl) {
    modeEl.innerHTML = "";
    for (const mode of DATA_MODES) {
      const option = document.createElement("option");
      option.value = mode;
      option.textContent = displayDataMode(mode);
      modeEl.appendChild(option);
    }
  }

  bindValue("recordCount", "count", Number);
  bindValue("dataMode", "mode", String);
  bindValue("msdBits", "msdBits", Number);
  bindValue("lsdBits", "lsdBits", Number);
  bindValue("workerCount", "workers", Number);
  bindValue("tinyThreshold", "tinyThreshold", Number);
  bindValue("tupleBits", "tupleBits", Number);
  bindChecked("tuplesEnabled", "tuplesEnabled");

  const resetBtn = document.getElementById("resetBtn");
  const stepBtn = document.getElementById("stepBtn");
  const playBtn = document.getElementById("playBtn");

  if (resetBtn) resetBtn.addEventListener("click", reset);
  if (stepBtn) stepBtn.addEventListener("click", stepForward);
  if (playBtn) {
    playBtn.addEventListener("click", () => {
      const justStarted = startRunIfNeeded();
      if (state.stepIndex >= state.steps.length - 1 && !state.playing && !justStarted) {
        state.stepIndex = 0;
      }
      state.playing = !state.playing;
      playBtn.textContent = state.playing ? "Pause" : "Play";
      render();
      if (state.playing) {
        const firstDelay = justStarted ? 650 : 0;
        window.setTimeout(() => requestAnimationFrame(playLoop), firstDelay);
      }
    });
  }

  reset();
}

function bindValue(id, key, cast, shouldReset = true) {
  const el = document.getElementById(id);
  if (!el) return;
  el.value = state.cfg[key];
  el.addEventListener("change", e => {
    state.cfg[key] = cast(e.target.value);
    if (shouldReset) reset();
  });
  el.addEventListener("input", e => {
    if (id !== "speed") return;
    state.cfg[key] = cast(e.target.value);
  });
}

function bindChecked(id, key, shouldReset = true) {
  const el = document.getElementById(id);
  if (!el) return;
  el.checked = Boolean(state.cfg[key]);
  el.addEventListener("change", e => {
    state.cfg[key] = e.target.checked;
    if (shouldReset) reset();
  });
}

window.addEventListener("DOMContentLoaded", wireDom);
