const RECORD_BYTES = 16;
const SEED = 0x9E3779B97F4A7C15n;
const MASK64 = (1n << 64n) - 1n;

const sourceCanvas = document.getElementById("sourceCanvas");
const bucketCanvas = document.getElementById("bucketCanvas");
const lsdCanvas = document.getElementById("lsdCanvas");
const sortedCanvas = document.getElementById("sortedCanvas");
const statusEl = document.getElementById("status");
const msdLabel = document.getElementById("msdLabel");
const lsdLabel = document.getElementById("lsdLabel");
const sortedLabel = document.getElementById("sortedLabel");

const controls = {
  recordCount: document.getElementById("recordCount"),
  dataMode: document.getElementById("dataMode"),
  msdBits: document.getElementById("msdBits"),
  lsdBits: document.getElementById("lsdBits"),
  workerCount: document.getElementById("workerCount"),
  workStealing: document.getElementById("workStealing"),
  speed: document.getElementById("speed"),
  reset: document.getElementById("resetBtn"),
  play: document.getElementById("playBtn"),
  step: document.getElementById("stepBtn"),
};

let state = null;
let playing = false;
let timer = 0;

function u64(x) {
  return BigInt.asUintN(64, x);
}

function mix64(x) {
  let z = u64(BigInt(x) + SEED);
  z = u64((z ^ (z >> 30n)) * 0xBF58476D1CE4E5B9n);
  z = u64((z ^ (z >> 27n)) * 0x94D049BB133111EBn);
  return u64(z ^ (z >> 31n));
}

function scaleOrderedKey(rank, count) {
  if (count <= 1) return 0n;
  const bits = count <= 1 ? 0 : Math.ceil(Math.log2(count));
  return u64(BigInt(rank) << BigInt(64 - bits));
}

function keyForMode(i, n, mode) {
  switch (mode) {
    case "SORTED":
      return scaleOrderedKey(i, n);
    case "REVERSE":
      return scaleOrderedKey(n - 1 - i, n);
    case "DUPLICATES":
      return scaleOrderedKey(Number(mix64(i) & ((1n << 8n) - 1n)), 1 << 8);
    case "LOW_BITS_ONLY":
      return mix64(i) & 0xFFFFFFFFn;
    case "HIGH_BITS_ONLY":
      return mix64(i) & 0xFFFFFFFF00000000n;
    case "DELAYED_ENTROPY":
      return 0x123456789ABC0000n | (mix64(i) & 0xFFFFn);
    case "SAWTOOTH":
      return scaleOrderedKey(i % 32, 32);
    case "ALL_EQUAL":
      return 0n;
    default:
      return mix64(i);
  }
}

function keyHeight(key) {
  return Number((key >> 48n) & 0xFFFFn) / 65535;
}

function recordHeight(record) {
  return record.visualHeight ?? keyHeight(record.key);
}

function keyLabel(key) {
  return "0x" + key.toString(16).padStart(16, "0").slice(0, 8);
}

function makeRecords() {
  const n = clamp(parseInt(controls.recordCount.value, 10) || 96, 8, 100000);
  controls.recordCount.value = n;
  const mode = controls.dataMode.value;
  return Array.from({ length: n }, (_, i) => ({
    key: keyForMode(i, n, mode),
    value: i,
    sourceIndex: i,
    color: colorForIndex(i, n),
  }));
}

function colorForIndex(i, n) {
  const hue = Math.round((i / Math.max(1, n - 1)) * 300 + 25);
  return `hsl(${hue} 78% 58%)`;
}

function buildMsdPlan(records, msdBits) {
  const bucketCount = 1 << msdBits;
  const shift = 64 - msdBits;
  const buckets = Array.from({ length: bucketCount }, (_, i) => ({
    id: i,
    records: [],
    finalRecords: [],
  }));

  for (const rec of records) {
    const b = Number((rec.key >> BigInt(shift)) & BigInt(bucketCount - 1));
    buckets[b].records.push(rec);
  }

  return { bucketCount, shift, buckets };
}

function makeSteps(records, cfg) {
  const plan = buildMsdPlan(records, cfg.msdBits);
  const steps = [];

  steps.push({ phase: "source", message: "Source array generated from keyForMode()." });

  const scatterQueues = makeScatterQueues(records, plan, cfg.workers);
  let scatterTick = 0;
  while (scatterQueues.some((queue) => queue.length > 0)) {
    const scatterOps = scatterQueues.map((queue, thread) => queue.shift() || {
      phase: "scatterIdle",
      thread,
      message: `Thread ${thread}: MSD scatter idle`,
    });
    const active = scatterOps.filter((op) => op.phase !== "scatterIdle");
    steps.push({
      phase: "parallelMsd",
      tick: scatterTick++,
      scatterOps,
      message: `Parallel MSD radix bucket scatter tick ${scatterTick}: ${active.length} thread lanes moved records.`,
    });
  }

  const visualWorkers = Math.min(cfg.workers, Math.max(1, plan.bucketCount));
  steps.push(...(cfg.workStealing
    ? makeWorkStealingLsdSteps(plan, cfg, visualWorkers)
    : makeStrideLsdSteps(plan, cfg, visualWorkers)));

  steps.push({
    phase: "done",
    message: "DONE: the sort is complete when the last active bucket finishes its final LSD pass.",
  });

  return { plan: { ...plan, visualWorkers }, steps };
}

function makeScatterQueues(records, plan, threads) {
  const chunk = Math.floor(records.length / threads);
  return Array.from({ length: threads }, (_, thread) => {
    const start = thread * chunk;
    const end = thread === threads - 1 ? records.length : start + chunk;
    const queue = [];

    for (let i = start; i < end; i++) {
      const rec = records[i];
      const bucket = Number((rec.key >> BigInt(plan.shift)) & BigInt(plan.bucketCount - 1));
      queue.push({
        phase: "msdScatter",
        thread,
        index: i,
        bucket,
        record: rec,
        message: `Thread ${thread}: MSD radix bucket scatter record ${i} key ${keyLabel(rec.key)} -> bucket ${bucket}`,
      });
    }

    return queue;
  });
}

function makeStrideLsdSteps(plan, cfg, visualWorkers) {
  const steps = [];
  const workerQueues = Array.from({ length: visualWorkers }, (_, tid) => {
    const ops = [];
    for (let b = tid; b < plan.bucketCount; b += visualWorkers) {
      ops.push(...makeBucketLsdOps(plan.buckets[b], plan, cfg, tid));
    }
    return ops;
  });

  let parallelTick = 0;
  while (workerQueues.some((ops) => ops.length > 0)) {
    const workerOps = workerQueues.map((ops, tid) => ops.shift() || idleOp(tid));
    const active = workerOps.filter((op) => op.phase !== "workerIdle");
    steps.push({
      phase: "parallelLsd",
      tick: parallelTick++,
      workerOps,
      message: active.length
        ? `Parallel LSD tick ${parallelTick}: ${active.length} thread lanes active.`
        : `Parallel LSD tick ${parallelTick}: threads idle.`,
    });
  }

  return steps;
}

function makeWorkStealingLsdSteps(plan, cfg, visualWorkers) {
  const steps = [];
  const localJobs = Array.from({ length: visualWorkers }, (_, worker) => {
    const jobs = [];
    for (let b = worker; b < plan.bucketCount; b += visualWorkers) {
      const bucket = plan.buckets[b];
      if (bucket.records.length) {
        jobs.push({ bucket, cost: estimateBucketCost(bucket, plan, cfg), stolen: false });
      }
    }
    return jobs;
  });
  const queues = Array.from({ length: visualWorkers }, () => []);
  let parallelTick = 0;

  while (localJobs.some((jobs) => jobs.length > 0) || queues.some((queue) => queue.length > 0)) {
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
          queue.push(...makeBucketLsdOps(job.bucket, plan, cfg, worker));
          if (stolen) {
            return {
              phase: "workSteal",
              worker,
              bucket: job.bucket.id,
              message: `Thread ${worker}: idle, stole bucket ${job.bucket.id} (${job.bucket.records.length} records) from thread ${job.from}.`,
            };
          }
        }
      }

      if (queue.length === 0) {
        return idleOp(worker);
      }

      const op = queue.shift();
      if (op.phase === "bucketDone" && op.sorted.length <= 1) {
        return {
          ...op,
          worker,
        };
      }
      return op;
    });
    const active = workerOps.filter((op) => op.phase !== "workerIdle");
    const steals = workerOps.filter((op) => op.phase === "workSteal").length;

    steps.push({
      phase: "parallelLsd",
      tick: parallelTick++,
      workerOps,
      message: steals
        ? `Work stealing tick ${parallelTick}: ${steals} idle thread lane${steals === 1 ? "" : "s"} grabbed remaining bucket work.`
        : `Work stealing tick ${parallelTick}: ${active.length} thread lanes active.`,
    });
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

  if (bestWorker === -1) {
    return { job: null, stolen: false };
  }

  const [job] = localJobs[bestWorker].splice(bestIndex, 1);
  return { job: { ...job, from: bestWorker }, stolen: true };
}

function estimateBucketCost(bucket, plan, cfg) {
  if (bucket.records.length <= 1) return 1;
  const passes = Math.ceil(plan.shift / cfg.lsdBits);
  return bucket.records.length * passes + passes;
}

function idleOp(worker) {
  return {
    phase: "workerIdle",
    worker,
    message: `Thread ${worker}: idle`,
  };
}

function makeBucketLsdOps(bucket, plan, cfg, worker) {
  const ops = [];

  if (bucket.records.length === 0) {
    return ops;
  }

  if (bucket.records.length <= 1) {
    ops.push({
      phase: "bucketDone",
      worker,
      bucket: bucket.id,
      sorted: [...bucket.records],
      message: `Worker ${worker}: bucket ${bucket.id} size ${bucket.records.length}, no LSD pass needed.`,
    });
    return ops;
  }

  let working = [...bucket.records];
  for (let shift = 0; shift < plan.shift; shift += cfg.lsdBits) {
    const bitsThisPass = Math.min(cfg.lsdBits, plan.shift - shift);
    const radix = 2 ** bitsThisPass;
    const mask = BigInt(radix - 1);
    const bins = new Map();

    for (const rec of working) {
      const bin = Number((rec.key >> BigInt(shift)) & mask);
      if (!bins.has(bin)) bins.set(bin, []);
      bins.get(bin).push(rec);
      ops.push({
        phase: "lsdScatter",
        worker,
        bucket: bucket.id,
        shift,
        bitsThisPass,
        radix,
        bin,
        binEntries: snapshotBins(bins),
        working: [...working],
        record: rec,
        message: `Worker ${worker}: bucket ${bucket.id}, shift ${shift}, key ${keyLabel(rec.key)} -> bin ${bin}`,
      });
    }

    working = flattenBins(bins);
    ops.push({
      phase: "lsdPassDone",
      worker,
      bucket: bucket.id,
      shift,
      bitsThisPass,
      radix,
      binEntries: snapshotBins(bins),
      working: [...working],
      message: `Worker ${worker}: bucket ${bucket.id}, shift ${shift} pass complete.`,
    });
  }

  ops.push({
    phase: "bucketSorted",
    worker,
    bucket: bucket.id,
    sorted: [...working],
    message: `Worker ${worker}: bucket ${bucket.id} sorted.`,
  });

  return ops;
}

function snapshotBins(bins) {
  return [...bins.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([id, records]) => ({ id, records: [...records] }));
}

function flattenBins(bins) {
  return snapshotBins(bins).flatMap((entry) => entry.records);
}

function reset() {
  stop();
  const records = makeRecords();
  const cfg = {
    msdBits: clamp(parseInt(controls.msdBits.value, 10) || 4, 2, 13),
    lsdBits: clamp(parseInt(controls.lsdBits.value, 10) || 3, 2, 17),
    workers: clamp(parseInt(controls.workerCount.value, 10) || 16, 1, 64),
    workStealing: controls.workStealing.checked,
  };
  controls.msdBits.value = cfg.msdBits;
  controls.lsdBits.value = cfg.lsdBits;
  controls.workerCount.value = cfg.workers;

  const { plan, steps } = makeSteps(records, cfg);
  state = {
    records,
    cfg,
    plan,
    steps,
    stepIndex: 0,
    deployed: Array.from({ length: plan.bucketCount }, () => []),
    scatteredCount: 0,
    scatterDone: false,
    workerViews: Array.from({ length: plan.visualWorkers }, (_, worker) => ({
      phase: "workerIdle",
      worker,
      message: `Worker ${worker}: waiting for MSD scatter.`,
    })),
    sortedBuckets: new Map(),
    done: false,
  };
  render();
}

function step(shouldRender = true) {
  if (!state) reset();
  if (state.stepIndex >= state.steps.length) {
    stop();
    statusEl.textContent = "Complete";
    return;
  }

  const s = state.steps[state.stepIndex++];
  if (s.phase === "parallelMsd") {
    for (const op of s.scatterOps) {
      if (op.phase === "msdScatter") {
        state.deployed[op.bucket].push(op.record);
        state.scatteredCount++;
      }
    }
    if (state.scatteredCount >= state.records.length) {
      state.scatterDone = true;
    }
  } else if (s.phase === "parallelLsd") {
    for (const op of s.workerOps) {
      state.workerViews[op.worker] = op;
      if (op.phase === "bucketSorted" || op.phase === "bucketDone") {
        state.sortedBuckets.set(op.bucket, op.sorted || state.deployed[op.bucket]);
      }
    }
  } else if (s.phase === "done") {
    state.workerViews = state.workerViews.map((view) => ({
      phase: "workerIdle",
      worker: view.worker,
      message: `Worker ${view.worker}: idle, all assigned buckets complete.`,
    }));
    state.done = true;
    stop();
  }

  statusEl.textContent = s.message;
  if (shouldRender) {
    render();
  }
}

function play() {
  playing = !playing;
  controls.play.textContent = playing ? "Pause" : "Play";
  if (playing) tick();
}

function stop() {
  playing = false;
  controls.play.textContent = "Play";
  if (timer) window.clearTimeout(timer);
  timer = 0;
}

function tick() {
  if (!playing) return;
  const speed = parseInt(controls.speed.value, 10);
  const stepsPerTick = speed < 70
    ? Math.max(1, Math.floor(speed / 10))
    : Math.ceil((speed - 69) * 12);

  for (let i = 0; i < stepsPerTick && playing; i++) {
    step(false);
  }

  render();

  const delay = speed < 70 ? Math.round(180 - speed * 2.4) : 1;
  timer = window.setTimeout(tick, Math.max(1, delay));
}

function render() {
  if (!state) return;
  msdLabel.textContent = `${state.plan.bucketCount} buckets from top ${state.cfg.msdBits} bits, shift ${state.plan.shift}`;
  const laneLayout = state.plan.visualWorkers > 8 ? "2 columns" : "1 column";
  const scheduler = state.cfg.workStealing ? "work stealing" : "stride buckets";
  lsdLabel.textContent = `${state.plan.visualWorkers} parallel thread lanes, ${laneLayout}, ${scheduler}, ${state.cfg.lsdBits} bits per LSD pass`;
  drawSource();
  drawBuckets();
  drawLsd();
  drawSorted();
}

function setupCanvas(canvas) {
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  const cssWidth = Math.max(320, rect.width);
  const cssHeight = Math.max(120, rect.height);
  canvas.width = Math.floor(cssWidth * dpr);
  canvas.height = Math.floor(cssHeight * dpr);
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, w: cssWidth, h: cssHeight };
}

function drawSource() {
  const { ctx, w, h } = setupCanvas(sourceCanvas);
  ctx.clearRect(0, 0, w, h);
  drawArray(ctx, state.records, 18, 18, w - 36, h - 42, {
    mutedAfter: state.scatteredCount,
    labelEvery: Math.max(1, Math.ceil(state.records.length / 12)),
  });
}

function drawBuckets() {
  const visibleBuckets = bucketDisplayList();
  const pad = 12;
  const noteH = 38;
  const footerH = state.plan.bucketCount > visibleBuckets.length ? 22 : 0;
  const rect = bucketCanvas.getBoundingClientRect();
  const minCellW = 118;
  const minCellH = 58;
  const cols = Math.max(1, Math.min(visibleBuckets.length || 1, Math.floor((rect.width - pad * 2) / minCellW)));
  const rows = Math.max(1, Math.ceil((visibleBuckets.length || 1) / cols));

  bucketCanvas.style.height = `${pad * 2 + noteH + footerH + rows * minCellH}px`;

  const { ctx, w, h } = setupCanvas(bucketCanvas);
  ctx.clearRect(0, 0, w, h);

  const cellW = (w - pad * 2) / cols;
  const cellH = (h - pad * 2 - noteH - footerH) / rows;

  ctx.fillStyle = "#9fb0ac";
  ctx.font = "12px Segoe UI, sans-serif";
  ctx.fillText("Bucket order reads left to right, then top down.", pad, 15);
  ctx.fillText(`${state.cfg.workers} thread lanes scatter up to ${state.cfg.workers} records per MSD cycle.`, pad, 31);
  if (state.scatterDone || state.plan.bucketCount > visibleBuckets.length) {
    ctx.fillText(`Active view: empty buckets are done and hidden after scatter completes; ${visibleBuckets.length} buckets shown, 256 max.`, pad, h - 8);
  }

  for (let i = 0; i < visibleBuckets.length; i++) {
    const b = visibleBuckets[i];
    const col = i % cols;
    const row = Math.floor(i / cols);
    const x = pad + col * cellW;
    const y = pad + noteH + row * cellH;
    const records = state.sortedBuckets.get(b) || state.deployed[b];
    drawBucketCell(ctx, x + 4, y + 4, cellW - 8, cellH - 8, b, records, state.sortedBuckets.has(b));
  }
}

function bucketDisplayList() {
  const active = new Set();
  for (let b = 0; b < state.deployed.length; b++) {
    if (state.deployed[b].length) active.add(b);
    const sorted = state.sortedBuckets.get(b);
    if (sorted && sorted.length) active.add(b);
  }
  for (const view of state.workerViews) {
    if (view.bucket !== undefined) active.add(view.bucket);
  }

  if (state.scatterDone && active.size > 0) {
    return [...active].sort((a, b) => a - b).slice(0, 256);
  }

  return Array.from({ length: Math.min(256, state.plan.bucketCount) }, (_, i) => i);
}

function drawLsd() {
  if (!state.workerViews.length) {
    const { ctx, w, h } = setupCanvas(lsdCanvas);
    ctx.clearRect(0, 0, w, h);
    drawCentered(ctx, w, h, "Parallel LSD worker lanes appear here after MSD deployment.");
    return;
  }

  const lanes = state.workerViews;
  const pad = 12;
  const gap = 8;
  const columns = lanes.length > 8 ? 2 : 1;
  const rows = Math.ceil(lanes.length / columns);
  const minLaneH = 128;
  lsdCanvas.style.height = `${pad * 2 + rows * minLaneH + gap * (rows - 1)}px`;

  const { ctx, w, h } = setupCanvas(lsdCanvas);
  ctx.clearRect(0, 0, w, h);

  const laneW = (w - pad * 2 - gap * (columns - 1)) / columns;
  const laneH = (h - pad * 2 - gap * (rows - 1)) / rows;

  for (let i = 0; i < lanes.length; i++) {
    const col = i % columns;
    const row = Math.floor(i / columns);
    const x = pad + col * (laneW + gap);
    const y = pad + row * (laneH + gap);
    drawWorkerLane(ctx, x, y, laneW, laneH, lanes[i]);
  }
}

function drawSorted() {
  const { ctx, w, h } = setupCanvas(sortedCanvas);
  ctx.clearRect(0, 0, w, h);

  if (!state.done) {
    sortedLabel.textContent = "Appears when the final bucket is complete.";
    drawCentered(ctx, w, h, "Sorted output appears here after the final LSD bucket completes.");
    return;
  }

  const sorted = sortedOutputRecords();
  sortedLabel.textContent = `${sorted.length} records, bucket order left to right.`;
  drawArray(ctx, sorted, 18, 18, w - 36, h - 42, {
    labelEvery: Math.max(1, Math.ceil(sorted.length / 12)),
  });
}

function sortedOutputRecords() {
  const out = [];
  for (let b = 0; b < state.plan.bucketCount; b++) {
    const records = state.sortedBuckets.get(b) || [];
    out.push(...records);
  }
  return out;
}

function drawWorkerLane(ctx, x, y, w, h, op) {
  const active = op.phase !== "workerIdle";
  const steal = op.phase === "workSteal";
  ctx.fillStyle = steal ? "#251f13" : active ? "#182022" : "#14191a";
  ctx.strokeStyle = steal ? "#f2b84b" : active ? "#46d6a7" : "#314044";
  roundRect(ctx, x, y, w, h, 6);
  ctx.fill();
  ctx.stroke();

  ctx.fillStyle = active ? "#edf3f1" : "#9fb0ac";
  ctx.font = "12px Segoe UI, sans-serif";
  const title = active
    ? `Thread ${op.worker} | bucket ${op.bucket} | ${op.phase === "lsdScatter" ? `shift ${op.shift} -> bin ${op.bin}` : op.phase}`
    : `Thread ${op.worker} | idle`;
  ctx.fillText(title, x + 8, y + 15);

  ctx.fillStyle = "#9fb0ac";
  ctx.font = "11px Segoe UI, sans-serif";
  ctx.fillText(trimTextToWidth(ctx, op.message || "", w - 16), x + 8, y + 31);

  if (!active || op.phase === "bucketDone" || op.phase === "workSteal") {
    return;
  }

  const leftW = Math.max(110, w * 0.28);
  const bucketRecords = state.sortedBuckets.get(op.bucket) || state.deployed[op.bucket] || [];
  drawArray(ctx, op.working || op.sorted || bucketRecords, x + 8, y + 42, leftW - 12, h - 50, { compact: true });

  if (!op.binEntries) {
    return;
  }

  const entries = op.binEntries;
  const binGap = 3;
  const binsX = x + leftW + 6;
  const binsW = w - leftW - 14;
  const shown = Math.max(1, entries.length);
  const binW = Math.max(12, (binsW - binGap * (shown - 1)) / shown);
  const binLabelY = y + 48;
  const binY = y + 56;
  const binH = h - 64;

  ctx.fillStyle = "#9fb0ac";
  ctx.font = "10px Segoe UI, sans-serif";
  ctx.fillText(`${entries.length} non-empty bins of ${op.radix}`, binsX, binLabelY);

  for (let i = 0; i < entries.length; i++) {
    const bx = binsX + i * (binW + binGap);
    drawCompactBin(ctx, bx, binY, binW, binH, entries[i].id, entries[i].records, entries[i].id === op.bin);
  }
}

function trimTextToWidth(ctx, text, maxWidth) {
  if (ctx.measureText(text).width <= maxWidth) {
    return text;
  }

  let lo = 0;
  let hi = text.length;
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2);
    if (ctx.measureText(text.slice(0, mid) + "...").width <= maxWidth) {
      lo = mid;
    } else {
      hi = mid - 1;
    }
  }

  return text.slice(0, lo) + "...";
}

function drawCompactBin(ctx, x, y, w, h, id, records, active) {
  ctx.fillStyle = active ? "#2c2716" : "#111617";
  ctx.strokeStyle = active ? "#f2b84b" : "#314044";
  ctx.strokeRect(x, y, w, h);

  ctx.fillStyle = active ? "#f2b84b" : "#9fb0ac";
  ctx.font = "9px Segoe UI, sans-serif";
  if (w >= 18) ctx.fillText(String(id), x + 3, y + 10);

  const chartY = y + 12;
  const chartH = Math.max(4, h - 14);
  const displayRecords = compressRecordsForWidth(records, w - 2, 1);
  const n = Math.max(1, displayRecords.length);
  const barW = Math.max(0.5, (w - 2) / n);
  for (let i = 0; i < displayRecords.length; i++) {
    const rec = displayRecords[i];
    const barH = Math.max(2, recordHeight(rec) * chartH);
    ctx.fillStyle = rec.color;
    ctx.fillRect(x + 1 + i * barW, chartY + chartH - barH, Math.max(1, barW - 0.5), barH);
  }
}

function drawArray(ctx, records, x, y, w, h, options = {}) {
  const displayRecords = compressRecordsForWidth(records, w, options.compact ? 1 : 2);
  const n = Math.max(1, displayRecords.length);
  const gap = n > 180 ? 0 : 1;
  const barW = Math.max(0.5, (w - gap * (n - 1)) / n);

  ctx.strokeStyle = "#314044";
  ctx.strokeRect(x, y, w, h);

  for (let i = 0; i < displayRecords.length; i++) {
    const rec = displayRecords[i];
    const barH = Math.max(2, recordHeight(rec) * (h - 12));
    const bx = x + i * (barW + gap);
    const by = y + h - barH - 1;
    ctx.fillStyle = rec.color;
    ctx.globalAlpha = options.mutedAfter && rec.sourceIndex < options.mutedAfter ? 0.28 : 0.95;
    ctx.fillRect(bx, by, barW, barH);
  }
  ctx.globalAlpha = 1;

  if (!options.compact) {
    ctx.fillStyle = "#9fb0ac";
    ctx.font = "11px Segoe UI, sans-serif";
    const every = options.labelEvery || 12;
    for (let i = 0; i < records.length; i += every) {
      const displayIndex = Math.floor((i / Math.max(1, records.length - 1)) * Math.max(0, displayRecords.length - 1));
      ctx.fillText(String(i), x + displayIndex * (barW + gap), y + h + 14);
    }
  }
}

function compressRecordsForWidth(records, width, minPixelsPerBar) {
  const maxBars = Math.max(1, Math.floor(width / minPixelsPerBar));
  if (records.length <= maxBars) {
    return records;
  }

  const out = [];
  for (let i = 0; i < maxBars; i++) {
    const start = Math.floor((i * records.length) / maxBars);
    const end = Math.max(start + 1, Math.floor(((i + 1) * records.length) / maxBars));
    out.push(records[Math.min(records.length - 1, Math.floor((start + end - 1) / 2))]);
  }

  return out;
}

function drawBucketCell(ctx, x, y, w, h, id, records, sorted) {
  const empty = records.length === 0;
  ctx.fillStyle = empty ? "#14191a" : sorted ? "#17251f" : "#182022";
  ctx.strokeStyle = empty ? "#3b4548" : sorted ? "#46d6a7" : "#314044";
  ctx.lineWidth = 1;
  roundRect(ctx, x, y, w, h, 6);
  ctx.fill();
  ctx.stroke();

  ctx.fillStyle = empty ? "#6f7f7c" : sorted ? "#46d6a7" : "#9fb0ac";
  ctx.font = "11px Segoe UI, sans-serif";
  ctx.fillText(empty ? `B${id} empty done` : `B${id} (${records.length})`, x + 6, y + 14);

  if (empty) {
    return;
  }

  const chartY = y + 20;
  const chartH = h - 26;
  const displayRecords = compressRecordsForWidth(records, w - 12, 1);
  const n = Math.max(1, displayRecords.length);
  const barW = Math.max(0.5, (w - 12) / Math.max(1, n));
  for (let i = 0; i < displayRecords.length; i++) {
    const rec = displayRecords[i];
    const barH = Math.max(2, recordHeight(rec) * chartH);
    ctx.fillStyle = rec.color;
    ctx.fillRect(x + 6 + i * barW, chartY + chartH - barH, Math.max(1, barW - 0.5), barH);
  }
}

function drawBinCell(ctx, x, y, w, h, id, records, active) {
  ctx.fillStyle = active ? "#2c2716" : "#182022";
  ctx.strokeStyle = active ? "#f2b84b" : "#314044";
  roundRect(ctx, x, y, w, h, 6);
  ctx.fill();
  ctx.stroke();

  ctx.fillStyle = active ? "#f2b84b" : "#9fb0ac";
  ctx.font = "11px Segoe UI, sans-serif";
  ctx.fillText(`bin ${id}`, x + 6, y + 14);

  const chartY = y + 20;
  const chartH = h - 24;
  const displayRecords = compressRecordsForWidth(records, w - 12, 1);
  const n = Math.max(1, displayRecords.length);
  const barW = Math.max(0.5, (w - 12) / n);
  for (let i = 0; i < displayRecords.length; i++) {
    const rec = displayRecords[i];
    const barH = Math.max(2, recordHeight(rec) * chartH);
    ctx.fillStyle = rec.color;
    ctx.fillRect(x + 6 + i * barW, chartY + chartH - barH, Math.max(1, barW - 0.5), barH);
  }
}

function drawCentered(ctx, w, h, text) {
  ctx.fillStyle = "#9fb0ac";
  ctx.font = "14px Segoe UI, sans-serif";
  ctx.textAlign = "center";
  ctx.fillText(text, w / 2, h / 2);
  ctx.textAlign = "left";
}

function roundRect(ctx, x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y, x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r);
  ctx.arcTo(x, y, x + w, y, r);
  ctx.closePath();
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

controls.reset.addEventListener("click", reset);
controls.play.addEventListener("click", play);
controls.step.addEventListener("click", step);
for (const key of ["recordCount", "dataMode", "msdBits", "lsdBits", "workerCount", "workStealing"]) {
  controls[key].addEventListener("change", reset);
}
window.addEventListener("resize", render);

reset();
