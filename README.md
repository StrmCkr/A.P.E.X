# APEX 
/*
 * A.P.E.X : Adaptive Parallel Exact-Variability Dispatch
 *
 * High-performance bit-consensus adaptive radix sorting for large-scale
 * 64-bit key/value datasets on modern multi-core processors.
 *
 * Core idea:
 *   A.P.E.X dynamically shapes radix partition topology using deterministic
 *   per-bucket bit-consistency analysis derived from bitwise AND/OR/XOR
 *   reductions over key sets. This enables elimination of refinement work
 *   on invariant bit positions, improving locality, balancing parallel work
 *   distribution, and reducing unnecessary memory movement and radix passes.
 *
 * Architectural model:
 *   - Bit-consistency–adaptive MSD partition planning
 *   - Parallel radix histogram, scatter, and partition execution
 *   - Hybrid MSD–LSD radix pipeline
 *   - Locality-aware memory dispatch and execution routing
 *   - Adaptive in-place and off-heap processing strategies
 *
 * A : Adaptive
 *     - dynamically selects radix windows, partition geometry,
 *       execution thresholds, and processing strategies from
 *       observed key distributions and runtime characteristics
 *
 * P : Parallel
 *     - fully parallel histogramming, scatter, partition execution,
 *       and LSD processing across available CPU cores
 *
 * E : Exact
 *     - computes deterministic per-bucket bit-consistency masks using
 *       AND/OR reduction; identifies invariant and variable bit positions
 *       exactly with no probabilistic or learned model assumptions
 *
 * X : Dispatch
 *     - coordinates adaptive execution paths, partition scheduling,
 *       memory locality, and heap/off-heap execution strategies
 *
 * Bit-consistency analysis engine:
 *   A.P.E.X uses deterministic bitwise reduction to classify per-bit
 *   behavior within radix buckets.
 *
 *   Bitwise consistency operators:
 *     - XOR analysis to detect disagreement across keys
 *     - AND reduction to identify universally set bits
 *     - OR reduction to identify any-set bits
 *
 *   From these, a per-bucket variability mask is computed:
 *     - invariant bit positions: OR == AND
 *     - variable bit positions: OR != AND
 *
 *   This enables:
 *     - skipping refinement on invariant radix dimensions
 *     - reducing unnecessary LSD passes
 *     - suppressing degenerate partition amplification
 *     - improving cache locality and workload balance
 *     - dynamically adjusting MSD extraction windows
 *
 * Hybrid radix pipeline:
 *   - MSD radix scatter for global partitioning
 *   - LSD radix refinement for intra-bucket ordering
 *   - deterministic tuple-cycle optimization for low-dimensional domains
 *   - fallback execution paths for skewed or adversarial distributions
 *
 * Features:
 *   - Bit-consistency–driven MSD window planning
 *   - Adaptive radix-width selection
 *   - Runtime auto-tuning of radix geometry and thresholds
 *   - Packed tuple-cycle execution
 *   - Locality-aware partition scheduling
 *   - Hybrid heap and off-heap execution
 *   - Parallel histogram, scatter, and partition processing
 *   - Adaptive small-bucket handling
 *   - Degenerate partition mitigation
 *
 * Tuple projection optimization:
 *   - A.P.E.X may remap sparse bit-variability distributions into compact
 *     tuple domains when doing so reduces refinement cost and improves
 *     locality.
 *
 *   - Tuple execution is enabled only when projected cardinality yields
 *     net reduction in radix refinement work.
 *
 * Sparse partition collapse model:
 *   - The system models key distribution across radix dimensions as a
 *     sparse occupancy structure over bit positions.
 *
 *   - Deterministic analysis identifies:
 *       - inactive bit dimensions (no variation)
 *       - correlated bit structures
 *       - degenerate partition splits
 *
 *   - These are used to:
 *       - eliminate invariant radix axes
 *       - compress sparse partitions into dense representations
 *       - reduce histogram and scatter overhead
 *       - suppress unnecessary recursion depth
 *       - improve cache locality and scheduling efficiency
 *
 *   - Collapse decisions are driven by:
 *       - bit occupancy
 *       - duplicate density
 *       - bucket cardinality
 *       - observed variability mask structure
 *
 * Designed for:
 *   - Large-scale 64-bit sorting workloads
 *   - High-core-count desktop and server processors
 *   - Memory-bandwidth-sensitive workloads
 *   - adversarial and real-world distributions
 *
 * Robustness:
 *   Maintains stable behavior across:
 *     - duplicate-heavy datasets
 *     - low-cardinality distributions
 *     - sparse and delayed variability
 *     - clustered and skewed partitions
 *     - nearly sorted and partially ordered runs
 *     - pathological radix distributions
 *     - highly imbalanced partition topologies
 *
 * Philosophy:
 *   A.P.E.X prioritizes adaptive partition quality, deterministic bit-level
 *   structure analysis, locality, and scalability over fixed radix geometry
 *   or probabilistic modeling.
 *
 *   The architecture is designed for experimentation, extensibility,
 *   and runtime adaptability within a deterministic execution model.
 */
 
 * - Visualizer notes:
- an approimator for the effects of the real code
- missing optimization for Entropic distributions
https://strmckr.github.io/A.P.E.X/
 */


 /* Runtime notes:
 *     --enable-native-access=ALL-UNNAMED
 *     --enable-preview
 *     -XX:MaxDirectMemorySize=80g
 *      -Xmx16G
*     --add-modules jdk.incubator.vector
*     -XX:UseAVX=2 | -XX:UseAVX=3 
 *
 * Options:
 *   mode          Single data mode for default run.
 *   modes         Multiple modes or mode range for default run.
 *   records       Default-run record count, list, or range.
 *   msd           MSD_BITS auto-tune range.
 *   lsd           LSD_BITS auto-tune range.
 *   tiny          Tiny partition threshold auto-tune range.
 *   tupleBits     Entropy tuple dimension cap for direct tuple-space passes.
 *   config        Locked config: MSD_BITS,LSD_BITS,TINY. Bypasses auto-tune.
 *   threads       Worker thread count. auto uses available processors.
 *   largePermits  Concurrent off-heap large partitions. auto uses max(1, threads/8).
 *   workStealing  Dynamic largest-first LSD bucket scheduling. true by default.
 *   tuplePacking  Packs sparse entropy coordinates into LSD passes. false by default.
 *   heapScratch   Max heap-backed scratch records per worker before off-heap path.
 *   tuneRecords   Record count used during the single auto-tune pass.
 *   warmupRecords Selected-config warmup record count.
 *   sweep         Run the full mode sweep instead of the default selected-mode run.
 *   sweepRecords  Records per mode during sweep.
 */
