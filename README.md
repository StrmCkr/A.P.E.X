# APEX 
/*
 * Apex : Adaptive Parallel Entropic Dispatch 
 *
 * High-performance entropy-adaptive radix sorting for large-scale 64-bit
 * key/value datasets on modern multi-core processors.
 *
 * Core idea:
 *   Apex dynamically shapes radix partition topology around observed entropy,
 *   selecting optimal MSD bit windows to maximize partition quality,
 *   improve locality, balance parallel work distribution,
 *   and minimize unnecessary memory movement and radix passes.
 *
 * Architectural model:
 *   - Entropy-adaptive MSD partition planning
 *   - Parallel radix scatter and partition execution
 *   - Hybrid MSD-LSD radix pipeline
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
 *       and LSD processing across all available CPU cores
 *
 * E : Entropic
 *     - analyzes entropy distribution across bit regions to identify
 *       high-information radix windows while avoiding degenerate
 *       or low-cardinality partition collapse
 *
 * X : Dispatch
 *     - coordinates adaptive execution paths, partition scheduling,
 *       memory locality, and heap/off-heap execution strategies
 *
 * Entropy determination engine:
 *   Apex uses bitwise statistical analysis to determine effective
 *   entropy regions and eliminate non-contributing radix windows.
 *
 *   Bitwise entropy reduction includes:
 *     - XOR analysis to detect changing bit regions
 *     - AND reduction to identify globally shared bit patterns
 *     - OR reduction to identify globally active bit regions
 *     - Constant-prefix elimination
 *     - Sparse-entropy detection
 *     - Delayed-entropy recognition
 *     - Duplicate-density detection
 *
 *   These analyses allow Apex to:
 *     - skip entropy-free radix passes
 *     - dynamically suppress refinement amplification and partition fanout
 *     - reduce tiny partition explosion
 *     - improve cache locality and partition balance
 *     - dynamically relocate MSD extraction windows
 *
 * Hybrid radix pipeline:
 *   - MSD radix scatter for global entropy-guided partitioning
 *   - LSD radix refinement for efficient in-partition ordering
 *   - Adaptive tuple-cycle optimization for compact entropy domains
 *   - Dynamic fallback paths for pathological distributions
 *
 * Features:
 *   - Entropy-aware MSD window planning
 *   - Adaptive radix-width selection
 *   - Runtime auto-tuning of radix geometry and thresholds
 *   - Packed tuple-cycle execution
 *   - Locality-aware partition scheduling
 *   - Hybrid heap and off-heap execution
 *   - Parallel histogram, scatter, and partition processing
 *   - Adaptive tiny-partition handling
 *   - Degenerate partition mitigation
 *
 *  Sparse entropy tuple projection:
 *  - Apex can dynamically remap sparse distributed entropy regions
 *   into compact tuple domains, reducing radix space, histogram size,
 *   and memory traffic for low-density entropy distributions.  
 *  - Tuple-cycle execution is adaptively enabled only when projected
 *   entropy density produces a net locality and radix-efficiency gain.
 *   
 *    *
 * 	Sparse hypercube entropy collapse:
 *  - Apex models distributed key entropy as occupancy across a sparse
 *    high-dimensional radix hypercube.
 *
 *  - Entropy analysis identifies inactive, correlated, or degenerate
 *    radix dimensions and dynamically collapses the effective search
 *    space into compact executable tuple domains.
 *
 *  - This dimensionality reduction allows Apex to:
 *      - eliminate entropy-empty radix axes
 *      - compress sparse radix occupancy into dense tuple projections
 *      - reduce histogram and scatter amplification
 *      - suppress degenerate partition fanout
 *      - minimize unnecessary radix traversal depth
 *      - improve cache locality and work distribution
 *
 *  - Collapse planning is driven by observed bit occupancy,
 *    tuple cardinality, duplicate density, and entropy continuity
 *    across radix dimensions.
 *
  *  - The resulting execution topology behaves as an adaptive
 *    dimensional projection system, remapping sparse entropy
 *    regions into lower-dimensional executable spaces.
 *
 *
 * Designed for:
 *   - Large-scale 64-bit sorting workloads
 *   - High-core-count desktop and server processors
 *   - Memory-bandwidth-sensitive workloads
 *   - Adversarial and real-world data distributions
 *
 * Robustness:
 *   Maintains stable behavior across:
 *     - duplicates and low-cardinality datasets
 *     - sparse and delayed entropy
 *     - clustered and skewed partitions
 *     - sorted and partially sorted runs
 *     - pathological radix distributions
 *     - highly imbalanced partition topologies
 *
 * Philosophy:
 *   Apex prioritizes adaptive partition quality, locality,
 *   scalability, and memory efficiency over fixed radix geometry
 *   or microbenchmark specialization.
 *
 *   The architecture is designed for experimentation,
 *   extensibility, and deep runtime adaptability rather than
 *   static one-size-fits-all radix behavior.
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
