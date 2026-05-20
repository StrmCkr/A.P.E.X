package generator;

public final class DataTopology {

    private DataTopology() {}

    // =========================================================================
    // DESCRIPTION
    // =========================================================================

    public static String description(DataMode mode) {

        return switch (mode) {

            case RANDOM -> "Uniform full-span entropy distribution";
            case SORTED -> "Perfect monotonic ascending ordering";
            case REVERSE -> "Perfect monotonic descending ordering";
            case NEARLY_SORTED -> "Mostly monotonic ordering with sparse disorder";
            case DUPLICATES -> "High duplicate density with reduced cardinality";
            case LOW_BITS_ONLY -> "Entropy isolated to low-order bits";
            case HIGH_BITS_ONLY -> "Entropy isolated to high-order bits";
            case ZIPFIANISH -> "Zipf-like skewed entropy distribution";

            case ALL_EQUAL -> "Complete entropy collapse";
            case EMPTY -> "Zero-element dataset";
            case SINGLE_ELEMENT -> "Single record dataset";
            case TWO_ELEMENTS_SORTED -> "Minimal sorted dataset";
            case TWO_ELEMENTS_REVERSED -> "Minimal reversed dataset";

            case SAWTOOTH -> "Periodic ascending reset topology";
            case ORGAN_PIPE -> "Ascending then descending organ-pipe topology";
            case ROTATED_SORTED -> "Sorted sequence rotated around pivot";
            case PARTIALLY_SHUFFLED -> "Mostly ordered with randomized disruption";
            case BLOCK_SORTED -> "Locally sorted blocks with global disorder";
            case STAGGERED_RUNS -> "Interleaved monotonic runs";

            case FEW_UNIQUE_VALUES -> "Extremely low cardinality entropy";
            case MANY_DUPLICATES_WITH_OUTLIERS -> "Duplicate-heavy with sparse outliers";
            case ALTERNATING_LOW_HIGH -> "Alternating low/high entropy oscillation";
            case MIN_MAX_ALTERNATING -> "Alternating minimum and maximum keys";

            case NEGATIVE_VALUES -> "Signed-value crossover topology";
            case EXTREME_VALUES -> "Extreme boundary-value concentration";
            case INTEGER_OVERFLOW_RISK -> "Arithmetic-boundary stress topology";

            case GAUSSIAN -> "Gaussian bell-curve entropy";
            case EXPONENTIAL -> "Exponentially decaying distribution";
            case BIMODAL -> "Dual-cluster entropy topology";
            case POWER_LAW -> "Power-law skewed entropy";

            case DELAYED_ENTROPY -> "Entropy delayed into low-order bits";
            case CLUSTERED_KEYS -> "Localized entropy clusters";
            case BIT_REVERSAL -> "Bit-reversed locality disruption";
            case STRIDED -> "Regular stride-based entropy spacing";

            case SPIKE_NOISE -> "Smooth topology with entropy spikes";
            case DESCENDING_BLOCKS -> "Descending monotonic blocks";
            case DENSE_16BIT -> "Dense entropy within 16-bit domains";
            case ENTROPY_BANDS -> "Entropy concentrated into bit bands";

            case TINY_PARTITIONS_STRESS -> "Massive tiny-partition explosion";
            case ALMOST_SORTED_WITH_SPIKES -> "Near-monotonic with sharp spikes";
            case HIGH_BIT_ONLY_MOVING -> "Entropy isolated to highest bits";
            case ALTERNATING_BUCKET_DESTINATIONS -> "Alternating radix locality disruption";
            case EXTREME_DUPLICATE_DENSITY -> "Near-total duplicate collapse";
            case WORST_CASE_MSD_COLLAPSE -> "Adversarial MSD collapse topology";
            case ENTROPY_OSCILLATION -> "Oscillating entropy density";
            case SINGLE_BIT_TAIL_ENTROPY -> "Single delayed entropy tail bit";
            case SPARSE_ENTROPY_EXPLOSION -> "Ultra-sparse distributed entropy";
            case TUPLE_31BIT_OVERFLOW -> "Tuple packing overflow boundary";
            case CROSS_THREAD_BUCKET_SKEW -> "Extreme parallel imbalance";
            case CACHE_THRASH -> "Cache-locality destruction topology";
            case SIGN_BIT_BOUNDARY -> "Entropy around sign-bit boundary";
            case PREFIX_CONSTANT_RANDOM_TAIL -> "Constant prefixes with random tails";

            case TUPLE_29BIT -> "Tuple projection near 29-bit density";
            case TUPLE_30BIT -> "Tuple projection near 30-bit density";

            case PERMUTATION_STRESS -> "Permutation-heavy reorder stress";
            case HIGH_ENTROPY_PREFIX_CONSTANT_TAIL -> "High-order entropy with constant tails";
            case TWO_BUCKET_COLLISION -> "Entropy collapse into two buckets";
            case LARGE_EQUAL_REGION_WITH_RANDOM_END -> "Large equal region with random suffix";
            case RADIX_PATHOLOGICAL -> "Pathological radix fanout";
            case BIT_SPARSE_POWERLAW -> "Sparse bit activation with power-law skew";
            case MICRO_CLUSTERS -> "Tiny localized entropy islands";
            case INTERLEAVED_SORTED_RANDOM -> "Interleaved sorted/random topology";
            case SAWTOOTH_DESCENDING -> "Descending sawtooth topology";
            case SINGLE_HOT_BUCKET -> "Single dominant refinement bucket";
            case LOW_CARDINALITY_HIGH_VOLUME -> "Huge volume with tiny cardinality";
        };
    }

    // =========================================================================
    // TOPOLOGY CLASS
    // =========================================================================

    public static String topologyClass(DataMode mode) {

        return switch (mode) {

            case RANDOM,
                 GAUSSIAN,
                 EXPONENTIAL,
                 BIMODAL ->
                "FULL_ENTROPY";

            case SORTED,
                 REVERSE,
                 NEARLY_SORTED,
                 ROTATED_SORTED ->
                "MONOTONIC";

            case DUPLICATES,
                 FEW_UNIQUE_VALUES,
                 EXTREME_DUPLICATE_DENSITY,
                 LOW_CARDINALITY_HIGH_VOLUME,
                 ALL_EQUAL ->
                "DUPLICATE_HEAVY";

            case LOW_BITS_ONLY,
                 SINGLE_BIT_TAIL_ENTROPY ->
                "LOW_BIT_ENTROPY";

            case HIGH_BITS_ONLY,
                 HIGH_BIT_ONLY_MOVING,
                 HIGH_ENTROPY_PREFIX_CONSTANT_TAIL ->
                "HIGH_BIT_ENTROPY";

            case ZIPFIANISH,
                 POWER_LAW,
                 BIT_SPARSE_POWERLAW ->
                "SKEWED_DISTRIBUTION";

            case EMPTY,
                 SINGLE_ELEMENT,
                 TWO_ELEMENTS_SORTED,
                 TWO_ELEMENTS_REVERSED ->
                "TRIVIAL";

            case SAWTOOTH,
                 SAWTOOTH_DESCENDING,
                 ENTROPY_OSCILLATION,
                 ALTERNATING_LOW_HIGH,
                 MIN_MAX_ALTERNATING ->
                "OSCILLATING";

            case ORGAN_PIPE,
                 BLOCK_SORTED,
                 STAGGERED_RUNS,
                 DESCENDING_BLOCKS,
                 INTERLEAVED_SORTED_RANDOM ->
                "STRUCTURED_MONOTONIC";

            case PARTIALLY_SHUFFLED,
                 ALMOST_SORTED_WITH_SPIKES ->
                "PARTIAL_DISORDER";

            case MANY_DUPLICATES_WITH_OUTLIERS,
                 LARGE_EQUAL_REGION_WITH_RANDOM_END ->
                "COLLAPSED_WITH_OUTLIERS";

            case NEGATIVE_VALUES,
                 SIGN_BIT_BOUNDARY ->
                "SIGNED_BOUNDARY";

            case EXTREME_VALUES,
                 INTEGER_OVERFLOW_RISK ->
                "NUMERIC_BOUNDARY";

            case DELAYED_ENTROPY,
                 PREFIX_CONSTANT_RANDOM_TAIL ->
                "DELAYED_ENTROPY";

            case CLUSTERED_KEYS,
                 MICRO_CLUSTERS ->
                "CLUSTERED_ENTROPY";

            case BIT_REVERSAL,
                 CACHE_THRASH,
                 ALTERNATING_BUCKET_DESTINATIONS ->
                "LOCALITY_PATHOLOGICAL";

            case STRIDED,
                 ENTROPY_BANDS ->
                "STRUCTURED_ENTROPY";

            case SPIKE_NOISE ->
                "LOCALIZED_DISRUPTION";

            case DENSE_16BIT ->
                "DENSE_LOCAL_DOMAIN";

            case TINY_PARTITIONS_STRESS,
                 SINGLE_HOT_BUCKET ->
                "REFINEMENT_STRESS";

            case WORST_CASE_MSD_COLLAPSE,
                 RADIX_PATHOLOGICAL ->
                "RADIX_PATHOLOGICAL";

            case SPARSE_ENTROPY_EXPLOSION ->
                "SPARSE_ENTROPY";

            case TUPLE_29BIT,
                 TUPLE_30BIT,
                 TUPLE_31BIT_OVERFLOW ->
                "TUPLE_PROJECTION";

            case CROSS_THREAD_BUCKET_SKEW ->
                "PARALLEL_SKEW";

            case PERMUTATION_STRESS ->
                "PERMUTATION_PATHOLOGY";

            case TWO_BUCKET_COLLISION ->
                "BINARY_COLLISION";
        };
    }

    // =========================================================================
    // APEX BEHAVIOR
    // =========================================================================

    public static String apexBehavior(DataMode mode) {

        return switch (mode) {

            case RANDOM -> "Balanced refinement pressure";
            case SORTED -> "Ideal monotonic skipping";
            case REVERSE -> "Full convergence reversal";
            case NEARLY_SORTED -> "Partial refinement suppression";
            case DUPLICATES -> "Duplicate-collapse optimization";
            case LOW_BITS_ONLY -> "Heavy LSD refinement";
            case HIGH_BITS_ONLY -> "Aggressive MSD fanout";
            case ZIPFIANISH -> "Skewed partition pressure";

            case ALL_EQUAL -> "Immediate entropy collapse";
            case EMPTY -> "Immediate completion";
            case SINGLE_ELEMENT -> "Zero refinement";
            case TWO_ELEMENTS_SORTED -> "Trivial convergence";
            case TWO_ELEMENTS_REVERSED -> "Single-swap convergence";

            case SAWTOOTH -> "Oscillating convergence";
            case ORGAN_PIPE -> "Centralized entropy convergence";
            case ROTATED_SORTED -> "Delayed monotonic recognition";
            case PARTIALLY_SHUFFLED -> "Localized refinement";
            case BLOCK_SORTED -> "Block-local convergence";
            case STAGGERED_RUNS -> "Distributed run refinement";

            case FEW_UNIQUE_VALUES -> "Extreme duplicate pressure";
            case MANY_DUPLICATES_WITH_OUTLIERS -> "Sparse outlier refinement";
            case ALTERNATING_LOW_HIGH -> "Oscillating radix routing";
            case MIN_MAX_ALTERNATING -> "Extreme bucket oscillation";

            case NEGATIVE_VALUES -> "Signed-boundary handling";
            case EXTREME_VALUES -> "Boundary-value pressure";
            case INTEGER_OVERFLOW_RISK -> "Overflow-safe execution";

            case GAUSSIAN -> "Dense central bucket refinement";
            case EXPONENTIAL -> "Highly skewed fanout";
            case BIMODAL -> "Dual-cluster convergence";
            case POWER_LAW -> "Long-tail sparse refinement";

            case DELAYED_ENTROPY -> "Adaptive MSD relocation";
            case CLUSTERED_KEYS -> "Localized cluster refinement";
            case BIT_REVERSAL -> "Radix locality degradation";
            case STRIDED -> "Structured fanout behavior";

            case SPIKE_NOISE -> "Localized amplification";
            case DESCENDING_BLOCKS -> "Block reversal refinement";
            case DENSE_16BIT -> "Dense local radix convergence";
            case ENTROPY_BANDS -> "Sparse radix activation";

            case TINY_PARTITIONS_STRESS -> "Tiny-sort explosion stress";
            case ALMOST_SORTED_WITH_SPIKES -> "Selective refinement";
            case HIGH_BIT_ONLY_MOVING -> "Strong MSD extraction";
            case ALTERNATING_BUCKET_DESTINATIONS -> "Cache-routing instability";
            case EXTREME_DUPLICATE_DENSITY -> "Near-total entropy suppression";
            case WORST_CASE_MSD_COLLAPSE -> "Entropy deception";
            case ENTROPY_OSCILLATION -> "Unstable convergence";
            case SINGLE_BIT_TAIL_ENTROPY -> "Delayed final convergence";
            case SPARSE_ENTROPY_EXPLOSION -> "Sparse tuple projection candidate";
            case TUPLE_31BIT_OVERFLOW -> "Tuple overflow protection";
            case CROSS_THREAD_BUCKET_SKEW -> "Parallel scheduling imbalance";
            case CACHE_THRASH -> "Cache miss amplification";
            case SIGN_BIT_BOUNDARY -> "Sign-boundary refinement";
            case PREFIX_CONSTANT_RANDOM_TAIL -> "Tail-only refinement";

            case TUPLE_29BIT -> "Efficient tuple projection";
            case TUPLE_30BIT -> "High-density tuple refinement";

            case PERMUTATION_STRESS -> "Heavy reorder amplification";
            case HIGH_ENTROPY_PREFIX_CONSTANT_TAIL -> "Aggressive MSD partitioning";
            case TWO_BUCKET_COLLISION -> "Dual-bucket collapse";
            case LARGE_EQUAL_REGION_WITH_RANDOM_END -> "Late entropy activation";
            case RADIX_PATHOLOGICAL -> "Worst-case radix behavior";
            case BIT_SPARSE_POWERLAW -> "Sparse irregular refinement";
            case MICRO_CLUSTERS -> "Tiny local convergence";
            case INTERLEAVED_SORTED_RANDOM -> "Mixed topology convergence";
            case SAWTOOTH_DESCENDING -> "Reverse oscillating convergence";
            case SINGLE_HOT_BUCKET -> "Refinement amplification";
            case LOW_CARDINALITY_HIGH_VOLUME -> "Massive duplicate pressure";
        };
    }

    // =========================================================================
    // ENTROPY VISUALIZATION
    // =========================================================================

    public static String entropyExample(DataMode mode) {

        return switch (mode) {

            case RANDOM ->
                "1111111111111111111111111111111111111111111111111111111111111111";

            case SORTED ->
                "0000000000000000000000000000000000000000000000000000000000000001";

            case REVERSE ->
                "1111111111111111111111111111111111111111111111111111111111111110";

            case LOW_BITS_ONLY ->
                "0000000000000000000000000000000000000000000011111111111111111111";

            case HIGH_BITS_ONLY ->
                "1111111111111111000000000000000000000000000000000000000000000000";

            case DELAYED_ENTROPY ->
                "0000000000000000000000000000000000000000000000001111111111111111";

            case SPARSE_ENTROPY_EXPLOSION ->
                "1000000010000000100000001000000010000000100000001000000010000001";

            case ENTROPY_OSCILLATION ->
                "1111000011110000111100001111000011110000111100001111000011110000";

            case ALTERNATING_BUCKET_DESTINATIONS ->
                "1010101010101010101010101010101010101010101010101010101010101010";

            case CACHE_THRASH ->
                "1100110011001100110011001100110011001100110011001100110011001100";

            case BIT_REVERSAL ->
                "1000000000000001000000000000000100000000000000010000000000000001";

            case SINGLE_HOT_BUCKET ->
                "0000000000000000000000000000000000000000000000000000000000001111";

            default ->
                "N/A";
        };
    }

    // =========================================================================
    // PRINT TOPOLOGY
    // =========================================================================

    public static void printTopology(DataMode mode) {

        System.out.println("Data mode        : " + mode);
        System.out.println("Topology class   : " + topologyClass(mode));
        System.out.println("Description      : " + description(mode));
        System.out.println("Apex behavior    : " + apexBehavior(mode));
        System.out.println("Entropy example  : " + entropyExample(mode));

        switch (mode) {

            case RANDOM ->
                System.out.println("Example          : Uniform entropy across all radix regions");

            case SORTED ->
                System.out.println("Example          : Strict ascending monotonic ordering");

            case REVERSE ->
                System.out.println("Example          : Strict descending monotonic ordering");

            case NEARLY_SORTED ->
                System.out.println("Example          : Mostly sorted with sparse local disorder");

            case DUPLICATES ->
                System.out.println("Example          : Large repeated key populations");

            case LOW_BITS_ONLY ->
                System.out.println("Example          : Entropy only appears in low-order bits");

            case HIGH_BITS_ONLY ->
                System.out.println("Example          : Entropy concentrated in highest-order bits");

            case ZIPFIANISH ->
                System.out.println("Example          : Heavy skew toward dominant values");

            case ALL_EQUAL ->
                System.out.println("Example          : Every key is identical");

            case EMPTY ->
                System.out.println("Example          : No records");

            case SINGLE_ELEMENT ->
                System.out.println("Example          : Single-record dataset");

            case TWO_ELEMENTS_SORTED ->
                System.out.println("Example          : Already sorted pair");

            case TWO_ELEMENTS_REVERSED ->
                System.out.println("Example          : Minimal reversed pair");

            default ->
                System.out.println("Example          : Specialized entropy topology");
        }
    }
 
 
    
}