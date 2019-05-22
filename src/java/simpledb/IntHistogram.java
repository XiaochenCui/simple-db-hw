package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;

    private int ntups;
    private int[] histogram;
    private int width;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;

        this.ntups = 0;

        int range = max-min+1;
        if (range > buckets) {
            width = (int) Math.ceil(range/(double)buckets);
            histogram = new int[buckets];
        } else {
            width = 1;
            histogram = new int[range];
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int bucket_index = (int) Math.floor((double)(v-min)/width);

        histogram[bucket_index]++;
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        int bucket_index = (int) Math.ceil((double)(v-min)/width);
        double bucket_fraction;
        int fellow;
        int bucket_right;
        int bucket_left;

        switch (op) {
            case EQUALS:
                if ((v < min) || (v > max)) return 0;

                int height = histogram[bucket_index];
                return (double) height/width/ntups;

            case GREATER_THAN:
                if (v < min) return 1;
                if (v >= max) return 0;

                bucket_right = (bucket_index + 1) * width;
                bucket_fraction = (double) (bucket_right - v) / width;
                fellow = (int) (bucket_fraction * histogram[bucket_index]);
                for (int i = bucket_index+1; i < histogram.length; i++) {
                    fellow += histogram[i];
                }
                return (double) fellow / ntups;

            case LESS_THAN:
                if (v > max) return 1;
                if (v <= min) return 0;

                bucket_left = bucket_index * width + 1;
                bucket_fraction = (double) (v - bucket_left) / width;
                fellow = (int) (bucket_fraction * histogram[bucket_index]);
                for (int i = bucket_index-1; i >= 0; i--) {
                    fellow += histogram[i];
                }
                return (double) fellow / ntups;

            case LESS_THAN_OR_EQ:
                if (v >= max) return 1;
                if (v < min) return 0;

                bucket_left = bucket_index * width + 1;
                bucket_fraction = (double) (v - bucket_left + 1) / width;
                fellow = (int) (bucket_fraction * histogram[bucket_index]);
                for (int i = bucket_index-1; i >= 0; i--) {
                    fellow += histogram[i];
                }
                return (double) fellow / ntups;
            case GREATER_THAN_OR_EQ:
                if (v <= min) return 1;
                if (v > max) return 0;

                bucket_right = (bucket_index + 1) * width;
                bucket_fraction = (double) (bucket_right - v + 1) / width;
                fellow = (int) (bucket_fraction * histogram[bucket_index]);
                for (int i = bucket_index+1; i < histogram.length; i++) {
                    fellow += histogram[i];
                }
                return (double) fellow / ntups;

            case LIKE:
                break;
            case NOT_EQUALS:
                if ((v < min) || (v > max)) return 1;

                height = histogram[bucket_index];
                return 1 - (double) height/width/ntups;
        }
        return 0;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
