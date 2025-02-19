package simpledb.optimizer;

import simpledb.execution.Predicate;
import simpledb.execution.Predicate.Op;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    int[] buckets;

    int min;

    int max;

    double width;

    int ntups;

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
        // TODO: some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        // 10个桶， min 1， max 10
        // bucket1, bucket2, bucket3,...,bucket10
        // 所以要max - min + 1
        // 又因为桶宽如果是小数，会导致数据误差偏大，所以桶宽最小应该为1
        this.width = Math.max(1.0, 1.0 * (max - min + 1) / buckets);
        ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // TODO: some code goes here
        int index = findIndex(v);
        buckets[index]++;
        ntups++;
    }

    private int findIndex(int v) {
        if (v > max || v < min) {
            throw new IllegalArgumentException("v is not int the bucket");
        }
        if (v == max) return buckets.length - 1;
        else return (int)((v - min)/width);
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
        // TODO: some code goes here
        if (op.equals(Op.GREATER_THAN)) {
            if (v > max) {
                return 0.0;
            }
            if (v <= min) {
                return 1.0;
            }
            int index = findIndex(v);
            double selectivity = 0.0;
            for (int i = index + 1; i < buckets.length; i++) {
                selectivity += 1.0 * buckets[i] / ntups;
            }
            double b_part = ((index + 1.0) * width + min - v) / width;
            double b_f = 1.0 * buckets[index] / ntups;
            return b_part * b_f + selectivity;
        } else if (op.equals(Op.GREATER_THAN_OR_EQ)) {
            // 因为width至少为1，所以>=可以写为>和=的和
            return estimateSelectivity(Op.GREATER_THAN, v) + estimateSelectivity(Op.EQUALS, v);
        } else if (op.equals(Op.LESS_THAN)) {
            return 1 - estimateSelectivity(Op.GREATER_THAN, v);
        } else if (op.equals(Op.LESS_THAN_OR_EQ)) {
            return estimateSelectivity(Op.LESS_THAN, v) + estimateSelectivity(Op.EQUALS, v);
        } else if (op.equals(Op.EQUALS)) {
            if (v >= max || v <= min) {
                return 0.0;
            }
            int index = findIndex(v);
            return 1.0 * buckets[index] / width / ntups;
        } else if (op.equals(Op.NOT_EQUALS)) {
            return 1 - estimateSelectivity(Op.EQUALS, v);
        }
        return 0.0;
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // TODO: some code goes here
        String toString = "width is: " + width + " bucket num is: " + buckets.length;
        return null;
    }
}
