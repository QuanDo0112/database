package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private int _numberOfBuckets;
	private int _min;
	private int _max;
	private int _bucketStep;
	private int _range;
	private int[] _buckets;
	
	private int UNDER_BUCKET = -1; // Using an aggregate that is less than the min
	private int OVER_BUCKET = -2; // Using an aggregate greater than the max

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	_numberOfBuckets = buckets;
    	_min = min;
    	_max = max;
    	
    	initialize();
    }
    
    private void calculateRange() {
    	_range = _max - _min;
    }
    
    private void calculateBucketParameters() {
    	if (requestedTooManyBuckets()) {
    		_numberOfBuckets = _range;
    		_bucketStep = 1;
    	} else {
    		_bucketStep = _range / _numberOfBuckets;
    	}
    }
    
    private void initialize() {
    	calculateRange();
    	calculateBucketParameters();
    	createBuckets();
    }
    
    /***
     * Returns true if we have a case where
     * the number of buckets is greater than the
     * the difference between the max and min.
     * e.g. requests 200 buckets for a min - max range of
     * (0, 100). Then just use the minimum bucket step of 1
     * and each integer in the range has its own bucket
     * @return
     */
    private boolean requestedTooManyBuckets() {
    	return _numberOfBuckets > _range;
    }
    
    private void createBuckets() {
    	_buckets = new int[_numberOfBuckets];
    	assert (_bucketStep != 0);
    	int stepCount = 0;
    	for (int i = _min; i < _max; i += _bucketStep) {
    		_buckets[stepCount++] = 0;
    	}
    	
    	assert (stepCount == _numberOfBuckets);
    }
    
    private int getBucketIndex(int value) {
    	if (value < _min) return UNDER_BUCKET;
    	if (value > _max) return OVER_BUCKET;
    	
    	// Fix to ensure that the max value goes in the last bucket
    	if ((value == _max) && (value % _bucketStep == 0)) value--;
    	
    	int bottom = value  - _min;
    	int index = bottom / _bucketStep;
    	assert (index < _numberOfBuckets);
    	return index;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	assert (v >= _min);
    	assert (v <= _max);
    	
    	int index = getBucketIndex(v);
    	int newValue = _buckets[index] + 1;
    	//System.out.println("Setting index; " + index+ " to value: " + newValue);
    	_buckets[index] = newValue;
    }
    
    public int valueCount() {
    	int sum = 0;
    	for (int i = 0; i < _buckets.length; i++) {
    		sum += _buckets[i];
    	}
    	
    	return sum;
    }
    
    private int bucketCount(int index) {
    	if (index == UNDER_BUCKET) return 0;
    	if (index == OVER_BUCKET) return 0;
    	
    	assert (index < _buckets.length);
    	return _buckets[index];
    }
    
    private int greaterThan(int bucketIndex) {
    	int sum = 0;
    	if (bucketIndex == UNDER_BUCKET) return valueCount();
    	if (bucketIndex == OVER_BUCKET) return 0;
    	
    	for (int i = bucketIndex + 1; i < _buckets.length; i++) {
    		//System.out.println("Index is: " + i + " = " + _buckets[i]);
    		sum += _buckets[i];
    	}
    	//System.out.println("Getting greater than index: " + index + " sum: " + sum);
    	return sum;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	int bucketIndex = getBucketIndex(v);
    	
    	// Have to be double otherwise int division chops off incorrect values
    	double valuesInBucket = bucketCount(bucketIndex);
    	double totalValues = valueCount();
    	double result = 0;
    	
    	switch (op) {
		case EQUALS:
			result = valuesInBucket / totalValues;
			break;
		case GREATER_THAN:
			result = greaterThan(bucketIndex) / totalValues;
			break;
		case GREATER_THAN_OR_EQ:
			//System.out.println("Greater than: " + greaterThan(bucketIndex) + " buckets: " + valuesInBucket + " total values: " + totalValues);
			result = greaterThan(bucketIndex) + valuesInBucket;
			result /= totalValues;
			//System.out.println("Result in GEQ: " + result);
			break;
		case LESS_THAN:
			result = (totalValues - greaterThan(bucketIndex) - valuesInBucket) / totalValues;
			break;
		case LESS_THAN_OR_EQ:
			result = (totalValues - greaterThan(bucketIndex)) / totalValues;
			break;
		case LIKE:
			assert (false);
			break;
		case NOT_EQUALS:
			result = (totalValues - valuesInBucket) / totalValues;
			break;
		default:
			assert (false);
			break;
    	}

    	//System.out.println("result: " + result);
    	assert (result <= 1); // Should always be a percentage
    	return result;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
    	assert (false);
        return null;
    }
}
