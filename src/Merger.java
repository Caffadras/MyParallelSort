
/**
 * Provides merge methods to use in parallel. 
 * Should be used only in MyParallelSort class.
 */
public class Merger {
	
	/**
	 * Merge version which merges two arrays using same algorithm as partialMerge method uses, however, cannot be used in parallel in this implementation.
	 * @param destinationArray array to place merged elements
	 * @param leftArray first array to merge
	 * @param rightArray second array to merge
	 */
	public static void merge(ArrayWrapper destinationArray, ArrayWrapper leftArray, ArrayWrapper rightArray) {
		int smallerElements = 0;
		for(int i =0; i<leftArray.length; ++i) {
			smallerElements = countSmallerElements(rightArray, leftArray.get(i), smallerElements == 0? smallerElements : smallerElements -1 );
			destinationArray.set(i + smallerElements, leftArray.get(i));
		}
		smallerElements = 0; 
		for(int i =0; i<rightArray.length; ++i) {
			smallerElements = countSmallerEqualElements(leftArray, rightArray.get(i), smallerElements == 0? smallerElements : smallerElements -1);
			destinationArray.set(i + smallerElements, rightArray.get(i));
		}
	} 
	
	/**
	 * Merges only a specified range of elements from one array. 
	 * This method is used in parallel. 
	 * @param destinationArray array to place merged elements
	 * @param arrayToMerge elements from this array are merged 
	 * @param from start index
	 * @param to end index
	 * @param secondArray to calculate position of an element form the arrayToMerge, we need to reference the second array, the array that we are merging with
	 * @param countEqual if we should count equal elements as smaller elements
	 * @throws IllegalArgumentException is the specified range is illegal (from < 0 || from > to || to > arrayToMerge.length )
	 */
	public static void partialMerge(ArrayWrapper destinationArray, ArrayWrapper arrayToMerge, int from, int to, ArrayWrapper secondArray, boolean countEqual) {
		if (from < 0 || from > to || to > arrayToMerge.length )
			throw new IllegalArgumentException();
		int smallerElements = 0; 
		for(int i=from; i<to; ++i) {
			
			if (countEqual) {
				smallerElements = countSmallerEqualElements(secondArray, arrayToMerge.get(i), smallerElements == 0? smallerElements : smallerElements -1);
			}
			else {
				smallerElements = countSmallerElements(secondArray, arrayToMerge.get(i), smallerElements == 0? smallerElements : smallerElements -1 );
			}
			destinationArray.set( i + smallerElements, arrayToMerge.get(i));
		}
	}
	
	/**
	 * Count how many elements are smaller in the array than a specified element.
	 * @param array array to count elements 
	 * @param target the element to compare with
	 * @param minIdx the starting position to look for
	 * @return the amount of elements that are smaller then a target
	 */
	private static int countSmallerElements(ArrayWrapper array, int target, int minIdx) {
		for(int i = minIdx; i<array.length; ++i ) {
			if (array.get(i) >= target ) {
				return i;
			}
		}
		return array.length;
	}
	
	/**
	 * Count how many elements are smaller or equal in the array than a specified element.
	 * @param array array to count elements 
	 * @param target the element to compare with
	 * @param minIdx the starting position to look for
	 * @return the amount of elements that are smaller or equal then a target
	 */
	private static int countSmallerEqualElements(ArrayWrapper array, int target, int minIdx) {
		for(int i = minIdx; i<array.length; ++i ) {
			if (array.get(i) > target ) {
				return i;
			}
		}
		return array.length;
	}
	
	/**
	 * Count how many elements are smaller or equal in the array than a specified element with binary search.
	 * Binary search in this implementation ends up being redundant.  
	 * @param array array to count elements 
	 * @param target the element to compare with
	 * @param minIdx the starting position to look for
	 * @param countEqual if we should also count equal elements
	 * @return the amount of elements that are smaller (or equal) then a target
	 * @throws IllegalArgumentException if the minIdx > array.lenth
	 */
	@Deprecated
	private static int binaryCountSmallerElements(ArrayWrapper array, int target, int minIdx, boolean countEqual) {
		int maxIdx = array.length-1; 
		int currentIdx = minIdx + (maxIdx-minIdx)/2;
		if (minIdx > maxIdx) {
			throw new IllegalArgumentException();
		}
		while (minIdx < maxIdx ) {
			if (array.get(currentIdx) <target || (countEqual && array.get(currentIdx) == target)) {
				if(array.get(currentIdx + 1) > target) {
					return currentIdx+1;
				}
				else {
					if (maxIdx-minIdx == 1) {
						return currentIdx + 1 + ((array.get(currentIdx + 1) < target 
								|| (countEqual && array.get(currentIdx + 1) == target)) ? 1 :0);
					}
					minIdx = currentIdx;
					currentIdx = minIdx + (maxIdx-minIdx)/2;
				}
			}
			else {
				maxIdx = currentIdx;
				currentIdx = minIdx + (maxIdx-minIdx)/2 ;
			}	
		}
		return currentIdx + ((array.get(maxIdx) < target || (countEqual && array.get(currentIdx) == target))? 1 :0);
	}
}
