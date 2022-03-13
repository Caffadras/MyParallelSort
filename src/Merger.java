import java.util.Arrays;
import java.util.Random;

public class Merger {
	
	public static void main(String [] agrs) {
		int[] testArray = new int[] {3, 3, 4};
		ArrayWrapper testWrapper = new ArrayWrapper(testArray, 0, 3, false);
		System.out.println(countSmallerElements(testWrapper, 4, 0, false));
		System.out.println(countSmallerElements(testWrapper, 4, 0, true));
		final int SIZE = 1111;
		Random rand = new Random();
		int[] array = new int[SIZE];
		for(int i =0; i<array.length; ++i) {
			array[i] = rand.nextInt(1000) +1;
		}
		int[] array2 = array.clone();
		int[] array3 = array.clone();

		System.out.println("Array1: " + Arrays.toString(array));
		System.out.println("Array2: " + Arrays.toString(array2));
		Arrays.sort(array);
		
		ArrayWrapper array2Wrapper = new ArrayWrapper(array2, 0, array2.length, false);
		ArrayWrapper part1 = new ArrayWrapper(array3, 0, SIZE/2, false);
		ArrayWrapper part2 = new ArrayWrapper(array3, SIZE/2, SIZE-(SIZE/2), false);
		Arrays.sort(array3, 0, SIZE/2);
		Arrays.sort(array3, SIZE/2, SIZE);

		
		System.out.println("Sorted Array2: " + Arrays.toString(array2));
		System.out.println("Sorted Array3: " + Arrays.toString(array3));
		
		merge(array2Wrapper, part1, part2);
		
		System.out.println("Sorted Array1: " + Arrays.toString(array));
		System.out.println("Sorted Array3: " + Arrays.toString(array2));
		System.out.println(Arrays.equals(array, array2));
	}
	
	public static int[] checkSorting(ArrayWrapper leftArray, ArrayWrapper rightArray) {
		int[] resultArray = new int[leftArray.length + rightArray.length];
		for(int i=0; i< leftArray.length; ++i) {
			resultArray[i] = leftArray.get(i);
		}
		for(int i =0; i<rightArray.length; ++i) {
			resultArray[leftArray.length + i] = rightArray.get(i);
		}
		
		Arrays.sort(resultArray);
		return resultArray;
	}
	
	public static void merge(ArrayWrapper destinationArray, ArrayWrapper leftArray, ArrayWrapper rightArray) {
		if (isSorted(leftArray) == false) {
			System.out.println("###Left input array was not sorted! " + leftArray.toString());

		}
		if (isSorted(rightArray) == false) {
			System.out.println("###Right input array was not sorted! " + rightArray.toString());
		}

		int smallerElements = 0;
		for(int i =0; i<leftArray.length; ++i) {
			smallerElements = countSmallerElementsSimple(rightArray, leftArray.get(i), smallerElements == 0? smallerElements : smallerElements -1 );
			destinationArray.set(i + smallerElements, leftArray.get(i));
		}
		smallerElements = 0; 
		for(int i =0; i<rightArray.length; ++i) {
			smallerElements = countSmallerEqualElementsSimple(leftArray, rightArray.get(i), smallerElements == 0? smallerElements : smallerElements -1);
			destinationArray.set(i + smallerElements, rightArray.get(i));
		}
	} 
	
	public static boolean isSorted(ArrayWrapper array) {
		for(int i =0; i<array.length-1; ++i) {
			if (array.get(i) > array.get(i+1)) return false;
		}
		return true;
	}
	 
	public static int countSmallerElementsSimple(ArrayWrapper array, int target, int minIdx) {
		for(int i = minIdx; i<array.length; ++i ) {
			if (array.get(i) >= target ) {
				return i;
			}
		}
		return array.length;
	}
	
	public static int countSmallerEqualElementsSimple(ArrayWrapper array, int target, int minIdx) {
		for(int i = minIdx; i<array.length; ++i ) {
			if (array.get(i) > target ) {
				return i;
			}
		}
		return array.length;
	}
	public static int countSmallerElements(ArrayWrapper array, int target, int minIdx, boolean countEqual) {
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
