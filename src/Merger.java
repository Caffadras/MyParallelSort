import java.util.Arrays;
import java.util.Random;

public class Merger {
	
	public static void main(String [] agrs) {
		final int SIZE = 10;
		Random rand = new Random();
		int[] array = new int[SIZE];
		for(int i =0; i<array.length; ++i) {
			array[i] = rand.nextInt(100) +1;
		}
		int[] array2 = array.clone();
		System.out.println("Array1: " + Arrays.toString(array));
		System.out.println("Array2: " + Arrays.toString(array2));
		Arrays.sort(array);
		
		int[] part1 = Arrays.copyOf(array2, SIZE /2);
		int[] part2 = Arrays.copyOfRange(array2, SIZE /2, SIZE );
		Arrays.sort(part1);
		Arrays.sort(part2);
		System.out.println("Subpart1: " + Arrays.toString(part1));
		System.out.println("Subpart2: " + Arrays.toString(part2));
		
		merge(array2, part1, part2);
		
		System.out.println("Sorted Array1: " + Arrays.toString(array));
		System.out.println("Sorted Array2: " + Arrays.toString(array2));
		System.out.println(Arrays.equals(array, array2));
	}
	
	
	public static void merge(int[] destinationArray, int[] leftArray, int[] rightArray) {
		int smallerElements = 0;
		for(int i =0; i<leftArray.length; ++i) {
			smallerElements = countSmallerElements(rightArray, leftArray[i], 0, false);
			destinationArray[i + smallerElements] = leftArray[i];
		}
		smallerElements = 0; 
		for(int i =0; i<rightArray.length; ++i) {
			smallerElements = countSmallerElements(leftArray, rightArray[i], 0, true);
			destinationArray[i + smallerElements] = rightArray[i];
		}
	} 
	
	 
	public static int countSmallerElements(int[] array, int target, int minIdx, boolean countEqual) {
		int maxIdx = array.length-1; 
		int currentIdx = minIdx + (maxIdx-minIdx)/2;
		if (minIdx > maxIdx) {
			throw new IllegalArgumentException();
		}
		while (minIdx < maxIdx ) {
			if (array[currentIdx]<target || (countEqual && array[currentIdx] == target)) {
				if(array[currentIdx + 1] > target) {
					return currentIdx+1;
				}
				else {
					if (maxIdx-minIdx == 1) {
						return currentIdx + 1 + ((array[maxIdx] < target 
								|| (countEqual && array[currentIdx] == target)) ? 1 :0);
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
		return currentIdx + ((array[maxIdx] < target || (countEqual && array[currentIdx] == target))? 1 :0);
	}
}
