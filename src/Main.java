import java.util.Random;
import java.util.Arrays;

public class Main {
	private static Random rand = new Random(); 
	public static void main(String[] args) {
		int availableThreads = Runtime.getRuntime().availableProcessors();
		System.out.printf("\nMax number of threads == %d\n\n", availableThreads);
	
		int[] array;		
//		array = generateRandomArray(100536000);		
//		long startTime1 = System.currentTimeMillis();
//		MyParallelSort.sort(array);
//		long endTime1 = System.currentTimeMillis();
//		System.out.printf("%10d elements  =>  %6d ms \n", 65536000, endTime1 - startTime1);
		int iterations = 16;
		int totalIterations = 0;
		long totalSeconds = 0;
		for (int i = 1; i<=availableThreads; i*=2) {
			System.out.println("\nThreads count: " + i);
			int size = 1000; 
			for(int j =0; j<iterations; ++j) {
				array = generateRandomArray(size);	
				int[] testArray = Arrays.copyOf(array, array.length);
				Arrays.sort(testArray);
				
				//System.out.println("Array: " + Arrays.toString(array));
				long startTime = System.currentTimeMillis();
				MyParallelSort.sort(array, i);
				long endTime = System.currentTimeMillis();
				System.out.printf("%10d elements  =>  %6d ms \n", size, endTime - startTime);
				totalSeconds += endTime - startTime;
				++totalIterations;
				loggingIsSorted(array);
				System.out.println(Arrays.equals(array, testArray));
				size *= 2;
			}
			
		}
		
		MyParallelSort.shutdown();
		System.out.println("Absolute average time: " + ((double)totalSeconds) / totalIterations + " ms");
		System.out.println("Terminated.");
	}
	
	public static int[] generateRandomArray(int size) {
		if (size <= 0 ) throw new IllegalArgumentException();
		int[] array = new int[size];
		for(int i =0; i<size; ++i) {
			array[i] = rand.nextInt(1000)+1;
		}
		
		return array;
	}
	 
	public static void loggingIsSorted(int[] array) {		
		if (isSorted(array) == false) {
			System.out.println("This array was not fully sorted!!!");			
		}
	}
	
	public static boolean isSorted(int[] array) {
		for(int i=0;i<array.length - 1;++i) {
			if (array[i] > array[i+1]) return false;	
		}
		return true;
	}

}
