import java.lang.Math;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public final class MyParallelSort {
	private static int maxThreads = Runtime.getRuntime().availableProcessors();
	private static int elementsPerThread; 
	private static int totalTaskCount;
	private static int[] mainArray;
	private static boolean isSorting = false;
	private static final ExecutorService executor = Executors.newFixedThreadPool(maxThreads); 
	private static final CompletionService<Pair> completionService= new ExecutorCompletionService<Pair>(executor);
	
	public static void sort(int [] array) {
		if (isSorting == true) {
			throw new IllegalStateException();
		}
		isSorting = true;
		mainArray = array;
		conductSort();
	}
	
	public static void sort(int[] array, int threadsToUse) {
		if (threadsToUse <=0) throw new IllegalArgumentException();
		maxThreads = Math.min(Runtime.getRuntime().availableProcessors(), threadsToUse);
		sort(array);
	}
	
	private static void splitAndQueue() {
		elementsPerThread = mainArray.length / maxThreads;
		totalTaskCount = maxThreads*2 -1;
		int remainder = mainArray.length - (mainArray.length / maxThreads * maxThreads) ;
		if (mainArray.length < maxThreads) maxThreads = mainArray.length;
		
		
		for (int i=0; i<maxThreads; ++i) {
			int from = i*elementsPerThread;
			int to = i*elementsPerThread+elementsPerThread + ((i == maxThreads -1) ? remainder : 0);
			int[] subArray = Arrays.copyOfRange(mainArray, from, to);
			
			completionService.submit(new Callable<Pair>(){
				@Override
				public Pair call(){
					Arrays.sort(subArray);
					return new Pair(from, subArray);
				}
			});	
		}
	}
	
	private static void conductSort() {
		splitAndQueue();
		
		ArrayList<Pair> sortedBlocks = new ArrayList<Pair>(maxThreads);
		try {
			
			for(int i=0; i<totalTaskCount ; ++i) {
				Future<Pair> futureResult = completionService.take();
				Pair result = futureResult.get();
				if (result.arr.length == mainArray.length) {
					for (int j=0; j<mainArray.length; ++j) {
						mainArray[j] = result.arr[j];
					}
					break; 
				}
				sortedBlocks.add(result);
				for(int j=0; j<sortedBlocks.size()- 1; ++j) {
					for(int k = j+1; k < sortedBlocks.size(); ++k) {
						
						Pair leftSubArray, rightSubArray;
						if (sortedBlocks.get(j).startPos < sortedBlocks.get(k).startPos) {
							leftSubArray = sortedBlocks.get(j);
							rightSubArray = sortedBlocks.get(k);
						}
						else {
							leftSubArray = sortedBlocks.get(k);
							rightSubArray = sortedBlocks.get(j);
						}
						
						
						if (leftSubArray.startPos + leftSubArray.arr.length == rightSubArray.startPos) {
							int[] resultArray = new int[(rightSubArray.startPos + rightSubArray.arr.length) - leftSubArray.startPos];
							
							Callable<Pair> task = new Callable<Pair>() {
								@Override 
								public Pair call(){
									merge(resultArray, leftSubArray.arr, rightSubArray.arr, leftSubArray.arr.length, rightSubArray.arr.length);
									return new Pair(leftSubArray.startPos, resultArray);
								}
							};
							
							completionService.submit(task);
							sortedBlocks.remove(k);
							sortedBlocks.remove(j);
							--i; 
							break;
						}
					}
				}
				
			}
		
		}
		catch(InterruptedException e) {
			e.printStackTrace();
		}
		catch(ExecutionException e) {
			e.printStackTrace();
		}
		
		isSorting = false;
	}
	
	public static void shutdown() {
		executor.shutdown();
	}

	
	public static void merge(
			  int[] a, int[] l, int[] r, int left, int right) {
			 
			    int i = 0, j = 0, k = 0;
			    while (i < left && j < right) {
			        if (l[i] <= r[j]) {
			            a[k++] = l[i++];
			        }
			        else {
			            a[k++] = r[j++];
			        }
			    }
			    while (i < left) {
			        a[k++] = l[i++];
			    }
			    while (j < right) {
			        a[k++] = r[j++];
			    }
	}	
}

class Pair{
	int startPos;
	int[] arr;
	
	Pair(int x, int[] arr){
		this.startPos = x; 
		this.arr = arr;
	}
	
}
