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
	private static int[] auxiliaryArray;
	private static boolean isSorting = false;
	private static final ExecutorService executor = Executors.newFixedThreadPool(maxThreads); 
	private static final CompletionService<ArrayWrapper> completionService= new ExecutorCompletionService<ArrayWrapper>(executor);
	
	public static void sort(int [] array) {
		if (isSorting == true) {
			throw new IllegalStateException();
		}
		isSorting = true;
		mainArray = array;
		auxiliaryArray = new int [array.length];
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
			ArrayWrapper subArray = new ArrayWrapper(mainArray, from, to-from, false);
			
			completionService.submit(new Callable<ArrayWrapper>(){
				@Override
				public ArrayWrapper call(){
					Arrays.sort(subArray.originalArray, subArray.offset, subArray.offset+subArray.length);
					return subArray;
				} 
			});	
		}
	}
	
	private static Callable<ArrayWrapper> createMergingTask(ArrayWrapper destinationArray, 
			ArrayWrapper leftSubArry, ArrayWrapper rightSubAarray)
	{
		return new Callable<ArrayWrapper>() {
			@Override 
			public ArrayWrapper call(){
				Merger.merge(destinationArray, leftSubArry, rightSubAarray);
;				return destinationArray;
			}
		};
	}
	
	private static boolean finalBlock(ArrayWrapper sortedBlock) {
		if (sortedBlock.length == mainArray.length) {
			if (sortedBlock.isAuxiliary) {
				for (int j=0; j<mainArray.length; ++j) {
					mainArray[j] = sortedBlock.get(j);
				}
			}
			return true;
		}
		return false;
	}
	
	private static ArrayWrapper moveBlock(ArrayWrapper subArray, boolean moveToAuxiliary) {
		ArrayWrapper newSubArray = new ArrayWrapper(moveToAuxiliary? auxiliaryArray : mainArray, subArray.offset, subArray.length, moveToAuxiliary); 
		newSubArray.copyFrom(subArray);
		return newSubArray;
	}
	
	private static void addSortedBlock(ArrayList<ArrayWrapper> sortedBlocks, ArrayWrapper block) {
		for(int i= 0; i<sortedBlocks.size(); ++i) {
			if (sortedBlocks.get(i).offset > block.offset) {
				sortedBlocks.add(i, block);
				return;
			}
		}
		sortedBlocks.add(block);
	}
	
	private static void tryMerging(ArrayList<ArrayWrapper> sortedBlocks) {
		for(int i=0; i<sortedBlocks.size()- 1; ++i) {
			ArrayWrapper leftSubArray, rightSubArray;
			if (sortedBlocks.get(i).offset < sortedBlocks.get(i + 1).offset) {
				leftSubArray = sortedBlocks.get(i);
				rightSubArray = sortedBlocks.get(i + 1);
			}
			else {
				leftSubArray = sortedBlocks.get(i + 1);
				rightSubArray = sortedBlocks.get(i);
			}
			
			if (leftSubArray.offset  + leftSubArray.length == rightSubArray.offset ) {
				
				if (leftSubArray.isAuxiliary != rightSubArray.isAuxiliary) {
					if (leftSubArray.length < rightSubArray.length) {
						leftSubArray = moveBlock(leftSubArray, !leftSubArray.isAuxiliary);
					}
					else {
						rightSubArray = moveBlock(rightSubArray, !rightSubArray.isAuxiliary);
					}
				}
				
				boolean shouldBeAuxiliary = !leftSubArray.isAuxiliary;
				int offset= leftSubArray.offset;
				int length = leftSubArray.length + rightSubArray.length;
				ArrayWrapper combinedArray = new ArrayWrapper(shouldBeAuxiliary ? auxiliaryArray : mainArray, offset, length, shouldBeAuxiliary);

				Callable<ArrayWrapper> task = createMergingTask(combinedArray, leftSubArray, rightSubArray);
				
				completionService.submit(task);
				sortedBlocks.remove(i + 1);
				sortedBlocks.remove(i);
				break;
			}
		}
		
	}
	
	private static void conductSort() {
		splitAndQueue();
		
		ArrayList<ArrayWrapper> sortedBlocks = new ArrayList<ArrayWrapper>(maxThreads);
		try {
			
			for(int i=0; i<totalTaskCount ; ++i) {
				Future<ArrayWrapper> futureBlock = completionService.take();
				ArrayWrapper block = futureBlock.get();
				if (finalBlock(block)) {
					break; 
				}
				
				addSortedBlock(sortedBlocks, block);
				tryMerging(sortedBlocks);
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

}
