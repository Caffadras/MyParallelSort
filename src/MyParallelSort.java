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
import java.util.concurrent.CountDownLatch;

public final class MyParallelSort {
	private static int maxThreads = Runtime.getRuntime().availableProcessors();
	private static int elementsPerThread; 
	
	//two arrays that will contain all elements from the original array
	private static int[] mainArray;
	private static int[] auxiliaryArray;
	
	//prevents the user to run sort again while the previous one has not been finished. 
	private static boolean isSorting = false;
	
	private static final ExecutorService executor = Executors.newFixedThreadPool(maxThreads); 
	private static final CompletionService<ArrayWrapper> completionService= new ExecutorCompletionService<ArrayWrapper>(executor);
	
	/**
	 * Sorts the specified array into ascending numerical order in parallel.
	 * @param array array to sort
	 * @throws IllegalStateException if the previous sort has not been finished. 
	 */
	public static void sort(int [] array) {
		if (isSorting == true) {
			throw new IllegalStateException();
		}
		isSorting = true;
		mainArray = array;
		auxiliaryArray = new int [array.length];
		conductSort();
	}

	/**
	 * Sorts the specified array into ascending numerical order in parallel.
	 * @param array array to sort
	 * @param threadsToUse number of threads to use.
	 * @throws IllegalArgumentException if the specified number of threads <=0.
	 */
	public static void sort(int[] array, int threadsToUse) {
		if (threadsToUse <=0) throw new IllegalArgumentException();
		//if the specified number of threads is bigger than the number of available threads, only available threads will be used. 
		maxThreads = Math.min(Runtime.getRuntime().availableProcessors(), threadsToUse);
		sort(array);
	}
	
	/**
	 * This method performs stage 1 of parallel sort. 
	 * It splits the original array into blocks. The number of blocks is equal to the number of threads.
	 * Each block is later sorted with Arrays.sort() method separately in each thread. 
	 */
	private static void splitAndQueue() {
		/*
		 * 1.
		 * x-----------------------------------x
		 * |          Original Array           |
		 * x-----------------------------------x
		 *					|
		 * 2.				|					
		 * x-----x-----x-----x-----x-----x-----x
		 * |     |     |     |     |     |     |
		 * x-----x-----x-----x-----x-----x-----x
		 */
		
		elementsPerThread = mainArray.length / maxThreads;
		
		//we cannot split the array evenly. So we calculate the remainder.
		int remainder = mainArray.length % maxThreads;
		
		//It is not worth to sort in parallel when an array is smaller very small. 
		//But we still can allow it. 
		//In case, when the original array has length smaller than number of available threads, we just use a thread per element.
		if (mainArray.length < maxThreads) maxThreads = mainArray.length;
		
		for (int i=0; i<maxThreads; ++i) {
			int from = i*elementsPerThread;
			
			//we add the remainder if it is the last block. 
			int to = from + elementsPerThread + ((i == maxThreads -1) ? remainder : 0);
			
			//the sub array that will be sorted 
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
	 
	/**
	 * Splits an array into multiple merging tasks, which allows to merge two arrays in parallel.
	 * @param destinationArray array to put merged elements in
	 * @param arrayToMerge elements from this array are merged 
	 * @param numberOfTasks number of task to split into 
	 * @param secondArray to calculate position of an element form the arrayToMerge, we need to reference the second array, the array that we are merging with
	 * @param countEqual if we should count equal elements as smaller elements
	 * @param endGate a CountDownLatch all tasks should refer to
	 * @param lastSort should the last task wait the endGate and return sorted array
	 * @return
	 */
	private static ArrayList<Callable<ArrayWrapper>> splitIntoTasks(ArrayWrapper destinationArray, ArrayWrapper arrayToSplit, 
			int numberOfTasks, ArrayWrapper secondArray, boolean countEqual, CountDownLatch endGate, boolean lastSort)
	{
		//In other words, elements per thread
		int splitBlockSize = arrayToSplit.length / numberOfTasks;
		
		//all tasks will be added in this array, which will be returned by the method
		ArrayList<Callable<ArrayWrapper>> tasks = new ArrayList<>(numberOfTasks);
		//we do no create last task in this loop
		for(int i=0; i<numberOfTasks - 1; ++i) {
			int from = splitBlockSize * i; 
			int to = from + splitBlockSize ;
			
			tasks.add(new Callable<ArrayWrapper>() {
				@Override
				public ArrayWrapper call() {
					Merger.partialMerge(destinationArray, arrayToSplit, from, to, secondArray, countEqual);
					endGate.countDown();
					//do no return the array
					return null;
				}
			});
		}
		//creating last task 
		int from = splitBlockSize * (numberOfTasks-1);
		int to = arrayToSplit.length; 
		
		tasks.add(new Callable<ArrayWrapper>() {
			@Override 
			public ArrayWrapper call() {
				Merger.partialMerge(destinationArray, arrayToSplit, from, to, secondArray, countEqual);
				endGate.countDown();
				if (lastSort) {
					try {
						//we await the endGate and return the sorted array only if this is the last task. 
						//it minimizes the time a thread will spend waiting. 
						endGate.await();
						return destinationArray;
					} catch(InterruptedException e) {
						e.printStackTrace();
						return null;
					}
				}
				else {	
					return null;
				}
			}
		});
		
		return tasks;
	}
	
	/**
	 * Submits all tasks from an array to a completion service. 
	 * @param service completion service to submit to.
	 * @param taskList an array with all tasks.
	 */
	private static void addToCompletionService(CompletionService<ArrayWrapper> service, ArrayList<Callable<ArrayWrapper>> taskList) {
		for(int i=0; i<taskList.size(); ++i) {
			service.submit(taskList.get(i));
		}
	}
	
	/**
	 * Creates multiple tasks of merging two sub arrays.
	 * @param destinationArray array to put merged elements in
	 * @param leftSubArray first array to merge
	 * @param rightSubArray second array to merge 
	 * @param service completion service to submit to
	 */
	private static void createMergingTask(ArrayWrapper destinationArray, 
			ArrayWrapper leftSubArray, ArrayWrapper rightSubArray, CompletionService<ArrayWrapper> service)
	{
		//number of threads that is optimal to use to merge this two sub arrays
		int threadsToUse = (int) Math.round(((double)destinationArray.length) /elementsPerThread);
		
		//The result of the merge must be accessible only when all the threads have finished their tasks. 
		//For this purpose we will use CountDownLatch
		CountDownLatch endGate = new CountDownLatch(threadsToUse);
		
		//we use more threads to merge the bigger sub array
		if (leftSubArray.length < rightSubArray.length) {
			addToCompletionService(service, 
				splitIntoTasks(destinationArray, leftSubArray, threadsToUse/2, rightSubArray, false, endGate, false));
			addToCompletionService(service, 
				splitIntoTasks(destinationArray, rightSubArray, threadsToUse-(threadsToUse/2), leftSubArray, true, endGate, true));
		}
		else {
			addToCompletionService(service, 
				splitIntoTasks(destinationArray, leftSubArray, threadsToUse-(threadsToUse/2), rightSubArray, false, endGate, false));
			addToCompletionService(service, 
				splitIntoTasks(destinationArray, rightSubArray,threadsToUse/2, leftSubArray, true, endGate, true));
		}
	}
	
	
	/**
	 * Checks if the given block has the same as the original array, i.e. is final block. 
	 * In that case the original array is sorted. 
	 * Because the the class uses two storages, final block can end up in the auxiliary array. 
	 * In that case we copy it to the main array. 
	 * @param sortedBlock block to check 
	 * @return true if the block was final, false otherwise.
	 */
	private static boolean finalBlock(ArrayWrapper sortedBlock) {
		if (sortedBlock.length == mainArray.length) {
			if (sortedBlock.isAuxiliary) {
				//if the block is not located in the main array
				for (int j=0; j<mainArray.length; ++j) {
					mainArray[j] = sortedBlock.get(j);
				}
			}
			return true;
		}
		return false;
	}
	
	
	/**
	 * Copies the subArray to the different array (mainArray, auxiliaryArray)
	 * @param subArray array to copy.
	 * @param moveToAuxiliary should the subArray be copied to the auxiliary storage or the main storage.
	 * @return new copied array located in the other storage.
	 */
	private static ArrayWrapper moveBlock(ArrayWrapper subArray, boolean moveToAuxiliary) {
		ArrayWrapper newSubArray = new ArrayWrapper(moveToAuxiliary? auxiliaryArray : mainArray, subArray.offset, subArray.length, moveToAuxiliary); 
		newSubArray.copyFrom(subArray);
		return newSubArray;
	}
	
	
	/**
	 * Adds a block into a sorted array with all sorted blocks.
	 * Keeps the array sorted. 
	 * @param sortedBlocks array with sorted blocks
	 * @param block A block to add into the array
	 */
	private static void addSortedBlock(ArrayList<ArrayWrapper> sortedBlocks, ArrayWrapper block) {
		for(int i= 0; i<sortedBlocks.size(); ++i) {
			if (sortedBlocks.get(i).offset > block.offset) {
				sortedBlocks.add(i, block);
				return;
			}
		}
		//If the block is located to the right relative to all the other sorted blocks, it is added in the end.
		sortedBlocks.add(block);
	}
	
	/**
	 * This method is effectively the stage 2.
	 * Every time a block from the original array has been sorted, this method is called. 
	 * Iterating through all of the sorted blocks, it looks for 2 adjacent blocks, that can be merged together. 
	 * These 2 blocks must be located in the same array (mainArray / auxiliaryArray). If they are not, the smallest 
	 * of them is copied to the other array.
	 * After constructing the combined array, where these 2 blocks will be merged, createMergingTask is called to 
	 * create needed amount of tasks.
	 * @param sortedBlocks The array with all sorted blocks.
	 */
	private static void tryMerging(ArrayList<ArrayWrapper> sortedBlocks) {
		/*
		 * Note that the order may vary depending on which blocks were sorted first.
		 * Note that only adjacent blocks are merged. 
		 * 4.
		 * x-----------------------------------x
		 * |         Final Sorted Array        |
		 * x-----------------------------------x
		 * 					^
		 * 3.				|
		 * x-----------------------x-----------x
		 * |                       |           |
		 * x-----------------------x-----------x
		 * 					^
		 * 2.				|
		 * x-----------x-----------x-----------x
		 * |           |           |           |
		 * x-----------x-----------x-----------x
		 * 					^
		 * 1.				|
		 * x-----x-----x-----x-----x-----x-----x
		 * |     |     |     |     |     |     |
		 * x-----x-----x-----x-----x-----x-----x
		 * (These blocks are all sorted after stage 1)
		 */
		
		for(int i=0; i<sortedBlocks.size()- 1; ++i) {
			/*
			 * leftSubArray is always located to the left of the rightSubArray 
			 * x----x--------------x---------------x-----x
			 * |    | leftSubArray | rightSubArray |     |
			 * x----x--------------x---------------x-----x
			 */
			ArrayWrapper leftSubArray, rightSubArray;
			
			//Because the array is sorted, the block at position i is always located in the original array 
			//Before the block at position i + 1
			leftSubArray = sortedBlocks.get(i);
			rightSubArray = sortedBlocks.get(i + 1);

			//Checking if the 2 blocks are adjacent
			if (leftSubArray.offset + leftSubArray.length == rightSubArray.offset ) {
				
				//Checking if the 2 blocks are not located it the same array (mainArray / auxiliaryArray)
				if (leftSubArray.isAuxiliary != rightSubArray.isAuxiliary) {
					//In that case finding the smaller array and moving it to the other array.
					if (leftSubArray.length < rightSubArray.length) {
						leftSubArray = moveBlock(leftSubArray, !leftSubArray.isAuxiliary);
					}
					else {
						rightSubArray = moveBlock(rightSubArray, !rightSubArray.isAuxiliary);
					}
				}
				
				//Calculating the dimensions for the combined array. 
				//The combined array should not be in the same array as 2 block that are being merged. 
				boolean shouldBeAuxiliary = !leftSubArray.isAuxiliary;
				int offset= leftSubArray.offset;
				int length = leftSubArray.length + rightSubArray.length;
				ArrayWrapper combinedArray = new ArrayWrapper(shouldBeAuxiliary ? auxiliaryArray : mainArray, offset, length, shouldBeAuxiliary);

				createMergingTask(combinedArray, leftSubArray, rightSubArray, completionService);
				//Two blocks can be removed from the array with sorted blocks, because a new merge task with them was created. 
				sortedBlocks.remove(i + 1);
				sortedBlocks.remove(i);
				
				//This method is called every time a block is added, so if a pair is found, no other adjacent pair exists yet.
				break;
			}
		}
		
	}
	
	/**
	 * Responsible for conducting sort. 
	 * After the first stage it fetches the sorted blocks and passes it to the other methods. 
	 * If the fetched block is final, stops the sort. 
	 */
	private static void conductSort() {
		//first stage
		splitAndQueue();
		
		//Array with all sorted blocks
		ArrayList<ArrayWrapper> sortedBlocks = new ArrayList<ArrayWrapper>(maxThreads);
		try {
			while (true) {
				//Fetching the sorted blocks
				Future<ArrayWrapper> futureBlock = completionService.take();
				ArrayWrapper block = futureBlock.get();
				
				//The block can intentionally be null. 
				if (block == null) continue;
				
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
	
	/**
	 * Returns if the sort is in progress or not.
	 * @return true if the previous sort has not been finished yet.
	 */
	public static boolean isSorting() {
		return isSorting;
	}

}
