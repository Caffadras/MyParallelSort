import java.util.Arrays;

/**
 * Provides an array wrapper for convenience and better memory usage. 
 * A part of the original array can be used as an independent array.
 * Should be used only in MyParallelSort class.
 */
public class ArrayWrapper {
	//Array to wrap
	final int[] originalArray; 
	
	//starting offset
	final int offset;
	
	//the length if the array wrapper
	final int length; 
	
	//indicates if this array wrapper is used on an auxiliary array
	final boolean isAuxiliary;

	
	/**
     * Constructs an array wrapper containing the elements of the specified
     * array in the specified range.
	 * @param array the original array to wrap
	 * @param offset the starting offset 
	 * @param length the length of the array wrapper
	 * @param isAuxiliary indicates if this array wrapper is used on an auxiliary array
	 * @throws ArrayIndexOutOfBoundsException
	 */
	public ArrayWrapper(int[] array, int offset, int length, boolean isAuxiliary) {
		/*
		 * |   		    Original Array 				|
		 * x------------------x---------------x-----x
		 * |      Offset      | Array Wrapper |     |
		 * x------------------x---------------x-----x
		 * 					  |     Length    |
		 */
		
		if (offset < 0 || offset + length > array.length) {
			throw new ArrayIndexOutOfBoundsException();
		}
		originalArray = array; 
		this.offset = offset; 
		this.length = length; 
		this.isAuxiliary = isAuxiliary;
	}
	
	
    /**
     * Returns the element at the specified position in this array.
     * @param  index index of the element to return
     * @return the element at the specified position in this array
     * @throws ArrayIndexOutOfBoundsException 
     */
	public int get (int index) {
		if (index < 0 || index >= length) {
			throw new ArrayIndexOutOfBoundsException();
		}
		return originalArray[offset + index];
	}
	
	
	/**
	 * Replaces the element at the specified position in this array with
     * the specified element.
     * @param index index of the element to replace
     * @param newValue value to be stored at the specified position
     * @throws ArrayIndexOutOfBoundsException 
	 */
	public void set(int index, int newValue) {
		if (index < 0 || index >= length) {
			throw new ArrayIndexOutOfBoundsException();
		}
		originalArray[offset + index] = newValue;
	}
	
	
	/**
	 * Copies the contents of the specified array. 
	 * Specified array can be both shorter and longer than this array. 
	 * In that case, extra elements are ignored or left untouched. 
	 * @param from an array to copy the contents from.
	 */
	public void copyFrom(ArrayWrapper from) {
		for(int i=0; i<from.length && i < this.length; ++i) {
			this.set(i, from.get(i));
			//Arrays.toString(originalArray);
		}
	}
	

	/**
	 * Returns a string representation of the contents of the specified array,
     * enclosed in square brackets.
	 * Mostly used for the debug purposes.
	 */
	public String toString() {
		String result = "["; 
		for(int i =0; i<length; ++i) {
			result = result + get(i) + (i == length -1 ? "" : ", ");
		}
		result = result + "]";
		return result;
	}
}
