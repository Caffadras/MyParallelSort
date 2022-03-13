import java.util.Random;
import java.util.Arrays;

public class ArrayWrapper {
	final int[] originalArray; 
	final int offset; 
	final int length; 
	final boolean isAuxiliary;
	
	public ArrayWrapper(int[] array, int offset, int length, boolean isAuxiliary) {
		if (offset + length > array.length) {
			//System.out.println("Lower bound: " + (offset + length) + "  upper bound: " + array.length);
			throw new ArrayIndexOutOfBoundsException();
		}
		originalArray = array; 
		this.offset = offset; 
		this.length = length; 
		this.isAuxiliary = isAuxiliary;
	}
	
	public int get (int index) {
		if (index < 0 || index >= length) {
			throw new ArrayIndexOutOfBoundsException();
		}
		return originalArray[offset + index];
	}
	
	public void set(int index, int newValue) {
		if (index < 0 || index >= length) {
			throw new ArrayIndexOutOfBoundsException();
		}
		originalArray[offset + index] = newValue;
	}
	
	public void copyFrom(ArrayWrapper from) {
		for(int i=0; i<from.length && i < this.length; ++i) {
			this.set(i, from.get(i));
		}
	}
	public static void main(String[] args) {
		final int SIZE = 10; 
		Random rand = new Random();
		int[] array = new int[SIZE];
		for (int i=0; i<SIZE; ++i) {
			array[i] = rand.nextInt(1000);
		}
		System.out.println("Original array: " + Arrays.toString(array));
		ArrayWrapper arrayBlock1 = new ArrayWrapper(array, 0, 5, false);
		ArrayWrapper arrayBlock2 = new ArrayWrapper(array, 5, 5, false);
		for (int i=0; i<arrayBlock1.length; ++i) {
			System.out.print(arrayBlock1.get(i) + " ");
		}
		System.out.println();
		for (int i=0; i<arrayBlock2.length; ++i) {
			System.out.print(arrayBlock2.get(i) + " ");
		}
		for (int i=0; i<arrayBlock1.length; ++i) {
			arrayBlock1.set(i, i);
		}
		System.out.println();
		for (int i=0; i<arrayBlock2.length; ++i) {
			arrayBlock2.set(i, i+(SIZE/2));
		}
		
		System.out.println("Original array: " + Arrays.toString(array));
		for (int i=0; i<arrayBlock1.length; ++i) {
			System.out.print(arrayBlock1.get(i) + " ");
		}
		System.out.println();
		for (int i=0; i<arrayBlock2.length; ++i) {
			System.out.print(arrayBlock2.get(i) + " ");
		}
	}
	
	public String toString() {
		String result = "["; 
		for(int i =0; i<length; ++i) {
			result = result + get(i) + (i == length -1 ? "" : ", ");
		}
		result = result + "]";
		return result;
	}
}
