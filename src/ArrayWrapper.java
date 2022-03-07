import java.util.Random;
import java.util.Arrays;

public class ArrayWrapper {
	final int[] originalArray; 
	final int offset; 
	final int length; 
	
	public ArrayWrapper(int[] array, int offset, int length) {
		if (offset + length > array.length) {
			throw new ArrayIndexOutOfBoundsException();
		}
		originalArray = array; 
		this.offset = offset; 
		this.length = length; 
		
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
	
	public static void main(String[] args) {
		final int SIZE = 10; 
		Random rand = new Random();
		int[] array = new int[SIZE];
		for (int i=0; i<SIZE; ++i) {
			array[i] = rand.nextInt(1000);
		}
		ArrayWrapper arrayBlock1 = new ArrayWrapper(array, 0, 5);
		ArrayWrapper arrayBlock2 = new ArrayWrapper(array, 5, 5);
		for (int i=0; i<arrayBlock1.length; ++i) {
			System.out.print(arrayBlock1.get(i) + " ");
		}
		System.out.println();
		for (int i=0; i<arrayBlock2.length; ++i) {
			System.out.print(arrayBlock2.get(i) + " ");
		}
	}
}
