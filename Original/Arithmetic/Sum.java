package grigory;

import java.util.List;
import java.util.Arrays;
import java.lang.Integer;

public class Sum {	
	public static void main(String[] args) {
		List<Integer> numbers = Arrays.asList(10, 5, 7, 12, 3);
		sumList(numbers);
	}
	
	public static int sumList(List<Integer> data) {
		int sum = 0;
		for(int i=0; i<data.size(); i++) {
			sum += data.get(i);
		}
		return sum;
	}
}