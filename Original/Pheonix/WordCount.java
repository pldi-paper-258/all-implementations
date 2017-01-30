package mold;

import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.lang.Integer;
import java.lang.String;

public class WordCount {

	public static void main(String[] args) {
		List<String> words = Arrays.asList("foo", "bar", "cat", "bar", "dog");
		countWords(words);
	}

	private static Map<String, Integer> countWords(List<String> words) {
		Map<String, Integer> counts = new HashMap<String, Integer>();
		for (int j = 0; j < words.size(); j++) {
			String word = words.get(j);
			Integer prev = counts.get(word);
			if (prev == null)
				prev = 0;
			counts.put(word, prev + 1);
		}
		return counts;
	}
}
