package biglambda;

import java.util.ArrayList;
import java.util.List;

public class TwitterPairs {
	class Pair {
	    public String tag1;
	    public String tag2;
	}

    public List<Pair> pairs(List<String> tweets) {
        List<Pair> result = new ArrayList<Pair>();

        for (String tweet : tweets) {
            List<String> hashtags = new ArrayList<String>();
            for (String word : tweet.split(" ")) {
                if (word.charAt(0) == '#') {
                    hashtags.add(word);
                }
            }
            
            for (int i = 0; i < hashtags.size(); i++) {
                String h1 = hashtags.get(i);
                for(int j = i+1; j < hashtags.size(); j++) {
                    String h2 = hashtags.get(j);
                    Pair p = new Pair();
                    if (h1.compareTo(h2) < 0) {
                        p.tag1 = h1;
                        p.tag2 = h2;
                    }
                    else {
                        p.tag1 = h2;
                        p.tag2 = h1;
                    }
                    result.add(p);
                }
            }
        }

        return result;
    }
}