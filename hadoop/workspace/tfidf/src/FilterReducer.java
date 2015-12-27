import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FilterReducer extends Reducer<Text, Text, Text, Text> {

        public FilterReducer() {
        }
        
        private boolean isPhrase(String wordPhrase) {
    		if(Character.isUpperCase(wordPhrase.charAt(0)) || wordPhrase.indexOf(" ") != -1) 
    			return true;
    		else
    			return false;
    	}
        private Map<String, TreeSet<WordEntry>> tmap = new HashMap<String, TreeSet<WordEntry>>();
        private static final DecimalFormat DF = new DecimalFormat("###.########");

        /**
         *     Before: file1.txt  hello=0.5
         *     After: hello@file1.txt 0.5
         */
        protected void reduce(Text filename, Iterable<Text> values, Context context) throws IOException, 
                 InterruptedException {
        	String doc = filename.toString();
        	// Get WMAX
            int WMAX = context.getConfiguration().getInt("WMAX", 0);
            for (Text val : values) {
                String[] wordAndTfidf = val.toString().split("=");
                Double tfidf = Double.parseDouble(wordAndTfidf[1]);
                
                if(tmap.get(doc) == null) {
                	tmap.put(doc, new TreeSet<WordEntry>());
                }
                
                if(isPhrase(wordAndTfidf[0])) {
                	context.write(new Text(wordAndTfidf[0] + "@" + filename), new Text(wordAndTfidf[1]));
                	continue;
                }
                
                tmap.get(doc).add(new WordEntry(wordAndTfidf[0], tfidf));
                
                if (tmap.get(doc).size() > WMAX) {
                	tmap.get(doc).remove(tmap.get(doc).first());
        		}
            }
            
            Iterator<WordEntry> treeIterator = tmap.get(doc).iterator();
            while(treeIterator.hasNext()) {
            	WordEntry word = treeIterator.next();
            	context.write(new Text(word.key + "@" + doc), new Text(DF.format(word.value)));
            }
        }
    }