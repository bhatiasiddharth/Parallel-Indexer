import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<Object, Text, Text, Text> {
	private static final DecimalFormat DF = new DecimalFormat("###.########");
	private Map<String, TreeSet<WordEntry>> tmap = new HashMap<String, TreeSet<WordEntry>>();
	
	private boolean isPhrase(String wordPhrase) {
		if(Character.isUpperCase(wordPhrase.charAt(0)) || wordPhrase.indexOf(" ") != -1) 
			return true;
		else
			return false;
	}
	@Override
	/**
     *     Before: hello@file1.txt    0.5
     *     After: file1.txt  hello=0.5
     */
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] wordAndTfidf = value.toString().split("\t");
		String filename = wordAndTfidf[0].split("@")[1];
		String wordPhrase = wordAndTfidf[0].split("@")[0];
		
		if(isPhrase(wordPhrase)) {
			context.write(new Text(filename), new Text(wordPhrase + "=" + wordAndTfidf[1]));
			return;
		}
		// Get WMAX
        int WMAX = context.getConfiguration().getInt("WMAX", 0);
        if(tmap.get(filename) == null) {
        	tmap.put(filename, new TreeSet<WordEntry>());
        }
        
		tmap.get(filename).add(new WordEntry(wordPhrase, Double.parseDouble(wordAndTfidf[1])));

		if (tmap.get(filename).size() > WMAX) {
			tmap.get(filename).remove(tmap.get(filename).first());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		Iterator<String> mapIterator = tmap.keySet().iterator();
		while(mapIterator.hasNext()){
			String filename = mapIterator.next();
			Iterator<WordEntry> treeIterator = tmap.get(filename).iterator();
			while(treeIterator.hasNext()){
				WordEntry entry = treeIterator.next();
		        context.write(new Text(filename), new Text(entry.key + "="  + DF.format(entry.value)));
		   }
		}
	}
}
