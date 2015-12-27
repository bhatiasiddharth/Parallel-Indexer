import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FinalReducer extends Reducer<Text, Text, Text, Text> {

	private TreeSet <WordEntry> tset = new TreeSet<WordEntry>();
	private Text docs = new Text();

	public FinalReducer() {
	}

	/**
     *     Before: hello@file1.txt    3
     *     After: hello file1.txt, file2.txt
     */
	protected void reduce(Text phrase, Iterable<Text> values, Context context) throws IOException,
	InterruptedException {
		int FMAX = context.getConfiguration().getInt("FMAX", 0);
		for(Text doc: values) {
			String[] docAndTfidf = doc.toString().split(",");
			double tfidf = Double.parseDouble(docAndTfidf[1]);
			tset.add(new WordEntry(docAndTfidf[0], tfidf));
			
			if (tset.size() > FMAX) {
				tset.remove(tset.first());
			}
		}
		String docList = "";
		Iterator<WordEntry> treeIterator = tset.iterator();
        while(treeIterator.hasNext()) {
        	WordEntry word = treeIterator.next();
        	if(docList != "") docList += ", ";
			docList += word.key;
        }
		this.docs.set(docList);
		context.write(phrase, docs);
		tset.clear();
	}
}