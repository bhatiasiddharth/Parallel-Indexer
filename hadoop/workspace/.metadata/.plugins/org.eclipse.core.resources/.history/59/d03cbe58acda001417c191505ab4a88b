import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

	private static final DecimalFormat DF = new DecimalFormat("###.########");

    private Text wordAtDocument = new Text();

    private Text tfidf = new Text();

    public TFIDFReducer() {
    }

    /**
     *     Before: hello@file1.txt    3
     *     After: hello file1.txt=3
     */
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {

        // get the number of documents indirectly from the file-system
        int numberOfDocumentsInCorpus = context.getConfiguration().getInt("numberOfDocsInCorpus", 0);
        // total frequency of this word
        int numberOfDocumentsInCorpusWhereKeyAppears = 0;
        Map<String, Integer> tempFrequencies = new HashMap<String, Integer>();
        for (Text val : values) {
            String[] documentAndFrequencies = val.toString().split("=");
            // in case the counter of the words is > 0
            if (Integer.parseInt(documentAndFrequencies[1]) > 0) {
                numberOfDocumentsInCorpusWhereKeyAppears++;
            }
            tempFrequencies.put(documentAndFrequencies[0], Integer.parseInt(documentAndFrequencies[1]));
        }
        
        for (String document : tempFrequencies.keySet()) {
            // Term frequency = occurrences of the term in document
            double tf = tempFrequencies.get(document);

            // inverse document frequency quotient between the number of docs in corpus and number of docs the 
            // term appears Normalize the value in case the number of appearances is 0.
            double idf = Math.log10((double) numberOfDocumentsInCorpus / 
               (double) ((numberOfDocumentsInCorpusWhereKeyAppears == 0 ? 1 : 0) + 
                     numberOfDocumentsInCorpusWhereKeyAppears));

            double tfIdf = tf * idf;

            this.wordAtDocument.set(key + "@" + document);
            this.tfidf.set(DF.format(tfIdf));
            context.write(this.wordAtDocument, this.tfidf);
        }	
    }

    }