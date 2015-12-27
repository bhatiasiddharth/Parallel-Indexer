import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalMapper extends Mapper<LongWritable, Text, Text, Text> {
	

     private Text phrase = new Text();
     private Text docAndTFIDF = new Text();

     /**
      *     Before: hello@file1.txt    3
      *     After: hello file1.txt,3
      */
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String[] phraseDocAndTfidf = value.toString().split("\t");
         String[] phraseAndDoc = phraseDocAndTfidf[0].split("@");
         this.phrase.set(phraseAndDoc[0]);
         this.docAndTFIDF.set(phraseAndDoc[1] + ", " + phraseDocAndTfidf[1]);
         context.write(this.phrase, this.docAndTFIDF);
     }


}
