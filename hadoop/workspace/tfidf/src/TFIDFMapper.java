import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {


     private Text wordAndDoc = new Text();
     private Text wordAndCounters = new Text();

     /**
      *     Before: hello@file1.txt    3
      *     After: hello file1.txt=3
      */
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String[] wordDocAndCounters = value.toString().split("\t");
         String[] wordDoc = wordDocAndCounters[0].split("@");  //3/1500
         this.wordAndDoc.set(new Text(wordDoc[0]));
         this.wordAndCounters.set(wordDoc[1] + "=" + wordDocAndCounters[1]);
         context.write(this.wordAndDoc, this.wordAndCounters);
     }


}
