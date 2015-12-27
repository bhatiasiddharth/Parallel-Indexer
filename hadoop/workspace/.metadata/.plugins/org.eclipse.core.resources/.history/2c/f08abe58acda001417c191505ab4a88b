import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable wordSum = new IntWritable();
        
        public FrequencyReducer() {
        }

        /**
         *     Before: hello@file1.txt 1
         *     		   hello@file1.txt 1
         *     After: hello@file1.txt 2
         */
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
                 InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            //write the key and the adjusted value (removing the last comma)
            this.wordSum.set(sum);
            context.write(key, this.wordSum);
        }
    }