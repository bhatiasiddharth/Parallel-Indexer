import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Pattern PHRASE_PATTERN = Pattern.compile("([A-Z]\\w+)(\\s[A-Z]\\w+)+");
    private static final Pattern WORD_PATTERN = Pattern.compile("\\b\\w+");
    private Text word = new Text();
    private IntWritable singleCount = new IntWritable(1);

    public FrequencyMapper() {
    }
    
    private void addMatchingGroup(Matcher m, String filename, Context context) throws IOException, InterruptedException {
        StringBuilder valueBuilder = new StringBuilder();
        while (m.find()) {
            String matchedKey = m.group().trim();
            // remove stop words and words not starting with character
            if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0))
                    || StopWords.contains(matchedKey) || matchedKey.contains("_") || 
                        matchedKey.length() < 3) {
                continue;
            }
            valueBuilder.append(matchedKey);
            valueBuilder.append("@");
            valueBuilder.append(filename);
            this.word.set(valueBuilder.toString());
            context.write(this.word, this.singleCount);
            valueBuilder.setLength(0);
        }
    	
    }
    /**
     *     Before: file1.txt: hello world hello
     *     After: hello@file1.txt 1
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Matcher m1 = WORD_PATTERN.matcher(value.toString());
        Matcher m2 = PHRASE_PATTERN.matcher(value.toString());
        
        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

        addMatchingGroup(m1, filename, context);
        addMatchingGroup(m2, filename, context);
    }
}
