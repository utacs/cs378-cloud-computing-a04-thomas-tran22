package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.TreeMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;


public class PositionErrorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Logger logger = Logger.getLogger(PositionErrorReducer.class);
    private Map<String, IntWritable> map = new TreeMap<String, IntWritable>();;

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int errorCount = 0;

        for (IntWritable value : values) {
            errorCount += value.get();
        }

        map.put(key.toString(), new IntWritable(errorCount));
        context.write(key, new IntWritable(errorCount));
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, IntWritable> entry : map.entrySet()) {
            logger.info("(" + (Integer.parseInt(entry.getKey())) + ", " + entry.getValue() + ")");
        }
    }
    
}