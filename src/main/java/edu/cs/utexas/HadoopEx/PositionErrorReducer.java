package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;


public class PositionErrorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Logger logger = Logger.getLogger(PositionErrorReducer.class);
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        int errorCount = 0;
        
        for (IntWritable value : values) {
            errorCount += value.get();
        }
        
        context.write(key, new IntWritable(errorCount)); 
    }
}