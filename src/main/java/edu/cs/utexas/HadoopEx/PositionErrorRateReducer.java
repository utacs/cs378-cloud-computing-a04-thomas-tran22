package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PositionErrorRateReducer extends Reducer<Text, ErrorCount, Text, ErrorCount> {

    public void reduce(Text key, Iterable<ErrorCount> values, Context context) throws IOException, InterruptedException {
        int totalError = 0;
        int totalCount = 0;

        for (ErrorCount errorCount : values) {
            totalError += errorCount.error.get();
            totalCount += errorCount.count.get();
       }
        context.write(new Text(key), new ErrorCount(new IntWritable(totalError), new IntWritable(totalCount)));
    }
}
