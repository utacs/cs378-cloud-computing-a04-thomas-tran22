package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EarningsReducer extends  Reducer<Text, EarningsPerMinuteCount, Text, EarningsPerMinuteCount> {

   public void reduce(Text text, Iterable<EarningsPerMinuteCount> values, Context context)
           throws IOException, InterruptedException {
       
       int totalTime = 0;
       float totalAmount = 0;
       
       for (EarningsPerMinuteCount earningsCount : values) {
            totalTime += earningsCount.count.get();
            totalAmount += earningsCount.earnings.get();
       }
       context.write(text, new EarningsPerMinuteCount(new FloatWritable(totalAmount), new IntWritable(totalTime)));
   }
}
