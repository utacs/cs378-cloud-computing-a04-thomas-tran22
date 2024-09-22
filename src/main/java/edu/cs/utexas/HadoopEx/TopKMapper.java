package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class TopKMapper extends Mapper<Text, Text, Text, ErrorCount> {

    private Logger logger = Logger.getLogger(TopKMapper.class);

    private PriorityQueue<WordAndCount> pq;

    public void setup(Context context) {
        pq = new PriorityQueue<>();

    }

    /**
     * Reads in results from the first job and filters the topk results
     *
     * @param key
     * @param value a float value stored as a string
     */
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        ErrorCount errorCount = ErrorCount.parseString(value.toString());
        pq.add(new WordAndCount(key, new ErrorCount(new IntWritable(errorCount.error.get()),new IntWritable(errorCount.count.get()))));

        if (pq.size() > 5) {
            pq.poll();
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        while (pq.size() > 0) {
            WordAndCount wordAndCount = pq.poll();
            context.write(new Text(wordAndCount.getWord()), new ErrorCount(new IntWritable(wordAndCount.getErrorCount().error.get()), new IntWritable(wordAndCount.getErrorCount().count.get())));
            logger.info("Five Taxis with Highest GPS Error Rates: " + pq.toString());
        }
    }

}
