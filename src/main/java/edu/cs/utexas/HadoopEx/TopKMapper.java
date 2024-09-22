package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class TopKMapper extends Mapper<Text, Text, Text, EarningsPerMinuteCount> {

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
        EarningsPerMinuteCount earningsPerMinuteCount = EarningsPerMinuteCount.parseString(value.toString());
        pq.add(new WordAndCount(new Text(key), new EarningsPerMinuteCount( new FloatWritable(earningsPerMinuteCount.earnings.get()),new IntWritable(earningsPerMinuteCount.count.get()))));

        if (pq.size() > 10) {
            pq.poll();
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        while (pq.size() > 0) {
            WordAndCount wordAndCount = pq.poll();
            context.write(new Text(wordAndCount.getWord()), new EarningsPerMinuteCount( new FloatWritable(wordAndCount.getEarningsPerMinuteCount().earnings.get()), new IntWritable(wordAndCount.getEarningsPerMinuteCount().count.get())));
            logger.info("TopKMapper PQ Status: " + pq.toString());
        }
    }

}
