package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;



public class TopKReducer extends  Reducer<Text, IntWritable, Text, FloatWritable> {

    private PriorityQueue<WordAndCount> pq = new PriorityQueue<WordAndCount>(10);;
    private Logger logger = Logger.getLogger(TopKReducer.class);


    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * @param text
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
   public void reduce(Text key, Iterable<EarningsPerMinuteCount> values, Context context)
           throws IOException, InterruptedException {


       // A local counter just to illustrate the number of values here!
        int counter = 0 ;


       // size of values is 1 because key only has one distinct value
       for (EarningsPerMinuteCount value : values) {
           counter = counter + 1;
           logger.info("Reducer Text: counter is " + counter);
           logger.info("Reducer Text: Add this item  " + new WordAndCount(key, value).toString());

           pq.add(new WordAndCount( key, new EarningsPerMinuteCount(new FloatWritable(value.earnings.get()), new  IntWritable(value.count.get())) ) );

           logger.info("Reducer Text: " + key.toString() + " , Count: " + value.toString());
           logger.info("PQ Status: " + pq.toString());
       }

       // keep the priorityQueue size <= heapSize
       while (pq.size() > 10) {
           pq.poll();
       }
   }


    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<WordAndCount> values = new ArrayList<WordAndCount>(10);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());


        // reverse so they are ordered in descending order
        Collections.reverse(values);


        for (WordAndCount value : values) {
            context.write(value.getWord(), value.getRatioW());
            logger.info("TopKReducer: Top-10 drivers are:  " + value.getWord() + "  Count:"+ value.getRatio());
        }
    }
}
