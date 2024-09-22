package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PositionErrorMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text hour = new Text();

    public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String[] itr = value.toString().split(",");

        if (itr.length < 17) {
            return;
        }

        String pickupTime = itr[2];
        int errorCount = 0;

        double pickup_longitude = Math.round(Float.parseFloat(itr[6]) * 100.0) / 100.0; 
        double pickup_latitude = Math.round(Float.parseFloat(itr[7]) * 100.0) / 100.0;
        double dropoff_longitude = Math.round(Float.parseFloat(itr[8]) * 100.0) / 100.0;
        double dropoff_latitude = Math.round(Float.parseFloat(itr[9]) * 100.0) / 100.0;

        if (pickup_longitude == 0 || pickup_latitude == 0) {
             errorCount += 1;
        }

        if (dropoff_longitude == 0 ||dropoff_latitude == 0) {
            errorCount += 1;
        }

        if (errorCount > 0) {
            // System.out.println("*******Error coount > 0:" + errorCount + " value:" + value.toString());
            String hourSplit = pickupTime.split(" ")[1].split(":")[0];
            hour.set(hourSplit);
            context.write(hour, new IntWritable(errorCount));
        }
    }
}
