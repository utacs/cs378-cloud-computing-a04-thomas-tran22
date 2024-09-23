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
        String dropoffTime = itr[3];
        int pickupErrorCount = 0;
        int dropoffErrorCount = 0;

        double pickup_longitude = Math.round(Float.parseFloat(itr[6]) * 100.0) / 100.0;
        double pickup_latitude = Math.round(Float.parseFloat(itr[7]) * 100.0) / 100.0;
        double dropoff_longitude = Math.round(Float.parseFloat(itr[8]) * 100.0) / 100.0;
        double dropoff_latitude = Math.round(Float.parseFloat(itr[9]) * 100.0) / 100.0;

        if (pickup_longitude == 0 || pickup_latitude == 0) {
            pickupErrorCount += 1;
        }

        if (dropoff_longitude == 0 || dropoff_latitude == 0) {
            dropoffErrorCount += 1;
        }

        if (pickupErrorCount > 0) {
            // System.out.println("*******Error count > 0:" + errorCount + " value:" + value.toString());
            String hourSplit = pickupTime.split(" ")[1].split(":")[0];
            if (hourSplit.equals("00")) {
                hourSplit = "24";
            }
            hour.set(hourSplit);
            context.write(hour, new IntWritable(pickupErrorCount));
        }

        if (dropoffErrorCount > 0) {
            // System.out.println("*******Error count > 0:" + errorCount + " value:" + value.toString());
            String hourSplit = dropoffTime.split(" ")[1].split(":")[0];
            if (hourSplit.equals("00")) {
                hourSplit = "24";
            }
            hour.set(hourSplit);
            context.write(hour, new IntWritable(dropoffErrorCount));
        }
    }
}
