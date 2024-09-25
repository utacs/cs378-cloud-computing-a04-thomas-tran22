package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;

public class PositionErrorMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final Pattern FLOAT_PATTERN = Pattern.compile("^[-+]?\\d*\\.\\d+([eE][-+]?\\d+)?$");
    private Text hour = new Text();

    public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {

        if (value == null) {
            return;
        }

        String[] itr = value.toString().split(",");

        if (itr.length < 17) {
            return;
        }

        String pickupTime = itr[2];
        String dropoffTime = itr[3];
        int pickupErrorCount = 0;
        int dropoffErrorCount = 0;
        String pickupHourSplit = pickupTime.split(" ")[1].split(":")[0];
        String dropoffHourSplit = dropoffTime.split(" ")[1].split(":")[0];
        boolean emptyField = false;

        if (!pickupHourSplit.matches("-?\\d+") || !dropoffHourSplit.matches("-?\\d+")) {
            System.err.println("Error: Invalid int value for pickup/dropoff: " + value.toString());
            return;
        }

        if (itr[6] == "" || itr[7] == "" || !isFloat(itr[6]) || !isFloat(itr[7])) {
            pickupErrorCount += 1;
            emptyField = true;
        }

        if (itr[8] == "" || itr[9] == "" || !isFloat(itr[8]) || !isFloat(itr[9])) { 
            dropoffErrorCount += 1;
            emptyField = true;
        }

        if (!emptyField) {
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
        }

        if (pickupErrorCount > 0) {
            // System.out.println("*******Error count > 0:" + errorCount + " value:" + value.toString());
            if (pickupHourSplit.equals("00")) {
                pickupHourSplit = "24";
            }
            hour.set(pickupHourSplit);
            context.write(hour, new IntWritable(pickupErrorCount));
        }

        if (dropoffErrorCount > 0) {
            // System.out.println("*******Error count > 0:" + errorCount + " value:" + value.toString());
            if (dropoffHourSplit.equals("00")) {
                dropoffHourSplit = "24";
            }
            hour.set(dropoffHourSplit);
            context.write(hour, new IntWritable(dropoffErrorCount));
        }
    }
    
    private static boolean isFloat(String value) {
        return value != null && FLOAT_PATTERN.matcher(value).matches();
    }
}
