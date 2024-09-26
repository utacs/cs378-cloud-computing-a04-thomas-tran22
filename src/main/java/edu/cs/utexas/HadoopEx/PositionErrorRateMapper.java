package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PositionErrorRateMapper extends Mapper<Object, Text, Text, ErrorCount> {

    //private final Text taxiId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] itr = value.toString().split(",");
        
        if (itr.length < 17) {
            return; 
        }

        String medallion = itr[0]; 
        int errorCount = 0;

        if(!itr[6].equals("") && !itr[7].equals("") && !itr[8].equals("")  && !itr[9].equals("") ){
            double pickup_longitude = Math.round(Float.parseFloat(itr[6]) * 100.0) / 100.0; 
            double pickup_latitude = Math.round(Float.parseFloat(itr[7]) * 100.0) / 100.0;
            double dropoff_longitude = Math.round(Float.parseFloat(itr[8]) * 100.0) / 100.0;
            double dropoff_latitude = Math.round(Float.parseFloat(itr[9]) * 100.0) / 100.0;
    
            if (pickup_longitude == 0 || pickup_latitude == 0 || dropoff_longitude == 0 ||dropoff_latitude == 0) {
                 errorCount += 1;
            }
        }else{
            errorCount += 1;
        }
    
            context.write(new Text(medallion), new ErrorCount(new IntWritable(errorCount), new IntWritable(1))); 
        
        
       
    }
}
