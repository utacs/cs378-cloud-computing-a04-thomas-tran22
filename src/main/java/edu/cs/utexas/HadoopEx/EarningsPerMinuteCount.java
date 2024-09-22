package edu.cs.utexas.HadoopEx;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;  

public class EarningsPerMinuteCount implements Writable{
    public final IntWritable count;
    public final FloatWritable earnings;

    public EarningsPerMinuteCount(FloatWritable earnings, IntWritable count){
        this.earnings = earnings;
        this.count = count;
    }

    public EarningsPerMinuteCount(){
        this.count = new IntWritable();
        this.earnings = new FloatWritable();
    }

    public static EarningsPerMinuteCount parseString(String s) {
        String[] elements = s.split(",");
        FloatWritable earningsW = new FloatWritable(Float.parseFloat(elements[0]));
        IntWritable countW = new IntWritable(Integer.parseInt(elements[1]));
        return new EarningsPerMinuteCount(earningsW,countW);
    }
    
   
    @Override
    public void readFields(DataInput arg0) throws IOException {
        count.readFields(arg0);
        earnings.readFields(arg0);        
    }
    @Override
    public void write(DataOutput arg0) throws IOException {
        count.write(arg0);
        earnings.write(arg0);
    }
    
    public String toString(){
        return "" + earnings.get() + "," + count.get();
    }

}

