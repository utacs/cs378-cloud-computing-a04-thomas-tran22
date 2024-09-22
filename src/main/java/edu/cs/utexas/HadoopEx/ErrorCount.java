package edu.cs.utexas.HadoopEx;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;  

public class ErrorCount implements Writable{
    public final IntWritable count;
    public final IntWritable error;

    public ErrorCount(IntWritable error, IntWritable count){
        this.error = error;
        this.count = count;
    }

    public ErrorCount(){
        this.count = new IntWritable();
        this.error = new IntWritable();
    }

    public static ErrorCount parseString(String s) {
        String[] elements = s.split(",");
        IntWritable errorW = new IntWritable(Integer.parseInt(elements[0]));
        IntWritable countW = new IntWritable(Integer.parseInt(elements[1]));
        return new ErrorCount(errorW,countW);
    }
   
    @Override
    public void readFields(DataInput arg0) throws IOException {
        count.readFields(arg0);
        error.readFields(arg0);        
    }
    @Override
    public void write(DataOutput arg0) throws IOException {
        count.write(arg0);
        error.write(arg0);
    }
    
    public String toString(){
        return "" + error.get() + "," + count.get();
    }
}

