package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EarningsMapper extends Mapper<Object, Text, Text, EarningsPerMinuteCount> {
    

    private static final Pattern FLOAT_PATTERN = Pattern.compile("^[-+]?\\d*\\.\\d+([eE][-+]?\\d+)?$");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = parseLine(value.toString());
    
        if (line == null) {
            return;
        }

        String[] fields = line.split(",");

        String driverID = fields[0]; 
        String amount = fields[1];
        String duration = fields[2]; 

        
        try {
            int tripTime = Integer.parseInt(duration.trim());
            float totalAmount = Float.parseFloat(amount);

            // check to see trip duration is positive
            if (tripTime > 0) {

                context.write(new Text(driverID), new EarningsPerMinuteCount( new FloatWritable(totalAmount), new IntWritable(tripTime)));
            }
        } catch (NumberFormatException e) {
        }
    }

    private static String parseLine(String line) {
        if (line == null) {
            return null;
        }

        String[] fields = line.split(",", -1);
        if (fields.length != 17) {
            return null;
        }

		if (!fields[4].matches("-?\\d+") || Integer.parseInt(fields[4]) == 0) {
			return null;
		}

        if (!isFloat(fields[11]) || !isFloat(fields[12]) || !isFloat(fields[13]) || !isFloat(fields[14]) 
        || !isFloat(fields[15]) || !isFloat(fields[16]) ) {
            return null;
        }

        double total_amount = Math.round((Float.parseFloat(fields[11]) + Float.parseFloat(fields[12]) + Float.parseFloat(fields[13])
                                     + Float.parseFloat(fields[14]) + Float.parseFloat(fields[15])) * 100.0 )/ 100.0;
        if (total_amount != (Math.round(Float.parseFloat(fields[16]) * 100.0) / 100.0)) {
            return null;
        }
        if (total_amount > 500) {
            return null;
        }
        String updated_line = fields[1] + ',' + fields[16] + ',' + fields[4] + '\n';
        return updated_line;
    }

    private static boolean isFloat(String value) {
        return value != null && FLOAT_PATTERN.matcher(value).matches();
    }

}
