package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class WordAndCount implements Comparable<WordAndCount> {

        private final Text word;
        private ErrorCount errorCount;

        public WordAndCount(Text word, ErrorCount ErrorCount) {
            this.word = word;
            this.errorCount = ErrorCount;
        }

        public WordAndCount(Text word, IntWritable error, IntWritable count) {
            this.word = word;
            errorCount = new ErrorCount(error, count);
        }

        public IntWritable getCount() {
            return errorCount.count;
        }

        public ErrorCount getErrorCount(){
            return errorCount;
        }

        public IntWritable geterror() {
            return errorCount.error;
        }

        public Text getWord() {
            return word;
        }
        
        public FloatWritable getRatioW() {
            float ratio = errorCount.error.get()/(float)errorCount.count.get();
            FloatWritable ratioW = new FloatWritable(ratio);
            return ratioW;
        }

        public float getRatio() {
            float ratio = errorCount.error.get()/(float)errorCount.count.get();
            return ratio;
        }


    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        @Override
        public int compareTo(WordAndCount other) {

            float diff = getRatio() - other.getRatio();
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            }
            return 0;
        }


        public String toString(){
            return "("+word.toString() +" , "+ getRatio()+")";
        }
    }


