package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class WordAndCount implements Comparable<WordAndCount> {

        private final Text word;
        private EarningsPerMinuteCount earningsCount;
        public WordAndCount(Text word, EarningsPerMinuteCount EarningsPerMinuteCount) {
            this.word = word;
            this.earningsCount = EarningsPerMinuteCount;
        }

        public WordAndCount(Text word, FloatWritable earnings, IntWritable count) {
            this.word = word;
            earningsCount = new EarningsPerMinuteCount(earnings, count);
        }

        public IntWritable getCount() {
            return earningsCount.count;
        }

        public EarningsPerMinuteCount getEarningsPerMinuteCount(){
            return earningsCount;
        }

        public FloatWritable getearnings() {
            return earningsCount.earnings;
        }

        public Text getWord() {
            return word;
        }
        
        public FloatWritable getRatioW() {
            float ratio = (earningsCount.earnings.get()/(float)earningsCount.count.get()) * 60;
            FloatWritable ratioW = new FloatWritable(ratio);
            return ratioW;
        }

        public float getRatio() {
            float ratio = (earningsCount.earnings.get()/(float)earningsCount.count.get()) * 60;
            FloatWritable ratioW = new FloatWritable(ratio);
            return ratio;
        }


    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        @Override
        public int compareTo(WordAndCount other) {

            float diff = getRatio() - other.getRatio() ;
            if (diff > 0) {
                return 1;
            } else if (diff < 0) {
                return -1;
            }
            return 0;
        }


        public String toString(){

            return "("+word.toString() +" , "+ getRatioW()+")";
        }
    }


