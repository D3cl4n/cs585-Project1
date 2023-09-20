import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// While iterating through each line of accesslogs.csv, need to keep track of the page just accessed.
// Feed that information into the mapper and the reducer can be used to determine which page has been accessed the most.
// We just need to order the final list that has the WhatPage number with the number of times it was accessed.
// Then, we query FaceIn.csv with the WhatPage attribute since it's linked to the ID attribute there.
public class taskB {

    // List of tuples to store 10 highest WhatPage IDs with the number of times they were accessed
    private static int[][] mostPopularPages = new int[10][2];
    private static int count = 0;

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static Text output = new Text();
        private final static IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] csvLine;
            String delimiter = ",";
            csvLine = value.toString().split(delimiter);
            // Initialize the mostPopularPages array with the first 10 pages with the ID and "1" for the count
            if(count < 10) {
                mostPopularPages[count][0] = csvLine[2];
                mostPopularPages[count][1] = 1;
            }
            count++;
            // Push the WhatPage ID into the output
            output.set(csvLine[2]);
            context.write(output, one);
        }

    }
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values) {
                sum += val.get();
            }
            // Loop through the mostPopularPages array and replace the page with
            // the lowest popularity if it's lower than the current page being looked at
            int minIndex = 0;
            int min = mostPopularPages[minIndex][1];
            for(int n = 1;n < mostPopularPages.length;n++){
                if(mostPopularPages[n][1] < min) {
                    min = mostPopularPages[n][1];
                    minIndex = n;
                }
            }
            // Compare the current page's popularity with the page that has the lowest popularity in the list
            // Replace the page ID and count in that row of mostPopularPages if the current page is more popular
            if(mostPopularPages[minIndex][1] < sum) {
                mostPopularPages[minIndex][0] = Integer.valueOf(key.toString());
                mostPopularPages[minIndex][1] = sum;
            }
            result.set(sum);
            context.write(key, result);
        }

    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task B");
        job.setJarByClass(taskB.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}