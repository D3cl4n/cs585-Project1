import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;


public class TaskA
{
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        //String to write to output file "name + hobby"
        private final static Text output = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            // Access the data from FaceIn.csv
            // If the nationality is the same as a randomly selected nationality,
            // then report the name and hobby of the current user
            String nationality = "Bulgarian";
            String name = "";
            String hobby = "";
            String delimiter = ",";
            String line = "";
            String[] csvLine;

            csvLine = value.toString().split(delimiter);
            if (csvLine[2].equals(nationality))
            {
                name = csvLine[1];
                hobby = csvLine[4];
                output.set(name + "," + hobby);
                context.write(output, one);
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        //System.out.println("start");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task A");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("src/FaceIn.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output/out.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
