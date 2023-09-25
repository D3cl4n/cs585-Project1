import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TaskH {
    public static int totalRelationships = 0;
    public static int totalUsers = 0;
    public static class PersonAMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static Text outKey = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String delimiter = ",";
            String[] csvLine;
            String personA;

            csvLine = value.toString().split(delimiter);
            personA = csvLine[1];

            outKey.set(personA);
            context.write(outKey, one);

            totalRelationships += 1;
            totalUsers += 1;
        }
    }

    public static class PersonBMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static Text outKey = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String delimiter = ",";
            String[] csvLine;
            String personB;

            csvLine = value.toString().split(delimiter);
            personB = csvLine[2];

            outKey.set(personB);
            context.write(outKey, one);

            totalRelationships += 1;
            totalUsers += 1;
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int average = totalRelationships / totalUsers;

            for (IntWritable val : values) {
                sum += val.get();
            }

            if (sum > average)
            {
                result.set(sum);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Task H");
        job.setJarByClass(TaskH.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/associates.csv"), TextInputFormat.class, PersonAMapper.class);
        MultipleInputs.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/associates.csv"), TextInputFormat.class, PersonBMapper.class);
        Path outputPath = new Path("hdfs://localhost:9000/project1/taskHTest.txt");

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}