import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;


public class TaskE
{
    public static class FaceInMapper extends Mapper<Object, Text, Text, Text>
    {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private Text charLabel = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {
            System.out.println("here1");
            if (!value.toString().contains("ID")) {
                String delimiter = ",";
                String line = value.toString();
                String[] elements = line.split(delimiter);
                outKey.set(elements[0]);
                charLabel.set("F");
                outValue.set(charLabel);
                context.write(outKey, outValue);
            }
        }
    }

    public static class AccessMapper extends Mapper<Object, Text, Text, Text>
    {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private Text outLabel = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {
            if (!value.toString().contains("AccessID"))
            {
                String delimiter = ",";
                String line = value.toString();
                String[] elements = line.split(delimiter);
                outKey.set(elements[1]);
                outLabel.set("A");
                outValue.set(outLabel + elements[2]);
                context.write(outKey, outValue);
            }
        }
    }
    public static class ReduceJoinReducer extends Reducer<Text,Text,Text,Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            ArrayList<Integer> uniquePages = new ArrayList<Integer>();
            int totalVisits = 0;
            boolean flag = false;
            for (Text val : values)
            {
                if (val.toString().contains("A"))
                {
                    String currValue = val.toString().replace("A", "");
                    for (int x : uniquePages)
                    {
                        if (x == Integer.parseInt(currValue))
                        {
                            flag = true;
                        }
                    }

                    if (!flag)
                    {
                        uniquePages.add(Integer.parseInt(currValue));
                    }
                    totalVisits = totalVisits + 1;
                }
            }
            Text finalText = new Text("Unique Pages: " + uniquePages + "Total Visits" + totalVisits);
            context.write(key, finalText);
        }
    }

    public static void main(String[] args) throws Exception
    {
        System.out.println("start");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task E");
        job.setJarByClass(TaskE.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FaceInMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, AccessMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}