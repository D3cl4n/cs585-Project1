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

public class TaskA
{
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        //String to write to output file "name + hobby"
        private final static Text output = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            String nationality = "test";
            String name = "";
            String hobby = "";
            Bool flag = false;
            int counter = 0;

            while (itr.hasMoreTokens())
            {
                if (counter == 1)
                {
                    name = itr.nextToken();
                    counter = counter + 1;
                }

                else if (counter == 4)
                {
                    hobby = itr.nextToken();
                    counter = counter + 1;
                }

                else if (itr.nextToken() == nationality)
                {
                    flag = true;
                }
                counter = counter + 1;
            }

            if (flag == true)
            {
                context.add(...);
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task A");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(TokenizerMapper.class);
    }
}