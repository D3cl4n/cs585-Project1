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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

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
            String nationality = "test";
            String name = "";
            String hobby = "";
            String delimiter = ",";
            File file = new File("dataset_generation\\FaceIn.csv");
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            String[] csvLine;

            while((line = br.readLine()) != null) {
                csvLine = line.split(delimiter);
                nationality = csvLine[2];
                // Need to randomly pick a nationality to compare to this user's
                if(nationality.equals("")) {
                    name = csvLine[1];
                    hobby = csvLine[4];
                    output.set(name + "," + hobby);
                    context.write(output,one);
                }
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