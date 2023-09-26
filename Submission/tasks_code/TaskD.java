import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;

public class TaskD {
    public static class FaceInMapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();
        public void map (Object key, Text value, Context context)
            throws IOException, InterruptedException{

                String[] parts = value.toString().split(",");
                if (parts.length >= 2) {
                    outkey.set(parts[0]);  // set key values
                    outvalue.set("Name\t" + parts[1]); //set {'Name\t' + Name} as an output
                    context.write(outkey, outvalue);
                }

            }
    }
    public static class AssociatesMapper extends Mapper<Object, Text, Text, Text> {
        private Text perId = new Text();
        private Text relID = new Text();
        public void map (Object key, Text value, Context context)
                throws IOException, InterruptedException{
                String[] parts = value.toString().split(",");
                if (parts.length >= 2) {
                    perId.set(parts[1]);  // set personid as a key
                    relID.set("RelCount\t" + parts[0]); //set {'RelCount'+relation id} as value
                    context.write(perId, relID);
                }

        }

    }
    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text>{
        private Text pName = new Text();
        private IntWritable relCount = new IntWritable();
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
            String name = null;
            int count = 0;  // counter

            for (Text value : values) {
                String[] parts = value.toString().split("\t");

                    String keyType = parts[0];
                    String dataValue = parts[1];

                    if (keyType.equals("Name")) {
                        name = dataValue;  // assign name
                    }
                    else if (keyType.equals("RelCount")) {
                        count++; // increase counter
                    }
            }

            if (name != null) {
                pName.set(name);
                relCount.set(count);
                context.write(pName, new Text(relCount.toString()));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf= new Configuration();
        Job job=new Job(conf,"Reduce Side Join");
        job.setJarByClass(TaskD.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/facein.csv"),TextInputFormat.class, FaceInMapper.class);
        MultipleInputs.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/associates.csv"),TextInputFormat.class, AssociatesMapper.class);
        Path outputPath= new Path("hdfs://localhost:9000/project1/taskD.txt");

        FileOutputFormat.setOutputPath(job,outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
