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

public class TaskF {
    public static class FaceInMapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();
        public void map (Object key, Text value, Context context)
                throws IOException, InterruptedException{

                String[] parts = value.toString().split(",");
                if (parts.length >= 2) {
                    outkey.set(parts[0]); // personid as a key
                    outvalue.set("Name\t" + parts[1]+"\t"+parts[0]); //concanate the name and id as a value
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
                    perId.set(parts[1]);  // key
                    relID.set("RelCount\t" + parts[0]); //'Relcount'+relation id as value
                    context.write(perId, relID);
                }
        }

    }

    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {
        private Text perId = new Text();
        private Text accessID = new Text();
        public void map (Object key, Text value, Context context)
                throws IOException, InterruptedException{

                String[] parts = value.toString().split(",");
                if (parts.length >= 2) {
                    perId.set(parts[1]);  // set ByWho as key
                    accessID.set("AccessCount\t" + parts[0]); //'AccessCount'+relation id as value
                    context.write(perId, accessID);
                }
        }

    }
    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text>{
        private Text pName = new Text();
        private Text perid= new Text();
        private IntWritable relCount = new IntWritable();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            String name = null;
            String pid=null;
            int countrel = 0; //count relation counter
            int countaccess=0; // count access counter

            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                String keyType = parts[0];
                String dataValue = parts[1];

                    if (keyType.equals("RelCount")) {
                        countrel++;
                    }
                    else if (keyType.equals("AccessCount")) {
                        countaccess++;
                    }
                    else if (keyType.equals("Name")) {
                        String dataValue1 = parts[2];
                        name = dataValue;
                        pid = dataValue1;
                    }
            }

            if (name != null & countrel>0 & countaccess==0) // filter users who has relations but don't access to the other pages
            {
                pName.set(name); // set person name
                perid.set(pid);  // set personid
                context.write(perid,pName);
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf= new Configuration();
        Job job=new Job(conf,"Task F");
        job.setJarByClass(TaskF.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/FaceIn.csv"),TextInputFormat.class, FaceInMapper.class);
        MultipleInputs.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/associates.csv"),TextInputFormat.class, AssociatesMapper.class);
        MultipleInputs.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/access.csv"),TextInputFormat.class, AccessMapper.class);
        Path outputPath= new Path("hdfs://localhost:9000/project1/taskF.txt");

        FileOutputFormat.setOutputPath(job,outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
