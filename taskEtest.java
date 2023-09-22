import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

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

public class taskEtest {
    public static class FaceInMapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();
        public void map (Object key, Text value, Context context)
                throws IOException, InterruptedException{
            if (!isHeader(value)) {
                String[] parts = value.toString().split(",");
                if (parts.length >= 2) {
                    outkey.set(parts[0]);
                    outvalue.set("Name\t" + parts[1]); //concanate the name
                    context.write(outkey, outvalue);
                }
            }

        }
        private boolean isHeader(Text value) {
            return value.toString().startsWith("ID,Name,Nationality,Country Code,Hobby");
        }
    }

    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {
        private Text perId = new Text();
        private Text accessID = new Text();
        //private datetime accessDate = new
        public void map (Object key, Text value, Context context)
                throws IOException, InterruptedException{
            if (!isHeader(value)) {
                String[] parts = value.toString().split(",");
                if (parts.length >= 2) {
                    perId.set(parts[1]);  // fk
                    accessID.set("AccessCount\t" + parts[2]); //access id as value
                    context.write(perId, accessID);
                }
            }

        }
        private boolean isHeader(Text value) {
            return value.toString().startsWith("AccessId, ByWho,WhatPage,TypeOfAccess,AccessTime");
        }
    }
    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text>{
        private Text pName = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
           // ArrayList<Integer> uniquePages = new ArrayList<Integer>();
            Set<String> uniquePages = new HashSet<>();
            String name = null;
            int totalVisits=0;
            boolean flag = false;
            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                if (parts.length == 2) {
                    String dataType = parts[0];
                    String dataValue = parts[1];

                   if (dataType.equals("AccessCount"))
                    {
                        uniquePages.add(value.toString());
                        totalVisits++;
                    }
                   else if (dataType.equals("Name"))
                   {
                       name = dataValue;
                   }
                }


            }

            if (name != null) {
                pName.set(name);
                int distinctCount = uniquePages.size();
                Text finalText = new Text("Unique Pages: " + distinctCount + " Total Visits: " + totalVisits);
                context.write(pName,finalText);
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf= new Configuration();
        Job job=new Job(conf,"Task E");
        job.setJarByClass(taskG.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/FaceIn.csv"),TextInputFormat.class, FaceInMapper.class);
        MultipleInputs.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/access.csv"),TextInputFormat.class, AccessMapper.class);
        Path outputPath= new Path("hdfs://localhost:9000/project1/taskETest.txt");

        FileOutputFormat.setOutputPath(job,outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
