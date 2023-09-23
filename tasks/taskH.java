import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

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

    public static int totalRelationshipCount = 0;
    public static ArrayList<String> uniqueSeenUsers = new ArrayList<String>();

    public static class AssociatesMapperA extends Mapper<Object, Text, Text, IntWritable> {
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException{
            Text userAKey = new Text();
            IntWritable one = new IntWritable(1);

            if (!isHeader(value)) {
                String[] csvLine = value.toString().split(",");
                String userA = csvLine[1];
                if(!uniqueSeenUsers.contains(userA)) {
                    uniqueSeenUsers.add(userA);
                }
                totalRelationshipCount += 1;

                userAKey.set(userA);
                context.write(userAKey,one);
            }
        }

        private boolean isHeader(Text value) {
            return value.toString().startsWith("FriendRel,PersonA_ID,PersonB_ID,DateOfFriendship,Desc");
        }

        public static class AssociatesMapperB extends Mapper<Object, Text, Text, IntWritable> {
            public void map (Object key, Text value, Context context) throws IOException, InterruptedException{
                Text userAKey = new Text();
                Text userBKey = new Text();
                Text combinedKey = new Text();
                IntWritable two = new IntWritable(2);

                if (!isHeader(value)) {
                    String[] csvLine = value.toString().split(",");
                    String userA = csvLine[1];
                    String userB = csvLine[2];
                    if(!uniqueSeenUsers.contains(userA)) {
                        uniqueSeenUsers.add(userA);
                    }
                    if(!uniqueSeenUsers.contains(userB)) {
                        uniqueSeenUsers.add(userB);
                    }
                    totalRelationshipCount += 2;

                    combinedKey.set(userA + "," + userB);
                    context.write(combinedKey,two);
                }
            }

            private boolean isHeader(Text value) {
                return value.toString().startsWith("FriendRel,PersonA_ID,PersonB_ID,DateOfFriendship,Desc");
            }
        }
        public static class TaskHReducer extends Reducer<Text, Text, Text, Text>{

            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

                // Compute average number of relationships
                int average = totalRelationshipCount / uniqueSeenUsers.size();

                int numRelationships = 0;
                for(IntWritable val: values) {
                    numRelationships += val.get();
                }
                System.out.println("Total Relationships: " + totalRelationshipCount);
                System.out.println("Total Users: " + uniqueSeenUsers.size());
                System.out.println(key.toString() + " relationships: " + numRelationships);
                System.out.println("Average:" + average);
                if(numRelationships > average) {
                    System.out.println("Here");
                    context.write(key,new Text(Integer.toString(numRelationships) + "\tAverage Relationships: " + average));
                }

            }
        }

        public void debug(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Task H");
            job.setJarByClass(TaskH.class);
            job.setMapperClass(AssociatesMapper.class);
            job.setReducerClass(TaskHReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Task H");
            job.setJarByClass(TaskH.class);
            job.setMapperClass(AssociatesMapper.class);
            job.setReducerClass(TaskHReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }