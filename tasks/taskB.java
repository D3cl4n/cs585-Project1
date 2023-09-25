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

// While iterating through each line of accesslogs.csv, need to keep track of the page just accessed.
// Feed that information into the mapper and the reducer can be used to determine which page has been accessed the most.
// We just need to order the final list that has the WhatPage number with the number of times it was accessed.
// Then, we query FaceIn.csv with the WhatPage attribute since it's linked to the ID attribute there.
public class TaskB {

    // List of tuples to store 10 highest WhatPage IDs with the number of times they were accessed
    private static String[][] mostPopularPages = new String[10][3];
    private static boolean popularPagesFilled = false;
    private static ArrayList<String> allPageIDs = new ArrayList<>();
    private static ArrayList<String> allFaceInInfo = new ArrayList<>();
    public static class FaceInMapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException{
            if (!isHeader(value)) {
                String[] csvLine = value.toString().split(",");
                if (csvLine.length >= 2) {
                    outkey.set(csvLine[0]); //fk - ID
                    outvalue.set("ID\t" + csvLine[0] + "\t" + "Name\t" + csvLine[1] + "\t" + "Nationality\t" + csvLine[2]);
                    context.write(outkey, outvalue);
                }
            }
        }
        private boolean isHeader(Text value) {
            return value.toString().startsWith("ID,Name,Nationality,Country Code,Hobby");
        }
    }
    public static class AccesslogsMapper extends Mapper<Object, Text, Text, Text> {

        private final static Text output = new Text();
        private final static Text one = new Text("A1");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!isHeader(value)) {
                String[] csvLine;
                csvLine = value.toString().split(",");
                // Push the WhatPage ID into the output
                output.set(csvLine[2]);
                context.write(output, one); // fk - WhatPage
            }
        }

        private boolean isHeader(Text value) {
            return value.toString().startsWith("AccessId, ByWho, WhatPage, TypeOfAccess, AccessTime");
        }
    }
    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Text outKey = new Text();
            Text outValue = new Text();
            int sum = 0;
            boolean fromAccessMapper = false;
            for(Text val: values) {
                // Check if the values are from the Accesslogs Mapper
                fromAccessMapper = val.toString().equals("A1");
                if (fromAccessMapper) {
                    // Input from AccesslogsMapper
                    sum++;
                    // System.out.println(key + ": " + sum);
                } else {
                    // Input from FaceInMapper
                    String[] faceInInput = val.toString().split("\t");
                    allPageIDs.add(faceInInput[1]);
                    allFaceInInfo.add(val.toString());
//                    System.out.println(faceInInput[5]);
                }
            }

            // Determine if the current page is one of the most popular pages
            if(fromAccessMapper) {
                /* Loop through the mostPopularPages array and replace the page with
                   the lowest popularity if it's lower than the current page being looked at.
                   If there's an empty row, then just insert the current page in there.
                */
                if(!popularPagesFilled) {
                    for (int n = 0; n < mostPopularPages.length; n++) {
                        // First, check if this row is empty
                        // Do this for each row until the popularPagesFilled flag has been set
                        if (mostPopularPages[n][0] == null && mostPopularPages[n][1] == null && mostPopularPages[n][2] == null) {
                            mostPopularPages[n][0] = key.toString();
                            mostPopularPages[n][1] = Integer.toString(sum);
                            mostPopularPages[n][2] = "";
                            // System.out.println(mostPopularPages[n][0]);
                            if(n == 9) {
                                popularPagesFilled = true;
                                // System.out.println("Popular Pages Array Filled.");
                                // System.out.println("Value of mostPopularPages:\n" + convertPopularPagesArrayToString());
                            }
                            break;
                        }
                    }
                } else {
                    // Loop through the popular pages array and find the min
                    // Also, check if any of the IDs in notSeenPageIDs are in mostPopularPages
                    // If they are, then store the information and remove the ID from notSeenPageIDs
                    int minIndex = 0;
                    String min = mostPopularPages[minIndex][1];
                    String[] notSeenPageIDsInfo;
                    for (int n = 0; n < mostPopularPages.length; n++) {
                        if (Integer.parseInt(mostPopularPages[n][1]) < Integer.parseInt(min)) {
                            min = mostPopularPages[n][1];
                            minIndex = n;
                        }
                    }
                    // Compare the current page's popularity with the page that has the lowest popularity in the list
                    // Replace the page ID and count in that row of mostPopularPages if the current page is more popular
                    if (Integer.parseInt(mostPopularPages[minIndex][1]) < sum) {
                        mostPopularPages[minIndex][0] = key.toString();
                        mostPopularPages[minIndex][1] = Integer.toString(sum);
                    }
                }
            }
            // Will write the mostPopularPages array into the context
            // The last time the reducer writes to the context will be when the array has the 10 most popular pages
            storeFaceInInfoInArray();
//            System.out.println(allPageIDs.toString());
//            System.out.println(allFaceInInfo.toString());
            outKey.set("Most Popular Pages\n");
            outValue.set(convertPopularPagesArrayToString());
            context.write(outKey,outValue);
        }
    }

    private static void storeFaceInInfoInArray() {
        // Will store the relevant FaceIn info for the most popular pages here
        if(popularPagesFilled) {
            for (int n = 0; n < mostPopularPages.length; n++) {
                for (int i = 0; i < allPageIDs.size(); i++) {
                    if (mostPopularPages[n][0].equals(allPageIDs.get(i))) {
                        mostPopularPages[n][2] = allFaceInInfo.get(i);
                        break;
                    }
                }
            }
        }
    }

    private static String convertPopularPagesArrayToString() {
        StringBuilder str = new StringBuilder();
        String[] parts;
        if(popularPagesFilled) {
            for (int n = 0; n < mostPopularPages.length; n++) {
                parts = mostPopularPages[n][2].split("\t");
                str.append(parts[0]);
                str.append(":");
                str.append(parts[1]);
                str.append("\t");
                str.append(parts[2]);
                str.append(":");
                str.append(parts[3]);
                str.append("\t");
                str.append(parts[4]);
                str.append(":");
                str.append(parts[5]);
                str.append("\n");
//            str.append("ID:");
//            str.append(mostPopularPages[n][0]);
//            str.append("\tCount:");
//            str.append(mostPopularPages[n][1]);
//            str.append("\n");
//            str.append(mostPopularPages[n][2]);
//            str.append("\n");
            }
        }
        return str.toString();
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task B");
        job.setJarByClass(TaskB.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FaceInMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, AccesslogsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task B");
        job.setJarByClass(TaskB.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FaceInMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, AccesslogsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
