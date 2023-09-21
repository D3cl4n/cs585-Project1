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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// While iterating through each line of accesslogs.csv, need to keep track of the page just accessed.
// Feed that information into the mapper and the reducer can be used to determine which page has been accessed the most.
// We just need to order the final list that has the WhatPage number with the number of times it was accessed.
// Then, we query FaceIn.csv with the WhatPage attribute since it's linked to the ID attribute there.
public class taskB {

    // List of tuples to store 10 highest WhatPage IDs with the number of times they were accessed
    private static String[][] mostPopularPages = new String[10][3];
    private static boolean popularPagesFilled = false;
    private static ArrayList<String> seenPageIDs = new ArrayList<String>();
    private static ArrayList<String> notSeenPageIDs = new ArrayList<String>();

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
        private final static Text one = new Text("1");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] csvLine;
            csvLine = value.toString().split(",");

            // Push the WhatPage ID into the output
            output.set(csvLine[2]);
            context.write(output, one); // fk - WhatPage
        }
    }
    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            boolean fromAccessMapper = false;
            // Check if the values are from the Accesslogs Mapper
            fromAccessMapper = val.toString().equals("1");
            if(fromAccessMapper){
                // Input from AccesslogsMapper
                for(Text val: values) {
                    sum++;
                }
            } else {
                // Input from FaceInMapper
                for(Text val: values) {
                    String[] reducerInput = val.toString().split("\t");
                    /* If the page ID has been seen before, then need to check if it's in the mostPopularPages array.
                       If it's not in mostPopularPages, then the information can be disregarded.
                       Otherwise, the information needs to be stored in the respective page ID found in mostPopularPages.
                       If the page ID has not been seen before, then we need to store it until
                       it has been seen and compared to the mostPopularPages.
                       The page IDs that haven't been seen before can be stored in notSeenPageIDs.
                     */
                    if(seenPageIDs.contains(reducerInput[0])) {
                        // Need to check mostPopularPages
                        for(int n = 0;n < mostPopularPages.length;n++) {
                            if(mostPopularPages[n][0].equals(reducerInput[0])){
                                mostPopularPages[n][2] = val.toString();
                            }
                        }
                    } else {
                        notSeenPageIDs.add(val.toString());
                    }
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
                            if(n == 9) {
                                popularPagesFilled = true;
                            }
                            break;
                        }
                    }
                } else {
                    // Loop through the popular pages array and find the min
                    // Also, check if any of the IDs in notSeenPageIDs are in mostPopularPages
                    // If they are, then store the information and remove the ID from notSeenPageIDs
                    int minIndex = 0;
                    int min = mostPopularPages[minIndex][1];
                    String[] notSeenPageIDsInfo;
                    for (int n = 0; n < mostPopularPages.length; n++) {
                        if (Integer.valueOf(mostPopularPages[n][1]) < min) {
                            min = Integer.valueOf(mostPopularPages[n][1]);
                            minIndex = n;
                        }
                        // Check to see if the page ID hasn't been seen before
                        // Loop through the list of unseen IDs
                        for(int i = 0;i < notSeenPageIDs.size();i++){
                            notSeenPageIDsInfo = notSeenPageIDs.get(i).split("\t");
                            if(notSeenPageIDsInfo[0] == mostPopularPages[n][0]) {
                                // The page IDs have been matched so the FaceIn information can be stored
                                mostPopularPages[n][2] = notSeenPageIDs.get(i);
                                // Remove the item at this index
                                notSeenPageIDs.remove(i);
                                break;
                            }
                        }
                    }
                }
                // Compare the current page's popularity with the page that has the lowest popularity in the list
                // Replace the page ID and count in that row of mostPopularPages if the current page is more popular
                if(Integer.toString(mostPopularPages[minIndex][1]) < sum) {
                    mostPopularPages[minIndex][0] = key.toString();
                    mostPopularPages[minIndex][1] = Integer.toString(sum);
                }
                seenPageIDs.add(key.toString());
            }
            // Will write the mostPopularPages array into the context
            // The last time the reducer writes to the context will be when the array has the 10 most popular pages
            // context.write(key, result);
            context.write(1,mostPopularPages);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task B");
        job.setJarByClass(taskB.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}