import java.io.IOException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TaskG {
    public static class FaceInMapper extends Mapper<Object, Text, Text, Text> {
        private Text outkey = new Text();
        private Text outvalue = new Text();
        public void map (Object key, Text value, Context context)
                throws IOException, InterruptedException{

                String[] parts = value.toString().split(",");
                    outkey.set(parts[0]);
                    outvalue.set("Name\t" + parts[1]+"\t"+parts[0]); //concanate the name and id in the the value section
                    context.write(outkey, outvalue);
        }
    }

    public static class AccessMapper extends Mapper<Object, Text, Text, Text> {
        private Text perId = new Text();
        private Text accessID = new Text();
        private SimpleDateFormat dateFormat = new SimpleDateFormat("M/dd/yyyy HH:mm"); //  date format
        private long currentDateTime;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            currentDateTime = System.currentTimeMillis(); // Set the current date time in milliseconds
        }
        public void map (Object key, Text value, Context context)
                throws IOException, InterruptedException{
            Date today= new Date();

                String[] parts = value.toString().split(",");

                if (parts.length >= 2) {
                    String accessDateStr = parts[4].trim(); // get accesstime
                    Date accessDate = null;
                    try {
                        accessDate = dateFormat.parse(accessDateStr); // parse date value
                        long accessTimeMillis = accessDate.getTime(); // assign current date time to variable
                        long dayDifference = (currentDateTime - accessTimeMillis) / (24L * 60L * 60L * 1000L); //get difference of current date and access date

                        if (dayDifference <= 90) {   // fetch the users who accessed platform within last 90days
                            perId.set(parts[1]); // Set personId - ByWho
                            accessID.set("AccessCount\t" + parts[0]); // concanate AccessCount and access id as value
                            context.write(perId, accessID);
                        }
                    } catch (ParseException e) {
                        System.err.println("Error parsing date: " + accessDateStr);
                    }
                }

        }

    }
    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text>{
        private Text pName = new Text();
        private Text perid= new Text();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            String name = null;
            String pid=null;
            int countaccess=0;
            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                String joinkey = parts[0];
                String dataValue = parts[1];
                if (joinkey.equals("AccessCount"))//(parts.length == 2) // if values from Accessmapper
                {
                        countaccess++;
                }
                else if (joinkey.equals("Name")) // if values from FaceIn mapper
                {
                        String dataValue1 = parts[2];
                        name = dataValue;
                        pid = dataValue1;
                }
            }
            if (name != null  & countaccess==0) // if the user doesn't have any access within last 90 days
            {
                pName.set(name);
                perid.set(pid);
                context.write(perid,pName);
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf= new Configuration();
        Job job=new Job(conf,"Task G");
        job.setJarByClass(TaskG.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/FaceIn.csv"),TextInputFormat.class, FaceInMapper.class);
        MultipleInputs.addInputPath(job, new Path("C:///Users/ganer/Documents/classes2023/fall/Big_data/access.csv"),TextInputFormat.class, AccessMapper.class);
        Path outputPath= new Path("hdfs://localhost:9000/project1/TaskG.txt");

        FileOutputFormat.setOutputPath(job,outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
