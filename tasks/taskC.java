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

// Iterate through the FaceIn.csv file and keep track of the nationality
// The nationality should be fed into the mapper along with the IntWritable
// The reducer will be able to determine how many times each nationality appeared in FaceIn.csv, which shows how many citizens have a page

public class taskC {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static Text output = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] csvLine;
            String delimiter = ",";
            csvLine = value.toString().split(delimiter);
            // Push the country code into the output with IntWritable one
            output.set(csvLine[3]);
            context.write(output, one);
        }



    }

}