package hadoop.assignment1.Q4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserAvgFrndsAgeMapr31
        extends Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * Key - ignore
         * Value - user and average age friends
         */
        String[] AgeSplit = value.toString().split("\t");
        context.write(
                new Text(AgeSplit[0]),
                new Text("avgAge#"+AgeSplit[1])
        );
    }
}
