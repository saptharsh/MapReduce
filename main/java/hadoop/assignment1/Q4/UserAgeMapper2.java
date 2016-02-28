package hadoop.assignment1.Q4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserAgeMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text userToAge, Context context) throws IOException, InterruptedException {
        /**
         * Key - ignore the key
         * Value - (user id, friendsAge)
         */
        String[] AgeSplit = userToAge.toString().split("\t");
        context.write(
                new Text(AgeSplit[0]),
                new Text(AgeSplit[1])
        );
    }
}
