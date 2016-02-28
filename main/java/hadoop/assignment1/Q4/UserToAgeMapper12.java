package hadoop.assignment1.Q4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserToAgeMapper12 extends Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text userDetails, Context context) throws IOException, InterruptedException {
        /**
         * Key - ignore
         * Value - Example 
         * 4,Angeline,Taylor,3496 Roger Street,Loveland,Ohio,45240,USA,Unfue1496,11/22/1993
         */
        String[] userdataSplit = userDetails.toString().split(",");
        String userId = userdataSplit[0];
        String DOB = userdataSplit[9];
        Integer age = 2016 - Integer.parseInt(DOB.split("/")[2]);
        context.write(
                new Text(userId),
                new Text("age#"+String.valueOf(age))
        );
    }
}
