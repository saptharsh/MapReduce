package hadoop.assignment1.Q4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserdataToUsrAddr32
        extends Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text userDetails, Context context) throws IOException, InterruptedException {
        /**
         * Key - ignore
         * Value - Example 
         * 4,Angeline,Taylor,3496 Roger Street,Loveland,Ohio,45240,USA,Unfue1496,11/22/1993
         */
        String[] userdataSplit = userDetails.toString().split(",");
        context.write(
                new Text(userdataSplit[0]),
                new Text("address#"+
                userdataSplit[3] + ", " +
                userdataSplit[4] + ", " +
                userdataSplit[5])
        );
    }
}
