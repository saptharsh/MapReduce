package hadoop.assignment1.Q4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserToFriendMapper11 extends Mapper<LongWritable, Text, Text, Text>{

	/**
     * Key - ignore
     * Value - (user, friend)
     */
    public void map (LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String[] usrFriends = value.toString().split("\t");
        if(usrFriends.length != 2){
            // Invalid line
            return;
        }
        // Get the user id
        String userId = usrFriends[0];
        // Get list of friends for this user. Trim for any spaces
        String[] friends = usrFriends[1].split(",");
        for(int index=0; index < friends.length; index++){
            friends[index] = friends[index].trim();
        }
        for(String friendId : friends){
            context.write(
                    new Text(friendId),
                    new Text("id#"+userId)
            );
        }
    }
}
