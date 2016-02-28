package hadoop.assignment1.Q4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class UserFriendsAgeReducer2 extends Reducer<Text, Text, Text, Text> {

	/**
     * key - userId
     * Values - List of ages of user's friends
     */
    public void reduce(Text userId, Iterable<Text> userFriendsAgeIterable, Context context)
            throws IOException, InterruptedException {
        
        int sumAges=0, directFriends=0;
        Iterator<Text> userFriendsAge = userFriendsAgeIterable.iterator();
        while (userFriendsAge.hasNext()){
            sumAges += Integer.parseInt(userFriendsAge.next().toString());
            directFriends += 1;
        }
        if(directFriends > 0){
            context.write(
                    userId,
                    new Text(String.valueOf(sumAges/directFriends))
            );
        }
    }
}
