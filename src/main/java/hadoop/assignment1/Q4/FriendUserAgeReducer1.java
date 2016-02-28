package hadoop.assignment1.Q4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FriendUserAgeReducer1 extends Reducer<Text, Text, Text, Text> {

	/**
	 * key - userId values - any of the below two 
	 * (id#user'sFriendId)
	 * (age#ageOfUser)
	 */

	public void reduce(Text friendId, Iterable<Text> FriendIdOAge,
			Context context) throws IOException, InterruptedException {

		String age = null;
		List<String> friendsList = new ArrayList<>();
		Iterator<Text> Iterator = FriendIdOAge
				.iterator();
		while (Iterator.hasNext()) {
			String[] usersFriendIdOrAgeSplit = Iterator
					.next().toString().split("#");
			if (usersFriendIdOrAgeSplit[0].equals("age")) {
				age = usersFriendIdOrAgeSplit[1];
			} else {
				friendsList.add(usersFriendIdOrAgeSplit[1]);
			}
		}
		// Age of this user was not found in user_details.txt
		for (String usersFriendId : friendsList) {
			context.write(new Text(usersFriendId), new Text(age));
		}
	}
}
