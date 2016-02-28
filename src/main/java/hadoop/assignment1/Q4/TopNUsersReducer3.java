package hadoop.assignment1.Q4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


public class TopNUsersReducer3
        extends Reducer<Text, Text, Text, Text>{

    TreeMap<Integer, List<String>> avgFriendsAgeNAddress =
            new TreeMap<>();
    int TOP_N_USERS=20;

    public void reduce(Text userId, Iterable<Text> avgFriendsAgeOrAddressIterable, Context context){

        String address=null;
        int avgFriendsAge=0;
        Iterator<Text> Iterator = avgFriendsAgeOrAddressIterable.iterator();
        while (Iterator.hasNext()){
            String[] avgFrndsAgeOrAddressSplit = Iterator.next().toString().split("#");
            if(avgFrndsAgeOrAddressSplit[0].equals("address")){
                address = avgFrndsAgeOrAddressSplit[1];
            } else {
                avgFriendsAge = Integer.parseInt(avgFrndsAgeOrAddressSplit[1]);
            }
        }
        if(avgFriendsAgeNAddress.containsKey(avgFriendsAge)){
        	avgFriendsAgeNAddress.get(avgFriendsAge).add("id#"+userId+"#address"+address);
        } else {
            List<String> userAddress = new ArrayList<>();
            userAddress.add("id#"+userId+"#address"+address);
            avgFriendsAgeNAddress.put(
                    avgFriendsAge,
                    userAddress
            );
        }
    }


    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException
    {
        int foundTopN =0;
        for(Map.Entry<Integer, List<String>> entryMap : avgFriendsAgeNAddress.descendingMap().entrySet()){
            for(String userAddress : entryMap.getValue()){
                if(foundTopN < TOP_N_USERS){
                    String[] userAddressSplit = userAddress.split("#");
                    context.write(
                            new Text(userAddressSplit[1]),
                            new Text(
                                    String.valueOf(entryMap.getKey()) +", " +
                                    userAddressSplit[2]
                            )
                    );
                    foundTopN++;
                } else {
                    break;
                }
            }
        }
    }
}
