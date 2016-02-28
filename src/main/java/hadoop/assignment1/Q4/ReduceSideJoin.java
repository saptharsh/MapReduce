package hadoop.assignment1.Q4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// Mapper-1 to emit (friend_id, user_id) - users friend list
// Mapper-2 to emit (user_id, age)  - users age list
// Reduce-1 to emit (user_id, age of all friends)
public class ReduceSideJoin {

    /**
     * args[0] - user_ids list
     * args[1] - user data
     * args[2] - reducer 1 output path
     * args[3] - reducer 2 output path
     * args[4] - reducer 3 output path
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        JobConf conf = new JobConf();
        
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "Join Reduceside");

        job.setJarByClass(ReduceSideJoin.class);
        
        MultipleInputs.addInputPath(
                job,
                new Path(args[0]),
                TextInputFormat.class,
                UserToFriendMapper11.class
        );
        MultipleInputs.addInputPath(
                job,
                new Path(args[1]),
                TextInputFormat.class,
                UserToAgeMapper12.class
        );

        job.setReducerClass(FriendUserAgeReducer1.class);

        // Both the mappers emit (text, text)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // If the job completes successfully, go for the next job
        if(job.waitForCompletion(true)){
        	
            @SuppressWarnings("deprecation")
			Job job2 = new Job(conf, "Average age of friends");
            job2.setJarByClass(ReduceSideJoin.class);
            
            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));

            job2.setMapperClass(UserAgeMapper2.class);
            job2.setReducerClass(UserFriendsAgeReducer2.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            if(job2.waitForCompletion(true)){
                // Proceed to job 3 to get user address
            	
                @SuppressWarnings("deprecation")
				Job job3 = new Job(conf, "Getting Top 20 users");
                job3.setJarByClass(ReduceSideJoin.class);
                
                MultipleInputs.addInputPath(job3,
                        new Path(args[3]),
                        TextInputFormat.class,
                        UserAvgFrndsAgeMapr31.class);

                MultipleInputs.addInputPath(
                        job3,
                        new Path(args[1]),
                        TextInputFormat.class,
                        UserdataToUsrAddr32.class
                );
                FileOutputFormat.setOutputPath(job3, new Path(args[4]));

                job3.setReducerClass(TopNUsersReducer3.class);

                job3.setMapOutputKeyClass(Text.class);
                job3.setMapOutputValueClass(Text.class);

                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(Text.class);

                job3.waitForCompletion(true);
            }
        }
    }
}