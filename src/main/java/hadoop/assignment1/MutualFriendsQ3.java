package hadoop.assignment1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MutualFriendsQ3 extends Configured implements Tool {

	public static class FriendsMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		Long userOne = new Long(-1L);
		Long userTwo = new Long(-1L);
		Long mainUser = new Long(-1L);
		Long temp = new Long(-1L);
		String user1 = "";
		String user2 = "";

		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			user1 = config.get("userA");
			user2 = config.get("userB");
			userOne = Long.parseLong(user1);
			userTwo = Long.parseLong(user2);
		}

		int count = 0;
		
		private Text friends = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] split = line.split("\t");

			String subject = split[0];
			mainUser = Long.parseLong(subject);
			
			if (split.length == 2) {
				String others = split[1];

				if ((mainUser.equals(userOne))
						|| (mainUser.equals(userTwo))) {
					
					friends.set(others);
					if (mainUser.equals(userOne))
						temp = userTwo;
					else
						temp = userOne;
					
					UserPageWritable data = null;
					String harami = "";
					
					if (mainUser.compareTo(temp) < 0) {

						data = new UserPageWritable(mainUser, temp);
						harami = data.toString();
						context.write(new Text(harami), friends);

					} else {
						data = new UserPageWritable(temp, mainUser);
						harami = data.toString();
						context.write(new Text(harami), friends);
					}
				}

			}
		}
	}

	public static class FriendsReducer extends Reducer<Text, Text, Text, Text> {
		
		HashMap<String, Integer> hash = new HashMap<String, Integer>();

		/*
		 *  intersection() Calculates the intersection of two given Strings,
		 *   i.e. friends lists
		 */
		private HashSet<Integer> intersection(String s1, String s2) {

			HashSet<Integer> h1 = new HashSet<Integer>();
			HashSet<Integer> h2 = new HashSet<Integer>();
			
			if (null != s1) {
				String[] s = s1.split(",");
				for (int i = 0; i < s.length; i++) {
					h1.add(Integer.parseInt(s[i]));
				}
			}

			if (null != s2) {
				String[] sa = s2.split(",");
				for (int i = 0; i < sa.length; i++) {
					if (h1.contains(Integer.parseInt(sa[i]))) {
						h2.add(Integer.parseInt(sa[i]));
					}
				}
			}

			return h2;

		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			String[] combined = new String[2];
			int cur = 0;
			for (Text value : values) {
				combined[cur++] = value.toString();
			}

			if (null != combined[0]) {
				combined[0] = combined[0].replaceAll("[^0-9,]", "");

			}
			if (null != combined[1]) {
				combined[1] = combined[1].replaceAll("[^0-9,]", "");
			}

			HashSet<Integer> ca = intersection(combined[0], combined[1]);

			context.write(new Text(key.toString()),
					new Text(StringUtils.join(",", ca)));

		}
	}

	public static class UserPageWritable implements
			WritableComparable<UserPageWritable> {

		private Long userId;
		private Long friendId;

		public UserPageWritable(Long user, Long friend1) {
			
			this.userId = user;
			this.friendId = friend1;
		}

		public UserPageWritable() {
		}

		public void readFields(DataInput in) throws IOException {
			userId = in.readLong();
			friendId = in.readLong();
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(userId);
			;
			out.writeLong(friendId);
			;
		}

		public int compareTo(UserPageWritable o) {
			

			int result = userId.compareTo(o.userId);
			if (result != 0) {
				return result;
			}
			return this.friendId.compareTo(o.friendId);
			
		}

		@Override
		public String toString() {
			return userId.toString() + ":" + friendId.toString();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final UserPageWritable other = (UserPageWritable) obj;
			if (this.userId != other.userId
					&& (this.userId == null || !this.userId
							.equals(other.userId))) {
				return false;
			}
			if (this.friendId != other.friendId
					&& (this.friendId == null || !this.friendId
							.equals(other.friendId))) {
				return false;
			}
			return true;
		}

		@Override
		public int hashCode() {
			return this.userId.hashCode() * 163 + this.friendId.hashCode();
		}

	}

	public static void main(String args[]) throws Exception {
		// Standard Job setup procedure.
		int res = ToolRunner.run(new Configuration(), new MutualFriendsQ3(), args);
		System.exit(res);

	}

	public int run(String[] otherArgs) throws Exception {
		
		Configuration conf = new Configuration();

		if (otherArgs.length != 4) {
			System.err
					.println("Usage: No error should occur here, run ZipcodeMutualQ3");
			System.exit(2);
		}

		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);

		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "MutualFriendsOf2Users");

		job.setJarByClass(MutualFriendsQ3.class);

		job.setMapperClass(FriendsMapper.class);
		job.setReducerClass(FriendsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		boolean sucess = job.waitForCompletion(true);
		return (sucess ? 0 : 1);
		// return 0;
	}
}