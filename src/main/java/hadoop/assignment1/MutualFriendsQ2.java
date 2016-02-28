package hadoop.assignment1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Logger;

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

public class MutualFriendsQ2 extends Configured implements Tool {

	public static class MututalFriendsMapper extends
			Mapper<LongWritable, Text, UsrPageWritable, Text> {
		
		LongWritable userA = new LongWritable();
		LongWritable userB = new LongWritable();
		LongWritable user = new LongWritable();
		Long one = new Long(-1L);
		Long two = new Long(-1L);
		Long three = new Long(-1L);
		Long temp = new Long(-1L);
		String user1 = "";
		String user2 = "";

		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			user1 = config.get("userA");
			user2 = config.get("userB");
		
			one = Long.parseLong(user1);
			two = Long.parseLong(user2);
		}

		int count = 0;
		private Text friends = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] split = line.split("\t");
			
			@SuppressWarnings("deprecation")
			final Logger log = Logger.global;

			String subject = split[0];
			
			// User IDs on the left hand side which is compared with user1 and user2
			three = Long.parseLong(subject);
			
			if (split.length == 2) {
				
				String others = split[1];
				log.info("Inside the loop" + user);
				
				if ((three.equals(one)) || (three.equals(two))) {
					
					friends.set(others);
					if (three.equals(one))
						temp = two;
					else
						temp = one;
					if (three.compareTo(temp) < 0) {
						context.write(new UsrPageWritable(three, temp),
								friends);

					} else {
						context.write(new UsrPageWritable(temp, three),
								friends);
					}
				}
			}
		}
	}

	public static class MututalFriendsReducer extends
			Reducer<UsrPageWritable, Text, UsrPageWritable, Text> {
		
		HashMap<String, Integer> hash = new HashMap<String, Integer>();

		/*
		 * intersection() Calculates intersection of two given Strings, 
		 * i.e. friends lists
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

		public void reduce(UsrPageWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			String[] combine = new String[2];
			int cur = 0;
			for (Text value : values) {
				combine[cur++] = value.toString();
			}
		
			combine[0] = combine[0].replaceAll("[^0-9,]", "");
			if (null != combine[1])
				combine[1] = combine[1].replaceAll("[^0-9,]", "");
			
			HashSet<Integer> ca = intersection(combine[0], combine[1]);

			context.write(key, new Text(StringUtils.join(",", ca)));

		}
	}

	public static class UsrPageWritable implements
			WritableComparable<UsrPageWritable> {

		private Long userId;
		private Long friendId;

		public UsrPageWritable(Long userid, Long friendid) {
			
			this.userId = userid;
			this.friendId = friendid;
		}

		public UsrPageWritable() {
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

		public int compareTo(UsrPageWritable o) {
			

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
			final UsrPageWritable other = (UsrPageWritable) obj;
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

	}/* End of UserPageWritable Class */

	public static void main(String args[]) throws Exception {
		// Standard Job setup procedure.
		int res = ToolRunner.run(new Configuration(), new MutualFriendsQ2(),
				args);
		System.exit(res);

	}

	public int run(String[] otherArgs) throws Exception {
		
		Configuration conf = new Configuration();

		if (otherArgs.length != 4) {
			System.err
					.println("Usage: MutualFriendsQ2 <User1> <User2> </input/> </output/>");
			System.exit(2);
		}

		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);

		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "MutualFriends");
		job.setJarByClass(MutualFriendsQ2.class);

		job.setMapperClass(MututalFriendsMapper.class);
		job.setReducerClass(MututalFriendsReducer.class);

		job.setOutputKeyClass(UsrPageWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		boolean sucess = job.waitForCompletion(true);
		return (sucess ? 0 : 1);
		// return 0;
	}
}