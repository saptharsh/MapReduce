package hadoop.assignment1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Using in-memory join
public class ZipcodeMutualsQ3 extends Configured implements Tool {
	
	static String test = "";
	static HashMap<String, String> myMap;
	static HashSet<Integer> tempMap = new HashSet<Integer>();

	public static class FriendData extends Mapper<Text, Text, Text, Text> {

		String friendData;

		public void setup(Context context) throws IOException {
			
			Configuration config = context.getConfiguration();

			myMap = new HashMap<String, String>();
			String userdataPath = config.get("userdata");

			Path path = new Path(userdataPath);// Location of file in HDFS
			FileSystem fs = FileSystem.get(config);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			String ln;
			ln = br.readLine();
			while (ln != null) {
				String[] arr = ln.split(",");
				if (arr.length == 10) {
					String data = arr[1] + ":" + arr[6];
					myMap.put(arr[0].trim(), data);
				}
				ln = br.readLine();
			}

		}

		int count = 0;

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String ln = value.toString();
			String[] split = ln.split(",");

			if (null != myMap && !myMap.isEmpty()) {

				for (String str : split) {
					if (myMap.containsKey(str)) {

						// data=split[1]+split[3];
						friendData = myMap.get(str);
						myMap.remove(str);

						context.write(key, new Text(friendData));
					}
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Text myCount = new Text();
			String str = "";
			for (Text t : values) {
				if (str.equals(""))
					str = "[";
				str = str + t + ",";

			}
			str = str.substring(0, str.length() - 1);
			str = str + "]";
			myCount.set(str);
			context.write(key, myCount);
		}
	}

	public static void main(String args[]) throws Exception {
		// Standard Job setup procedure.
		int res = ToolRunner.run(new Configuration(), new ZipcodeMutualsQ3(),
				args);
		System.exit(res);

	}

	public int run(String[] otherArgs) throws Exception {
		
		Configuration conf = new Configuration();
		
		if (otherArgs.length != 6) {
			System.err
					.println("Usage: ZipcodeOfMutuals <User1> <User2> <input,SocNetData> <output> <userdata> <output>");
			System.exit(2);
		}

		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "TakingMutuals");
		job.setJarByClass(ZipcodeMutualsQ3.class);

		job.setMapperClass(MutualFriendsQ3.FriendsMapper.class);
		job.setReducerClass(MutualFriendsQ3.FriendsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		Path p = new Path(otherArgs[3]);
		FileOutputFormat.setOutputPath(job, p);

		int code = job.waitForCompletion(true) ? 0 : 1;

		Configuration conf1 = getConf();
		conf1.set("userdata", otherArgs[4]);
		
		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf1, "ZipcodeOfMutuals");
		job2.setJarByClass(ZipcodeMutualsQ3.class);
		
		job2.setInputFormatClass(KeyValueTextInputFormat.class);

		job2.setMapperClass(FriendData.class);
		job2.setReducerClass(Reduce.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, p);
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[5]));

		// Execute job and grab exit code
		code = job2.waitForCompletion(true) ? 0 : 1;

		FileSystem.get(conf).delete(p, true);
		System.exit(code);
		return code;
	}
}