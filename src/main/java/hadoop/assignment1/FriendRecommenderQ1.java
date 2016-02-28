package hadoop.assignment1;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FriendRecommenderQ1 {

	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		
		String h;
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] userFriends = line.split("\t");
			if (userFriends.length > 1) {
				String user = userFriends[0];
				IntWritable userKey = new IntWritable(Integer.parseInt(user));
				
				String[] friends = userFriends[1].split(",");
				String friend1;
				IntWritable friend1Key = new IntWritable();
				Text friend1Value = new Text();
				
				String friend2;
				IntWritable friend2Key = new IntWritable();
				Text friend2Value = new Text();
				
				for (int i = 0; i < friends.length; i++) {
					friend1 = friends[i];
					friend1Value.set("1," + friend1);
					context.write(userKey, friend1Value); // Paths of length 1.
					
					friend1Key.set(Integer.parseInt(friend1));
					friend1Value.set("2," + friend1);
					
					for (int j = i + 1; j < friends.length; j++) {
						friend2 = friends[j];
						friend2Key.set(Integer.parseInt(friend2));
						friend2Value.set("2," + friend2);
						context.write(friend1Key, friend2Value); // Paths of
																	// length 2.
						context.write(friend2Key, friend1Value); // Paths of
																	// length 2.
					}
				}
			}
		}
	}

	public static class Reduce extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			String[] str;
			HashMap<String, Integer> hash = new HashMap<String, Integer>();
			
			for (Text val : values) {
				str = (val.toString()).split(",");
				
				if (str[0].equals("1")) { // Paths of length 1.
					hash.put(str[1], -1);
				} else if (str[0].equals("2")) { // Paths of length 2.
					if (hash.containsKey(str[1])) {
						if (hash.get(str[1]) != -1) {
							hash.put(str[1], hash.get(str[1]) + 1);
						}
					} else {
						hash.put(str[1], 1);
					}
				}
			}
			
			// Converts hash to list and removes paths of length 1.
			ArrayList<Entry<String, Integer>> arlist = new ArrayList<Entry<String, Integer>>();
			for (Entry<String, Integer> entry : hash.entrySet()) {
				if (entry.getValue() != -1) { // Neglect paths of length 1.
					arlist.add(entry);
				}
			}
			
			// Sort key-value pairs in the list by values
			Collections.sort(arlist, new Comparator<Entry<String, Integer>>() {
				public int compare(Entry<String, Integer> e1,
						Entry<String, Integer> e2) {
					return e2.getValue().compareTo(e1.getValue());
				}
			});
			
			int RECOMMENDATION_COUNT = 10;
			if (RECOMMENDATION_COUNT < 1) {
				// Output all the key-value pairs in the list.
				context.write(key, new Text(StringUtils.join(arlist, ",")));
			} else {
				// Output at most RECOMMENDATION_COUNT keys 
				ArrayList<String> top = new ArrayList<String>();
				for (int i = 0; i < Math.min(RECOMMENDATION_COUNT,
						arlist.size()); i++) {
					top.add(arlist.get(i).getKey());
				}
				context.write(key, new Text(StringUtils.join(top, ",")));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "FriendshipRecommender");
		job.setJarByClass(FriendRecommenderQ1.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}