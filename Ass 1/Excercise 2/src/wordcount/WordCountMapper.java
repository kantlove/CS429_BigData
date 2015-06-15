package wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		Map<String, Integer> map = new HashMap<String, Integer>();
		
		String token;
		while(tokenizer.hasMoreElements()) {
			token = tokenizer.nextToken();
			System.out.print(token + " ");
			if(map.containsKey(token)) {
				map.put(token, map.get(token) + 1);
			}
			else {
				map.put(token, 1);
			}
		}
		
		System.out.println("\n===================\nOutput of Map phase\n===================");
		System.out.println("Map size = " + map.size());
		for(Map.Entry<String, Integer> entry : map.entrySet()) {
			String k = entry.getKey();
			Integer val = entry.getValue();
			word.set(k);
			IntWritable intVal = new IntWritable(val);
			context.write(word, intVal);
			
			System.out.println(k + " : " + val);
		}
		
		cleanup(context);
	}

}
