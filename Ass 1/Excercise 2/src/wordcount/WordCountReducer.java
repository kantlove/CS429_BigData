package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		
		System.out.println("===================\nOutput of Reducer phase\n===================");
		System.out.println("key=" + key);
		for(IntWritable value : values) {
			sum += value.get();
			System.out.println(value);
		}
		
		context.write(key, new IntWritable(sum));
		cleanup(context);
	}

}
