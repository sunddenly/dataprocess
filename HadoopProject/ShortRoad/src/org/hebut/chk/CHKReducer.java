package org.hebut.chk;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
public class CHKReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
	public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
		int sum = 0;
		for (IntWritable tempint: values){
			sum += tempint.get();
		}
		context.write(new IntWritable(1), new IntWritable(sum));
	}
}