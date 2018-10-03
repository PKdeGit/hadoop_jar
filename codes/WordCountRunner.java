import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class WordCountMapper extends Mapper<LongWritable, Text,Text,IntWritable> {
	@Override
	protected void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
		String line=value.toString();
		String[] words=line.split(" ");
		for(String word:words) {
			context.write(new Text(word), new IntWritable(1));
		}
	}

}

class WordCountReducer extends Reducer<Text,IntWritable, Text,IntWritable>{
	@Override
	protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
		//定义计数器
		int count=0;
		for(IntWritable value:values) {
			count+=value.get();
		}
		context.write(key, new IntWritable(count));
	}
}


public class WordCountRunner {

	public static void main(String[] args)throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job wcJob =Job.getInstance(conf);
		wcJob.setJar("wc.jar");
		wcJob.setMapperClass(WordCountMapper.class);
		wcJob.setReducerClass(WordCountReducer.class);
		
		wcJob.setMapOutputKeyClass(Text.class); 
		wcJob.setMapOutputValueClass(IntWritable.class); 
		
		wcJob.setOutputKeyClass(Text.class); 
		wcJob.setOutputValueClass(IntWritable.class); 
		FileInputFormat.setInputPaths( wcJob,"hdfs://hdp-node-01:9000/input/test"); 
		FileOutputFormat.setOutputPath(wcJob, new Path("hdfs://hdp-node-01:9000/output/")); 
		
		boolean res = wcJob.waitForCompletion(true); 
		System.exit(res?0:1); 
	}
}

