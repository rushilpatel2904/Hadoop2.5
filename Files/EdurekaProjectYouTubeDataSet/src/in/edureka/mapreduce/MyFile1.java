package in.edureka.mapreduce;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.examples.MultiFileWordCount.MyInputFormat;
import org.apache.hadoop.fs.Path;


public class MyFile1 {
	
	public static class Map extends Mapper<LongWritable,MyValue,Text,IntWritable>{

		public void map(LongWritable key, MyValue value,
				Context context)
				throws IOException,InterruptedException {
			context.write(value.getCategory(),new IntWritable(1));
		}
		
	}
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException,InterruptedException {
			int sum=0;
			// TODO Auto-generated method stub
			for(IntWritable x: values)
			{
				sum+=x.get();
			}
			context.write(key, new IntWritable(sum));
			
		}
		
	}
	

	
	public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException{
	    if (args.length != 2) {
	        System.err.println("Usage: <input path> <output path>");
	        System.exit(-1);
	    }
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		
		
		job.setJarByClass(MyFile1.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(YouTubeInputFileFormat.class);
		
		//job.setCombinerClass(ReducerCustom.class);
		//job.setPartitionerClass(TextPartitioner.class);
		//job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
