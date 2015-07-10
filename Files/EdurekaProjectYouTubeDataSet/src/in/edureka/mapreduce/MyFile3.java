package in.edureka.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.examples.MultiFileWordCount.MyInputFormat;
import org.apache.hadoop.fs.Path;

public class MyFile3 {

	public static class Map extends
			Mapper<LongWritable, MyValue, MyValue, LongWritable> {

		public void map(LongWritable key, MyValue value, Context context)
				throws IOException, InterruptedException {
			context.write(value, value.getNumberOfViews());
		}

	}

	public static class Reduce extends
			Reducer<MyValue, LongWritable, MyValue, LongWritable> {

		public void reduce(MyValue key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			for (LongWritable x : values) {
				context.write(key, x);
			}

		}

	}

	public static class ViewCountKeyComparator extends WritableComparator {

		protected ViewCountKeyComparator() {
			super(MyValue.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			MyValue k1 = (MyValue) w1;
			MyValue k2 = (MyValue) w2;
			return -1 * k1.getNumberOfViews().compareTo(k2.getNumberOfViews());
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: <input path> <output path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf);

		job.setJarByClass(MyFile3.class);
		job.setMapperClass(Map.class);
		job.setSortComparatorClass(ViewCountKeyComparator.class);
		//job.setReducerClass(Reduce.class);
		job.setInputFormatClass(YouTubeInputFileFormat.class);

		// job.setCombinerClass(ReducerCustom.class);
		// job.setPartitionerClass(TextPartitioner.class);
		// job.setNumReduceTasks(0);

		job.setOutputKeyClass(MyValue.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
