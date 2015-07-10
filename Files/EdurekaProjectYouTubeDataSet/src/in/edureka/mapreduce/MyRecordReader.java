package in.edureka.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class MyRecordReader extends RecordReader<LongWritable, MyValue> {
	private LongWritable key;
	private MyValue value;
	private LineRecordReader reader = new LineRecordReader();

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		reader.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public MyValue getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		reader.initialize(arg0, arg1);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		boolean readerHasNextValue = reader.nextKeyValue();
		if (readerHasNextValue) {
			if (key == null) {
				key = new LongWritable();
			}
			if (value == null) {
				value = new MyValue();
			}
			Text line = (Text) reader.getCurrentValue();

			StringTokenizer tokenizer = new StringTokenizer(line.toString(),"\t");

			key = reader.getCurrentKey();
			// Column1: Video id of 11 characters.
			if (tokenizer.hasMoreTokens()) {
				value.setVideoId(new Text(tokenizer.nextToken().trim()));
			}
			// Column2: uploader of the video of string data type.
			if (tokenizer.hasMoreTokens()) {
				value.setUploader(new Text(tokenizer.nextToken().trim()));
			}
			// Column3: Interval between day of establishment of Youtube and the
			// date of
			// uploading of the video of integer data type.
			if (tokenizer.hasMoreTokens()) {
				value.setInterval(new IntWritable(Integer.parseInt(tokenizer
						.nextToken().trim())));
			}
			// Column4: Category of the video of String data type.
			if (tokenizer.hasMoreTokens()) {
				value.setCategory(new Text(tokenizer.nextToken().trim()));
			}
			// Column5: Length of the video of integer data type.
			if (tokenizer.hasMoreTokens()) {
				value.setVideoLength(new IntWritable(Integer.parseInt(tokenizer
						.nextToken().trim())));
			}
			// Column6: Number of views for the video of integer data type.
			if (tokenizer.hasMoreTokens()) {
				value.setNumberOfViews(new LongWritable(Long
						.parseLong(tokenizer.nextToken().trim())));
			}
			// Column7: Rating on the video of float data type.
			if (tokenizer.hasMoreTokens()) {
				value.setRating(new FloatWritable(Float.parseFloat(tokenizer
						.nextToken().trim())));
			}
			// Column8: Number of ratings given on the video.
			if (tokenizer.hasMoreTokens()) {
				value.setNumberOfRelatedVideoIds(new IntWritable(Integer
						.parseInt(tokenizer.nextToken().trim())));
			}
			// Column9: Number of comments on the videos in integer data type.
			if (tokenizer.hasMoreTokens()) {
				value.setNumberOfComments(new IntWritable(Integer
						.parseInt(tokenizer.nextToken().trim())));
			}
			int relatedVideoCount = 0;

			while (tokenizer.hasMoreTokens()) {
				relatedVideoCount++;
				value.getRelatedVideoIds().add(new Text(tokenizer.nextToken().trim()));
			}
			value.setNumberOfRelatedVideoIds(new IntWritable(relatedVideoCount));
		} else {
			key = null;
			value = null;
		}

		return readerHasNextValue;
	}

}
