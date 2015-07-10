package in.edureka.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MyValue implements WritableComparable {

	// Column1: Video id of 11 characters.
	Text videoId = new Text();

	// Column2: uploader of the video of string data type.
	Text uploader = new Text();

	// Column3: Interval between day of establishment of Youtube and the date of
	// uploading of the video of integer data type.
	IntWritable interval = new IntWritable(0);

	// Column4: Category of the video of String data type.
	Text category = new Text();

	// Column5: Length of the video of integer data type.
	IntWritable videoLength = new IntWritable(0);

	// Column6: Number of views for the video of integer data type.
	LongWritable numberOfViews = new LongWritable(0);

	// Column7: Rating on the video of float data type.
	FloatWritable rating = new FloatWritable(0);

	// Column8: Number of ratings given on the video.
	IntWritable numberOfRatings = new IntWritable(0);

	// Column9: Number of comments on the videos in integer data type.
	IntWritable numberOfComments = new IntWritable(0);

	// Column10: Related video ids with the uploaded video.
	List<Text> relatedVideoIds = new ArrayList<Text>();

	// Determine number of related Video Ids
	IntWritable numberOfRelatedVideoIds = new IntWritable(0);
	
	public MyValue() {
		// TODO Auto-generated constructor stub
	}

	// Getter Setter and ToStirng methods
	public IntWritable getNumberOfRelatedVideoIds() {
		return numberOfRelatedVideoIds;
	}

	public void setNumberOfRelatedVideoIds(IntWritable numberOfRelatedVideoIds) {
		this.numberOfRelatedVideoIds = numberOfRelatedVideoIds;
	}

	public Text getVideoId() {
		return videoId;
	}

	public void setVideoId(Text videoId) {
		this.videoId = videoId;
	}

	public Text getUploader() {
		return uploader;
	}

	public void setUploader(Text uploader) {
		this.uploader = uploader;
	}

	public IntWritable getInterval() {
		return interval;
	}

	public void setInterval(IntWritable interval) {
		this.interval = interval;
	}

	public Text getCategory() {
		return category;
	}

	public void setCategory(Text category) {
		this.category = category;
	}

	public IntWritable getVideoLength() {
		return videoLength;
	}

	public void setVideoLength(IntWritable videoLength) {
		this.videoLength = videoLength;
	}

	public LongWritable getNumberOfViews() {
		return numberOfViews;
	}

	public void setNumberOfViews(LongWritable numberOfViews) {
		this.numberOfViews = numberOfViews;
	}

	public FloatWritable getRating() {
		return rating;
	}

	public void setRating(FloatWritable rating) {
		this.rating = rating;
	}

	public IntWritable getNumberOfRatings() {
		return numberOfRatings;
	}

	public void setNumberOfRatings(IntWritable numberOfRatings) {
		this.numberOfRatings = numberOfRatings;
	}

	public IntWritable getNumberOfComments() {
		return numberOfComments;
	}

	public void setNumberOfComments(IntWritable numberOfComments) {
		this.numberOfComments = numberOfComments;
	}

	public List<Text> getRelatedVideoIds() {
		return relatedVideoIds;
	}

	public void setRelatedVideoIds(List<Text> relatedVideoIds) {
		this.relatedVideoIds = relatedVideoIds;
	}

	@Override
	public String toString() {
		return "videoId=" + videoId;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		videoId.readFields(in);
		uploader.readFields(in);
		interval.readFields(in);
		category.readFields(in);
		videoLength.readFields(in);
		numberOfViews.readFields(in);
		rating.readFields(in);
		numberOfRatings.readFields(in);
		numberOfComments.readFields(in);
		numberOfRelatedVideoIds.readFields(in);
		if (numberOfRelatedVideoIds.get() > 0) {
			for (int i = 0; i < numberOfRelatedVideoIds.get(); i++) {
				Text videoId = new Text();
				videoId.readFields(in);
				relatedVideoIds.add(videoId);
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		videoId.write(out);
		uploader.write(out);
		interval.write(out);
		category.write(out);
		videoLength.write(out);
		numberOfViews.write(out);
		rating.write(out);
		numberOfRatings.write(out);
		numberOfComments.write(out);
		numberOfRelatedVideoIds.write(out);
		if (numberOfRelatedVideoIds.get() > 0) {
			for (int i = 0; i < numberOfRelatedVideoIds.get(); i++) {
				relatedVideoIds.get(i).write(out);
			}
		}
	}

	@Override
	public int compareTo(Object o) {
		MyValue other = (MyValue) o;
		int cmp = videoId.compareTo(other.getVideoId());
		if (cmp != 0) {
			return cmp;
		}
		cmp = numberOfViews.compareTo(other.getNumberOfViews());
		if (cmp != 0) {
			return cmp;
		}
		return rating.compareTo(other.rating);
	}

}
