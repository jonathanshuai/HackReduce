package org.hackreduce.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.ModelMapper;

import java.io.IOException;


/**
 * This MapReduce job to process
 *
 */
public class LoceratureStageOneJob extends Configured implements Tool {

	public enum RecordCounterCount {
		UNIQUE_KEYS
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new LoceratureStageOneJob(), args);
		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

        if (args.length != 0) {
        	System.err.println("Not sure what you think you're passing.");
        	System.exit(2);
        }

        // Creating the MapReduce job (configuration) object
        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        // Tell the job which Mapper and Reducer to use (classes defined above)
        //job.setMapperClass();      // Will be python script
		job.setReducerClass(LargestPopulationReduce.class);

        job.setInputFormatClass(TextInputFormat.class);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);    // Streaming can only output Text

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);           // Since The distributedCache is being saved to a file

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));        // Hardcoding sucks if someone is trying to test

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path("/tmp/locerature_stage1");
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

        new DistributedCache.addCacheFile("/datasets/geonames/cities15000/cities15000.txt", job);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
