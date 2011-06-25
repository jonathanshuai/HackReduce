package org.hackreduce.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.mappers.BixiMapper;
import org.hackreduce.models.BixiRecord;
import org.hackreduce.models.CityYearRecord;
import org.hackreduce.models.CityRecord;
import java.io.IOException;
import java.math.*;

/**
 *
 */
public class Stage2Reducer extends Reducer<Text, CityYearRecord, Text,CityYearRecord> {

	 
	 
	   public void reduce(Text key, Iterable<CityYearRecord> values, 
	                      Context context) throws IOException,InterruptedException {
	     int sum = 0;
	     Integer maxPop = 0;
	     Integer currentPop;
	     CityRecord maxCity = null;
	     CityRecord currentCity = null;
	     for (CityYearRecord val : values) {
	    	 currentCity = val.getCityRecord();
	    	 currentPop = currentCity.getPopulation();
	    	 if (currentPop > maxPop){
	    		 maxPop = currentPop;
	    		 maxCity = currentCity;
	    		 sum = val.getCount();
	    	 }
	    	 else if (currentCity.equals(maxCity)){
	    		 sum += val.getCount();
	    	 }
	     }

	     CityYearRecord result = new CityYearRecord(maxCity,key.toString(),sum) ;
	     
	     context.write(key, result);
	     
	     
	   }

}
