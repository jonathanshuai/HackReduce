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
import java.util.HashMap;
import java.util.Map;

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
           Map<String, CityYearRecord> records = new HashMap<String, CityYearRecord>();

	     for (CityYearRecord val : values) {
             String cityName = val.getCityRecord().name;
             CityYearRecord previous = records.get(cityName);
             if( previous == null || previous.getCityRecord().getPopulation() < val.getCityRecord().getPopulation()) {
                 records.put(cityName,  val);
             }
	     }

         for(CityYearRecord record: records.values()) {
             context.write(key, record);
         }
	     
	   }

}
