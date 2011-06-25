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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class Stage2Reducer extends Reducer<Text, CityYearRecord, Text,CityYearRecord> {

	 
	 
	   public void reduce(Text key, Iterable<CityYearRecord> values, 
	                      Context context) throws IOException,InterruptedException {

                      Map<String, CityYearRecord> records = new HashMap<String, CityYearRecord>();

	     for (CityYearRecord val : values) {
             context.write(key, val);
         }
//
//           Map<String, CityYearRecord> records = new HashMap<String, CityYearRecord>();
//
//	     for (CityYearRecord val : values) {
//             String cityName = val.getCityRecord().name;
//             CityYearRecord previous = records.get(cityName);
//             if( previous == null || previous.getCityRecord().getPopulation() < val.getCityRecord().getPopulation()) {
//                 System.out.println("*************** Adding " + cityName + " for " + key);
//                 records.put(cityName,  val);
//             }
//	     }
//
//           System.out.println(" key size" + records.keySet().size());
//           List<CityYearRecord> list = new ArrayList<CityYearRecord>(records.values());
//         for(CityYearRecord someCity: list) {
//             //CityYearRecord someCity = records.get(cityName);
//             System.out.println("*************** Writing " + someCity.getCityRecord().name + " for " + key);
//             context.write(key, someCity);
//         }
	     
	   }

}
