package org.hackreduce.mappers;

import org.apache.hadoop.mapreduce.Mapper;
import org.hackreduce.mappers.ngram.OneGramMapper;
import org.hackreduce.models.CityRecord;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.hackreduce.models.ngram.OneGram;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.HashMap;
import java.util.logging.Level;
import org.hackreduce.models.CityYearRecord;

/**
 */
public class Stage2Mapper extends Mapper<LongWritable, Text, String, CityYearRecord> {

    private HashMap<String, CityRecord> joinData = new HashMap<String, CityRecord>();

    @Override
    public void configure(Configuration conf) {
        try {
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
            Path cacheFile = cacheFiles[0];
            String line; String[] tokens;
            BufferedReader joinReader = new BufferedReader( new FileReader(cacheFile.toString()));
             try {
                 while ((line = joinReader.readLine()) != null) {
                     CityRecord record = new CityRecord(line);
                     joinData.put( record.name, record);
                 }
             } catch(IOException e) {
                 System.err.println("Exception reading DistributedCache: " + e);
             } finally {
                 joinReader.close();
             }
         } catch(Exception e) {
             System.err.println("Distribute cache issue");
         }
    }

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            OneGram gram = new OneGram(value);

            CityRecord cityRecord = joinData.get(gram.getGram1());

            if( cityRecord != null) {
                //context.getCounter(ModelMapperCount.RECORDS_MAPPED).increment(1);

                String year = Integer.toString( gram.getYear());

                CityYearRecord cityYearRecord = new CityYearRecord(cityRecord, gram);
                context.write(year, cityYearRecord);
            }
		} catch (Exception e) {
			//LOG.log(Level.WARNING, e.getMessage(), e);
			//context.getCounter(ModelMapperCount.RECORDS_SKIPPED).increment(1);
		}

	}
}