package org.hackreduce.mappers;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.record.Record;
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

import java.io.*;

import java.net.URI;
import java.util.HashMap;
import java.util.logging.Level;
import org.hackreduce.models.CityYearRecord;

/**
 */
public class Stage2Mapper extends Mapper<LongWritable, Text, Text, CityYearRecord> {

    private HashMap<String, CityRecord> joinData = new HashMap<String, CityRecord>();

    @Override
    public void setup(Context context) {
        try {
            URI citiesURI = new URI("/");

            FileSystem fileSystem= FileSystem.get(context.getConfiguration());

            Reader reader;
            Path citiesPath = new Path("/datasets/geonames/cities15000.txt");
            if( fileSystem.exists(citiesPath) ) {
                FSDataInputStream in = fileSystem.open(citiesPath);
                reader = new InputStreamReader(in);
            } else {
                reader = new FileReader("datasets/geonames/cities15000/cities15000.txt");
            }
            String line; String[] tokens;
            BufferedReader joinReader = new BufferedReader( reader);
            try {
                 while ((line = joinReader.readLine()) != null) {
                     CityRecord record = new CityRecord(line);
                     System.out.println("********** Reading in city " + record.name);
                     joinData.put( record.name, record);
                 }
            } catch(IOException e) {
                 System.err.println("Exception reading DistributedCache: " + e);
                throw new RuntimeException(e);
            } finally {
                 joinReader.close();
            }
         } catch(Exception e) {
             System.err.println("Distribute cache issue");
            throw new RuntimeException(e);
         }
    }

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            System.out.println("********** mapping ngram" + value.toString());

            OneGram gram = new OneGram(value);

            CityRecord cityRecord = joinData.get(gram.getGram1());

            if( cityRecord != null) {
                //context.getCounter(ModelMapperCount.RECORDS_MAPPED).increment(1);

                String year = Integer.toString( gram.getYear());

                CityYearRecord cityYearRecord = new CityYearRecord(cityRecord, year, gram.getMatchCount());
                context.write( new Text(year), cityYearRecord);
            }
		} catch (Exception e) {
			//LOG.log(Level.WARNING, e.getMessage(), e);
			//context.getCounter(ModelMapperCount.RECORDS_SKIPPED).increment(1);
		}

	}
}