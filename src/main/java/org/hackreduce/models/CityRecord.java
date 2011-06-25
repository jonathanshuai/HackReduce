package org.hackreduce.models;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.eclipse.jdt.internal.compiler.impl.StringConstant;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.lang.Integer;


/**
 * Parses a raw record (line of string data) from the geonames
 *
 */

public class CityRecord  implements Writable {

    public String geonameid         ; // 0 integer id of record in geonames database
    public String name            ; // 1name of geographical point (utf8) varchar(200)
    public String asciiname         ; // 2 name of geographical point in plain ascii characters, varchar(200)
    public String alternatenames    ; // 3 alternatenames, comma separated varchar(5000)
    public String latitude          ; // 4 latitude in decimal degrees (wgs84)
    public String longitude         ; // 5 longitude in decimal degrees (wgs84)
    public String feature_class     ; // 6 see http://www.geonames.org/export/codes.html, char(1)
    public String feature_code      ; // 7 see http://www.geonames.org/export/codes.html, varchar(10)
    public String country_code      ; // 8 ISO-3166 2-letter country code, 2 characters
    public String cc2               ; // 9 alternate country codes, comma separated, ISO-3166 2-letter country code, 60 characters
    public String admin1_code       ; // 10 fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
    public String admin2_code       ; // 11 code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
    public String admin3_code       ; // 12 code for third level administrative division, varchar(20)
    public String admin4_code       ; // 13 code for fourth level administrative division, varchar(20)
    public String population        ; // 14 bigint (8 byte int)
    public String elevation         ; // 15 in meters, integer
    public String gtopo30           ; // 16 average elevation of 30'x30' (ca 900mx900m) area in meters, integer
    public String timezone          ; // 17 the timezone id (see file timeZone.txt)
    public String modification_date; // 18 date of last modification in yyyy-MM-dd format

	public CityRecord(String inputString) throws IllegalArgumentException {
		// CSV header (parsing the inputString is based on this):
		// exchange, stock_symbol, date, stock_price_open, stock_price_high, stock_price_low,
		// 		stock_price_close, stock_volume, stock_price_adj_close
		String[] attributes = inputString.split("\t");

		if (attributes.length != 19)
			throw new IllegalArgumentException("Input string given did not have 18 values in CSV format, had " + attributes.length);

		try {
            geonameid = attributes[0];
            name = attributes[1];
            asciiname = attributes[2];
            alternatenames = attributes[3];
            latitude = attributes[4];
            longitude = attributes[5];
            feature_class = attributes[6];
            feature_code = attributes[7];
            country_code = attributes[8];
            cc2 = attributes[9];
            admin1_code = attributes[10];
            admin2_code = attributes[11];
            admin3_code = attributes[12];
            admin4_code = attributes[13];
            population = attributes[14];
            elevation = attributes[15];
            gtopo30 = attributes[16];
            timezone = attributes[17];
            modification_date = attributes[18];
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Input string contained an unknown number value that couldn't be parsed", e);
		}
	}

	public CityRecord(Text inputText) throws IllegalArgumentException {
		this(inputText.toString());
	}

	public boolean equals(Object o) {
		  
	    if (o instanceof CityRecord) {
	      CityRecord c = (CityRecord) o;
	      if (this.geonameid.equals(c.geonameid)) return true;
	    }
	    return false;
	    
	  }

   public Integer getPopulation(){
	   return new Integer(this.population);
   }


    public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeChars(geonameid);
            dataOutput.writeChars(name);
            dataOutput.writeChars(asciiname);
            dataOutput.writeChars(alternatenames);
            dataOutput.writeChars(latitude);
            dataOutput.writeChars(longitude);
            dataOutput.writeChars(feature_class);
            dataOutput.writeChars(feature_code);
            dataOutput.writeChars(country_code);
            dataOutput.writeChars(cc2);
            dataOutput.writeChars(admin1_code);
            dataOutput.writeChars(admin2_code);
            dataOutput.writeChars(admin3_code);
            dataOutput.writeChars(admin4_code);
            dataOutput.writeChars(population);
            dataOutput.writeChars(elevation);
            dataOutput.writeChars(gtopo30);
            dataOutput.writeChars(timezone);
            dataOutput.writeChars(modification_date);
    }

    public void readFields(DataInput dataInput) throws IOException {

    }
}
