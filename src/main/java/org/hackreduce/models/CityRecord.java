package org.hackreduce.models;

import org.apache.hadoop.io.Text;
import org.eclipse.jdt.internal.compiler.impl.StringConstant;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.lang.Integer;


/**
 * Parses a raw record (line of string data) from the geonames
 *
 */

public class CityRecord {

    String geonameid         ; // 0 integer id of record in geonames database
    String name            ; // 1name of geographical point (utf8) varchar(200)
    String asciiname         ; // 2 name of geographical point in plain ascii characters, varchar(200)
    String alternatenames    ; // 3 alternatenames, comma separated varchar(5000)
    String latitude          ; // 4 latitude in decimal degrees (wgs84)
    String longitude         ; // 5 longitude in decimal degrees (wgs84)
    String feature_class     ; // 6 see http://www.geonames.org/export/codes.html, char(1)
    String feature_code      ; // 7 see http://www.geonames.org/export/codes.html, varchar(10)
    String country_code      ; // 8 ISO-3166 2-letter country code, 2 characters
    String cc2               ; // 9 alternate country codes, comma separated, ISO-3166 2-letter country code, 60 characters
    String admin1_code       ; // 10 fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
    String admin2_code       ; // 11 code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
    String admin3_code       ; // 12 code for third level administrative division, varchar(20)
    String admin4_code       ; // 13 code for fourth level administrative division, varchar(20)
    String population        ; // 14 bigint (8 byte int)
    String elevation         ; // 15 in meters, integer
    String gtopo30           ; // 16 average elevation of 30'x30' (ca 900mx900m) area in meters, integer
    String timezone          ; // 17 the timezone id (see file timeZone.txt)
    String modification_date; // 18 date of last modification in yyyy-MM-dd format

	public CityRecord(String inputString) throws IllegalArgumentException {
		// CSV header (parsing the inputString is based on this):
		// exchange, stock_symbol, date, stock_price_open, stock_price_high, stock_price_low,
		// 		stock_price_close, stock_volume, stock_price_adj_close
		String[] attributes = inputString.split(",");

		if (attributes.length != 18)
			throw new IllegalArgumentException("Input string given did not have 9 values in CSV format");

		try {
            geonameid = attributes[0];
            name = attributes[1];
            asciiname = attributes[2];
            latitude = attributes[3];
            longitude = attributes[4];
            feature_class = attributes[5];
            feature_code = attributes[6];
            country_code = attributes[7];
            cc2 = attributes[8];
            admin1_code = attributes[9];
            admin2_code = attributes[10];
            admin3_code = attributes[11];
            admin4_code = attributes[12];
            population = attributes[13];
            elevation = attributes[14];
            gtopo30 = attributes[15];
            timezone = attributes[16];
            modification_date = attributes[17];
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
}
