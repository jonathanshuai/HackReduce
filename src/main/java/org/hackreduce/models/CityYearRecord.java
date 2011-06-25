package org.hackreduce.models;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import net.sf.json.*;

import org.apache.hadoop.io.Writable;

import sun.tools.tree.ThisExpression;

public class CityYearRecord implements Writable {
	private CityRecord cr;
	private String year;
	private int count;
	
	public CityYearRecord(CityRecord cr, String year, int count){
		this.cr = cr;
		this.year = year;
		this.count = count;
	}
	
	public CityYearRecord(){};
	
	public CityRecord getCityRecord(){
		return cr;
	}
	
	public String getYear(){
		return year;
	
	}
	
	public int getCount(){
		return count;
	}
	
	public void setCityRecord(CityRecord cr){
		this.cr = cr;
	}
	
	public void setCount(int count){
		this.count = count;
	}
	
	public void setYear(String year){
		this.year = year;
	}
	
	public void write(DataOutput out) throws IOException {
         cr.write(out);
         out.writeInt(count);
         out.writeUTF(year);
       }
       
       public void readFields(DataInput in) throws IOException {
         this.cr = new CityRecord(in);
         this.count = in.readInt();
         this.year = in.readUTF();
         
     }
     
      public JSONObject toJSON(){
    	JSONObject result = new JSONObject();  
    	result.element("count", this.count);
    	result.element("city", this.cr.asciiname);
    	result.element("long", this.cr.longitude);
    	result.element("lat", this.cr.latitude);
    	result.element("year", this.year);
    	result.element("id", this.cr.geonameid);
    	return result;
      
      }
	
}
