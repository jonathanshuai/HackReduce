package org.hackreduce.models;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


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
     
    
      
      public String toString(){
      	String result = "{" ; 
      	result += "count:" +  this.count + ",";
      	result += "city:\""  + this.cr.asciiname + "\",";
      	result += "long:" + this.cr.longitude + ",";
      	result += "lat:" + this.cr.latitude + ",";
      	result += "year:" + this.year + ",";
      	result += "id:" + this.cr.geonameid + "}";
      	return result;
        
        }
	
}
