package org.hackreduce.models;
import org.apache.hadoop.io.Writable;

public class CityYearRecord implements Writeable {
	private CityRecord cr;
	private String year;
	private int count;
	
	public CityYearRecord(CityRecord cr, String year, int count){
		this.cr = cr;
		this.year = year;
		this.count = count;
	}
	
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
       }
       
       public void readFields(DataInput in) throws IOException {
         cr.readFileds(in);
       }
       
      
     }
	
}
