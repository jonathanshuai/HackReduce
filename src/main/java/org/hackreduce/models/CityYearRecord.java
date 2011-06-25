package org.hackreduce.models;

public class CityYearRecord {
	private String cityName;
	private String year;
	private int count;
	
	public CityYearRecord(String cityName, String year, int count){
		this.cityName = cityName;
		this.year = year;
		this.count = count;
	}
	
	public String getCityName(){
		return cityName;
	}
	
	public String getYear(){
		return year;
	
	}
	
	public int getCount(){
		return count;
	}
	
	public void setCityRecord(String cityName){
		this.cityName = cityName;
	}
	
	public void setCount(int count){
		this.count = count;
	}
	
	public void setYear(String year){
		this.year = year;
	}
	
	
}
