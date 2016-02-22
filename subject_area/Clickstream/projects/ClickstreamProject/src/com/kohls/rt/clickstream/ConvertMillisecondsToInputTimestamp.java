package com.kohls.rt.clickstream;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/***
 * 
 * @author tkmaayr (Onkar Malewadikar)
 *Description : This UDF Takes input as milliseconds and convert them to time stamp.
 *			    
 */

public class ConvertMillisecondsToInputTimestamp extends EvalFunc<String>{
private String TIMESTAMP_FORMAT="yyyy-MM-dd HH:mm:ss";//target time stamp format
private String US_TIME_ZONE="GMT-6";//target cst time zone
private String BLANK="";// BLANK STRING
private String NULL=null;//null value
private Long inputMilliSeconds;//input variable
	@Override
	public String exec(Tuple input) throws IOException {
		if (input ==null||input.get(0)==null||input.get(0).toString().trim().equals(BLANK)) {
			return NULL;
		}else{
			inputMilliSeconds=Long.parseLong(input.get(0).toString());
			String inputDateTimeFormat=null;
			SimpleDateFormat formatter=new SimpleDateFormat(TIMESTAMP_FORMAT);//target time stamp format
			formatter.setTimeZone(TimeZone.getTimeZone(US_TIME_ZONE));//convert to target cst time zone
			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(inputMilliSeconds);
			inputDateTimeFormat=formatter.format((calendar.getTime()));//converting to date format 
			System.out.println(inputDateTimeFormat);	
			return inputDateTimeFormat;
		}
		
	}
	
}