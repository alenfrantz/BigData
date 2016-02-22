package com.kohls.rt.clickstream;



import java.io.IOException; 
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/***
 * 
 * @author tkmaayr (Onkar Malewadikar)
 *Description : This UDF Takes input as timestamp and convert it to milliseconds.
 *			    
 */

public class ConvertInputTimestampToMilliseconds extends EvalFunc<Long>{

private final String timeStampFormat="yyyy-MM-dd HH:mm:ss";//default time-stamp format
private final String BLANK="";
private String inputTimeStamp;
private long result;
	@Override
	public Long exec(Tuple input) throws IOException {
		if (input ==null||input.get(0)==null||input.get(0).equals("")||input.get(0).equals(" ")) {
			return null;
		}
		inputTimeStamp=input.get(0).toString();//get first column 
		
		Date inputDateTimeFormat=null;
		SimpleDateFormat formatter=new SimpleDateFormat(timeStampFormat);//input cst timestamp 
		try {
			inputDateTimeFormat=formatter.parse(inputTimeStamp);//converting to date format 
		} catch (ParseException e) {
			e.printStackTrace();
		}
		result=inputDateTimeFormat.getTime();
		System.out.println("Input Timestamp : "+inputTimeStamp+" Converted To Milliseconds : "+result);//converting the time stamp to milliseconds 	
		return result;
	}
	
}
