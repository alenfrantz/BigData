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
 *Description : This UDF Takes input as denorm business hour and last activity time-stamp
 *				, calculates the difference and returns a flag.
 *			    
 */

public class CalculateDateTimeDiff extends EvalFunc<String>{

private String flag;	
private String lastActivityValue;
private String dnormBusinessHour;
private String convertedDnormBusinessHour;
private final String timeStampFormat="yyyy-MM-dd HH:mm:ss";//default time-stamp format
private final String BLANK="";
private final int CONVERT_TO_MINUTE=60000;//minute conversion
private int DELAY_PARAMETER;

	 public CalculateDateTimeDiff() {
		this.flag="N";
		this.lastActivityValue="";
		this.dnormBusinessHour="";
		this.convertedDnormBusinessHour="";
		DELAY_PARAMETER=60;
	}//with default delay parameter
	 public CalculateDateTimeDiff(String DelayParameter){
		this.DELAY_PARAMETER=Integer.parseInt(DelayParameter);
		this.flag="N";
		this.lastActivityValue="";
		this.dnormBusinessHour="";
		this.convertedDnormBusinessHour="";
	 }//with user defined delay parameter

	@Override
	public String exec(Tuple input) throws IOException {
		if (input ==null||input.get(0)==null||input.get(1)==null) {
			return flag;
		}
		lastActivityValue=input.get(0).toString();//get first column 
		dnormBusinessHour=input.get(1).toString();//get second column
		if (lastActivityValue.isEmpty()||lastActivityValue.equalsIgnoreCase(BLANK)||dnormBusinessHour.isEmpty()||dnormBusinessHour.equalsIgnoreCase(BLANK)) {
			flag="N";//Move to Not Eligible Table
		}//validating input columns
		else{
			//converting business hour to time-stamp format 
			convertedDnormBusinessHour=dnormBusinessHour.substring(0, 4)+"-"+dnormBusinessHour.substring(4, 6)+"-"+dnormBusinessHour.substring(6, 8)+" "+dnormBusinessHour.substring(8, 10)+":00:00";
			Date lastActivityDate=null;
			Date dnormBusinessHourDate=null;
			SimpleDateFormat formatter=new SimpleDateFormat(timeStampFormat);
			try {
				lastActivityDate=formatter.parse(lastActivityValue);//converting to date format 
			} catch (ParseException e) {
				e.printStackTrace();
			}
			try {
				dnormBusinessHourDate=formatter.parse(convertedDnormBusinessHour);//converting to date format 
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			long diff1=dnormBusinessHourDate.getTime() - lastActivityDate.getTime();//calculate difference of two time-stamp 
			
			long diff=diff1/(CONVERT_TO_MINUTE);//convert to minutes
			if(diff >= DELAY_PARAMETER){
				flag="Y";//Move To Most Eligible Table
			}
			else
			{
				flag="N";//move to not eligible
			}
		}
		return this.flag;
	}
	
}
