package com.kohls.rt.clickstream;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
/**
 * This udf accept date (YYYYMMDD) as input and returns whether input date is weekday or weekend 
 * */
public class CheckWeekdayOrWeekend extends EvalFunc<String> {

	private final String WEEKDAY="weekday";
	private final String WEEKEND="weekend";
	private final String BLANK="";
	@Override
	public String exec(Tuple input) {
		try{
		if (input.get(0)==null || input.get(0).equals(BLANK)) {
			return null;
		} else {
			String inputDate=input.get(0).toString();
			int year=Integer.parseInt(inputDate.substring(0, 4));
			int month=Integer.parseInt(inputDate.substring(4, 6));
			int day = Integer.parseInt(inputDate.substring(6,8));
			Calendar cal=new GregorianCalendar(year, month-1, day);
			int dayOfWeek=cal.get(Calendar.DAY_OF_WEEK);
			if(Calendar.SUNDAY == dayOfWeek||Calendar.SATURDAY == dayOfWeek){
				return WEEKEND;
				}else{
				return WEEKDAY;
			}
			
		}//end of else
	}catch(IOException e){
		e.printStackTrace();
		return null;
	}catch (NumberFormatException e) {
		e.printStackTrace();
		return null;
	}
		
		
	}

}
