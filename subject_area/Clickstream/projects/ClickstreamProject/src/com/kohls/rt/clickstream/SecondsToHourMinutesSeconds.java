package com.kohls.rt.clickstream;

import java.io.IOException; 

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/***
 * 
 * @author tkmaayr (Onkar Malewadikar)
 *Description : This UDF Takes Input As seconds And Returns hour:minute:seconds String
 *			    
 */
public class SecondsToHourMinutesSeconds extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input ==null||input.get(0)==null||input.get(0).toString().trim().equals("")) {
			return null;
		} else {
			int inputSeconds=Integer.parseInt(input.get(0).toString());
			return hourMinutesSeconds(inputSeconds);
		}
	}
	public static String hourMinutesSeconds(int inputSeconds){
		int minutes=0;
		int hour = 0;
		int seconds=0;
		if (inputSeconds >= 60) {
			seconds=inputSeconds%60;
			minutes=inputSeconds/60;
			if (minutes >= 60) {
				hour=minutes/60;
				minutes=minutes%60;
				String outputHourMinutesSeconds=(hour >= 10 ? hour:("0"+hour))+":"+(minutes >= 10 ? minutes:("0"+minutes))+":"+(seconds >= 10 ? seconds:("0"+seconds));
				return outputHourMinutesSeconds;
			} else {
				String outputHourMinutesSeconds=(hour >= 10 ? hour:("0"+hour))+":"+(minutes >= 10 ? minutes:("0"+minutes))+":"+(seconds >= 10 ? seconds:("0"+seconds));
				return outputHourMinutesSeconds;
			}
		} else {
			seconds=inputSeconds;
			String outputHourMinutesSeconds=(hour >= 10 ? hour:("0"+hour))+":"+(minutes >= 10 ? minutes:("0"+minutes))+":"+(seconds >= 10 ? seconds:("0"+seconds));
			return outputHourMinutesSeconds;
		}
		
	}
}
