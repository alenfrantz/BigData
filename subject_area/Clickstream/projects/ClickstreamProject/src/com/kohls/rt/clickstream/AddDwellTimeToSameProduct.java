package com.kohls.rt.clickstream;

import java.io.IOException;
import java.util.Iterator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/***
 * 
 * @author tkmaayr(Onkar Malewadikar)
 *Description : This udf checks Dwell time for Page url.
 *This udf accepts 2 parameter as input ie product id and dwell time then calculate dwell time and returns dwell time for that product.			
 */
public class AddDwellTimeToSameProduct extends EvalFunc<String> {
	
	private  final int ZERO=0;//Zeroth element
	private  final int TWO=2;//Second element 
	private  final int THREE=3;//Second element 
	private  final String BLANK="";//BLANK Value ""
	private  final String DWELL_TIME="dwell_time";//output column name for dwell time

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.get(ZERO) == null) {
			return null;
		}// END OF IF
		String previousDwellTime="";//Previous products dwell time 
		DataBag inputBag=null;//Data bag Object 
		inputBag = (DataBag) input.get(ZERO);
		System.out.println("Input Bag : " + inputBag);
		Iterator<Tuple> iterator = inputBag.iterator();
		while (iterator.hasNext()) {
			Tuple test = iterator.next();
			String productId = test.get(TWO).toString();// get product id for current row
			System.out.println("Input Product Id : " + productId);
			String inputDwellTime = test.get(THREE).toString();// get dwell time value for product
			System.out.println("Input Dwell Time : " + inputDwellTime);

			if (inputDwellTime == null || inputDwellTime.trim().equals(BLANK) || productId == null || productId.trim().equals(BLANK)) {
				System.err.println("Input Dwell Time Null "+inputDwellTime);
			} else {
				if (previousDwellTime!=null && !previousDwellTime.equals(BLANK)) {
					//if previous dwell time is present 
					String outputDewllTime=calculateDifferenceOfTwoTime(previousDwellTime,inputDwellTime);
					System.out.println("Output Dwell Time is : "+outputDewllTime);
					previousDwellTime=outputDewllTime;
					
					} else {
					//if previous dwell time is not present 
						previousDwellTime=inputDwellTime;
						System.out.println("Output Dwell Time is : "+previousDwellTime);
				}// END OF ELSE IF
				
			}// END OF IF	

		}// END OF WHILE LOOP
		System.err.println("Dwell time is : "+previousDwellTime);
		return previousDwellTime;
		}
		
	//function which calculates difference between two string time values
	private String calculateDifferenceOfTwoTime(String firstTimeValue,String secondTimeValue) {
		int seconds1=Integer.parseInt(firstTimeValue.substring(6,8));
		int minutes1=Integer.parseInt(firstTimeValue.substring(3,5));
		int hour1=Integer.parseInt(firstTimeValue.substring(0,2));
		
		
		int seconds2=Integer.parseInt(secondTimeValue.substring(6,8));
		int minutes2=Integer.parseInt(secondTimeValue.substring(3,5));
		int hour2=Integer.parseInt(secondTimeValue.substring(0,2));
		
		int outputSeconds=((seconds1+seconds2)%60);
		int remainingMinutesFromSeconds=((seconds1+seconds2)/60);
		
		int outputMinutes=((minutes1+minutes2+remainingMinutesFromSeconds)%60);
		
		int remainHoursFromMinutes=((minutes1+minutes2)/60);
		int outputHour=(hour1+hour2+remainHoursFromMinutes);
		
		
		String outputHourMinutesSeconds=(outputHour >= 10 ? outputHour:("0"+outputHour))+":"+(outputMinutes >= 10 ? outputMinutes:("0"+outputMinutes))+":"+(outputSeconds >= 10 ? outputSeconds:("0"+outputSeconds));
		return outputHourMinutesSeconds;
	}

	//function which gives appropriate schema to output Bag
	@Override
	public Schema outputSchema(Schema input) {
		Schema tupleSchema = new Schema();
		tupleSchema.add(new Schema.FieldSchema(DWELL_TIME, DataType.CHARARRAY));
		return tupleSchema;
	}//end of function which gives appropriate schema to output Bag


}
