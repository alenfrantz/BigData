package com.kohls.rt.clickstream;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/***
 * 
 * @author tkmaayr(Onkar Malewadikar)
 *Description : This udf checks Dwell time for Page url.
 *This udf accepts 3 parameter as input and calculate dwell time and returns page_url and dwell time .			
 */
public class CheckForNextValueProductPageForClickstream extends EvalFunc<DataBag> {
	private TupleFactory factory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
	private final String TIMESTAMP_FORMAT="yyyy-MM-dd HH:mm:ss";//default time-stamp format
	private final String TIMESTAMP_FORMAT_WITH_TIMEZONE="yyyy-MM-dd HH:mm:ss z";// time-stamp format with timezone 
	private final String CDT_TIMEZONE="GMT-6";//CDT TIMEZONE
	private  final String PRODUCT_PAGE_URL_FORMAT = "product/prd-";// product page_url format
	private  final boolean FALSE_VALUE = false;// constant false value
	private  final boolean TRUE_VALUE = true;// constant false value
	private  final String ERROR_MSG = "USER IS IN SAME PRODUCT PAGE";// ERROR MSG
	private  final int ZERO=0;//Zeroth element 
	private  final int ONE=1;//First element
	private  final int TWO=2;//Second element 
	private  final String BLANK="";//BLANK Value ""
	private  final String PRODUCT_ID="product_id";//output column name for page url
	private  final String DWELL_TIME="dwell_time";//output column name for dwell time
	private  final String END_DAY_CST_FORMAT=" 23:59:59 CST";//End day CST time
	private  final String FORWARD_SLASH_DEMILITER="/";//FORWARD SLASH DEMILITER

	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.get(ZERO) == null) {
			return null;
		}// END OF IF
		List<Tuple> resultantTuples = new ArrayList<Tuple>();
		HashMap<String, Long> result = new HashMap<String, Long>();
		DataBag inputBag=null;//Data bag Object 
		String FIRST_VIEW_TIME = null;// first product page_url view time
		String LAST_VIEW_TIME = null;// first view time of that product page_url
		boolean IS_PRODUCT_PAGE_FOUND = false;// flag to set whether  product page_url found
		String LAST_PRODUCT_PAGE = null;// Value of last product page_url
		String PRODUCT_PAGE_LAST_DATE_TIME="";//Last date time for last product page

		inputBag = (DataBag) input.get(ZERO);
		System.out.println("inputBag : " + inputBag);
		Iterator<Tuple> iterator = inputBag.iterator();
		while (iterator.hasNext()) {
			Tuple test = iterator.next();
			String inputDateTime = test.get(ONE).toString();// get date time value for current row
			System.out.println("Input date time : " + inputDateTime);
			String inputPageUrl = test.get(TWO).toString();// get page_url value for current row
			System.out.println("Input page url : " + inputPageUrl);

			if (inputDateTime == null || inputDateTime.trim().equals(BLANK) || inputPageUrl == null || inputPageUrl.trim().equals(BLANK)) {
				System.err.println(ERROR_MSG);
			} else {
				if (inputPageUrl.contains(PRODUCT_PAGE_URL_FORMAT) && IS_PRODUCT_PAGE_FOUND == FALSE_VALUE) {
					System.out.println("in if inputPageUrl " + inputPageUrl+ " PRODUCT_PAGE_URL_FORMAT: " + PRODUCT_PAGE_URL_FORMAT+ " IS_PRODUCT_PAGE_FOUND : " + IS_PRODUCT_PAGE_FOUND + " FALSE_VALUE : "+ FALSE_VALUE);
					System.err.println("inputPageUrl: "+inputPageUrl);
					LAST_PRODUCT_PAGE = extractsProductId(inputPageUrl);
					System.out.println("LAST_PRODUCT_PAGE : "+LAST_PRODUCT_PAGE);
					FIRST_VIEW_TIME = inputDateTime;
					IS_PRODUCT_PAGE_FOUND = TRUE_VALUE;
					PRODUCT_PAGE_LAST_DATE_TIME=inputDateTime;
				} else if ((LAST_PRODUCT_PAGE!=null&& !LAST_PRODUCT_PAGE.equalsIgnoreCase(BLANK)) &&!inputPageUrl.contains(LAST_PRODUCT_PAGE)) {
					if (inputPageUrl.contains(PRODUCT_PAGE_URL_FORMAT)) {					
						LAST_VIEW_TIME = inputDateTime;
						PRODUCT_PAGE_LAST_DATE_TIME=inputDateTime;//capturing each date time
						long difference = Long.parseLong(LAST_VIEW_TIME) - Long.parseLong(FIRST_VIEW_TIME);// calculating difference of FIRST VIEW TIME AND LAST VIEW TIME
						if (result.get(LAST_PRODUCT_PAGE) != null) {
							long existingValueFromSameKey = result.get(LAST_PRODUCT_PAGE);// Get existing value for same key from result
							result.put(LAST_PRODUCT_PAGE, difference + existingValueFromSameKey);
						} else {
							result.put(LAST_PRODUCT_PAGE, difference);//if no value present for same key then insert new value					
						}	
						LAST_PRODUCT_PAGE = extractsProductId(inputPageUrl);
						System.out.println("LAST_PRODUCT_PAGE : "+LAST_PRODUCT_PAGE);
						FIRST_VIEW_TIME=inputDateTime;
						
					} else {
						LAST_VIEW_TIME = inputDateTime;
						PRODUCT_PAGE_LAST_DATE_TIME=inputDateTime;//capturing each date time
						long difference = Long.parseLong(LAST_VIEW_TIME) - Long.parseLong(FIRST_VIEW_TIME);// calculating difference of FIRST VIEW TIME AND LAST VIEW TIME
						if (result.get(LAST_PRODUCT_PAGE) != null) {
							long existingValueFromSameKey = result.get(LAST_PRODUCT_PAGE);// Get existing value for same key from result
							result.put(LAST_PRODUCT_PAGE, difference + existingValueFromSameKey);
						} else {
							result.put(LAST_PRODUCT_PAGE, difference);//if no value present for same key then insert new value					
						}
						LAST_PRODUCT_PAGE = inputPageUrl;
						FIRST_VIEW_TIME=inputDateTime;
					}
						
				}// END OF ELSE IF
				else {
					System.err.println(ERROR_MSG);
				}// END OF ELSE
			}// END OF NULL CHECK ELSE		

		}// END OF WHILE LOOP
		System.out.println("Hasmap result: "+result);
		System.out.println("Hasmap result size: "+result.size());
		Set<String> keys=result.keySet();
		if (!keys.isEmpty()) {
			for (String key : keys) {
				Tuple tuple = factory.newTuple(TWO);
				if (key.contains(PRODUCT_PAGE_URL_FORMAT)) {
					//tuple.set(ZERO,key);//replace product/prd- 
					tuple.set(ZERO,removesPerticularCharatersFromString(key));//replace product/prd- 
					String dateTimeDiff=hourMinutesSeconds(result.get(key)/1000L);//Converting input milliseconds value to seconds 
					System.err.println("Convered dateTimeDiff is : "+dateTimeDiff);//Converting value of difference to hour:minute:seconds format
					tuple.set(ONE,dateTimeDiff);
					resultantTuples.add(tuple);
				} else {
					System.err.println("This is not product page.No need to add to Bag");
				}//Converting value of difference to hour:minute:seconds format
				
			}//Adding Current tuple vales to List<Tuples>
		} else {
				System.err.println("HashMap key set is empty"+keys);
		}
		
		//check whether last page is of product page ? If yes add to current tuples otherwise exclude
		if (LAST_PRODUCT_PAGE!=null && !LAST_PRODUCT_PAGE.equals("")&&LAST_PRODUCT_PAGE.contains(PRODUCT_PAGE_URL_FORMAT)) {
			Tuple lastTuple = factory.newTuple(TWO);
			lastTuple.set(ZERO, removesPerticularCharatersFromString(LAST_PRODUCT_PAGE));//adding Last product entry
			lastTuple.set(ONE, convertInputMillisecondsToEndDayTimeStamp(PRODUCT_PAGE_LAST_DATE_TIME));
			resultantTuples.add(lastTuple);//adding last tuple to last of tuples
		} else {
			System.err.println("LAST PAGE : "+LAST_PRODUCT_PAGE +" IS NOT PRODUCT PAGE.SKIPPNG RECORD");
		}
		
		System.out.println("List of resultantTuples : "+resultantTuples);
		DataBag bag = bagFactory.newDefaultBag(resultantTuples);//Converting tuples to Bag
		System.out.println("Output bag : "+bag);
		return bag;
	}
	//function which removes product/prd- string from input String 
	private String removesPerticularCharatersFromString(String inputString) {
		String productId=inputString.replace(PRODUCT_PAGE_URL_FORMAT, BLANK);
		return productId;
		
	}
	
	//function which split the input page url and extracts product id 
	private String extractsProductId(String inputPageUrl) {
		String[] splitOnPageUrlFormat=inputPageUrl.split(PRODUCT_PAGE_URL_FORMAT);
		String[] splitOnpProductId=splitOnPageUrlFormat[1].split(FORWARD_SLASH_DEMILITER);
		String product_id=PRODUCT_PAGE_URL_FORMAT+splitOnpProductId[0];
		return product_id;
		
	}
	//function which converts input Seconds to Hour:Minute:Seconds format
	public  String hourMinutesSeconds(long inputSeconds){
		int minutes=0;
		int hour = 0;
		int seconds=0;
		if (inputSeconds >= 60) {
			seconds=(int) (inputSeconds%60);
			minutes=(int) (inputSeconds/60);
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
			seconds=(int) inputSeconds;
			String outputHourMinutesSeconds=(hour >= 10 ? hour:("0"+hour))+":"+(minutes >= 10 ? minutes:("0"+minutes))+":"+(seconds >= 10 ? seconds:("0"+seconds));
			return outputHourMinutesSeconds;
		}
		
	}//end of function which converts input Seconds to Hour:Minute:Seconds format
	
	//function which gives appropriate schema to output Bag
	@Override
	public Schema outputSchema(Schema input) {
		Schema tupleSchema = new Schema();
		tupleSchema.add(new Schema.FieldSchema(PRODUCT_ID, DataType.CHARARRAY));
		tupleSchema.add(new Schema.FieldSchema(DWELL_TIME, DataType.CHARARRAY));

		Schema bagSchema = new Schema(tupleSchema);
		bagSchema.setTwoLevelAccessRequired(true);
		try {
			Schema.FieldSchema bagFs = new Schema.FieldSchema(
					"dwell_time_bag", bagSchema, DataType.BAG);
			return new Schema(bagFs);
		} catch (FrontendException e) {
			e.printStackTrace();
			return null;
		}
	}//end of function which gives appropriate schema to output Bag

	//function which converts input milliseconds extracts date part and add 23:59:59 at end of that date 
	private  String convertInputMillisecondsToEndDayTimeStamp(String input) {
		long inputMilliSeconds=Long.parseLong(input.toString());//get first column 
		String inputDateTimeFormat=null;
		SimpleDateFormat formatter=new SimpleDateFormat(TIMESTAMP_FORMAT);//input cst timestamp 
		formatter.setTimeZone(TimeZone.getTimeZone(CDT_TIMEZONE));
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(inputMilliSeconds);
		inputDateTimeFormat=formatter.format((calendar.getTime()));//converting to date format 
		String endDayTimestamp=inputDateTimeFormat.substring(0,10)+END_DAY_CST_FORMAT;
		long endDayMilliseconds=convertInputTimestampToMilliseconds(endDayTimestamp);//converting end day to milliseconds 
		long difference=endDayMilliseconds  - inputMilliSeconds;
		String outputHourMinuteSeconds=hourMinutesSeconds(difference/1000);
		return outputHourMinuteSeconds;
	}//end of function which converts input milliseconds extracts date part and add 23:59:59 at end of that date 
	
	//function to convert input time stamp to milliseconds 
	private  long convertInputTimestampToMilliseconds(String inputTimestamp) {
		Date inputDateObj=null;
		SimpleDateFormat formatter=new SimpleDateFormat(TIMESTAMP_FORMAT_WITH_TIMEZONE);//"yyyy-MM-dd HH:mm:ss z"
		formatter.setTimeZone(TimeZone.getTimeZone(CDT_TIMEZONE));//CDT
		try {
			inputDateObj=formatter.parse(inputTimestamp);
		} catch (ParseException e) {
			e.printStackTrace();
		}//converting to date format 
		long toMilliseconds=inputDateObj.getTime();
		return toMilliseconds;
	}//end of function to convert input time stamp to milliseconds 
	
	//function which converts input milliseconds to CDT FORMAT
	private  String convertInputMillisecondsToTimeStamp(String input){
		long inputMilliSeconds=Long.parseLong(input.toString());//get first column 
		String inputDateTimeFormat=null;
		SimpleDateFormat formatter=new SimpleDateFormat(TIMESTAMP_FORMAT);//input cst timestamp 
		formatter.setTimeZone(TimeZone.getTimeZone(CDT_TIMEZONE));
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(inputMilliSeconds);
		inputDateTimeFormat=formatter.format((calendar.getTime()));//converting to date format 
		System.out.println("inputDateTimeFormat : "+inputDateTimeFormat);
		return inputDateTimeFormat;
		
	}//end of function which converts input milliseconds to CDT FORMAT
}
