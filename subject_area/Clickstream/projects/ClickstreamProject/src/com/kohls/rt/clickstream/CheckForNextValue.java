package com.kohls.rt.clickstream;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
public class CheckForNextValue extends EvalFunc<DataBag> {
	private TupleFactory factory = TupleFactory.getInstance();
	private BagFactory bagFactory = BagFactory.getInstance();
	List<Tuple> resultantTuples = new ArrayList<Tuple>();//List of resultant tuples
	private final static String TIMESTAMP_FORMAT="yyyy-MM-dd HH:mm:ss";//default time-stamp format
	private DataBag inputBag;//Data bag Object 
	private static String FIRST_VIEW_TIME = null;// first product page_url view time
	private static String LAST_VIEW_TIME = null;// first view time of that product page_url
	private static boolean IS_PRODUCT_PAGE_FOUND = false;// flag to set whether  product page_url found
	private static String LAST_PRODUCT_PAGE = null;// Value of last product page_url
	private static final String PRODUCT_PAGE_URL_FORMAT = "prd-";// product page_url format
	private static final boolean FALSE_VALUE = false;// constant false value
	private static final boolean TRUE_VALUE = true;// constant false value
	private static final String ERROR_MSG = "USER IS IN SAME PRODUCT PAGE";// ERROR MSG
	private HashMap<String, Long> result = new HashMap<String, Long>();// resultant map
	private static final int ZERO=0;//Zeroth element 
	private static final int ONE=1;//First element
	private static final int TWO=2;//Second element 
	private static final String BLANK="";//BLANK Value ""
	private static final String PAGE_URL="page_url";//output column name for page url
	private static final String DWELL_TIME="dwell_time";//output column name for dwell time

	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.get(ZERO) == null) {
			return null;
		}// END OF IF
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
				System.out.println(ERROR_MSG);
			} else {
				if (inputPageUrl.contains(PRODUCT_PAGE_URL_FORMAT) && IS_PRODUCT_PAGE_FOUND == FALSE_VALUE) {
					System.out.println("in if inputPageUrl " + inputPageUrl+ " PRODUCT_PAGE_URL_FORMAT: " + PRODUCT_PAGE_URL_FORMAT+ " IS_PRODUCT_PAGE_FOUND : "
							+ IS_PRODUCT_PAGE_FOUND + " FALSE_VALUE : "+ FALSE_VALUE);
					LAST_PRODUCT_PAGE = inputPageUrl;
					FIRST_VIEW_TIME = inputDateTime;
					IS_PRODUCT_PAGE_FOUND = TRUE_VALUE;
				} else if ((LAST_PRODUCT_PAGE!=null&& !LAST_PRODUCT_PAGE.equalsIgnoreCase("")) &&!LAST_PRODUCT_PAGE.equalsIgnoreCase(inputPageUrl)) {
					LAST_VIEW_TIME = inputDateTime;
					long difference = Long.parseLong(LAST_VIEW_TIME) - Long.parseLong(FIRST_VIEW_TIME);// calculating difference of FIRST VIEW TIME AND LAST VIEW TIME
					if (result.get(LAST_PRODUCT_PAGE) != null) {
						long existingValueFromSameKey = result.get(LAST_PRODUCT_PAGE);// Get existing value for same key from result
						result.put(LAST_PRODUCT_PAGE, difference + existingValueFromSameKey);
					} else {
						result.put(LAST_PRODUCT_PAGE, difference);//if no value present for same key then insert new value					
					}
					LAST_PRODUCT_PAGE = inputPageUrl;
					FIRST_VIEW_TIME=inputDateTime;
				}// END OF ELSE IF
				else {
					System.out.println(ERROR_MSG);
				}// END OF ELSE
			}// END OF NULL CHECK ELSE		

		}// END OF WHILE LOOP
		System.out.println(result);
		System.out.println(result.size());
		Set<String> keys=result.keySet();
		for (String key : keys) {
			Tuple tuple = factory.newTuple(TWO);
			tuple.set(ZERO,key);
			String dateTimeDiff=hourMinutesSeconds(result.get(key)/1000L);//Converting input milliseconds value to seconds 
			System.err.println("Convered dateTimeDiff is : "+dateTimeDiff);//Converting value of difference to hour:minute:seconds format
			tuple.set(ONE,dateTimeDiff);
			resultantTuples.add(tuple);
		}//Adding Current tuple vales to List<Tuples>
		System.out.println(resultantTuples);
		DataBag bag = bagFactory.newDefaultBag(resultantTuples);//Converting tuples to Bag
		System.out.println(bag);
		return bag;
	}
	
	//function which converts input Seconds to Hour:Minute:Seconds format
	public static String hourMinutesSeconds(long inputSeconds){
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
		tupleSchema.add(new Schema.FieldSchema(PAGE_URL, DataType.CHARARRAY));
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

	//function which converts input time stamp to milliseconds 
	private static Long convertInputTimestampToMilliseconds(String inputTimeStamp) {
		Date inputDateTimeFormat=null;
		SimpleDateFormat formatter=new SimpleDateFormat(TIMESTAMP_FORMAT);//input cst timestamp 
		try {
			inputDateTimeFormat=formatter.parse(inputTimeStamp);//converting to date format 
		} catch (ParseException e) {
			e.printStackTrace();
		}
		long result=inputDateTimeFormat.getTime();
		System.out.println("Input Timestamp : "+inputTimeStamp+" Converted To Milliseconds : "+result);//converting the time stamp to milliseconds 	
		return result;
		
	}//end of function which converts input time stamp to milliseconds 
}
