package com.kohls.rt.clickstream;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class DateGen extends EvalFunc<String> {
	private String formattedDateToReturn;

	@Override
	public String exec(Tuple tuple) {
		try {
			if (tuple != null && tuple.get(0) != null && tuple.size() != 0) {
				String unixTime = tuple.get(0).toString();
				Date date = new Date(new Long(unixTime) * 1000L);
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				formattedDateToReturn = sdf.format(date);
			}
		} catch (ExecException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			try {
				tuple.set(0, "");
				System.out
						.println("number format exception is occured for the input string " + tuple.get(0).toString());
			} catch (ExecException e1) {
				e1.printStackTrace();
			}

		}
		return formattedDateToReturn;
	}

}
