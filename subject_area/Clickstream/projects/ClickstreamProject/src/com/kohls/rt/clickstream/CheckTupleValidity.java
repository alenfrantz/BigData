package com.kohls.rt.clickstream;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class CheckTupleValidity {
	public boolean isValid(Tuple inputTuple) {
		try {
			if (inputTuple == null || inputTuple.get(0).equals("")
					|| inputTuple.get(0) == null)
				return false;
			else
				return true;
		} catch (ExecException e) {
			e.printStackTrace();
		}
		return false;
	}
}
