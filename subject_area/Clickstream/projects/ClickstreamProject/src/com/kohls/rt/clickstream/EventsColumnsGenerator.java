package com.kohls.rt.clickstream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * @author tkmaays this class is created for generating multiple columns from
 *         one column
 */
public class EventsColumnsGenerator extends EvalFunc<Tuple> {
	private TupleFactory factory = TupleFactory.getInstance();
	private Tuple tupleToReturn;
	private DataBag inputBag;
	private final int TOTALEVENTS = 100;
	private final int TOTALFIELDSINTUPLE = 109;
	private final String EVENTSKNOWN = "event1,event2,event3,event4,event5,event6,event7,event8,event9,event10,event11,event12,event13,event14,event15,event16,event17,event18,event19,event20,event21,event22,event23,event24,event25,event26,event27,event28,event29,event30,event31,event32,event33,event34,event35,event36,event37,event38,event39,event40,event41,event42,event43,event44,event45,event46,event47,event48,event49,event50,event51,event52,event53,event54,event55,event56,event57,event58,event59,event60,event61,event62,event63,event64,event65,event66,event67,event68,event69,event70,event71,event72,event73,event74,event75,event76,event77,event78,event79,event80,event81,event82,event83,event84,event85,event86,event87,event88,event89,event90,event91,event92,event93,event94,event95,event96,event97,event98,event99,event100,revenue,productviews,carts,checkouts,cartadditions,cartremovals,cartviews,purchase";

	/**
	 * creates the default tuple with default values as "N"
	 */
	public void createDefaultTuple() {
		tupleToReturn = factory.newTuple(TOTALFIELDSINTUPLE);
		for (int i = 0; i < TOTALFIELDSINTUPLE; i++) {
			try {
				tupleToReturn.set(i, "N");
			} catch (ExecException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * creates the tuple which will be returned if input list contains an event
	 * then corresponding event position in the tuple will have "Y" as value for
	 * example: if the input list contains only "event1" then the tuple which
	 * will be returned will have 107 fields in which first field will have
	 * value "Y" and others will have "N" as values
	 * 
	 * @param inputEvents
	 * @param otherEvents
	 */
	public void checkAndCreateTuple(List<String> inputEvents, String otherEvents) {
		this.createDefaultTuple();
		int i = 0;
		try {
			for (i = 1; i <= TOTALEVENTS; i++) {
				if (inputEvents.contains(new String("event" + i)))
					tupleToReturn.set(i - 1, "Y");
				else
					tupleToReturn.set(i - 1, "N");
			}
			if (inputEvents.contains(new String("revenue")))
				tupleToReturn.set(100, "Y");
			else if (inputEvents.contains(new String("productviews")))
				tupleToReturn.set(101, "Y");
			else if (inputEvents.contains(new String("carts")))
				tupleToReturn.set(102, "Y");
			else if (inputEvents.contains(new String("checkouts")))
				tupleToReturn.set(103, "Y");
			else if (inputEvents.contains(new String("cartadditions")))
				tupleToReturn.set(104, "Y");
			else if (inputEvents.contains(new String("cartremovals")))
				tupleToReturn.set(105, "Y");
			else if (inputEvents.contains(new String("cartviews")))
				tupleToReturn.set(106, "Y");
			else if (inputEvents.contains(new String("purchase")))
				tupleToReturn.set(107, "Y");
			tupleToReturn.set(108, otherEvents);
		} catch (ExecException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.get(0) == null) {
			this.createDefaultTuple();
			return tupleToReturn;
		}
		inputBag = (DataBag) input.get(0);
		Iterator<Tuple> iterator = inputBag.iterator();
		List<String> listOfEvents = new ArrayList<String>();
		String otherEvents = "";
		while (iterator.hasNext()) {
			String event = iterator.next().get(0).toString().trim().toLowerCase();
			if (!EVENTSKNOWN.contains(event)) {
				otherEvents = otherEvents + event + ":";
			}
			listOfEvents.add(event);
		}
		if (otherEvents.length() > 0)
			this.checkAndCreateTuple(listOfEvents, otherEvents.substring(0, otherEvents.length() - 1));
		else
			this.checkAndCreateTuple(listOfEvents, otherEvents);
		return tupleToReturn;
	}

	@Override
	public Schema outputSchema(Schema input) {
		Schema tupleSchema = new Schema();
		for (int i = 1; i <= TOTALEVENTS; i++) {
			tupleSchema.add(new Schema.FieldSchema("event" + i, DataType.CHARARRAY));
		}
		// @formatter:off
		tupleSchema.add(new Schema.FieldSchema("revenue", DataType.CHARARRAY));
		tupleSchema.add(new Schema.FieldSchema("productviews", DataType.CHARARRAY));
		tupleSchema.add(new Schema.FieldSchema("carts", DataType.CHARARRAY));
		tupleSchema.add(new Schema.FieldSchema("checkouts", DataType.CHARARRAY));
		tupleSchema.add(new Schema.FieldSchema("cartadditions", DataType.CHARARRAY));
		tupleSchema.add(new Schema.FieldSchema("cartremovals", DataType.CHARARRAY));
		tupleSchema.add(new Schema.FieldSchema("cartviews", DataType.CHARARRAY));
		
		tupleSchema.add(new Schema.FieldSchema("purchase", DataType.CHARARRAY));
		tupleSchema.add(new Schema.FieldSchema("other_events", DataType.CHARARRAY));
		// @formatter:on
		try {
			return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
					tupleSchema, DataType.TUPLE));
		} catch (FrontendException e) {
			e.printStackTrace();
		}
		return null;
	}
}
