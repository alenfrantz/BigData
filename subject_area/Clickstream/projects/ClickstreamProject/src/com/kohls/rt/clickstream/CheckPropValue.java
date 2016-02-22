package com.kohls.rt.clickstream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/***
 * 
 * @author tkmaayr(Onkar Malewadikar)
 *Description : This UDF Checks Input Prop Map 
				Exacts each prop out of it and assign its value	if found otherwise 
				put blank.			
 */

public class CheckPropValue
  extends EvalFunc<Tuple>
{
  private static final int TOTALPROPS = 75;
  private TupleFactory factory = TupleFactory.getInstance();
  private Tuple tuple = this.factory.newTuple(75);
  private final String BLANK="";
  private final String PROP_NAME="prop";  
//Adding known prop input values to ArrayList
  public ArrayList<String> knownProps()
  {
    ArrayList<String> inputProps = new ArrayList<String>(75);
    
    inputProps.add("prop1");
    inputProps.add("prop2");
    inputProps.add("prop3");
    inputProps.add("prop4");
    inputProps.add("prop5");
    inputProps.add("prop6");
    inputProps.add("prop7");
    inputProps.add("prop8");
    inputProps.add("prop9");
    inputProps.add("prop10");
    inputProps.add("prop11");
    inputProps.add("prop12");
    inputProps.add("prop13");
    inputProps.add("prop14");
    inputProps.add("prop15");
    inputProps.add("prop16");
    inputProps.add("prop17");
    inputProps.add("prop18");
    inputProps.add("prop19");
    inputProps.add("prop20");
    inputProps.add("prop21");
    inputProps.add("prop22");
    inputProps.add("prop23");
    inputProps.add("prop24");
    inputProps.add("prop25");
    inputProps.add("prop26");
    inputProps.add("prop27");
    inputProps.add("prop28");
    inputProps.add("prop29");
    inputProps.add("prop30");
    inputProps.add("prop31");
    inputProps.add("prop32");
    inputProps.add("prop33");
    inputProps.add("prop34");
    inputProps.add("prop35");
    inputProps.add("prop36");
    inputProps.add("prop37");
    inputProps.add("prop38");
    inputProps.add("prop39");
    inputProps.add("prop40");
    inputProps.add("prop41");
    inputProps.add("prop42");
    inputProps.add("prop43");
    inputProps.add("prop44");
    inputProps.add("prop45");
    inputProps.add("prop46");
    inputProps.add("prop47");
    inputProps.add("prop48");
    inputProps.add("prop49");
    inputProps.add("prop50");
    inputProps.add("prop51");
    inputProps.add("prop52");
    inputProps.add("prop53");
    inputProps.add("prop54");
    inputProps.add("prop55");
    inputProps.add("prop56");
    inputProps.add("prop57");
    inputProps.add("prop58");
    inputProps.add("prop59");
    inputProps.add("prop60");
    inputProps.add("prop61");
    inputProps.add("prop62");
    inputProps.add("prop63");
    inputProps.add("prop64");
    inputProps.add("prop65");
    inputProps.add("prop66");
    inputProps.add("prop67");
    inputProps.add("prop68");
    inputProps.add("prop69");
    inputProps.add("prop70");
    inputProps.add("prop71");
    inputProps.add("prop72");
    inputProps.add("prop73");
    inputProps.add("prop74");
    inputProps.add("prop75");
    return inputProps;
  }//Known prop Values
  
  //Processing Input prop MAP 
  public ArrayList<String> proccessingInput(HashMap<String, String> map)
  {
    ArrayList<String> str = new ArrayList<String>(75);
    ArrayList<String> inputProps = knownProps();
    for (int i = 0; i < inputProps.size(); i++) {
      if (map.containsKey(((String)inputProps.get(i)).toLowerCase())) {
        str.add(i, map.get(inputProps.get(i)));
      }//if prop value found get that
      else {
        str.add(i, BLANK);
      }//if prop is empty put blank
    }
    return str;
  }// Processing Function Ends
  
  public Tuple exec(Tuple input)
    throws IOException
  {
	  if (input.get(0) == null || input.get(0).equals("")) {
		  for (int i = 0; i < TOTALPROPS; i++) {
		        this.tuple.set(i, "");
		        }//If input is null adding empty string to each prop value
		      	return this.tuple;
	}
    HashMap<String, String> values = (HashMap)input.get(0);
	    if ((values == null) || (values.isEmpty())) {
	      for (int i = 0; i < TOTALPROPS; i++) {
	        this.tuple.set(i, "");
	        }//If input is null adding empty string to each prop value
	      	return this.tuple;
	    }
    ArrayList<String> result = proccessingInput(values);
    for (int i = 0; i < result.size(); i++) {
      this.tuple.set(i, result.get(i));
    }//map all output prop values to tuple 
    return this.tuple;
  }
  
  public Schema outputSchema(Schema input)
  {
    Schema tupleSchema = new Schema();
    for (int i = 1; i <= TOTALPROPS; i++) {
      tupleSchema.add(new Schema.FieldSchema(PROP_NAME + i, (byte)55));
    }
    try
    {
      return new Schema(new Schema.FieldSchema(getSchemaName(getClass().getName().toLowerCase(), input), tupleSchema, (byte)110));
    }
    catch (FrontendException e)
    {
      e.printStackTrace();
    }
    return null;
  }
}
