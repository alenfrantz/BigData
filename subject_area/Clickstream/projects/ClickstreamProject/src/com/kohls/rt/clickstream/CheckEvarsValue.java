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
 *Description : This UDF Checks Input EVARS Map 
				Exacts each EVAR out of it and assign its value	if found otherwise 
				put blank.			
 */

public class CheckEvarsValue
  extends EvalFunc<Tuple>
{
  private TupleFactory factory = TupleFactory.getInstance();
  private Tuple tuple = this.factory.newTuple(75);
  private static final int TOTALEVARS = 75;//Total Number Of EVARS
  private final String BLANK="";
  private final String EVAR_NAME="evar";
  
  //Adding known EVAR input values to ArrayList
  public ArrayList<String> knownEVars()
  {
    ArrayList<String> knownEVarValues = new ArrayList<String>(75);
    
    knownEVarValues.add("evar1");
    knownEVarValues.add("evar2");
    knownEVarValues.add("evar3");
    knownEVarValues.add("evar4");
    knownEVarValues.add("evar5");
    knownEVarValues.add("evar6");
    knownEVarValues.add("evar7");
    knownEVarValues.add("evar8");
    knownEVarValues.add("evar9");
    knownEVarValues.add("evar10");
    knownEVarValues.add("evar11");
    knownEVarValues.add("evar12");
    knownEVarValues.add("evar13");
    knownEVarValues.add("evar14");
    knownEVarValues.add("evar15");
    knownEVarValues.add("evar16");
    knownEVarValues.add("evar17");
    knownEVarValues.add("evar18");
    knownEVarValues.add("evar19");
    knownEVarValues.add("evar20");
    knownEVarValues.add("evar21");
    knownEVarValues.add("evar22");
    knownEVarValues.add("evar23");
    knownEVarValues.add("evar24");
    knownEVarValues.add("evar25");
    knownEVarValues.add("evar26");
    knownEVarValues.add("evar27");
    knownEVarValues.add("evar28");
    knownEVarValues.add("evar29");
    knownEVarValues.add("evar30");
    knownEVarValues.add("evar31");
    knownEVarValues.add("evar32");
    knownEVarValues.add("evar33");
    knownEVarValues.add("evar34");
    knownEVarValues.add("evar35");
    knownEVarValues.add("evar36");
    knownEVarValues.add("evar37");
    knownEVarValues.add("evar38");
    knownEVarValues.add("evar39");
    knownEVarValues.add("evar40");
    knownEVarValues.add("evar41");
    knownEVarValues.add("evar42");
    knownEVarValues.add("evar43");
    knownEVarValues.add("evar44");
    knownEVarValues.add("evar45");
    knownEVarValues.add("evar46");
    knownEVarValues.add("evar47");
    knownEVarValues.add("evar48");
    knownEVarValues.add("evar49");
    knownEVarValues.add("evar50");
    knownEVarValues.add("evar51");
    knownEVarValues.add("evar52");
    knownEVarValues.add("evar53");
    knownEVarValues.add("evar54");
    knownEVarValues.add("evar55");
    knownEVarValues.add("evar56");
    knownEVarValues.add("evar57");
    knownEVarValues.add("evar58");
    knownEVarValues.add("evar59");
    knownEVarValues.add("evar60");
    knownEVarValues.add("evar61");
    knownEVarValues.add("evar62");
    knownEVarValues.add("evar63");
    knownEVarValues.add("evar64");
    knownEVarValues.add("evar65");
    knownEVarValues.add("evar66");
    knownEVarValues.add("evar67");
    knownEVarValues.add("evar68");
    knownEVarValues.add("evar69");
    knownEVarValues.add("evar70");
    knownEVarValues.add("evar71");
    knownEVarValues.add("evar72");
    knownEVarValues.add("evar73");
    knownEVarValues.add("evar74");
    knownEVarValues.add("evar75");
    
    return knownEVarValues;
  }//Known EVAR Values
  
  //Processing Input EVAR MAP
  public ArrayList<String> proccessingInput(HashMap<String, String> map)
  {
    ArrayList<String> str = new ArrayList<String>(75);
    ArrayList<String> inputEvars = knownEVars();
    for (int i = 0; i < inputEvars.size(); i++) {
      if (map.containsKey(((String)inputEvars.get(i)).toLowerCase())) {
        str.add(i, map.get(inputEvars.get(i)));
      }//if evar value found get that
      else {
        str.add(i, BLANK);
      }//if evar is empty put blank
    }
    return str;
  }// Processing Function Ends
  
  
  public Tuple exec(Tuple input)
    throws IOException
  {

    if (input.get(0) == null || input.get(0).equals("")) {
    	for (int i = 0; i < TOTALEVARS; i++) {
	        this.tuple.set(i, BLANK);
	        }//If input is null adding empty string to each evar value
	      	return this.tuple;
    }
    HashMap<String, String> inputEvars = (HashMap)input.get(0);
    //take input tuple into arrayList and send that to processing function
    ArrayList<String> outputEvars = proccessingInput(inputEvars);
    for (int i = 0; i < outputEvars.size(); i++) {
      this.tuple.set(i, outputEvars.get(i));
    }//map all output evar values to tuple 
    return this.tuple;
  }
  
  public Schema outputSchema(Schema input)
  {
    Schema tupleSchema = new Schema();
    for (int i = 1; i <= 75; i++) {
      tupleSchema.add(new Schema.FieldSchema(EVAR_NAME + i, (byte)55));
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
