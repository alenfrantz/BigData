package com.kohls.rt.clickstream;



import java.io.IOException; 
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/***
 * 
 * @author tkmaayr (Onkar Malewadikar)
 *Description : This UDF Returns Y If It Contains Specific String else returns N
 *			    
 */

public class ContainsSpecificStringSequence extends EvalFunc<String>{
final private String N="N";//flag of value N
final private String Y="Y";//flag of value Y
String letterToMatch="";// letter to match string

	public ContainsSpecificStringSequence() {
		letterToMatch="prd-";
	}//with default letter to match 
	
	public ContainsSpecificStringSequence(String inputLetterToMatch){
		if (inputLetterToMatch.isEmpty() || inputLetterToMatch == null || inputLetterToMatch.trim().equals("")) {
			letterToMatch="prd-";// check if input letter string is null then use default input letter 
		} else {
			letterToMatch=inputLetterToMatch;//if input letter contains value then assign to variable
		}
	}//with user defined letter to match string parameter
	
	@Override
	public String exec(Tuple input) throws IOException {
		if (input ==null||input.get(0)==null) {
			return N;
		}else{
			String inputString=input.get(0).toString();
			if(inputString.trim().equals("") || inputString == null){
				return N;//returns N if input string does not contains any value or is null
			}
			else if (inputString.contains(letterToMatch)) {
				return Y;//returns Y if input string contains letter to match string
			} else {
				return N;//returns N if input string does not contains letter to match string
			}
		}
		
	}
	
}
