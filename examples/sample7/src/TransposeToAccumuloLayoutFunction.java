package cascading.accumulo;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class TransposeToAccumuloLayoutFunction extends BaseOperation implements Function {
	
	private String colFamily;
	private int keyIndex =0;
	private String colVisibility = "";

	public TransposeToAccumuloLayoutFunction(Fields fieldDeclaration, String colFamily, String colVisibility,int keyZeroBasedIndex)
	{
		this.colFamily=colFamily;
		this.keyIndex=keyZeroBasedIndex;
		this.colVisibility = colVisibility;

	}
	
	public void operate( FlowProcess flowProcess, FunctionCall functionCall )
	  {
		  TupleEntry inputTupleEntry = functionCall.getArguments();
		  int fieldCount = inputTupleEntry.getFields().size();
		  long colTimestamp = System.currentTimeMillis();
		  
		  String colRowKey = inputTupleEntry.getString(keyIndex);
		  
		  for(int i=0; i < fieldCount; i++)
		  {
			  if(i != keyIndex)
			  {
				  String colValue = inputTupleEntry.getString( i );
				  String colQualifier = inputTupleEntry.getFields().get(i).toString();
				  Tuple resultTuple = new Tuple();
				  resultTuple.add(colRowKey);
				  resultTuple.add(colFamily);
				  resultTuple.add(colQualifier);
				  resultTuple.add(colVisibility);
				  resultTuple.add(colTimestamp);
				  resultTuple.add(colValue);
				  functionCall.getOutputCollector().add( resultTuple );
			  }
		  }
	  }

}
