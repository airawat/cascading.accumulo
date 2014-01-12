package cascading.accumulo;

import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.MultiSourceTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;


public class TransposeToAccumuloLayoutSample {
	
	

	public static void main(String[] args) throws Exception {
		//{{
		// JOB RELATED
		JobConf jobConf = new JobConf();
		jobConf.setJarByClass( TransposeToAccumuloLayoutSample.class);
		
		Properties properties = AppProps.appProps()
		  .setName( "TransposeToAccumuloLayoutSample" )
		  .setVersion( "1.0.0" )
		  .buildProperties( jobConf);
		//}}

		//{{
		// ARGUMENTS
		String inputPath = args[1];
		String errorPath = args[2];
		String outputHDFSPath = args[3];
		//}}
		
		Fields inputFields = new Fields("EmpID","DOB","FName","LName","Gender","HireDate","DeptID");
		Fields outputFields = new Fields("rowID", "colF", "colQ", "colVis", "colTimestamp", "colVal");
		
		
		// {{
		// SOURCE tap - HDFS
		TextDelimited sourceScheme = new TextDelimited(inputFields,",");
		GlobHfs sourceFilesGlob = new GlobHfs(sourceScheme, inputPath);
		Tap sourceTapHDFS = new MultiSourceTap(sourceFilesGlob);
		
		HadoopFlowProcess hfp = new HadoopFlowProcess(jobConf);
		
		//}}

		// {{
		// TRAP tap - HDFS
		TextLine sinkTextLineScheme = new TextLine();
		sinkTextLineScheme.setNumSinkParts(1);
		Tap sinkTapTrapHDFS = new Hfs(sinkTextLineScheme, errorPath,
				SinkMode.REPLACE);
		// }}
		
		// {{
		// SINK tap - HDFS
		Tap sinkTapHDFS = new Hfs(new TextLine(), outputHDFSPath,SinkMode.REPLACE);
		// }}

		// {{
		// PIPE
		Pipe readAndFlattenPipe = new Each("import", inputFields, new TransposeToAccumuloLayoutFunction(inputFields,"employee","",0),Fields.RESULTS  );
		// }}

		// {{
		// EXECUTE
		Flow flow = new HadoopFlowConnector(properties).connect(
				"ReadSourceTransposeAndSink",sourceTapHDFS, sinkTapHDFS, sinkTapTrapHDFS,readAndFlattenPipe);
		flow.complete();
		// }}
		
		
	}

}
