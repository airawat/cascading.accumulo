package cascading.accumulo;

import java.util.Properties;
import org.apache.hadoop.mapred.JobConf;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Identity;
import cascading.pipe.Each;
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

public class ReadHDFSTransposeWriteToAccumulo {

	public static void main(String[] args) throws Exception {

		//{{
		// JOB 
		JobConf jobConf = new JobConf();
		jobConf.setJarByClass( ReadHDFSTransposeWriteToAccumulo.class);
		
		Properties properties = AppProps.appProps()
		  .setName( "ReadHDFSTransposeWriteToAccumulo" )
		  .setVersion( "1.0.0" )
		  .buildProperties( jobConf);
		//}}

		//{{
		// ARGUMENTS
		String inputPath = args[1];
		String errorPath = args[2];
		String accumuloConnectionString = args[3];
		//}}
		
		Fields inputFields = new Fields("EmpID","DOB","FName","LName","Gender","HireDate","DeptID");
		Fields outputFields = new Fields("rowID", "colF", "colQ", "colVis", "colTimestamp", "colVal");
		
		// {{
		// SOURCE tap - HDFS
		TextDelimited sourceScheme = new TextDelimited(inputFields,",");
		GlobHfs sourceFilesGlob = new GlobHfs(sourceScheme, inputPath);

		HadoopFlowProcess hfp = new HadoopFlowProcess(jobConf);
		Tap sourceTapHDFS = new MultiSourceTap(sourceFilesGlob);
		

		//}}

		// {{
		// TRAP tap - HDFS
		TextLine sinkTextLineScheme = new TextLine();
		sinkTextLineScheme.setNumSinkParts(1);
		Tap sinkTapTrapHDFS = new Hfs(sinkTextLineScheme, errorPath,
				SinkMode.REPLACE);
		// }}

		// {{
		// PIPE
		//
		Pipe readAndFlattenPipe = new Each("import", inputFields, new TransposeToAccumuloLayoutFunction(inputFields,"employee","",0),Fields.RESULTS  );
		readAndFlattenPipe = new Each(readAndFlattenPipe, new Identity(outputFields));
		// }}
		
		// {{
		// SINK tap - Accumulo
		Tap sinkTapAccumulo = new AccumuloTap(accumuloConnectionString,new AccumuloScheme(), SinkMode.UPDATE);
		// }}
		
		
		// {{
		// EXECUTE
		// Connect the taps, pipes, etc., into a flow & execute
		
		Flow flow = new HadoopFlowConnector(properties).connect(
				"SourceHDFS_Trap-HDFS_Sink-Accumulo",sourceTapHDFS, sinkTapAccumulo, sinkTapTrapHDFS,readAndFlattenPipe);
		
		flow.writeDOT( "cascadingSamples/dot/ReadHDFSTransposeWriteToAccumulo.dot" );
		flow.complete();
		// }}
				
		
	}

}
