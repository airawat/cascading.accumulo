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


public class ReadFormattedFileWriteToAccumulo {
	
	public static void main(String[] args) throws Exception {
		//{{
		// JOB 
		JobConf jobConf = new JobConf();
		jobConf.setJarByClass( ReadFormattedFileWriteToAccumulo.class);
		
		Properties properties = AppProps.appProps()
		  .setName( "ReadFormattedFileWriteToAccumulo" )
		  .setVersion( "1.0.0" )
		  .buildProperties( jobConf);
		//}}

		//{{
		// ARGUMENTS
		String inputPath = args[1];
		String errorPath = args[2];
		String accumuloUri = args[3];
		//}}
		
		// {{
		// SOURCE tap - HDFS
		
		TextDelimited sourceScheme = new TextDelimited(new Fields("rowID","colF","colQ","colVis","colTimestamp","colVal"),"\t");

		GlobHfs sourceFilesGlob = new GlobHfs(sourceScheme, inputPath);

		HadoopFlowProcess hfp = new HadoopFlowProcess(jobConf);
		Tap sourceTapHDFS = new MultiSourceTap(sourceFilesGlob);
		
		/*
		TupleEntryIterator tei = sourceTapHDFS.openForRead(hfp);
		if(!tei.equals(null))
		{
			while(tei.hasNext())
			{
				System.out.println( tei.next() );
			}
			tei.close();
		}
		*/
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
		Pipe readPipe = new Each( "read", new Identity() );
		// }}

		
		
		// {{
		// SINK tap - Accumulo
		Tap sinkTapAccumulo = new AccumuloTap(accumuloUri,new AccumuloScheme(), SinkMode.UPDATE);
		// }}
		
		
		// {{
		// EXECUTE
		Flow flow = new HadoopFlowConnector(properties).connect(
				"SourceHDFS_Trap-HDFS_Sink-Accumulo",sourceTapHDFS, sinkTapAccumulo, sinkTapTrapHDFS,readPipe);
		flow.complete();
		// }}
		
		
	}

}
