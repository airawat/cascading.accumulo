package cascading.accumulo;

import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;

public class ReadAccumuloWriteToHDFSAsIsSample {

	public static void main(String[] args) throws Exception {
		
		// {{
		// ARGUMENTS
		String accumuloConnectionString = args[1].toString();
		String accumuloQueryCriteria = args[2].toString();
		String outputHDFSPath = args[3].toString();
		// }}
		
		// {{
		// JOB 
		JobConf jobConf = new JobConf();
		jobConf.setJarByClass(ReadAccumuloWriteToHDFSAsIsSample.class);

		Properties properties = AppProps.appProps()
				.setName("ReadAccumuloWriteToHDFSAsIsSample").setVersion("1.0.0")
				.buildProperties(jobConf);

		// }}

		// {{
		// SOURCE tap - Accumulo
		HadoopFlowProcess hfp = new HadoopFlowProcess(jobConf);
		AccumuloTap sourceTapAccumulo = new AccumuloTap(accumuloConnectionString,
				new AccumuloScheme(accumuloQueryCriteria),SinkMode.REPLACE);

		// }}

		// {{
		// SINK tap - HDFS
		Tap sinkTapHDFS = new Hfs(new TextLine(), outputHDFSPath,
				SinkMode.REPLACE);
		// }}

		// {{
		// PIPE
		Pipe readPipe = new Each( "read", new Identity(sourceTapAccumulo.getDefaultAccumuloFields()) );
		// }}
		
		// {{
		// EXECUTE
		Flow flow = new HadoopFlowConnector( properties ).connect(
				sourceTapAccumulo, sinkTapHDFS, readPipe);
		flow.complete();
		// }}

		
	}

}
