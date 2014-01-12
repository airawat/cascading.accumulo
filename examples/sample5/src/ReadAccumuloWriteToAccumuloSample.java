package cascading.accumulo;

import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class ReadAccumuloWriteToAccumuloSample {

	public static void main(String[] args) {

		// {{
		// ARGUMENTS
		String accumuloSourceConnectionString = args[1].toString();
		String accumuloQueryCriteria = args[2].toString();
		String accumuloSinkConnectionString = args[3].toString();
		String errorPath = args[4].toString();
		// }}

		// {{
		// JOB
		JobConf jobConf = new JobConf();
		jobConf.setJarByClass(ReadAccumuloWriteToAccumuloSample.class);

		Properties properties = AppProps.appProps()
				.setName("ReadAccumuloWriteToAccumuloSample")
				.setVersion("1.0.0").buildProperties(jobConf);

		// }}

		try {
			// {{
			// SOURCE tap - Accumulo
			HadoopFlowProcess hfp = new HadoopFlowProcess(jobConf);
			AccumuloTap sourceTapAccumulo = new AccumuloTap(accumuloSourceConnectionString,
					new AccumuloScheme(accumuloQueryCriteria));
			// }}

			// {{
			// SINK tap - Accumulo
			AccumuloTap sinkTapAccumulo = new AccumuloTap(accumuloSinkConnectionString,
					new AccumuloScheme(), SinkMode.UPDATE);
			// }}
			
			// {{
			// TRAP tap - HDFS
			TextLine sinkTextLineScheme = new TextLine();
			sinkTextLineScheme.setNumSinkParts(1);
			Tap sinkTapTrapHDFS = new Hfs(sinkTextLineScheme, errorPath,
					SinkMode.REPLACE);
			// }}

			// {{
			// PIPE
			Pipe readPipe = new Each("read", new Identity(sinkTapAccumulo.getDefaultAccumuloFields()));
			//..........Add any transformation here..........
			// }}

			// {{
			// EXECUTE
			Flow flow = new HadoopFlowConnector(properties).connect("SourceAccumulo-SinkAccumulo",
					sourceTapAccumulo, sinkTapAccumulo,sinkTapTrapHDFS, readPipe);
			flow.complete();

			// }}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
