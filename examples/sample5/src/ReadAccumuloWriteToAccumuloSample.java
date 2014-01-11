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
import cascading.tuple.TupleEntryIterator;

public class ReadAccumuloWriteToAccumuloSample {

	public static void main(String[] args) {

		// {{
		// ARGUMENTS
		String accumuloSourceURI = args[1].toString();
		String accumuloSourceScheme = args[2].toString();
		String accumuloSinkURI = args[3].toString();
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
			AccumuloTap sourceTapAccumulo = new AccumuloTap(accumuloSourceURI,
					new AccumuloScheme(accumuloSourceScheme),
					SinkMode.REPLACE);

			/*
			TupleEntryIterator tei = sourceTapAccumulo.openForRead(hfp);
			while (tei.hasNext()) {
				System.out.println(tei.next());
			}
			tei.close();
			*/
			// }}

			// {{
			// SINK tap - Accumulo
			Tap sinkTapAccumulo = new AccumuloTap(accumuloSinkURI,
					new AccumuloScheme(), SinkMode.UPDATE);
			// }}

			// {{
			// PIPE
			Pipe readPipe = new Each("read", new Identity());
			//..........Add any transformation here..........
			// }}

			// {{
			// EXECUTE
			// Connect the taps, pipes, etc., into a flow & execute
			Flow flow = new HadoopFlowConnector(properties).connect(
					sourceTapAccumulo, sinkTapAccumulo, readPipe);
			flow.complete();
			// }}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
