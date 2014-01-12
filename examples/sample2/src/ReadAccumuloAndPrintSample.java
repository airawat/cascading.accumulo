package cascading.accumulo;

import java.util.Properties;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.TupleEntryIterator;
import cascading.scheme.hadoop.TextDelimited;

public class ReadAccumuloAndPrintSample {

	public static void main(String[] args) throws Exception {
		// {{
		// JOB 
		JobConf jobConf = new JobConf();
		jobConf.setJarByClass(ReadAccumuloAndPrintSample.class);

		Properties properties = AppProps.appProps()
				.setName("SourceAccumuloToStdOutSample").setVersion("1.0.0")
				.buildProperties(jobConf);

		// }}

		// {{
		// ARGUMENTS
		String accumuloConnectionString = args[1].toString();
		String accumuloQueryCriteria = args[2].toString();

		// }}

		// {{
		// READ and PRINT to standard out
		HadoopFlowProcess hfp = new HadoopFlowProcess(jobConf);
		AccumuloTap sourceTapAccumulo = new AccumuloTap(accumuloConnectionString,
				new AccumuloScheme(accumuloQueryCriteria));
		TupleEntryIterator tei = sourceTapAccumulo.openForRead(hfp);
		while (tei.hasNext()) {
			System.out.println(tei.next());
		}
		tei.close();
		// }}

	}

}
