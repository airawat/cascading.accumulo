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

public class TableOperationsSample {

	public static void main(String[] args) throws Exception {

		// {{
		// JOB
		JobConf conf = new JobConf();
		conf.setJarByClass(TableOperationsSample.class);
		Properties properties = AppProps.appProps()
				.setName("TableOperationsSample").setVersion("1.0.0")
				.buildProperties(conf);

		// }}

		// {{
		// ARGUMENTS - parse key arguments
		String accumuloURI = args[1].toString();
		String tableOperation = args[2].toString();

		// }}

		// {{
		// TABLE OPERATIONS
		if (tableOperation.equals("createTable")) {

			AccumuloTap accumuloTapTblOps;
			accumuloTapTblOps = new AccumuloTap(accumuloURI,
					new AccumuloScheme());

			boolean tblOpStatus = accumuloTapTblOps.createResource(conf);

			System.out.println("The table " + " was "
					+ (tblOpStatus ? "created successfully!" : "not created!"));
			
		} else if (tableOperation.equals("createTableWithSplits")) {

			String tableSplits = args[3].toString().trim();
			conf.set("TableSplits", tableSplits);

			AccumuloTap accumuloTapTblOps;
			accumuloTapTblOps = new AccumuloTap(accumuloURI,
					new AccumuloScheme());

			boolean tblOpStatus = accumuloTapTblOps.createResource(conf);

			System.out.println("The table " + " was "
					+ (tblOpStatus ? "created successfully!" : "not created!"));

		} else if (tableOperation.equals("checkIfTableExists")) {

			AccumuloTap accumuloTapTblOps;
			accumuloTapTblOps = new AccumuloTap(accumuloURI,
					new AccumuloScheme());

			boolean tblOpStatus = accumuloTapTblOps.resourceExists(conf);

			System.out.println("The table "
					+ (tblOpStatus ? " exists!" : " does not exist!"));

		} else if (tableOperation.equals("deleteTable")) {

			AccumuloTap accumuloTapTblOps;
			accumuloTapTblOps = new AccumuloTap(accumuloURI,
					new AccumuloScheme());

			boolean tblOpStatus = accumuloTapTblOps.deleteResource(conf);

			System.out.println("The table " + " was "
					+ (tblOpStatus ? "deleted successfully!" : "not deleted!"));

		} else {
			System.out.println("Could not match table operation - "
					+ tableOperation);
			System.out
					.println("---------------------------------------------------------------");
			System.out.println("Invalid table operation parameter!");
			System.out.println("Accepted parameters are - ");
			System.out.println("createTable");
			System.out.println("createTableWithSplits <<CSV list of split names>>");
			System.out.println("checkIfTableExists");
			System.out.println("deleteTable");
		}

		// }}
	}

}
