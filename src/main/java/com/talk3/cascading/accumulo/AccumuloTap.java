package com.talk3.cascading.accumulo;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloTap extends Tap<JobConf, RecordReader, OutputCollector>
        implements java.io.Serializable {

    private static final Logger LOG = LoggerFactory
            .getLogger(AccumuloTap.class);

    protected String tableName;
    private String resourceID;

    protected String accumuloInstanceName;
    protected String accumuloZookeeperQuorum;
    protected String accumuloUserName;
    private String accumuloPassword;
    protected transient AuthenticationToken accumuloAuthToken;
    protected Instance accumuloInstance;
    private Connector accumuloConnector;
    protected Authorizations accumuloAuthorizations;
    protected String auths;
    protected int maxWriteThreads = 10;
    protected long maxMutationBufferSize = 10 * 1000 * 1000;
    protected int maxLatency = 10 * 1000;

    private RecordReader<Key, Value> reader;

    private Fields accumuloFields = new Fields("rowID", "colF", "colQ", "colVis",
            "colTimestamp", "colVal");

    private String tapUUID;

    private Configuration conf;
    List<Pair<Text, Text>> columnFamilyColumnQualifierPairs = new LinkedList<Pair<Text, Text>>();

    // Constructor
    public AccumuloTap(String accumuloConnectionString,
            AccumuloScheme accumuloScheme) throws Exception {

        super(accumuloScheme, SinkMode.REPLACE);
        setResourceFromConnectionString(accumuloConnectionString);
    }

    // Constructor
    public AccumuloTap(String accumuloConnectionString,
            AccumuloScheme accumuloScheme, SinkMode sinkMode) throws Exception {

        super(accumuloScheme, sinkMode);
        setResourceFromConnectionString(accumuloConnectionString);

    }

	// Method setResourceFromConnectionString initializes Accumulo related data
    // members
    private void setResourceFromConnectionString(String accumuloConnectionString) throws IOException {
        try {
            this.tapUUID = UUID.randomUUID().toString();

            if (!accumuloConnectionString.startsWith("accumulo://")) {
                try {
                    LOG.error("Bad connection string!  Expected format=accumulo://table1?instance=myinstance&user=root&password=secret&zookeepers=127.0.0.1:2181&auths=PRIVATE,PUBLIC");
                    throw new Exception("Bad connection string.");
                } catch (Exception e) {
                    LOG.error(
                            "Error in AccumuloTap.setResourceFromConnectionString",
                            e);
                    e.printStackTrace();
                }
            }

            String[] urlParts = accumuloConnectionString.split("\\?");
            if (urlParts.length > 1) {
                for (String param : urlParts[1].split("&")) {
                    String[] pair = param.split("=");
                    if (pair[0].equals("instance")) {
                        accumuloInstanceName = pair[1];
                    } else if (pair[0].equals("user")) {
                        accumuloUserName = pair[1];
                    } else if (pair[0].equals("password")) {
                        accumuloPassword = pair[1];
                    } else if (pair[0].equals("zookeepers")) {
                        accumuloZookeeperQuorum = pair[1];
                    } else if (pair[0].equals("auths")) {
                        auths = pair[1];
                    } else if (pair[0].equals("write_buffer_size_bytes")) {
                        maxMutationBufferSize = Long.parseLong(pair[1]);
                    } else if (pair[0].equals("write_threads")) {
                        maxWriteThreads = Integer.parseInt(pair[1]);
                    } else if (pair[0].equals("write_latency_ms")) {
                        maxLatency = Integer.parseInt(pair[1]);
                    }
                }
            }
            String[] parts = urlParts[0].split("/+");
            if (parts.length > 0) {
                tableName = parts[1];
            }

            if (auths == null || auths.equals("")) {
                accumuloAuthorizations = new Authorizations();
            } else {
                accumuloAuthorizations = new Authorizations(auths.split(","));
            }

            accumuloAuthToken = new PasswordToken(accumuloPassword);
            this.resourceID = accumuloConnectionString;

        } catch (Exception e) {
            throw new IOException("Bad parameter; Format expected is: accumulo://table1?instance=myinstance&user=root&password=secret&zookeepers=CSVListofZooserver:portNum&auths=PRIVATE,PUBLIC&write_threads=3");
        }
    }

	// Method initializeAccumuloConnector
    // Used only for table operations
    private void initializeAccumuloConnector() {

        if (accumuloConnector == null) {
            accumuloInstance = new ZooKeeperInstance(accumuloInstanceName,
                    accumuloZookeeperQuorum);

            try {
                accumuloConnector = accumuloInstance.getConnector(
                        accumuloUserName, accumuloAuthToken);
            } catch (AccumuloException ae) {
                LOG.error("Error in AccumuloTap.initializeAccumuloConnector",
                        ae);
                ae.printStackTrace();
            } catch (AccumuloSecurityException ase) {
                LOG.error("Error in AccumuloTap.initializeAccumuloConnector",
                        ase);
                ase.printStackTrace();
            }

        }
    }

    // Method getTableName returns the table name
    public String getTableName() {
        return this.tableName;
    }

    // Method deleteTable deletes table if it exists
    private boolean deleteTable() {

        initializeAccumuloConnector();
        TableOperations ops = accumuloConnector.tableOperations();

        if (ops.exists(this.tableName)) {
            try {
                ops.delete(this.tableName);
            } catch (AccumuloException e) {
                LOG.error("Error in AccumuloTap.deleteTable", e);
                e.printStackTrace();
                return false;
            } catch (AccumuloSecurityException ase) {
                LOG.error("Error in AccumuloTap.deleteTable", ase);
                ase.printStackTrace();
                return false;
            } catch (TableNotFoundException tnfe) {
                LOG.error("Error in AccumuloTap.deleteTable", tnfe);
                tnfe.printStackTrace();
                return false;
            }
            return true;
        }
        return false;
    }

    public Fields getDefaultAccumuloFields() {
        return accumuloFields;
    }

    // Method tableExists returns true if table exists
    private boolean tableExists() {

        initializeAccumuloConnector();
        TableOperations ops = accumuloConnector.tableOperations();
        if (ops.exists(this.tableName)) {
            return true;
        }
        return false;
    }

    // Method createTable creates table and returns true if it succeeds
    private boolean createTable() {
        return createTable(true);
    }

    // Method createTable creates table and returns true if it succeeds
    private boolean createTable(boolean checkIfExists) {

        initializeAccumuloConnector();
        TableOperations ops = accumuloConnector.tableOperations();

        if (checkIfExists) {
            if (ops.exists(this.tableName)) {
                return false;
            }
        }
        try {
            ops.create(tableName);
        } catch (AccumuloException e) {
            LOG.error("Error in AccumuloTap.createTable", e);
            e.printStackTrace();
        } catch (AccumuloSecurityException e) {
            LOG.error("Error in AccumuloTap.createTable", e);
            e.printStackTrace();
        } catch (TableExistsException e) {
            LOG.error("Error in AccumuloTap.createTable", e);
            e.printStackTrace();
        }
        return true;

    }

	// Method createTable creates table with splits and returns true if it
    // succeeds
    private boolean createTable(String splits) {

        initializeAccumuloConnector();
        TableOperations ops = accumuloConnector.tableOperations();

        String[] splitArray = splits.split(",");

        if (!ops.exists(this.tableName)) {
            createTable(false);
        }

        // Add splits
        TreeSet<Text> intialPartitions = new TreeSet<Text>();
        for (String split : splitArray) {
            intialPartitions.add(new Text(split.trim()));
        }

        try {
            accumuloConnector.tableOperations().addSplits(this.tableName,
                    intialPartitions);
        } catch (TableNotFoundException tnfe) {
            LOG.error("Error in AccumuloTap.createTable", tnfe);
            tnfe.printStackTrace();
        } catch (AccumuloException ae) {
            LOG.error("Error in AccumuloTap.createTable", ae);
            ae.printStackTrace();
        } catch (AccumuloSecurityException ase) {
            LOG.error("Error in AccumuloTap.createTable", ase);
            ase.printStackTrace();
        }

        return true;
    }

    // @Override method createResource creates the underlying resource.
    @Override
    public boolean createResource(JobConf conf) {

        String tableSplits = "";

        if (conf.get("TableSplits") == null) {
            tableSplits = "";
        } else {
            tableSplits = conf.get("TableSplits");
        }

        if (tableSplits.length() == 0) {
            return createTable(true);
        } else {
            return createTable(tableSplits);
        }
    }

	// @Override method getIdentifier returns a String representing the resource
    // this Tap instance represents.
    @Override
    public String getIdentifier() {
        return this.resourceID;
    }

	// @Override method getModifiedTime returns the date this resource was last
    // modified.
    @Override
    public long getModifiedTime(JobConf arg0) throws IOException {
        // TODO: This is a low priority item
        return 0;
    }

    // @Override public method equals
    @Override
    public boolean equals(Object otherObject) {

        if (this == otherObject) {
            return true;
        }
        if (!(otherObject instanceof AccumuloTap)) {
            return false;
        }
        if (!super.equals(otherObject)) {
            return false;
        }

        AccumuloTap otherTap = (AccumuloTap) otherObject;
        EqualsBuilder eb = new EqualsBuilder();
        eb.append(this.getUUID().toString(), otherTap.getUUID().toString());

        return eb.isEquals();
    }

    // @Override public method hashCode
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder();
        hcb.append(getUUID());
        return hcb.toHashCode();
    }

	// @Override public method openForRead opens the resource represented by
    // this Tap instance for reading.
    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess,
            RecordReader recordReader) throws IOException {
        return new HadoopTupleEntrySchemeIterator(flowProcess, this,
                recordReader);
    }

	// @Override public method openForWrite opens the resource represented by
    // this tap instance for writing.
    @Override
    public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess,
            OutputCollector outputCollector) throws IOException {
        AccumuloCollector accumuloCollector = new AccumuloCollector(
                flowProcess, this);
        accumuloCollector.prepare();
        return accumuloCollector;
    }

    // @Override public method deleteResource deletes table if it exists
    @Override
    public boolean deleteResource(JobConf arg0) throws IOException {
        return deleteTable();
    }

    // @Override public method resourceExists checks if the table exists
    @Override
    public boolean resourceExists(JobConf arg0) throws IOException {
        return tableExists();
    }

    // Public method that supports flushing
    public boolean flushResource(JobConf conf) {
        return flushTable(conf);

    }

    // Private method flush
    private boolean flushTable(JobConf conf) {

        initializeAccumuloConnector();
        TableOperations ops = accumuloConnector.tableOperations();

        String rowKeyRangeStart = conf.get("rowKeyRangeStart");
        String rowKeyRangeEnd = conf.get("rowKeyRangeEnd");

        boolean flushRangeAvailable = (rowKeyRangeStart != null && rowKeyRangeEnd != null);

        if (ops.exists(this.tableName)) {
            try {
                try {
                    if (flushRangeAvailable) {
                        ops.flush(this.tableName, new Text(rowKeyRangeStart), new Text(rowKeyRangeEnd), true);
                        return true;
                    } else {
                        ops.flush(this.tableName);
                        return true;
                    }

                } catch (AccumuloSecurityException e) {
                    LOG.error("Error in AccumuloTap.flushResource", e);
                    e.printStackTrace();
                } catch (TableNotFoundException e) {
                    LOG.error("Error in AccumuloTap.flushResource", e);
                    e.printStackTrace();
                }

            } catch (AccumuloException e) {
                LOG.error("Error in AccumuloTap.flushResource", e);
                e.printStackTrace();
            }
        }
        return false;
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
        super.sinkConfInit(process, conf);
    }

    public String getUUID() {
        return tapUUID;
    }

}
