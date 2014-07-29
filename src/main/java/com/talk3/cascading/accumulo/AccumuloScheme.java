package com.talk3.cascading.accumulo;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.util.ConfiguratorBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class AccumuloScheme extends
        Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

    private static final Logger LOG = LoggerFactory
            .getLogger(AccumuloScheme.class);

    private String rowKeyStart = "*";
    private String rowKeyEnd = "*/0";
    private transient List<Pair<Text, Text>> columnFamilyColumnQualifierPairs = new LinkedList<Pair<Text, Text>>();
    private String accumuloSchemeID = "";
    private String rowRegex = null;
    private String columnFamilyRegex = null;
    private String columnQualifierRegex = null;
    private String valueRegex = null;
    private Long maxAge = null;
    private Long minAge = null;
    private Fields accumuloFields = new Fields("rowID", "colF", "colQ", "colVis",
            "colTimestamp", "colVal");

    private boolean addRegexFilter;

    enum TapType {

        SOURCE, SINK
    };

    protected String schemeUUID;

    public AccumuloScheme(String queryCriteria) throws IOException {
        // Expected format
        // columns=colFam1|cq1,colFam2|cq2&rowKeyRangeStart=X0001&rowKeyRangeEnd=X0005&rowRegex=*&columnFamilyRegex=H*
        try {
            initializeUUID();
            accumuloSchemeID = queryCriteria;

            String columns = "";

            if (queryCriteria.length() > 0) {

                String[] queryCriteriaParts = queryCriteria.split("\\&");

                if (queryCriteriaParts.length >= 1) {
                    for (String param : queryCriteria.split("&")) {
                        String[] pair = param.split("=");
                        if (pair[0].equals("columns")) {
                            columns = pair[1];
                        } else if (pair[0].equals("rowKeyRangeStart")) {
                            rowKeyStart = pair[1];
                        } else if (pair[0].equals("rowKeyRangeEnd")) {
                            rowKeyEnd = pair[1];
                        } else if (pair[0].equals("rowRegex")) {
                            addRegexFilter = true;
                            rowRegex = pair[1];
                        } else if (pair[0].equals("columnFamilyRegex")) {
                            addRegexFilter = true;
                            columnFamilyRegex = pair[1];
                        } else if (pair[0].equals("columnQualifierRegex")) {
                            addRegexFilter = true;
                            columnQualifierRegex = pair[1];
                        } else if (pair[0].equals("valueRegex")) {
                            addRegexFilter = true;
                            valueRegex = pair[1];
                        } else if (pair[0].equals("minAge")) {
                          minAge = Long.parseLong(pair[1]);
                        } else if (pair[0].equals("maxAge")) {
                          maxAge = Long.parseLong(pair[1]);
                        }
                    }

                    if (!columns.equals("")) {
                        for (String cfCq : columns.split(",")) {
                            if (cfCq.contains("|")) {
                                String[] cfCqParts = cfCq.split("\\|");
                                columnFamilyColumnQualifierPairs
                                        .add(new Pair<Text, Text>(new Text(cfCqParts[0]),
                                                        new Text(cfCqParts[1])));
                            } else {
                                columnFamilyColumnQualifierPairs
                                        .add(new Pair<Text, Text>(new Text(cfCq),
                                                        null));
                            }
                        }

                    }
                }
            }

        } catch (Exception e) {
            throw new IOException("Bad parameter; Format expected is: columns=colFam1|cq1,colFam1|cq2&rowKeyRangeStart=X0001&rowKeyRangeEnd=X0005&rowRegex=*&columnFamilyRegex=&columnQualifierRegex=*&valueRegex=*");
        }

    }

    public AccumuloScheme() {
        setSourceFields(accumuloFields);
        setSinkFields(accumuloFields);
        initializeUUID();

    }

    private void initializeUUID() {
        this.schemeUUID = UUID.randomUUID().toString();
    }

    // @Override method sourcePrepare is used to initialize resources needed
    // during each call of source(cascading.flow.FlowProcess, SourceCall).
    // Place any initialized objects in the SourceContext so each instance will
    // remain threadsafe.
    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess,
            SourceCall<Object[], RecordReader> sourceCall) {
        Object[] pair = new Object[]{sourceCall.getInput().createKey(),
            sourceCall.getInput().createValue()};
        sourceCall.setContext(pair);
    }

    // @Override method sourceCleanup is used to destroy resources created by
    // sourcePrepare(cascading.flow.FlowProcess, SourceCall).
    @Override
    public void sourceCleanup(FlowProcess<JobConf> flowProcess,
            SourceCall<Object[], RecordReader> sourceCall) {
        sourceCall.setContext(null);
    }

    // @Override method source-
    // Method source will read a new "record" or value from
    // SourceCall.getInput() and populate
    // the available Tuple via SourceCall.getIncomingEntry() and return true on
    // success or false
    // if no more values available.
    @Override
    public boolean source(FlowProcess<JobConf> flowProcess,
            SourceCall<Object[], RecordReader> sourceCall) {

        try {
            Key rowKey = (Key) sourceCall.getContext()[0];
            Value colQValue = (Value) sourceCall.getContext()[1];

            boolean hasNext = sourceCall.getInput().next(rowKey, colQValue);

            if (!hasNext) {
                return false;
            }

            Tuple resultTuple = new Tuple();
            resultTuple.add(rowKey.getRow());
            resultTuple.add(rowKey.getColumnFamily());
            resultTuple.add(rowKey.getColumnQualifier());
            resultTuple.add(rowKey.getColumnVisibility());
            resultTuple.add(rowKey.getTimestamp());
            resultTuple.add(new String(colQValue.get(), "UTF-8"));

            sourceCall.getIncomingEntry().setTuple(resultTuple);

            return true;
        } catch (Exception e) {
            LOG.error("Error in AccumuloScheme.source", e);
            e.printStackTrace();
        }

        return false;
    }

    // Method setFields sets the sourceFields & sinkFields of this Scheme
    // object.
    private void setFields(TapType tapType) {
        if (tapType == TapType.SOURCE) {
            setSourceFields(accumuloFields);
        } else {
            setSinkFields(accumuloFields);
        }

    }

    // @Override method sourceInit initializes this instance as a source.
    // This method is executed client side as a means to provide necessary
    // configuration
    // parameters used by the underlying platform.
    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess,
            Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

        conf.setInputFormat(AccumuloInputFormat.class);
        setFields(TapType.SOURCE);

        AccumuloTap accumuloTap = (AccumuloTap) tap;

        if (false == ConfiguratorBase.isConnectorInfoSet(
                AccumuloInputFormat.class, conf)) {
            try {
                AccumuloInputFormat.setConnectorInfo(conf,
                        accumuloTap.accumuloUserName,
                        accumuloTap.accumuloAuthToken);
                AccumuloInputFormat.setScanAuthorizations(conf,
                        accumuloTap.accumuloAuthorizations);
                AccumuloInputFormat.setInputTableName(conf,
                        accumuloTap.tableName);
                AccumuloInputFormat.setZooKeeperInstance(conf,
                        accumuloTap.accumuloInstanceName,
                        accumuloTap.accumuloZookeeperQuorum);
            } catch (AccumuloSecurityException ex) {
                LOG.error("Error in AccumuloScheme.sourceConfInit", ex);
            }
            if (columnFamilyColumnQualifierPairs.size() > 0) {
                AccumuloInputFormat.fetchColumns(conf,
                        columnFamilyColumnQualifierPairs);
            }

            if (!rowKeyStart.equals("*") && !rowKeyEnd.equals("*/0")) {
                AccumuloInputFormat.setRanges(conf,
                        Collections.singleton(new Range(rowKeyStart, rowKeyEnd)));
            }

            if (addRegexFilter) {
                IteratorSetting regex = new IteratorSetting(50, "regex",
                        RegExFilter.class);
                RegExFilter.setRegexs(regex, this.rowRegex,
                        this.columnFamilyRegex, this.columnQualifierRegex,
                        this.valueRegex, false);
                AccumuloInputFormat.addIterator(conf, regex);
            }

            if (maxAge != null) {
              IteratorSetting maxAgeFilter = new IteratorSetting(48, "maxAgeFilter",
                  AgeOffFilter.class);
              AgeOffFilter.setTTL(maxAgeFilter, maxAge);
              AccumuloInputFormat.addIterator(conf, maxAgeFilter);
            }

            if (minAge != null) {
              IteratorSetting minAgeFilter = new IteratorSetting(49, "minAgeFilter",
                  AgeOffFilter.class);
              AgeOffFilter.setTTL(minAgeFilter, minAge + 1);
              AgeOffFilter.setNegate(minAgeFilter, true);
              AccumuloInputFormat.addIterator(conf, minAgeFilter);
            }
        }
    }

    // @Override method Method sinkInit initializes this instance as a sink.
    // This method is executed client side as a means to provide necessary
    // configuration
    // parameters used by the underlying platform.
    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess,
            Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

        conf.setOutputFormat(AccumuloOutputFormat.class);
        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(Mutation.class);
        setFields(TapType.SINK);

        AccumuloTap accumuloTap = (AccumuloTap) tap;

        if (false == ConfiguratorBase.isConnectorInfoSet(
                AccumuloOutputFormat.class, conf)) {
            try {
                BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
                batchWriterConfig.setMaxLatency(accumuloTap.maxLatency,
                        TimeUnit.MINUTES);
                batchWriterConfig
                        .setMaxMemory(accumuloTap.maxMutationBufferSize);
                batchWriterConfig
                        .setMaxWriteThreads(accumuloTap.maxWriteThreads);
                batchWriterConfig.setTimeout(5, TimeUnit.MINUTES);

                AccumuloOutputFormat.setConnectorInfo(conf,
                        accumuloTap.accumuloUserName,
                        accumuloTap.accumuloAuthToken);
                AccumuloOutputFormat.setDefaultTableName(conf,
                        accumuloTap.tableName);
                AccumuloOutputFormat.setZooKeeperInstance(conf,
                        accumuloTap.accumuloInstanceName,
                        accumuloTap.accumuloZookeeperQuorum);

                AccumuloOutputFormat.setBatchWriterOptions(conf,
                        batchWriterConfig);
            } catch (AccumuloSecurityException ex) {
                LOG.error("Error in AccumuloScheme.sinkConfInit", ex);
            }
        }
    }

    // @Override method Method sink writes out the given Tuple found on
    // SinkCall.getOutgoingEntry()
    // to the SinkCall.getOutput().
    @Override
    public void sink(FlowProcess<JobConf> flowProcess,
            SinkCall<Object[], OutputCollector> sinkCall) throws IOException {

        TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
        OutputCollector outputCollector = sinkCall.getOutput();

        // {{
        Tuple tuple = tupleEntry.selectTuple(this.accumuloFields);
        outputCollector.collect(null, getMutations(tuple));
        // }}
    }

    @SuppressWarnings("deprecation")
    public Mutation getMutations(Tuple tuple) throws IOException {

        Mutation mut = new Mutation(Utils.objToText(tuple.get(0)));
        Text cf = Utils.objToText(tuple.get(1));
        Text cq = Utils.objToText(tuple.get(2));
        Text cv = Utils.objToText(tuple.get(3));
        Value val = new Value(Utils.objToBytes(tuple.get(5)));

        if (cv.getLength() == 0) {
            mut.put(cf, cq, val);
        } else {
            mut.put(cf, cq, new ColumnVisibility(cv), val);
        }

        return (mut);
    }

    private String getIdentifier() {
        return accumuloSchemeID;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof AccumuloScheme)) {
            return false;
        }
        if (!super.equals(other)) {
            return false;
        }

        AccumuloScheme otherScheme = (AccumuloScheme) other;
        EqualsBuilder eb = new EqualsBuilder();
        eb.append(this.getUUID().toString(), otherScheme.getUUID().toString());
        return eb.isEquals();

    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder();
        hcb.append(getUUID());
        return hcb.toHashCode();
    }

    public String getUUID() {
        return schemeUUID;
    }
}
