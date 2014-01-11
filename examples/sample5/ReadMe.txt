The following are prep tasks and commands to run the sample program under the src directory.  The program demonstrates how to read data in Accumulo and write it back to Accumulo using the Cascading extensions for Accumulo.

Create table in Accumulo
============================================

root@indra employeeDB_employee> createtable employeeCopy
root@indra employeeCopy>

Run the program
============================================

hadoop jar cascadingSamples/accumuloTapSample/jars/readAccumuloWriteToAccumuloSample.jar ReadAccumuloWriteToAccumuloSample "accumulo://employeeDB_employee?instance=indra&user=root&password=sun123123&zookeepers=cdh-dn01:2181" "rowKeyRangeStart=100004&rowKeyRangeEnd=99999" "accumulo://employeeCopy?instance=indra&user=root&password=sun123123&zookeepers=cdh-dn01:2181"

