The sample program in the “src” directory demonstrates how to read data in Accumulo and write it back to Accumulo using the Cascading extensions for Accumulo.

Create table in Accumulo
============================================

employee> createtable employeeCopy


Run the program
============================================

hadoop jar <<pathToJar>>/readAccumuloWriteToAccumuloSample.jar ReadAccumuloWriteToAccumuloSample "accumulo://employee?instance=inst&user=root&password=xxxxx&zookeepers=zooserver:2181" "rowKeyRangeStart=100004&rowKeyRangeEnd=99999" "accumulo://employeeCopy?instance=inst&user=root&password=xxxxx&zookeepers=zooserver:2181"

Verify
============================================
employeeCopy> scan -np

