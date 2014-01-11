The sample program in the “src” directory demonstrates how to read data in Accumulo and write it to HDFS using the Cascading extensions for Accumulo.

Run the program
============================================

hadoop jar cascadingSamples/accumuloTapSample/jars/readAccumuloWriteToHDFSAsIsSample.jar ReadAccumuloWriteToHDFSAsIsSample "accumulo://employeeDB_employee?instance=indra&user=root&password=IAdler&zookeepers=cdh-dn01:2181" "rowKeyRangeStart=100004&rowKeyRangeEnd=99999" "cascadingSamples/Output-ReadAccumuloWriteToHDFSAsIsSample"

Verify
============================================

a) Check for the output file

hadoop fs -ls -R cascadingSamples/Output-ReadAccumuloWriteToHDFSAsIsSample/part* | awk '{print $8}'
cascadingSamples/Output-ReadAccumuloWriteToHDFSAsIsSample/part-00000

b) Get record count in Accumulo

From Linux command line, run the command-
./bin/accumulo shell -u root -p IAdler -e  "scan -np -t employeeDB_employee" | wc -l 
1348099
(ignore the one extra record)

c) Get record count in HDFS

hadoop fs -cat cascadingSamples/Output-ReadAccumuloWriteToHDFSAsIsSample/part* | wc -l
1348098

d) Look up a record in Accumulo
root@indra employeeDB_employee> scan -b 99999 -e 99999/0
99999 employee:DOB []    1959-10-09
99999 employee:DeptID []    d004
99999 employee:FName []    Gila
99999 employee:Gender []    M
99999 employee:HireDate []    1992-04-20
99999 employee:LName []    Lammel

e) Compare record in d) with 
hadoop fs -cat cascadingSamples/Output-ReadAccumuloWriteToHDFSAsIsSample/part* | grep ^99999
99999	employee	DOB		1389386340039	1959-10-09
99999	employee	DeptID		1389386340039	d004
99999	employee	FName		1389386340039	Gila
99999	employee	Gender		1389386340039	M
99999	employee	HireDate		1389386340039	1992-04-20
99999	employee	LName		1389386340039	Lammel


