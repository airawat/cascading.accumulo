The following are prep tasks and commands to run the sample program under the src directory.  The program demonstrates how to read formatted data in HDFS and write it to Accumulo.

Format of input 
============================================
[rowID	colFamily	colQualifier	colVis	colTimestamp	colValue]

10003	employee	DOB		1389333718333	1959-12-03
10003	employee	FName		1389333718333	Parto
10003	employee	LName		1389333718333	Bamford
10003	employee	Gender		1389333718333	M
10003	employee	HireDate		1389333718333	1986-08-28
10003	employee	DeptID		1389333718333	d004

Record count of input
============================================
hadoop fs -cat cascadingSamples/data/employeeDB/employees_active  | wc -l
224683


Create table in Accumulo
============================================

root@indra> createtable employeeDB_employee
root@indra employeeDB_employee> 


Run the program
============================================
hadoop jar cascadingSamples/accumuloTapSample/jars/readAccumuloFileWritetoAccumulo.jar ReadFormattedFileWriteToAccumulo "cascadingSamples/Output-transposeToAccumuloLayout/part*" "cascadingSamples/Trap-ReadFormattedFileWriteToAccumulo" "accumulo://employeeDB_employee?instance=indra&user=root&password=sun123123&zookeepers=cdh-dn01:2181" 

Verify results in Accumulo
============================================
root@indra employeeDB_employee> scan -b 100004 -e 100005

100004 employee:DOB []    1960-04-16
100004 employee:DeptID []    d007
100004 employee:FName []    Avishai
100004 employee:Gender []    M
100004 employee:HireDate []    1986-01-03
100004 employee:LName []    Nitsch

100005 employee:DOB []    1958-03-09
100005 employee:DeptID []    d007
100005 employee:FName []    Anneke
100005 employee:Gender []    M
100005 employee:HireDate []    1988-10-22
100005 employee:LName []    Foong


Verify counts in Accumulo table
============================================

a) Attach the iterator - FirstEntryInRowIterator - this will return only the first record for each rowID
root@indra employeeDB_employee> setiter -t employee -class org.apache.accumulo.core.iterators.FirstEntryInRowIterator -scan -p 109
10

b) 
Scan the table for a range - you should get back only a record for each row ID
root@indra employeeDB_employee> scan -b 100004 -e 100005
100004 employee:DOB []    1960-04-16
100005 employee:DOB []    1958-03-09

c) Exit accumulo shell to count the number of records from Linux command line:

./bin/accumulo shell -u root -p sun123123 -e "scan -np -t employeeDB_employee" | wc -l 

224684

d) Delete the iterator
deleteiter -t employeeDB_employee -n firstEntry -scan

e) Verify if the iterator has been deleted
listiter -t employeeDB_employee -scan

firstEntry iterator should not be listed in the output


