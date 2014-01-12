The sample program in the “src” directory demonstrates how to read data in a flat layout in HDFS, transpose/transform it and save to Accumulo.

Input data format
=================================================
EmpID,DOB,FName,LName,Gender,HireDate,DeptID
10003,1959-12-03,Parto,Bamford,M,1986-08-28,d004

Input fields & output fields
=================================================
Input=[EmpID","DOB","FName","LName","Gender","HireDate","DeptID”]
Output=["rowID", "colF", "colQ", "colVis", "colTimestamp", "colVal”]
		
Create table in Accumulo
=================================================

root@indra> createtable employees_active
root@indra employees_active> clear


Command to run the program
=================================================
hadoop jar <<pathToJar>>/readHDFSTransposeWriteToAccumulo.jar  ReadHDFSTransposeWriteToAccumulo “<<pathToInputFileInHDFS>>” “<<pathToTrapFileInHDFS>” "accumulo://employees_active?instance=inst&user=root&password=pwd&zookeepers=zooserver:2181" 

Results in Accumuo
=================================================
root@indra employees_active> scan -b 10003 -e 10003/0 
10003 employee:DOB []    1959-12-03
10003 employee:DeptID []    d004
10003 employee:FName []    Parto
10003 employee:Gender []    M
10003 employee:HireDate []    1986-08-28
10003 employee:LName []    Bamford
