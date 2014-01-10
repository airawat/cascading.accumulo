The following are prep tasks and commands to run the sample program under the src directory.  The program demonstrates how to transpose data in HDFS in a flat layout 
to an Accumulo layout, and write it back to HDFS

1.  Create a file with some data
==================================
vi cascadingSamples/data/employeeDB/employees_active

10001,1953-09-02,Georgi,Facello,M,1986-06-26,d005
10002,1964-06-02,Bezalel,Simmel,F,1985-11-21,d007
10003,1959-12-03,Parto,Bamford,M,1986-08-28,d004
10004,1954-05-01,Chirstian,Koblick,M,1986-12-01,d004
10005,1955-01-21,Kyoichi,Maliniak,M,1989-09-12,d003
10006,1953-04-20,Anneke,Preusig,F,1989-06-02,d005
10009,1952-04-19,Sumant,Peac,F,1985-02-18,d006
10010,1963-06-01,Duangkaew,Piveteau,F,1989-08-24,d006
10012,1960-10-04,Patricio,Bridgland,M,1992-12-18,d005
10013,1963-06-07,Eberhardt,Terkki,M,1985-10-20,d003
10014,1956-02-12,Berni,Genin,M,1987-03-11,d005
10016,1961-05-02,Kazuhito,Cappelletti,M,1995-01-27,d007
10017,1958-07-06,Cristinel,Bouloucos,F,1993-08-03,d001
10018,1954-06-19,Kazuhide,Peha,F,1987-04-03,d004
10020,1952-12-24,Mayuko,Warwick,M,1991-01-26,d004
10022,1952-07-08,Shahaf,Famili,M,1995-08-22,d005
10023,1953-09-29,Bojan,Montemayor,F,1989-12-17,d005
10024,1958-09-05,Suzette,Pettey,F,1997-05-19,d004
10026,1953-04-03,Yongqiao,Berztiss,M,1995-03-20,d004
10027,1962-07-10,Divier,Reistad,F,1989-07-07,d005
10029,1956-12-13,Otmar,Herbst,M,1985-11-20,d006
10030,1958-07-14,Elvis,Demeyer,M,1994-02-17,d004
10031,1959-01-27,Karsten,Joslin,M,1991-09-01,d005

2. Fields (just FYI)
==================================
EmpID,DOB,FName,LName,Gender,HireDate,DeptID
10003,1959-12-03,Parto,Bamford,M,1986-08-28,d004

3. Load the file to HDFS
==================================
hadoop fs -put cascadingSamples/data/employeeDB/employees_active cascadingSamples/data/employeeDB/

4. Run program
==========================================
hadoop jar cascadingSamples/accumuloTapSample/jars/transposeToAccumuloLayout.jar TransposeToAccumuloLayout "cascadingSamples/data/employeeDB/employees_active" "cascadingSamples/Trap-transposeToAccumuloLayout" "cascadingSamples/Output-transposeToAccumuloLayout"

5. After execution, check for output
==========================================
hadoop fs -ls -R cascadingSamples/Output-transposeToAccumuloLayout/part* | awk '{print $8}'

cascadingSamples/Output-transposeToAccumuloLayout/part-00000
cascadingSamples/Output-transposeToAccumuloLayout/part-00001

6. Input
==========================================
EmpID,DOB,FName,LName,Gender,HireDate,DeptID
10003,1959-12-03,Parto,Bamford,M,1986-08-28,d004

7. Output 
==========================================
hadoop fs -cat cascadingSamples/Output-transposeToAccumuloLayout/part-00001 | grep 10003

10003	employee	DOB		1389333718333	1959-12-03
10003	employee	FName		1389333718333	Parto
10003	employee	LName		1389333718333	Bamford
10003	employee	Gender		1389333718333	M
10003	employee	HireDate		1389333718333	1986-08-28
10003	employee	DeptID		1389333718333	d004





