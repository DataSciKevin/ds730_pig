--DS730
--Project 2
--Question 2
--Kevin Ducat
--------------
mast = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' using PigStorage(',');
masters = FOREACH mast GENERATE $0 as pid, CONCAT($2,'/',$1) as mm_yyyy;

grp = GROUP masters BY (mm_yyyy);

cnt = FOREACH grp GENERATE group as masters, COUNT(masters.$1) as total;

ord = ORDER cnt BY $1 DESC;

--Calculate the Max 1 of 3.
--Find all rows that meet the Max Value
maxdob1  = LIMIT ord 1;
battermaster1 =  JOIN ord BY $1, maxdob1 BY $1;
print1 = FOREACH battermaster1 GENERATE $0 as mm_yyyy;
dump print1;

--Calculate the Max 2 of 3.
--Find all rows that meet the Max Value
maxsort2 = FILTER ord BY ($1 < (int)maxdob1.$1);
maxdob2  = LIMIT maxsort2 1;
battermaster2 =  JOIN ord BY $1, maxdob2 BY $1;
print2 = FOREACH battermaster2 GENERATE $0 as mm_yyyy;
dump print2;

--Calculate the Max 3 of 3.
--Find all rows that meet the Max Value
maxsort3 = FILTER ord BY ($1 < (int)maxdob2.$1);
maxdob3  = LIMIT maxsort3 1;
battermaster3 =  JOIN ord BY $1, maxdob3 BY $1;
print3 = FOREACH battermaster3 GENERATE $0 as mm_yyyy;
dump print3;