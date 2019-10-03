--DS730
--Project 2
--Question 7
--Kevin Ducat
--------------
batt = LOAD 'hdfs:/user/maria_dev/pigtest/Batting.csv' using PigStorage(',');
batters = FOREACH batt GENERATE $0 AS pid, ($8 + $9) as extra_bases;

grp = GROUP batters BY (pid);

calcsum = FOREACH grp GENERATE group as batters,SUM(batters.extra_bases) as total;

mast = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' using PigStorage(',');
mastr = FILTER mast BY (SUBSTRING(UPPER($6),0,1) == 'A' or SUBSTRING(UPPER($6),0,1) == 'E' or SUBSTRING(UPPER($6),0,1) == 'I' or SUBSTRING(UPPER($6),0,1) == 'O' or SUBSTRING(UPPER($6),0,1) == 'U');
master = FILTER mastr BY ($5 != ' ' and $5 != '');  
masters = FOREACH master GENERATE $0 as pid, CONCAT($6,'/',$5) as state_city;

--Join the 2 lists.
allmasters =  JOIN calcsum BY $0, masters BY pid;

grp = GROUP allmasters BY state_city;

cnt = FOREACH grp GENERATE group as allmasters, SUM(allmasters.$1) as total;

ord = ORDER cnt BY $1 DESC;

--DUMP ord;

--Take the top 5.
--maxbirth1 = LIMIT ord 1;
--sortord = LIMIT ord 1;

--sortord2 = FOREACH sortord GENERATE $0 as state_city;

--DUMP sortord2;

--Calculate the Max 1 of 5.
--Find all rows that meet the Max Value
maxdob1  = LIMIT ord 1;
battermaster1 =  JOIN ord BY $1, maxdob1 BY $1;
print1 = FOREACH battermaster1 GENERATE $0 as mm_yyyy;
dump print1;

--Calculate the Max 2 of 5.
--Find all rows that meet the Max Value
maxsort2 = FILTER ord BY ($1 < (int)maxdob1.$1);
maxdob2  = LIMIT maxsort2 1;
battermaster2 =  JOIN ord BY $1, maxdob2 BY $1;
print2 = FOREACH battermaster2 GENERATE $0 as mm_yyyy;
dump print2;

--Calculate the Max 3 of 5.
--Find all rows that meet the Max Value
maxsort3 = FILTER ord BY ($1 < (int)maxdob2.$1);
maxdob3  = LIMIT maxsort3 1;
battermaster3 =  JOIN ord BY $1, maxdob3 BY $1;
print3 = FOREACH battermaster3 GENERATE $0 as mm_yyyy;
dump print3;

--Calculate the Max 4 of 5.
--Find all rows that meet the Max Value
maxsort4 = FILTER ord BY ($1 < (int)maxdob3.$1);
maxdob4  = LIMIT maxsort4 1;
battermaster4 =  JOIN ord BY $1, maxdob4 BY $1;
print4 = FOREACH battermaster4 GENERATE $0 as mm_yyyy;
dump print4;

--Calculate the Max 5 of 5.
--Find all rows that meet the Max Value
maxsort5 = FILTER ord BY ($1 < (int)maxdob4.$1);
maxdob5  = LIMIT maxsort5 1;
battermaster5 =  JOIN ord BY $1, maxdob5 BY $1;
print5 = FOREACH battermaster5 GENERATE $0 as mm_yyyy;
dump print5;
