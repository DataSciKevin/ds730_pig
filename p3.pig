--DS730
--Project 2
--Question 3
--Kevin Ducat
--------------
mast = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' using PigStorage(',');
masters = FOREACH mast GENERATE $0 as pid, $13 as firstname, $14 as lastname, $17 as height;
allmasters = FILTER masters BY (height >0);

grp = GROUP allmasters BY height;

cnt = FOREACH grp GENERATE group as allmasters, COUNT(allmasters.$0) as total;

ord = ORDER cnt BY $1;

--Take unique heights
sortord = FILTER ord BY $1 ==1;

joindata = JOIN allmasters by height, sortord by $0;

alldata = FOREACH joindata GENERATE $1 as fname, $2 as lname;

DUMP alldata;