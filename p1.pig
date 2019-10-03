--DS730
--Project 2
--Question 1
--Kevin Ducat

batt = LOAD 'hdfs:/user/maria_dev/pigtest/Batting.csv' using PigStorage(',');
batters = FOREACH batt GENERATE $0 AS pid, $11 AS rbi;
rbibatters = FILTER batters BY (rbi >0);

mast = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' using PigStorage(',');
masters = FOREACH mast GENERATE $0 as pid, $6 as birthcity;

grp = GROUP rbibatters BY (pid);

calcsum = FOREACH grp GENERATE group as rbibatters,SUM(rbibatters.rbi) as total;

ordersum = ORDER calcsum BY $1 DESC;

maxrbi  = LIMIT ordersum 1;

--Find all rows that meet the Max Value
battermaster =  JOIN ordersum BY $0, masters BY pid;

allmax = FILTER battermaster BY $1 == (int)maxrbi.$1;

dump allmax;

maxcity = FOREACH allmax GENERATE allmax.$3;

dump maxcity;