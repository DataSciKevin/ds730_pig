--DS730
--Project 2
--Question 5
--Kevin Ducat
--------------

field = LOAD 'hdfs:/user/maria_dev/pigtest/Fielding.csv' using PigStorage(',');
fielding = FOREACH field GENERATE $0 as pid, $1 as yearid, $2 as teamid, $10 as errors;
fieldingsmall = FILTER fielding BY (yearid>=1951);

grp = GROUP fieldingsmall BY (pid, teamid);

calcsum = FOREACH grp GENERATE group as fieldingsmall,SUM(fieldingsmall.errors) as total;

ordersum = ORDER calcsum BY $1 DESC;

maxerr  = LIMIT ordersum 1;

maxcount = FOREACH maxerr GENERATE maxerr.$1;

--find all the rows that match the max.
allmax = FILTER calcsum BY $1 == (int)maxcount.total;

maxpid = FOREACH allmax GENERATE allmax.$0;

dump maxpid;