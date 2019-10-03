--DS730
--Project 2
--Question 8
--Kevin Ducat
--------------
mast = LOAD 'hdfs:/user/maria_dev/pigtest/Master.csv' using PigStorage(',');
masters1 = FOREACH mast GENERATE $0 as pid, $2 as mm , $5 as state;
masters2 = FILTER masters1 BY ($1 is not null and $2 is not null);
masters = FOREACH masters2 GENERATE $0 as pid, CONCAT($1,'/',$2) as mm_state;

batt = LOAD 'hdfs:/user/maria_dev/pigtest/Batting.csv' using PigStorage(',');
battr = FOREACH batt GENERATE $0 AS pid, $5 as atbats, $7 as hits ;
batter = FILTER battr BY $1 >=0;
batters = FILTER batter BY $2 >=0;

grp = GROUP batters BY (pid);

calcsum = FOREACH grp GENERATE group as batters, 
	SUM(batters.atbats) as totab, 
	SUM(batters.hits) as toth ;

--Join the 2 lists.
allmasters =  JOIN calcsum BY $0, masters BY $0;

grp2 = GROUP allmasters by $4;

calcsum2 = FOREACH grp2 GENERATE group as allmasters, 
	SUM(allmasters.totab) as totab, 
	SUM(allmasters.toth) as toth, 
    COUNT(allmasters.pid) as pidcnt, 
    SUM(allmasters.toth) / SUM(allmasters.totab) as equation;

calcsmall = FILTER calcsum2 BY ($3 >= 10 and $1 >1500);
    
worseord = FOREACH calcsmall GENERATE $0 as dob, $4 as eqation;
sorteord = ORDER worseord BY $1;
sortord = LIMIT sorteord 1;
dump sortord;

----find all the rows that match the max.
allmax = FILTER sorteord BY $1 == sortord.$1;
maxdob = FOREACH allmax GENERATE allmax.$0;
dump maxdob;