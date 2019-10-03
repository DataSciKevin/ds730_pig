--DS730
--Project 2
--Question 6
--Kevin Ducat
--------------
field = LOAD 'hdfs:/user/maria_dev/pigtest/Fielding.csv' using PigStorage(',');
field2 = FOREACH field GENERATE $0,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16;
field3 = FILTER field2 by ($6 is not null or $7 is not null or $8 is not null or $9 is not null or $10 is not null or $11 is not null or $12 is not null or $13 is not null or $14 is not null or $15 is not null or $16 is not null);
fielding = FOREACH field3 GENERATE $0 as pid, $1 as yearid,$5 as numgames, $10 as errors;
fieldingsmall = FILTER fielding BY (yearid>=2005 and yearid <=2009 and numgames >= 20 );

batt = LOAD 'hdfs:/user/maria_dev/pigtest/Batting.csv' using PigStorage(',');
batters = FOREACH batt GENERATE $0 AS pid, $1 as yearid, $5 as atbats, $7 as hits ;
rbismall = FILTER batters BY (yearid>=2005 and yearid <=2009 and atbats >=40);

grpfield = GROUP fieldingsmall BY (pid);
calcfld  = FOREACH grpfield GENERATE group as fieldingsmall,SUM(fieldingsmall.numgames) as totg ,SUM(fieldingsmall.errors) as tote;

grpbatt = GROUP rbismall BY (pid);
calcbat  = FOREACH grpbatt GENERATE group as rbismall,SUM(rbismall.atbats) as totab ,SUM(rbismall.hits) as toth;

joindata = JOIN calcfld by $0, calcbat by $0;

calcsum = FOREACH joindata GENERATE $0 as pid, (($5/$4)-($2/$1)) as total;

limitorder = FILTER calcsum BY ($1 >0);
ordersum = ORDER limitorder BY $1 DESC;

--Calculate the Max 1 of 3.
--Find all rows that meet the Max Value

maxteams1  = LIMIT ordersum 1;
battermaster1 =  JOIN ordersum BY $1, maxteams1 BY $1;
print1 = FOREACH battermaster1 GENERATE $0 as pid;
dump print1;

--Calculate the Max 2 of 3.
--Find all rows that meet the Max Value
maxsort2 = FILTER ordersum BY ($1 < maxteams1.$1);
maxorder2 = ORDER maxsort2 BY $1 DESC;
maxteams2  = LIMIT maxorder2 1;
battermaster2 =  JOIN ordersum BY $1, maxteams2 BY $1;
print2 = FOREACH battermaster2 GENERATE $0 as pid;
dump print2;

--Calculate the Max 3 of 3.
--Find all rows that meet the Max Value
maxsort3 = FILTER ordersum BY ($1 < maxteams2.$1);
maxorder3 = ORDER maxsort3 BY $1 DESC;
maxteams3  = LIMIT maxorder3 1;
battermaster3 =  JOIN ordersum BY $1, maxteams3 BY $1;
print3 = FOREACH battermaster3 GENERATE $0 as pid;
dump print3;
