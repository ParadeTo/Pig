--Load Data
Data = load 'data.txt' using PigStorage('|') as (imsi:chararray,time:chararray,loc:chararray);
--Register
REGISTER piggybank.jar
REGISTER joda-time-1.6.2.jar;
REGISTER datafu-1.0.0-SNAPSHOT.jar;
--Define
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
--TO ISO
toISO = FOREACH Data GENERATE imsi,CustomFormatToISO(SUBSTRING(time,0,13),'YYYY-MM-dd HH') AS time:chararray, loc;
--Group by imsi
grp = GROUP toISO BY imsi;
--Define MarkovPairs
DEFINE MarkovPairs datafu.pig.stats.MarkovPairs();
--MarkovPairs
pairs = FOREACH grp {
	sorted = ORDER toISO BY time;
	pair = MarkovPairs(sorted);
	GENERATE FLATTEN(pair) AS (data:tuple(imsi,time,loc),next:tuple(imsi,time,loc));}
--Unfold data
prj = FOREACH pairs GENERATE data.imsi AS imsi,data.time AS time, next.time AS next_time, data.loc AS loc,next.loc AS next_loc;
--Define ISODaysBetween
DEFINE ISOHoursBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOHoursBetween();
--filter data
flt = FILTER prj BY ISOHoursBetween(next_time,time) < 12L;
--To char
fltstr = FOREACH flt GENERATE time as time:chararray, next_time as next_time:chararray,loc,next_loc;
--substring time
subtime = FOREACH fltstr GENERATE CONCAT(CONCAT(SUBSTRING(time,11,13),'-'),SUBSTRING(next_time,11,13)) AS time1_time2,loc,next_loc;
--total_count
total_count = FOREACH (GROUP subtime BY (time1_time2,loc)) GENERATE FLATTEN(group) AS (time1_time2,loc),COUNT(subtime) AS total;
--pairs_count
pairs_count = FOREACH (GROUP subtime BY (time1_time2,loc,next_loc)) GENERATE FLATTEN(group) AS (time1_time2,loc,next_loc),COUNT(subtime) AS pairs_count;
--Join
jnd = JOIN pairs_count BY (time1_time2,loc),total_count BY (time1_time2,loc) USING 'replicated';
--Percentage
prob = FOREACH jnd GENERATE pairs_count::time1_time2 AS t1_t2, pairs_count::loc AS loc,pairs_count::next_loc AS next_loc,(double)pairs_count/(double)total AS probability;
--Top 3
top3 = FOREACH (GROUP prob BY loc) {
	sorted = ORDER prob BY probability DESC;
	top3 = LIMIT sorted 3;
	GENERATE FLATTEN(top3);}
--STORE
STORE top3 INTO 'output';
