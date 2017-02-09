c = LOAD 'hdfs://localhost/user/test/customer' USING PigStorage(',') as (custID:int,name:chararray,age:int,countrycode:int,salary:float);
country = GROUP c BY countrycode;
temp = FOREACH country GENERATE group, COUNT(c.custID) as cnt;
ans = FILTER temp by cnt>5000 or cnt<2000;
ret = FOREACH ans GENERATE group;
STORE ret INTO 'pig_3_output' using PigStorage(',');
