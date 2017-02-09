c = LOAD 'hdfs://localhost/user/test/customer' USING PigStorage(',') as (custID:int,name:chararray,age:int,countrycode:int,salary:float);
t = LOAD 'hdfs://localhost/user/test/transaction' USING PigStorage(',') as (transID:int,custID:int,transTotal:float,transNumItems:int,TransDesc:chararray);
temp = GROUP t BY custID;
sum = FOREACH temp GENERATE group, COUNT(t.transID) as total;
ans = JOIN c BY custID, sum BY group;
ret = FOREACH ans GENERATE name,total;
STORE ret INTO 'pig_1_output' using PigStorage(',');
