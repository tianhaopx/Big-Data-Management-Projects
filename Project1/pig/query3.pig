c = LOAD 'hdfs://localhost/user/test/customer' USING PigStorage(',') as (custID:int,name:chararray,age:int,countrycode:int,salary:float);
t = LOAD 'hdfs://localhost/user/test/transaction' USING PigStorage(',') as (transID:int,custID:int,transTotal:float,transNumItems:int,TransDesc:chararray);
country = GROUP c BY countrycode;
temp = FOREACH country GENERATE group, COUNT(custID) as total;
ans = FILTER temp by (total > 5000) OR (total < 2000);
ret = FOREACH ans GENERATE countrycode;
