c = LOAD 'hdfs://localhost/user/test/customer' USING PigStorage(',') as (custID:int,name:chararray,age:int,countrycode:int,salary:float);
t = LOAD 'hdfs://localhost/user/test/transaction' USING PigStorage(',') as (transID:int,custID:int,transTotal:float,transNumItems:int,TransDesc:chararray);
temp = GROUP t BY custID;
ans = FOREACH temp GENERATE group, COUNT(t.transID) as numsoftrans, SUM(t.transTotal) as totaloftrans, MIN(t.transNumItems) as minitem;
ret = JOIN c BY custID, ans BY group USING 'replicated';
fin = FOREACH ret GENERATE custID, name, salary, numsoftrans, totaloftrans, minitem;
STORE fin INTO 'pig_2_output' using PigStorage(',');
