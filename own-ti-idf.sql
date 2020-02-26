--hive 实现tf-idf统计
--tf-idf数据
create table encode_allfiles(wid string,doc_name string)
row format delimited fields terminated by '#'
lines terminated by '\n';

load data local inpath '/usr/usrdata/data.train' into table encode_allfiles

--hive统计tf，对应这篇文章（doc_name）中的每个单词的词频 wid+doc_name
select explode(split(wid,' ')) as wid from 
(select * from encode_allfiles limit 1)t
limit 30;

--lateral view 多列
select t2.wid,t1.doc_name from 
(select * from encode_allfiles limit 1)t1
lateral view explode(split(wid,' '))t2 as wid
limit 30;

select wid,doc_name,count(1) as tf from
(select t2.wid,t1.doc_name from 
(select * from encode_allfiles limit 1)t1
lateral view explode(split(wid,' '))t2 as wid
)t3
group by wid,doc_name
order by tf desc
limit 20;

--finally
select wid,doc_name,count(1) as tf from
(select t2.wid,t1.doc_name from encode_allfiles t1
lateral view explode(split(wid,' '))t2 as wid
)t3
group by wid,doc_name
order by tf desc
limit 20;

--统计df 每个单词出现过多少篇文章 p(y) p(x|y)
wid,doc_name,1
wid,doc_name,2
wid=>2

---
select wid,doc_name,count(distinct wid,doc_name) as df
from
(
select t2.wid,t1.doc_name from 
(select * from encode_allfiles limit 1)t1
lateral view explode(split(wid,' '))t2 as wid
)t3
group by wid,doc_name
limit 20;

---finally
select wid,count(1) as df from
(select wid,doc_name,count(distinct wid,doc_name) as df
from
(
select t2.wid,t1.doc_name from encode_allfiles t1
lateral view explode(split(wid,' '))t2 as wid
)t3
group by wid,doc_name)t4
group by wid
order by df desc
limit 20;

--finally
select t2.wid,t2.doc_name,tf*log(1982/(df+1)) as tf_idf from
(select wid,doc_name,count(1) as tf from
(select t22.wid,t21.doc_name from encode_allfiles t21
lateral view explode(split(wid,' '))t22 as wid)t23
group by wid,doc_name)t2
left outer join
(select wid,count(1) as df from
(select wid,doc_name,count(distinct wid,doc_name) as df
from
(select t12.wid,t11.doc_name from encode_allfiles t11
lateral view explode(split(wid,' '))t12 as wid)t13
group by wid,doc_name)t14
group by wid)t1
on t1.wid=t2.wid
order by t2.wid desc
limit 10;
