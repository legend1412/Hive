--1、建库
create database badou;
--2、建表
create table article(sentence string) 
row format delimited fields terminated BY '\n';
--3、查看表结构
desc article;
--4、查看表数据
select * from article limit 3;
--5、导入数据(本地磁盘数据)
load data local  inpath '/usr/usrdata/The_Man_of_Property.txt' into table article;
--6、word count 统计
select split('The word Saga might be objected to on the ground that it connotes the heroic and that there is little heroism in these pages.',' ');
select split('The word Saga might be objected to on the ground that it connotes the heroic and that there is little heroism in these pages.',' ');
--7、explode  把数组中每个元素单独编程一行，行转列
select explode(split('The word Saga might be objected to on the ground that it connotes the heroic and that there is little heroism in these pages.',' '));
--8、应用到文章中(article)
select explode(split(sentence,' ')) as word
from article limit 10;
 --9、hive-site.xml
set hive.cli.print.header=true;
 --10、
select word,count(*) as cnt from
(select explode(split(sentence,' ')) as word from article) a
group by  word;
--11、正则
--测试
select regexp_extract('Certainly,"','[A-Za-z]+',0);
 --12、大小写
select lower('Certainly')
--13、看topn，出现单词次数最多的
--应用
select regexp_extract(lower(word),'[A-Za-z0-9]+',0) as word,count(*) as cnt from 
(select explode(split(sentence,' ')) as word from article) a
group by word
order by cnt desc limit 10;
--hdfs上删除文件
hadoop fs -rm -r /data/The_Man_of_Property
--上传本地文件到hdfs中
hadoop fs -put /usr/usrdata/The_Man_of_Property.txt /data
--创建外部表(数据已经在hdfs上)
create external table art_ext(sentence string)
row format delimited fields terminated BY '\n'
--指定目录,使用这个方法，是正确的
location '/data/ext';
--指定文件 此方式不行，是错误的
location '/data/The_Man_of_Property.txt';
--partition
--1、创建分区表(表在创建过程中要指定分区表)
create table art_dt(senetence string)
partitioned by (dt string)
row format delimited fields terminated BY '\n';
--2、插入数据
insert into table art_dt partition (dt='20191109')
select * from art_ext limit 100
insert overwrite table art_dt partition (dt='20191110')
select * from art_ext limit 100;
--3、查询数据
select * from art_dt where dt='20191110' limit 10;
--/user/hive/warehouse/badou.db/art_dt/dt=20191109/000000_0
--查看分区
show partitions art_dt;
--bucket
set hive.enforce.bucketing=true;
create table bucket_user (id int) 
clustered by (id) into 32 buckets;
--先从location，导入数据到一个临时表bucket_temp
create table bucket_test(id int);
--row format delimited fields terminated BY '\n';
load data local inpath '/usr/usrdata/bucket_test.txt'
into table bucket_test ;
--insert 到bucket_user
insert overwrite table bucket_user
select * from bucket_test;
--sample
select * from bucket_user tablesample (bucket 1 out of 32 on id);
select * from bucket_user tablesample (bucket 1 out of 16 on id);
select * from bucket_user tablesample (bucket 1 out of 9 on id);
--由于创建bucket_user时，已经确定分32个桶，则必然会生成32个文件：000000_0到000031_0
--每个文件中的值保存的值，z=id%32
--bucket x out of y：m是总份数,以id进行分桶，
--x就是从哪个桶里获取，y是分成了几份，n:id的值
--计算公式:n%y=x-1，
--计算在哪个桶里：先计算出n，然后再去计算z，z的值就是文件名包含的数字
--注意一点：bucket文件名是从0开始，跟数组的索引类似。所以我们平时说第1个桶，在bucket的文件名中应该是0

--如果id不够，怎么办
create table bucket_user_40 (id int) 
clustered by (id) into 40 buckets;
insert overwrite table bucket_user_40
select * from bucket_test;
--采样90%
select * from bucket_test where id%10>0
--orders:订单数据表
order_id	user_id	eval_set	order_number	order_dow	order_hour_of_day	days_since_prior_order
2539329	1	prior	1	2	08	
2398795	1	prior	2	3	07	15.0
473747	1	prior	3	3	12	21.0
2254736	1	prior	4	4	07	29.0
431534	1	prior	5	4	15	28.0
3367565	1	prior	6	2	07	19.0
550135	1	prior	7	1	09	20.0
3108588	1	prior	8	1	14	14.0
2295261	1	prior	9	1	16	0.0
--order_id 订单编号
--user_id 用户id
--eval_set 表示订单是否为历史数据
--order_number 用户购买订单编号
--order_dow （dow：day of week） 星期几
--order_hour_of_day 一天中哪个小时产生的订单
--days_since_prior_order 下一个订单距离上一个订单间隔时间（按天）
create table orders (
order_id string,
user_id string,
eval_set string,
order_number string,
order_dow string,
order_hour_of_day string,
days_since_prior_order string
)
row format delimited fields terminated BY ',';

load data local inpath '/usr/usrdata/orders.csv' into table orders;
--order_dow word count 每一个值出现多少次，有多少个值
select order_dow,count(*) as cnt from orders group by order_dow;
order_dow	cnt
0	11604
1	11129
2	8909
3	8433
4	8025
5	8850
6	8585
select order_hour_of_day,count(*) as cnt from orders group by order_hour_of_day;
select distinct(order_hour_of_day) from orders;
--order_products__prior.csv order_products__train.csv
--priors
order_id	product_id	add_to_cart_order	reordered
2	33120	1	1
2	28985	2	1
2	9327	3	0
2	45918	4	1
2	30035	5	0
2	17794	6	1
2	40141	7	1
2	1819	8	1
2	43668	9	0

--order_id 订单id,
--product_id 商品id,
--add_to_cart_order 订单支付先后的位置,
--reordered 当前商品是否为重复下单的行为,
create table priors (
order_id string,
product_id string,
add_to_cart_order string,
reordered string
)
row format delimited fields terminated BY ',';

load data local inpath '/usr/usrdata/order_products__prior.csv' into table priors;


--统计每个用户购买过多少商品
--orders:订单与用户的关系
--priors：订单中的商品详情
--1.每个订单有多少商品
select order_id,count(*) as prod_cnt from priors group by order_id order by prod_cnt desc limit 10;
--2.每个用户有多个订单，把该用户所有订单商品数量求和
--order_id user_id orders
--order_id prod_cnt 从priors表中获取
select user_id,od.order_id,prod_cnt from orders od join 
(
select order_id,count(*) as prod_cnt from priors group by order_id
) pc
on od.order_id=pc.order_id
limit 10;
--具体一个用户的订单信息及商品数量
select user_id,od.order_id,prod_cnt from 
(select * from orders where user_id='135442') od join 
(
select order_id,count(*) as prod_cnt from priors group by order_id
) pc
on od.order_id=pc.order_id;
user_id	od.order_id	prod_cnt
135442	10	15
135442	1002429	14
135442	1033736	6
135442	1113824	13
135442	1322588	11
135442	1374856	10
135442	1401118	10
135442	1461499	16
135442	1547279	8
135442	1827299	8
135442	2166507	15
135442	2352393	8
135442	2397036	6
135442	2622738	14
135442	2721431	9
135442	2910160	8
135442	2987091	7
135442	3035934	9
135442	3147827	6
135442	335891	9
135442	368352	9
135442	379382	10
135442	594628	10
135442	954679	11
--关联之后求和
select user_id,sum(prod_cnt) as prod_cnt_sum from (
select user_id,od.order_id,prod_cnt from orders od join 
(
select order_id,count(*) as prod_cnt from priors group by order_id
) pc
on od.order_id=pc.order_id) t
group by user_id
limit 10;
--方法2
select od.user_id,sum(pc.prod_cnt) as prod_cnt_sum 
from orders od join (
select order_id,count(1) as prod_cnt from priors group by order_id) pc
on od.order_id=pc.order_id
group by od.user_id
limit 10;

select od.user_id,pr.order_id,count(*) from orders od,priors pr where od.order_id=pr.order_id
group by od.user_id,pr.order_id
limit 10;
---------------作业
1、将orders和priors建表入hive
2、每个用户有多少个订单
select user_id,count(*) from orders group by user_id limit 10;
3、每个用户平均每个订单是多少商品
--每个订单多少商品
select order_id,count(*) from priors group by order_id
--每个用户多少订单
select user_id,count(*) from orders group by user_id
--每个用户有多少商品
select user_id,sum(pc.prod_cnt) as prod_cnt_sum from orders od join 
(select order_id,count(*) as prod_cnt from priors group by order_id) pc
on od.order_id=pc.order_id
group by od.user_id 
--1、
select od2.user_id, cast(prod_cnt_sum as float)/order_num from (
select user_id,count(*) as order_num,sum(pc.prod_cnt) as prod_cnt_sum from orders od join 
(select order_id,count(*) as prod_cnt from priors group by order_id) pc
on od.order_id=pc.order_id
group by od.user_id) od2 
limit 10;
--2、avg
select user_id,avg(pc.prod_cnt) as prod_cnt_avg from orders od join 
(select order_id,count(*) as prod_cnt from priors group by order_id) pc
on od.order_id=pc.order_id
group by od.user_id
order by prod_cnt_avg desc
limit 10;

1	5.9
10	28.6
100	5.4
1000	14.714285714285714
10000	15.166666666666666
100000	12.666666666666666
100001	12.318181818181818
100002	4.333333333333333
100003	17.0
100004	5.25
4、每个用户在一周中的购买订单的分布情况
select user_id,
sum(case when order_dow=1 then 1 else 0 end) as Mon,
sum(case when order_dow=2 then 1 else 0 end) as Tue,
sum(case when order_dow=3 then 1 else 0 end) as Wed,
sum(case when order_dow=4 then 1 else 0 end) as Thu,
sum(case when order_dow=5 then 1 else 0 end) as Fri,
sum(case when order_dow=6 then 1 else 0 end) as Sat,
sum(case when order_dow=0 then 1 else 0 end) as Sun 
from orders group by user_id limit 10;
--单独回收user_id对应的所有order_dow数据放到list中
select user_id,collect_list(order_dow) as dows from orders group by user_id limit 10;
--如果用户周一没有买东西的话，下面语句返回NULL
select user_id,sum(case when order_dow=1 then 1 end)as Mon from orders group by user_id limit 10;
5. 一个用户平均每个月购买多少个商品（30天一个月）平均每30天
--1、有多少个月

select user_id,ceil(sum(cast(days_since_prior_order as float))/30) from orders group by user_id
--每个用户第一条记录的days_since_prior_order是空的，需要处理下，编程0.0
select user_id,if(days_since_prior_order='','0.0',days_since_prior_order) from orders limit 40

select user_id,sum(pc.prod_cnt) as prod_cnt_sum,
ceiling(sum(cast(if(days_since_prior_order='','0.0',days_since_prior_order) as float))/30),
sum(pc.prod_cnt)/ceiling(sum(cast(if(days_since_prior_order='','0.0',days_since_prior_order) as float))/30) 
from orders od join 
(select order_id,count(*) as prod_cnt from priors group by order_id) pc
on od.order_id=pc.order_id
group by od.user_id 
order by prod_cnt_sum desc
limit 10;
--如果一个人只有一个订单呢？？
6.每个用户最喜爱购买的三个product是什么。
--1、每个商品的购买次数
select user_id,product_id,count(*) as pro_cnt from orders od join priors pr
on od.order_id=pr.order_id 
group by od.user_id,pr.product_id
limit 10;
--2、top3
select user_id,product_id,pro_cnt from (
select user_id,product_id,pro_cnt,row_number() over(partition by user_id order by pro_cnt desc) as row_num
from (select user_id,product_id,count(*) as pro_cnt from orders od join priors  pr
on od.order_id=pr.order_id group by od.user_id,pr.product_id) t)t1
where row_num<4
limit 100;
user_id	product_id	pro_cnt
100001	21137	41
100001	13176	41
100001	35951	29
100003	17616	2
100003	7781	2
100003	47598	2
100005	21709	10
100005	42413	10
100005	12481	9
100007	13944	6
100007	33452	6
100007	45619	3
--3、拼接,每个用户一条记录
select user_id,collect_list(concat_ws('_',product_id,cast(pro_cnt as string))) as top3_prods from (
select user_id,product_id,pro_cnt,row_number() over(partition by user_id order by pro_cnt desc) as row_num
from (select user_id,product_id,count(*) as pro_cnt from orders od join priors  pr
on od.order_id=pr.order_id group by od.user_id,pr.product_id) t)t1
where row_num<4
group by user_id
limit 100;
user_id	top3_prods
1	["12427_10","196_10","10258_9"]
10	["47526_4","30489_4","16797_4"]
100	["21616_3","27344_3","30795_2"]
1000	["26165_7","30492_7","49683_7"]
10000	["21137_44","5077_41","42828_39"]
100000	["10151_5","3318_5","19348_5"]
100001	["21137_41","13176_41","35951_29"]
100002	["26172_10","38049_4","40091_4"]
100003	["17616_2","7781_2","47598_2"]
100004	["19660_7","43788_4","5085_4"]
100005	["21709_10","42413_10","12481_9"]
100006	["41290_4","2962_4","24852_4"]
100007	["13944_6","33452_6","45619_3"]
--对于上面的结果可以进行再次加工，比如分成几个字段，第1，第2，第3
--也可以使用case when then else，也可以使用数组[0],[1]的形式

select user_id,collect_list(concat_ws('_',product_id,cast(pro_cnt as string))) as top3_prods from (
select user_id,product_id,pro_cnt,row_number() over(distribute by user_id sort by pro_cnt desc) as row_num
from (select user_id,product_id,count(*) as pro_cnt from orders od join priors  pr
on od.order_id=pr.order_id group by od.user_id,pr.product_id) t)t1
where row_num<4
group by user_id
limit 100;
1	["12427_10","196_10","10258_9"]
10	["47526_4","30489_4","16797_4"]
100	["21616_3","27344_3","30795_2"]
1000	["26165_7","30492_7","49683_7"]
10000	["21137_44","5077_41","42828_39"]
100000	["10151_5","3318_5","19348_5"]
100001	["21137_41","13176_41","35951_29"]
100002	["26172_10","38049_4","40091_4"]

--用户在第几个30天内购买的商品最多
--订单对应商品数量
select order_id,count(*) as prod_cnt from priors group by order_id
--当前订单是在第几个30天
ceiling(cast(if(days_since_prior_order='','0.0',days_since_prior_order) as float)/30)

cast(if(days_since_prior_order='','0.0',days_since_prior_order) as float)


select user_id,cast(order_number as int) as od_num,days_since_prior_order,
sum(cast(if(days_since_prior_order='','0.0',days_since_prior_order) as float))
over(partition by user_id order by od_num) as sum_day
from orders limit 10;


select user_id,cast(order_number as int) as od_num,days_since_prior_order,
ceiling(sum(cast(if(days_since_prior_order='','0.0',days_since_prior_order) as float))
over(partition by user_id order by cast(order_number as int))/30) as per_day
from orders limit 10;


select user_id,od_num,days_since_prior_order,if(per_day=0,1,per_day) from (
select user_id,cast(order_number as int) as od_num,days_since_prior_order,
ceiling(sum(cast(if(days_since_prior_order='','0.0',days_since_prior_order) as float))
over(partition by user_id order by cast(order_number as int))/30) as per_day
from orders) t1 
limit 10;

select user_id,cast(order_number as int) as od_num,days_since_prior_order,
if(days_since_prior_order='',1,ceiling(sum(cast(if(days_since_prior_order='','0.0',days_since_prior_order) as float))
over(partition by user_id order by cast(order_number as int))/30)) as per_day
from orders limit 10;


select user_id,cast(order_number as int) as od_num,days_since_prior_order,
if(days_since_prior_order='',1,ceiling(sum(cast(days_since_prior_order as float))
over(partition by user_id order by cast(order_number as int))/30)) as per_day
from orders limit 10;
--基于用户在每个月份，统计商品数量
select od.user_id,od.per_day,sum(prod_cnt) as prods_per_sum from 
(select user_id,order_id,cast(order_number as int) as od_num,days_since_prior_order,
if(days_since_prior_order='',1,ceiling(sum(cast(days_since_prior_order as float))
over(partition by user_id order by cast(order_number as int))/30)) as per_day
from orders) od join
(select order_id,count(*) as prod_cnt from priors group by order_id) pr
on od.order_id=pr.order_id
group by od.user_id,od.per_day
limit 10;


select od.user_id,od.per_day,sum(prod_cnt) as prods_per_sum,
row_number() over(partition by user_id order by sum(prod_cnt) desc) as rk  from 
(select user_id,order_id,cast(order_number as int) as od_num,days_since_prior_order,
if(days_since_prior_order='',1,ceiling(sum(cast(days_since_prior_order as float))
over(partition by user_id order by cast(order_number as int))/30)) as per_day
from orders) od join
(select order_id,count(*) as prod_cnt from priors group by order_id) pr
on od.order_id=pr.order_id
group by od.user_id,od.per_day
limit 10;


select user_id,per_day,prods_per_sum from (
select od.user_id,od.per_day,sum(prod_cnt) as prods_per_sum,
row_number() over(partition by user_id order by sum(prod_cnt) desc) as rk  from 
(select user_id,order_id,cast(order_number as int) as od_num,days_since_prior_order,
if(days_since_prior_order='',1,ceiling(sum(cast(days_since_prior_order as float))
over(partition by user_id order by cast(order_number as int))/30)) as per_day
from orders) od join
(select order_id,count(*) as prod_cnt from priors group by order_id) pr
on od.order_id=pr.order_id
group by od.user_id,od.per_day
) t
where rk=1
limit 10;

--对每个用户最喜爱购买的三个product的改编
--每个用户最喜爱的top10%的商品
select user_id,collect_list(concat_ws('_',product_id,cast(row_num as string),
cast(prod_percent10_cnt as string))) as top_percent10_prods 
from (select user_id,product_id,pro_cnt,
row_number() over(distribute by user_id sort by pro_cnt desc) as row_num,
ceiling(cast(count(1) over(partition by user_id) as float)*0.1) as prod_percent10_cnt
from (
--用户对于每个商品购买次数
select user_id,product_id,count(*) as pro_cnt from orders od join priors  pr
on od.order_id=pr.order_id group by od.user_id,pr.product_id) t)t1
where row_num<=prod_percent10_cnt
group by user_id
limit 100;
user_id	top_percent10_prods
1	["12427_1_2","196_2_2"]
10	["47526_1_10","30489_2_10","16797_3_10","28535_4_10","46979_5_10","31506_6_10","40706_7_10","25931_8_10","28842_9_10","9339_10_10"]
100	["27344_2_2","21616_1_2"]
1000	["26165_1_4","30492_2_4","49683_3_4","28465_4_4"]
10000	["21137_1_26","5077_2_26","42828_3_26","42356_4_26","24852_5_26","42561_6_26","49235_7_26","30720_8_26","28985_9_26","47766_10_26","11520_11_26","10749_12_26","27695_13_26","21927_14_26","25005_15_26","27104_16_26","42585_17_26","26604_18_26","38293_19_26","4799_20_26","22935_21_26","22035_22_26","20842_23_26","7175_24_26","17600_25_26","37374_26_26"]
100000	["3318_2_7","19348_3_7","16797_4_7","1062_5_7","42625_6_7","5077_7_7","10151_1_7"]
100001	["27104_11_21","18288_12_21","21938_13_21","38383_5_21","34126_14_21","45603_15_21","36724_16_21","47601_17_21","21137_1_21","18362_18_21","37029_19_21","39877_8_21","47209_4_21","42445_6_21","11422_9_21","37792_20_21","21903_21_21","3957_7_21","13176_2_21","41220_10_21","35951_3_21"]
100002	["26172_1_3","38049_2_3","40091_3_3"]
100003	["9214_4_5","17616_1_5","7781_2_5","47598_3_5","38399_5_5"]
100004	["19660_1_2","43788_2_2"]
100005	["42413_2_7","12481_3_7","48203_4_7","21174_5_7","21868_6_7","21372_7_7","21709_1_7"]
100006	["41290_1_9","2962_2_9","24852_3_9","13500_4_9","11712_5_9","21616_6_9","49236_7_9","8518_8_9","16797_9_9"]
100007	["13944_1_3","33452_2_3","45619_3_3"]
100008	["42972_1_15","7157_2_15","47946_3_15","17616_4_15","43070_5_15","5067_6_15","11281_7_15","18398_8_15","44487_9_15","49176_10_15","47990_11_15","26885_12_15","47177_13_15","45979_14_15","34986_15_15"]
100009	["24852_1_8","20082_2_8","14381_3_8","3480_4_8","4962_5_8","38544_6_8","14063_7_8","39877_8_8"]
10001	["16398_1_11","24852_2_11","6287_3_11","16349_4_11","20794_5_11","8736_6_11","20947_7_11","29627_8_11","45066_9_11","48559_10_11","22935_11_11"]
100010	["41771_2_12","28427_3_12","2966_4_12","14999_5_12","40516_6_12","48370_7_12","21461_8_12","35535_9_12","1447_10_12","46526_11_12","17317_12_12","2295_1_12"]

--10%的商品量
ceiling(cast(count(1) over(partition by user_id) as float)*0.1) as prod_percent10_cnt

--建立分区表
create table orders_part(
order_id string,
user_id string,
eval_set string,
order_number string,
order_hour_of_day string,
days_since_prior_order string
)partitioned by (order_dow string)
row format delimited fields terminated by '\t';
--order_dow进行分区(order_dow里面有多少个值)
--动态插入分区表
set hive.exec.dynamic.partition=true;--使用动态分区
set hive.exec.dynamic.partition.mode=nonstrict;--使用无限制模式
--overwrite属于覆盖
insert overwrite table orders_part partition (order_dow)
select order_id,user_id,eval_set,order_number,order_hour_of_day,days_since_prior_order,order_dow
from orders;
--统计商品的销量（pv），购买用户数（uv），reorder数
--pv
select product_id,count(*) as pv
from priors
group by product_id
--uv sql:distinct,pandas:unique,spark:distinct
select product_id,count(distinct user_id) as uv
from priors pr join orders od
on pr.order_id=od.order_id
group by product_id,user_id