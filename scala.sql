定义变量 val不可变，var可变
val s="""Looking back on the Victorian era, whose ripeness, decline,and ‘fall-of’ is in some sort pictured in “The Forsyte Saga,” we see now that we have but jumped out of a frying-pan into a fire. It would be difficult to substantiate a claim that the case of England was better in 1913 than it was in 1886, when the Forsytes assembled at Old Jolyon’s to celebrate the engagement of June to Philip Bosinney. And in 1920, when again the clan gathered to bless the marriage of Fleur with Michael"""

s.split(" ")

res3: Array[String] =
Array(Looking, back, on, the, Victorian, era,, whose, ripeness,, decline,, and, ‘fall-of’, is, in, some, sort, pictured, in, “The, Forsyte, Saga,”, we, see, now, that, we, have, but, jumped, out, of, a, frying-pan, into, a, fire., It, would, be, difficult, to, substantiate, a, claim, that, the, case, of, England, was, better, in, 1913, than, it, was, in, 1886,, when, the, Forsytes, assembled, at, Old, Jolyon’s, to, celebrate, the, engagement, of, June, to, Philip, Bosinney., And, in, 1920,, when, again, the, clan, gathered, to, bless, the, marriage, of, Fleur, with, Michael)
--读取文本数据The_man_of_property
import scala.io.Source
val lines = Source.fromFile("/usr/usrdata/The_man_of_property.txt").getLines().toList
getLines().toList<=>python readlines()

List[String]
每一个string是一行"\n",string.split(" ")=>Array(word1,word2)
List[Array1()]
val a = List("Looking back a on the Victorian era","some on sort out a pictured in","jumped out of a frying-pan into a fire")

python正常函数
def f(str):
	return str.split(" ")
python匿名函数
lambda str:str.split(" ")
scala匿名函数
str=>str.split(" ")
_.split(" ")
str=>str等价_(通配符)
--1、对a中每一行进行提取单词
a.map(x=>x.split(" "))

a.map(_.split(" "))
res5: List[Array[String]] = List(Array(Looking, back, on, the, Victorian, era), 
	Array(some, sort, pictured, in), Array(jumped, out, of, a, frying-pan, into, a, fire))

a.map(_.split(" ")).flatten
res7: List[String] = List(Looking, back, on, the, Victorian, era, some, sort, pictured, in, jumped, out, of, a, frying-pan, into, a, fire)

a.flatMap(_.split(" "))
res8: List[String] = List(Looking, back, on, the, Victorian, era, some, sort, pictured, in, jumped, out, of, a, frying-pan, into, a, fire)

a.flatMap(_.split(" ")).map(x=>(x,1))
res10: List[(String, Int)] = List((Looking,1), (back,1), (on,1), (the,1), (Victorian,1), (era,1), (some,1), (sort,1), (pictured,1), (in,1), (jumped,1), (out,1), (of,1), (a,1), (frying-pan,1), (into,1), (a,1), (fire,1))

a.flatMap(_.split(" ")).map(x=>(x,1)).groupBy(x=>x._1)
res11: scala.collection.immutable.Map[String,List[(String, Int)]] = Map(in -> List((in,1)), jumped -> List((jumped,1)), fire -> List((fire,1)), Looking -> List((Looking,1)), a -> List((a,1), (a,1)), out -> List((out,1)), frying-pan -> List((frying-pan,1)), on -> List((on,1)), pictured -> List((pictured,1)), back -> List((back,1)), era -> List((era,1)), into -> List((into,1)), Victorian -> List((Victorian,1)), sort -> List((sort,1)), some -> List((some,1)), of -> List((of,1)), the -> List((the,1)))

--转map，map无序
a.flatMap(_.split(" ")).map(x=>(x,1)).groupBy(x=>x._1).mapValues(x=>x.size)
a.flatMap(_.split(" ")).map((_,1)).groupBy(_._1).mapValues(_.size)
res6: scala.collection.immutable.Map[String,Int] = Map(in -> 1, jumped -> 1, fire -> 1, Looking -> 1, a -> 2, out -> 1, frying-pan -> 1, on -> 1, pictured -> 1, back -> 1, era -> 1, into -> 1, Victorian -> 1, sort -> 1, some -> 1, of -> 1, the -> 1)

--转数据才能排序
a.flatMap(_.split(" ")).map(x=>(x,1)).groupBy(x=>x._1).mapValues(x=>x.size).toArray
res8: Array[(String, Int)] = Array((in,1), (jumped,1), (fire,1), (Looking,1), (a,2), (out,1), (frying-pan,1), (on,1), (pictured,1), (back,1), (era,1), (into,1), (Victorian,1), (sort,1), (some,1), (of,1), (the,1))

--排序
a.flatMap(_.split(" ")).map(x=>(x,1)).groupBy(x=>x._1).mapValues(x=>x.size).toArray.sortBy(_._2)
res12: Array[(String, Int)] = Array((in,1), (jumped,1), (fire,1), (Looking,1), (out,1), (frying-pan,1), (on,1), (pictured,1), (back,1), (era,1), (into,1), (Victorian,1), (sort,1), (some,1), (of,1), (the,1), (a,2))

--降序排列
a.flatMap(_.split(" ")).map(x=>(x,1)).groupBy(x=>x._1).mapValues(x=>x.size).toArray.sortWith((x,y)=>x._2>y._2)
res13: Array[(String, Int)] = Array((a,2), (in,1), (jumped,1), (fire,1), (Looking,1), (out,1), (frying-pan,1), (on,1), (pictured,1), (back,1), (era,1), (into,1), (Victorian,1), (sort,1), (some,1), (of,1), (the,1))

--文本文件中的
lines.flatMap(_.split(" ")).map(x=>(x,1)).groupBy(x=>x._1).mapValues(x=>x.size).toArray.sortWith((x,y)=>x._2>y._2)
res15: Array[(String, Int)] = Array((the,5144), (of,3407), (to,2782), (and,2573), (a,2543), (he,2139), (his,1912), (was,1702), (in,1694), (had,1526), (that,1273), (with,1029), (her,1020), (—,931), (at,815), (for,765), (not,723), (she,711), (He,695), (it,689), (on,668), (as,655), (him,470), (I,454), (be,452), (would,436), (all,423), (you,420), (this,412), (The,412), (but,411), (were,400), (by,391), (from,388), (they,368), (have,350), (an,344), (so,343), (no,338), (been,337), (which,337), (could,314), (Soames,303), (like,301), (.,292), (their,283), (old,277), (what,265), (into,262), (out,255), (And,253), (or,253), (Jolyon,251), (She,243), (It,242), (up,238), (said,236), (little,235), (is,232), (who,231), (if,221), (there,221), (one,218), (never,213), (about,210), (did,209), (over,188), (v...

sortWith((x,y)=>x._2>y._2)
--x表示数据中的前一个元素（tuple）
--y表示数据中的后一个元素（tuple）
("in",5)
val s = ("in",5)
s: (String, Int) = (in,5)
s._1
res3: String = in
s._2
res4: Int = 5

val l = List(("in",2),("in",1))
l.size
res7: Int = 2
l.map(_._2)
res8: List[Int] = List(2, 1)
l.map(_._2).sum
res9: Int = 3
--下面四句等价
a.flatMap(_.split(" ")).map(x=>(x,1)).groupBy(x=>x._1).mapValues(x=>x.size)
a.flatMap(_.split(" ")).map((_,1)).groupBy(_._1).mapValues(_.size)
a.flatMap(x=>x.split(" ")).map((x,1)).groupBy(x=>x._1).mapValues(l=<l.map(x=>x._2).sum)
a.flatMap(_.split(" ")).map((_,1)).groupBy(_._1).mapValues(_.map(_._2).sum)
res10: scala.collection.immutable.Map[String,Int] = Map(in -> 1, jumped -> 1, fire -> 1, Looking -> 1, a -> 4, out -> 2, frying-pan -> 1, on -> 2, pictured -> 1, back -> 1, era -> 1, into -> 1, Victorian -> 1, sort -> 1, some -> 1, of -> 1, the -> 1)

a.flatMap(_.split(" ")).map((_,1)).groupBy(_._1).mapValues(_.map(_._2).sum).toArray.sortWith((x,y)=>x._2>y._2)


val l = Range(0,7)
l: scala.collection.immutable.Range = Range(0, 1, 2, 3, 4, 5, 6)
l.map(x=>s"sum(case order_dow when '$x' then 1 else 0 end ) dow_$x")
res19: scala.collection.immutable.IndexedSeq[String] = Vector(sum(case order_dow when '0' then 1 else 0 end ) dow_0, sum(case order_dow when '1' then 1 else 0 end ) dow_1, sum(case order_dow when '2' then 1 else 0 end ) dow_2, sum(case order_dow when '3' then 1 else 0 end ) dow_3, sum(case order_dow when '4' then 1 else 0 end ) dow_4, sum(case order_dow when '5' then 1 else 0 end ) dow_5, sum(case order_dow when '6' then 1 else 0 end ) dow_6)

l.map(x=>s"sum(case order_dow when '$x' then 1 else 0 end ) dow_$x").mkString(",")
res20: String = sum(case order_dow when '0' then 1 else 0 end ) dow_0,sum(case order_dow when '1' then 1 else 0 end ) dow_1,sum(case order_dow when '2' then 1 else 0 end ) dow_2,sum(case order_dow when '3' then 1 else 0 end ) dow_3,sum(case order_dow when '4' then 1 else 0 end ) dow_4,sum(case order_dow when '5' then 1 else 0 end ) dow_5,sum(case order_dow when '6' then 1 else 0 end ) dow_6

hive 30个字段
每个字段的分布
select col,count(1) as cnt from table group by col
for(col<-df.colums){
	df.groupBy(col).count().show()
}
<-等价于in

--spark
var df = spark.sql("select * from badou.orders")
var ordCnt = df.groupBy("order_dow").count()
ordCnt.show()
ordCnt.cache
ordCnt.unpersist
ordCnt.collectAsList()
df.select("order_dow").distinct.show()
ordCnt.selectExpr("max(count) as max_cnt").show()
var priors = spark.sql("select * from badou.priors")
val orderpriors = df.join(priors,"order_id")
orderpriors.show()

---jieba
create table news_seg(sentence string);
load data local inpath '/usr/usrdata/allfiles.txt' into table news_seg;

--重新对中文进行切词
--去空格替代 hive replace
select regexp_replace(sentence,' ','') from news_seg limit 2;

--第一列sentence 新闻的文本信息，第二列label，新闻标签
select split(regexp_replace(sentence,' ',''),'##@@##')[0] as sentence,
split(regexp_replace(sentence,' ',''),'##@@##')[1] as label 
 from news_seg limit 2;
 --把数据放入新的表中(news_noseg)
 create table news_noseg as select split(regexp_replace(sentence,' ',''),'##@@##')[0] as sentence,
split(regexp_replace(sentence,' ',''),'##@@##')[1] as label 
 from news_seg;

select * from news_noseg limit 10;