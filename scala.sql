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
val a = List("Looking back on the Victorian era","some sort pictured in","jumped out of a frying-pan into a fire")

python正常函数
def f(str):
	return str.split(" ")
python匿名函数
lambda str:str.split(" ")
scala匿名函数
str=>str.split(" ")
_.split(" ")

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