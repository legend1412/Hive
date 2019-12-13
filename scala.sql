定义变量 val不可变，var可变
val s="""Looking back on the Victorian era, whose ripeness, decline,and ‘fall-of’ is in some sort pictured in “The Forsyte Saga,” we see now that we have but jumped out of a frying-pan into a fire. It would be difficult to substantiate a claim that the case of England was better in 1913 than it was in 1886, when the Forsytes assembled at Old Jolyon’s to celebrate the engagement of June to Philip Bosinney. And in 1920, when again the clan gathered to bless the marriage of Fleur with Michael"""

s.split(" ")

res3: Array[String] =
Array(Looking, back, on, the, Victorian, era,, whose, ripeness,, decline,, and, ‘fall-of’, is, in, some, sort, pictured, in, “The, Forsyte, Saga,”, we, see, now, that, we, have, but, jumped, out, of, a, frying-pan, into, a, fire., It, would, be, difficult, to, substantiate, a, claim, that, the, case, of, England, was, better, in, 1913, than, it, was, in, 1886,, when, the, Forsytes, assembled, at, Old, Jolyon’s, to, celebrate, the, engagement, of, June, to, Philip, Bosinney., And, in, 1920,, when, again, the, clan, gathered, to, bless, the, marriage, of, Fleur, with, Michael)
--读取文本数据The_man_of_property
import scala.io.Source
val lines = Source.fromFile("/usr/usrdata/The_man_of_property.txt").getLines().toList
