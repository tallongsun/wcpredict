package wcpredictor

import scala.actors._
import scala.actors.Actor._
import scala.collection.mutable.Map
import java.io.PrintWriter
import java.io.File

object MatchDataCrawler {
	val sourceUrls = List("http://www.fifa.com/tournaments/archive/worldcup/southafrica2010/matches/index.html",
	    "http://www.fifa.com/tournaments/archive/worldcup/germany2006/matches/index.html",
	    "http://www.fifa.com/tournaments/archive/worldcup/koreajapan2002/matches/index.html",
	    "http://www.fifa.com/tournaments/archive/worldcup/france1998/matches/index.html",
	    "http://www.fifa.com/tournaments/archive/worldcup/usa1994/matches/index.html",
	    "http://www.fifa.com/tournaments/archive/worldcup/italy1990/matches/index.html",
	    "http://www.fifa.com/tournaments/archive/worldcup/mexico1986/matches/index.html",
	    "http://www.fifa.com/tournaments/archive/worldcup/spain1982/matches/index.html",
	    "http://www.fifa.com/tournaments/archive/worldcup/argentina1978/matches/index.html",
	    "http://www.fifa.com/tournaments/archive/worldcup/germany1974/matches/index.html",
	    "http://www.fifa.com/tournaments/archive/worldcup/mexico1970/matches/index.html",
		"http://www.fifa.com/tournaments/archive/worldcup/england1966/matches/index.html")
		
	//非贪婪匹配
	val TableSymbol = """<table summary=[\s\S]+?</table>""".r
	val TrSymbol = """<tr [\s\S]+?</tr>""".r
	val HomeTeamSymbol = """<td class="l homeTeam"><a href="\S+?">([\s\S]+?)</a></td>""".r
	val AwayTeamSymbol = """<td class="r awayTeam"><a href="\S+?">([\s\S]+?)</a></td>""".r
	val ScoreSymbol = """<td style="width:120px" class="c ">(<div class>)?[\s\S]*?<a href="[\s\S]+?">(\d+):(\d+)[\s\S]*?</a>(</div>)?</td>""".r
  
	val totalTeam2score = Map[String,RateInfo]()
	
	def main(args: Array[String]) {
		val start = System.nanoTime()
	  
		val receiver = self//必须这里定义一下
		sourceUrls.foreach{sourceUrl=>
	  		scala.actors.Actor.actor{receiver ! getPeriodTeam2Score(sourceUrl)}
		}
		
		for(i <- (0 until sourceUrls.size)){
		  	receiveWithin(10000){
		  	  case map:Map[_,_]=>
		  	    map.foreach{value=>
		  	      	//泛型信息已经被擦除，需要强转
		  	      	val rateValue = value.asInstanceOf[(String,RateInfo)]
	  		      	if(!(totalTeam2score contains rateValue._1)){
			      	  totalTeam2score += rateValue
			      	}else{
			      	  totalTeam2score(rateValue._1).addRate(rateValue._2.sum,rateValue._2.count)
			      	}
		  	    }
		  	  case TIMEOUT=>
		  	    println("TIME OUT!")
		  	}
		}
		
		val buffer = new StringBuilder()
		totalTeam2score.foreach{value=>
			value._2.calcAvg
			buffer.append(value).append("\n")
		}
		flushToFile(buffer.toString)
		
		println("共花费"+(System.nanoTime()-start)/1000000+"ms")
	}
	
	def getPeriodTeam2Score(url:String):Map[String,RateInfo]={
	    val team2score =  Map[String,RateInfo]() 
		val data = io.Source.fromURL(url, "UTF-8").mkString
		val tables = TableSymbol.findAllMatchIn(data).toList
		
		var matchCounter = 0
		tables.foreach{table => 
		  val trs = TrSymbol.findAllMatchIn(table.matched).toList
		  var matchCounterInGroup = 0
		  //遍历所有场次赛事
		  trs.foreach{tr=>
		    val homeTeamMatch = HomeTeamSymbol.findFirstMatchIn(tr.matched)
		    val awayTeamMatch = AwayTeamSymbol.findFirstMatchIn(tr.matched)
		    val scoreMatch = ScoreSymbol.findFirstMatchIn(tr.matched)
	
		    if(homeTeamMatch!=None && awayTeamMatch!=None && scoreMatch!=None){
		    	val homeTeam = homeTeamMatch.get.group(1)
		    	val awayTeam = awayTeamMatch.get.group(1)
		      	val homeScore = scoreMatch.get.group(2).toDouble
		      	val awayScore = scoreMatch.get.group(3).toDouble
		      	if(!(team2score contains homeTeam)){
		      	  team2score += (homeTeam -> new RateInfo(homeScore,1,homeScore))
		      	}else{
		      	  team2score(homeTeam).addRate(homeScore,1)
		      	}
		      	if(!(team2score contains awayTeam)){
		      	  team2score += (awayTeam -> new RateInfo(awayScore,1,awayScore))
		      	}else{
		      	  team2score(awayTeam).addRate(awayScore,1)
		      	}
		      	matchCounter+=1
		      	matchCounterInGroup+=1
		    }
		  }
		  print(matchCounterInGroup+",")
		}
		println()
		println("【"+url+"】"+matchCounter)
		team2score
	}
	
	def flushToFile(content:String) = {
	  val writer = new PrintWriter(new File("result"+".txt"))
	  writer.write(content)
	  writer.close()
	}
}

class RateInfo(var sum:Double,var count:Int,var avg:Double){
	def addRate(rate:Double,cnt:Int)={
	  sum += rate
	  count += cnt
	}
	
	def calcAvg()={
	  avg = sum / count
	}
	
	override def toString():String = "CNT:"+count+" SUM:"+sum+" AVG:"+avg
}