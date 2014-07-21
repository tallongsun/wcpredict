package InsaneCrawler

import java.io.PrintWriter
import java.io.File
import scala.actors._
import scala.actors.Actor._
import scala.collection.mutable.Map
import java.net.Authenticator
import java.net.URL
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileOutputStream
import java.io.InputStream


object InsaneCrawler {
	val base_url = "http://38.103.161.147/forum/"
  
	val use_proxy = true
//	val mode = "down"
	val mode = "see"
	
	var forum_ids = Map("YM"->230,"WM"->143)
	val wtfdir = "test"
	  
	val TopicsSymbol = """<tbody[\s\S]*?normalthread[\s\S]*?>[\s\S]*?</tbody>""".r
	// GET: 1-url 2-title 3-star 4-comment 5-view 6-time
	val PSymbol = """<span id[\s\S]*?<a href="([\s\S]*?)"[\s\S]*?>([\s\S]*?)</a>[\s\S]*?<img[\s\S]*?<td[\s\S]*?author[\s\S]*?img[\s\S]*?>[\s\S]*?(\d+)[\s\S]*?</cite>[\s\S]*?<td[\s\S]*?nums">[\s\S]*?(\d+)[\s\S]*?<em>(\d+)[\s\S]*?lastpost[\s\S]*?<a href[\s\S]*?>([\s\S]*?)</a>""".r
  
	val ImgsSymbol = """img src="(http[^"]+)"""".r
	// GET: 1-url 2-title 
	val TorrentsSymbol = """a href="(attach[=0-9a-zA-Z\.\?]+)[\s\S]*?>([^<>"]*?torrent)""".r
	
	def main(args: Array[String]) {
	  	val start = System.nanoTime()
	  	if(mode == "see"){
	  	  get_all_analytics()
	  	}else if(mode == "down"){
	  	  down_imgs_torrents()
	  	}
	  	println("Elapsed time:"+(System.nanoTime()-start)/1000000000+"s")
	}
	
	def down_imgs_torrents(){
		install_proxy()
		val base_forum_url = base_url + "forum-%d-%d.html"
		
		val dir = new File(wtfdir)
		if(!dir.exists()){
		  dir.mkdirs()
		}
		
	    //val pages = Range(5,40,5)
	    val pages = Range(2,6,2)
	    //所有版块
	    for ((k,forum_id) <- forum_ids){
	      var end = 0
	      //所有页
          for (page <- pages.toList){
            val begin = end + 1
            end = page
            val receiver = self
            //从begin页到end页启动一个actor线程,actor中的函数的参数千万别用var，会有多线程问题
            actor{receiver ! get_from_pages(base_forum_url, forum_id, begin, page)}
            //get_from_pages(base_forum_url, forum_id, begin, end)
          }
	    }

	    for ((k, forum_id) <- forum_ids) {
	      for (page <- pages.toList) {//按ranges遍历要toList下
	    	  println("receving:"+forum_id+","+page)
	    	  receiveWithin(3600000){
	    	    case map:Map[_,_]=>
	    	      val topicMap = map.asInstanceOf[Map[String,Topic]]
	    	      println("finish:"+topicMap.size)
	    	    case TIMEOUT=>
	    	      println("TIME OUT!")
	    	  }
	      }
	    }
	}
	
	def get_all_analytics(){
	  	val receiver = self
	  	for((k,forum_id) <- forum_ids){
	  	  actor{receiver ! down_topics_and_store(forum_id)}
	  	}
	  	for((k,forum_id) <- forum_ids){
	  	  receiveWithin(3600000){
	  	    case result:String=>
	  	      println(result)
	  	  	case TIMEOUT=>
	    	  println("TIME OUT!")
	  	  }
	  	}
	}
	
	def down_topics_and_store(forum:Int) = {
	  	install_proxy()
		val base_forum_url = base_url + "forum-%d-%d.html"
		
		val pages = Range(1,1024)
		var topics = Map[String,Topic]()
		for(page <- pages.toList){
			topics = topics ++ get_links_from_page(base_forum_url.format(forum,page))
		}
	  	
	  	var topic_list = List[Topic]()
	  	for((k,topic) <- topics){
	  	  topic_list = topic :: topic_list 
	  	}
	  	val sortedList = topic_list.sortWith((x,y)=>x.star>y.star)
	  	val writer = new PrintWriter(new File(forum+".txt"))
	  	for(t<- sortedList){
	  	    writer.write(t.toString+"\n")
	  	}
	  	writer.close()
	  	"OK"
	}
	
	def get_from_pages(base_forum_url:String, forum_id:Int, begin:Int, end:Int)={
		println("actor start:"+forum_id+","+begin+"-"+end)
		var topics = Map[String,Topic]()
		for( i <- (begin to end)){
		  val url = base_forum_url.format(forum_id,i)
		  topics = topics ++ get_links_from_page(url)
		}
		println("actor end:"+forum_id+","+begin+"-"+end)
		topics
	}
	
	def get_links_from_page(url:String)={
		println("GET PAGE:"+url)
		val topics = Map[String,Topic]()
		val content = get_content_from_url(url)
		val topics_html = TopicsSymbol.findAllMatchIn(content).toList
		//所有主题
		for (h <- topics_html){
		  val matched =PSymbol.findFirstMatchIn(h.matched)
		  val url = base_url + matched.get.group(1)
		  val title = matched.get.group(2)
		  val star = matched.get.group(3).toInt
		  val comment = matched.get.group(4).toInt
		  val view = matched.get.group(5).toInt
		  val time = matched.get.group(6)
		  val topic = new Topic(title,url,star,comment,view,time)
		  topics(title) = topic
		  //println(topics(title))
		}
		//println(topics.size)
		
		if(mode == "down"){
		  for((k,v) <- topics){
		    down_link_imgs_torrents(v)
		  }
		}
		topics
	}

	
	def down_link_imgs_torrents(topic:Topic){
	  //println("GET TOPIC:"+topic.url)
	  
	  val dirname = wtfdir+"/"+get_valid_filename(topic.title)
	  val dir = new File(dirname)
	  if(!dir.exists()){
		dir.mkdirs()
	  }
	  
	  val content = get_content_from_url(topic.url)
	  
	  val imgs = ImgsSymbol.findAllMatchIn(content).toSet
	  val torrents = TorrentsSymbol.findAllMatchIn(content).toSet
	  
	  //println(imgs,torrents)
	  
	  for(img <- imgs){
	    down_link(img.group(1),dirname+"/"+get_valid_filename(img.group(1)))
	  }
	  for(t <- torrents){
	    down_link(base_url+t.group(1),dirname+"/"+get_valid_filename(t.group(2)))
	  }
	}
	
	def get_content_from_url(reqUrl:String) = {
	  val url = new URL(reqUrl)
	  val urlConn = url.openConnection();
  	  urlConn.setConnectTimeout(5000);
	  urlConn.setReadTimeout(5000);
	  var in:InputStream = null
	  var fos:FileOutputStream = null
	  var content = ""
	  try{
	    in = urlConn.getInputStream()
	    var buffer = new BufferedReader(new InputStreamReader(in,"gbk"))
	    var line = ""
	    do{
	      line = buffer.readLine()
	      if(line!=null){
	        content += line
	      }
	    }while(line != null)
	  }catch{
	  	  case ex:Exception => println(ex)
	  }finally{
		  if(in!=null)in.close()
		  if(fos!=null)fos.close()
	  }
	  content
	}

	def down_link(reqUrl:String,filename:String){
	  val file = new File(filename)
	  if(file.exists()){
		return
	  }
	  
	  val url = new URL(reqUrl)
	  val urlConn = url.openConnection();
	  urlConn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 5.1; rv:22.0) Gecko/20100101 Firefox/22.0")
	  //超时一定要加上，不然很容易把actor线程给阻塞住
	  urlConn.setConnectTimeout(5000);
	  urlConn.setReadTimeout(5000);
	  var in:InputStream = null
	  var fos:FileOutputStream = null
	  try{
	      in = urlConn.getInputStream()
		  var buffer = new Array[Byte](1024)
		  fos = new FileOutputStream(file)
		  var length = 0
		  do{
		    length = in.read(buffer)
		    if(length>0){
		    	fos.write(buffer, 0, length)
		    }
		  }while(length>0)
	  }catch{
	  	  case ex:Exception => println(ex)
	  }finally{
		  if(in!=null)in.close()
		  if(fos!=null)fos.close()
	  }

	}
	
	def install_proxy(){
		if(use_proxy == false)
			return
		System.setProperty("http.proxyHost","localhost")  
		System.setProperty("http.proxyPort","8087")  
	    return
	}
	
	
	def get_valid_filename(filename:String)={
	  var result = ""
	  val keepcharacters = List(' ','.','_')
	  for(c <- filename.seq){
	    if(c.isLetterOrDigit || keepcharacters.contains(c)){
	    	result=result+c
	    }
	  }
	  result
	}
	
}

class Topic(val title:String,val url:String,val star:Int,val comment:Int,val view:Int,val time:String){
  override def toString():String = star+","+url+","+title+","+comment+","+view+","+time
}