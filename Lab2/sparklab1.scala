// Read the tweets DB
import sys.process._
"wget -P /tmp https://www.datacrucis.com/media/datasets/stratahadoop-BCN-2014.json" !!
val localpath="file:/tmp/stratahadoop-BCN-2014.json"
dbutils.fs.mkdirs("dbfs:/datasets/")
dbutils.fs.cp(localpath, "dbfs:/datasets/")
display(dbutils.fs.ls("dbfs:/datasets/stratahadoop-BCN-2014.json"))
val df = sqlContext.read.json("dbfs:/datasets/stratahadoop-BCN-2014.json")
df.printSchema()

// Find hashtags in tweets and count them
def cleanHashtags(input: String):String = {
  return input.stripSuffix(".").stripSuffix(",")
              .stripSuffix("!").stripSuffix(":")
              .stripSuffix("\'").stripSuffix("?")
              .stripSuffix("…").stripSuffix("-")
              .stripSuffix("\"")
              .trim
}

val initRDD = df.select($"text").rdd.map(row => row.getString(0))
val hashtagsRDD = initRDD.flatMap(_.toLowerCase.split(" "))
                          .filter(_.startsWith("#"))
                          .map(cleanHashtags(_))
                          .map(word => (word,1)).reduceByKey((a,b) => a+b)
						  
// Select 10 most frequent
val tenMostFreqRDD = hashtagsRDD.sortBy(_._2, false).take(10).foreach(println)

// Select 10 users with more tweets
val initUsersRDD = df.select($"user.id_str",$"user.name").rdd//.map(row => row.getString(0))
val redUsersRDD = initUsersRDD.map(word => (word,1)).reduceByKey((a,b) => a+b)
val topTenUsersRDD = redUsersRDD.sortBy(_._2, false).take(10).foreach(println)

// Detect trending topics
// Steps:
/*
0) define a PERIOD and a minimum number of mentions for a topic to be trending
1) "text, time" (string containing the time and the text)
2) (text, time) (pair with time and text)
3) ((hashtag, period), frequency)
4) (period, top_hashtag)
*/
import java.time.Instant

def getMonth(input: String) : String = {
  input match {
    case "Jan" => return "01"
    case "Feb" => return "02"
    case "Mar" => return "03"
    case "Apr" => return "04"
    case "May" => return "05"    
    case "Jun" => return "06"
    case "Jul" => return "07"
    case "Aug" => return "08"
    case "Sep" => return "09"
    case "Oct" => return "10"
    case "Nov" => return "11"
    case "Dec" => return "12"
  }
}

def stringToParsable(input: String) : String = {
  val datearray = input.split(" ")
  return ""+datearray(5)+"-"+getMonth(datearray(1))+"-"+datearray(2)+"T"+datearray(3)+".000Z"
  //2017-07-02T17:04:39.911Z
}

val period = 86400
val minMention = 100

// Step 1 - obtain couple (creation_time, text)
val step1RDD =  df.select($"text",$"created_at").rdd
// Step 2 - tokenize -> obtain hashtags
val step2RDD = step1RDD.flatMap(row => {
  for (el <- row.getString(0).toLowerCase.split(" ")) yield{
    (el,row.getString(1))
  }
}).filter(_._1.startsWith("#"))
val step3RDD = step2RDD.map({
  case (x,y) => ((cleanHashtags(x),(Instant.parse(stringToParsable(y)).getEpochSecond/period).toLong), 1)
}).reduceByKey(_+_)
//step3RDD.count()
val step4RDD = step3RDD.filter(_._2 >= minMention).map({
  case (x,y) => {
    // x = (hashtag, period) - y = frequency
    (x._2, (x._1, y))
  }
}).sortByKey().groupByKey().map({
  case (x,y) => {
    var myMin = 99999;
    var myHT = "";
    for (el <- y){
      if (myMin > el._2){
        myMin = el._2
        myHT = el._1
      }
    }
    (x, (myHT, myMin))
  }
}).take(100).foreach(println)