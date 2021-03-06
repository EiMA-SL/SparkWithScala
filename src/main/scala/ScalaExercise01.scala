
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaExercise01 {



  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setAppName("App").setMaster("local")
    val sc = new SparkContext(conf)
    val File: RDD[String] = sc.textFile("in/clickstream.csv")
    //val x = SelectOption()
    println("Number Of Clicks Per User")
    ClicksPerUser(File)
    println("Number Of Clicks Per Product")
    ClicksPerProduct(File)
  }


  def ClicksPerUser(rdd :RDD[String]): Unit ={
   // rdd.take(1).foreach(println)  <-Take Lines
   // val userClicks = rdd.map(line => line.split(Utils.COMMA_DELIMITER)(0))

   // val userCount=userClicks.countByValue()
   // println(userCount)

    val wordRdd = rdd.map(line => line.split(",")(0))
    val wordPairRdd = wordRdd.map(word => (word, 1))

    val wordCounts = wordPairRdd.reduceByKey((x, y) => x + y)
    for ((word, count) <- wordCounts.collect()) println(word + " : " + count)

  }
  def ClicksPerProduct(rdd :RDD[String]): Unit ={

    val wordRdd = rdd.map(line => line.split(",")(1))
    val wordPairRdd = wordRdd.map(word => (word, 1))

    val wordCounts = wordPairRdd.reduceByKey((x, y) => x + y)
    for ((word, count) <- wordCounts.collect()) println(word + " : " + count)

  }



}
