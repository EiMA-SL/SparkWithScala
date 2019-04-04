package Ex01


import java.io._
import org.apache.spark.{SparkConf, SparkContext}

object Task01 {
  def main(args: Array[String]): Unit = {

    //countWords()
    countChars()

  }

  def countWords(): Unit ={

    val conf=new SparkConf().setAppName("Task01").setMaster("local")
    val sc=new SparkContext(conf)
    val TextFile= sc.textFile("Files/Test01.txt")
    val words = TextFile.flatMap(word => word.split(" "))

    val wordCounts = words.countByValue()
    for((word,count)  <- wordCounts) println(word + " : "+count)
  }

  def countChars(): Unit ={

    val conf=new SparkConf().setAppName("Task01").setMaster("local")
    val sc=new SparkContext(conf)
    val TextFile= sc.textFile("Files/Test01.txt")
    val chars = TextFile.flatMap(char => char.split(""))

    val charCount=chars.count();
    println("Character Count :"+charCount)

    val charUniqueCount=chars.countByValue()

    val writer=new PrintWriter(new File("Files/TestResults.txt"))


    println("Unique Character Count :")
    for((char,count)  <- charUniqueCount){
      println(char + " : "+count)
      writer.write(char + " : "+count+"\n")
    }
    writer.close()

  }


}
