package Ex03

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object Task01 {

  val bufferedSourceInput = Source.fromFile("Files/FirstNames.txt")
  val bufferedSourceOutput = Source.fromFile("Files/LastNames.txt")

  def main(args: Array[String]): Unit = {
    countWords()
    mergeNames(bufferedSourceInput,bufferedSourceOutput)
  }

  def countWords(): Unit ={

    val conf= new SparkConf().setAppName("EX03").setMaster("local")
    val sc=new SparkContext(conf)
    val TextFile= sc.textFile("Files/Task01.txt")
    val words = TextFile.flatMap(char => char.split(" "))

    val wordsCount=words.count();
    println("Words Count :"+wordsCount)

  }

  def mergeNames(sourceInput: Source,sourceOutput: Source): Unit ={
    val Fnamelines = (for (line <- sourceInput.getLines()) yield line).toList
    val LNamelines = (for (line <- sourceOutput.getLines()) yield line).toList
    var count =0;
    for(name <- Fnamelines){
      println(name+" "+LNamelines(count))
      count=count+1
    }
  }

}
