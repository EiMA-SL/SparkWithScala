package Ex02


import commons.Utils
import org.apache.spark.{SparkConf, SparkContext, util}

object Task02 {

  def main(args: Array[String]): Unit = {
    AirPortsLocatedInIreland()
  }

  def AirPortsLocatedInIreland(): Unit ={
    val conf= new SparkConf().setAppName("EX02").setMaster("local")
    val sc=new SparkContext(conf)
    val airPorts= sc.textFile("Files/airports.text")

    val airPortsInIreland=airPorts.filter(line => line.split(Utils.COMMA_DELIMITER)(3)=="\"Ireland\"")

    val count=airPortsInIreland.count()
    println("countOfAirportsInIreLand : "+count)

    val AirPortsLatitude40=airPorts.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toFloat>40)
    val count40 = AirPortsLatitude40.count()
    println("countLatitudeGreaterThan40 : " + count40)

    val airPortsInUSA=airPorts.filter(line => line.split(Utils.COMMA_DELIMITER)(3)=="\"United States\"")

    val airportsNameAndCityNames = airPortsInUSA.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(6)
    })

    airportsNameAndCityNames.saveAsTextFile("Files/airports_in_usa.text")

    val CountriesAndCities=airPorts.flatMap(line => line.split(Utils.COMMA_DELIMITER)(3))

    for (country <- CountriesAndCities){

    }
  }
}
