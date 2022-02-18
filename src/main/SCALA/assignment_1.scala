import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object assignment_1 extends App {

  //setting log level for error printing

  Logger.getLogger("org").setLevel(Level.ERROR)

  //sparkcontext creation for local machine

  val sc = new SparkContext("local[*]", "Spark_Assignment_1")
  //loading data to RDD
  val userData = sc.textFile("src/main/resources/user.csv")
  val transactionData = sc.textFile("src/main/resources/transaction.csv")

  def parseLine1(line1: String): (Int, String) = {
    val fields = line1.split(",")
    val userID = fields(0).toInt
    val location = fields(3)
    (userID, location)
  }
  def parseLine2(line2: String): (Int, Int, String) = {
    val fields = line2.split(",")
    val userID = fields(2).toInt
    val price = fields(3).toInt
    val productDescription = fields(4)
    (userID, price, productDescription)
  }
  val filterRDD1 = userData.filter(x => x != userData.first())
  val filterRDD2 = transactionData.filter(x => x != transactionData.first())

  val RDD1 = filterRDD1.map(parseLine1)
  val RDD2 = filterRDD2.map(parseLine2)

  val col1: List[(Int, String)] = RDD1.collect().toList
  val col2: List[(Int, Int, String)] = RDD2.collect().toList

  def countingUniqueLocations(): Int = {
    println(col1.distinct)
    println(col2)

    val sortingLocationData = col1.flatMap(x => col2.map(y => if (x._1 == y._1)(x._1 -> x._2)))
    val findingProducts = sortingLocationData.distinct
    val uniqueLoactions = col1.groupBy(m => (m._2)).keys.toList
    val sortOutData = findingProducts.filter(x => x.!=())
    println(sortOutData)
    println(uniqueLoactions)

    val countingTotalLocation = uniqueLoactions.size
    return countingTotalLocation
  }
  def uniqueUserByProduct(): List[Any] = {
    val dataSorting = col1.flatMap(x => col2.map(y => if(x._1 == y._1) (y._3)))
    val sortingProducts = dataSorting.filter(x => x != ())
    return sortingProducts
  }
  def totalAmountSpent(): Int = {
    var totalSpending = 0
    val sortingproducts = col1.distinct.flatMap(x => col2.distinct.map(y => if (x._1 == y._1) totalSpending += y._2))
    val sortingUsers = col1.distinct.flatMap(x => col2.distinct.map(y => if (x._1 == y._1)(x._1, y._2, y._3)))
    val sortingUserSpendings = sortingUsers.filter(x => x !=())
    println(sortingUserSpendings)
    return totalSpending
  }
  println("Total number of unique locations are" + "\n" + countingUniqueLocations)
  //println(countingUniqueLocations + "\n")
  println("Total products brought by each user" + "\n" + uniqueUserByProduct)
  //println(uniqueUserByProduct + "\n")
  println("Total spendings by each user on every Products" + "\n" + totalAmountSpent)
  //println(totalAmountSpent + "\n")
}
