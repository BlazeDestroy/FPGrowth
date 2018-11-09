import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.SparkSession


object FpGrowthRetail {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val inputFile = "Online Retail.csv"

    val ss = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    //read file with purchases
    //val fileRDD: RDD[String] = sc.textFile(inputFile)

    //get transactions
    //val products: RDD[Array[String]] = fileRDD.map(s => s.split(";"))

    val dataSetSQL = new DataSetSQL(ss, inputFile)
    val dataFrameOfInvoicesAndStockCodes = dataSetSQL.getInvoiceNoAndStockCode()

    val keyValue = dataFrameOfInvoicesAndStockCodes.rdd.map(row => (row(0), row(1).toString))
    val groupedKeyValue = keyValue.groupByKey()
    val transactions = groupedKeyValue.map(row => row._2.toArray.distinct)

    //get frequent patterns via FPGrowth
    val fpg = new FPGrowth().setMinSupport(0.02)

    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach { itemset => println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    //get association rules
    val minConfidence = 0.01
    val rules2 = model.generateAssociationRules(minConfidence)
    val rules = rules2.sortBy(r => r.confidence, ascending = false)

    val dataFrameOfStockCodeAndDescription = dataSetSQL.getStockCodeAndDescription()
    val dictionary = dataFrameOfStockCodeAndDescription.rdd.map(row => (row(0).toString, row(1).toString)).collect().toMap

    rules.collect().foreach { rule =>
      println(
        rule.antecedent.map(s => dictionary(s)).mkString("[", ",", "]")
          + " => " + rule.consequent.map(s => dictionary(s)).mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }
}
