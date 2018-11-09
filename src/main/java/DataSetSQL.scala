import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

class DataSetSQL(sparkSession: SparkSession, input: String) {
  var dataSetSchema = StructType(Array(
    StructField("InvoiceNo", StringType, true),
    StructField("StockCode", StringType, true),
    StructField("Description", StringType, true),
    StructField("Quantity", IntegerType, true),
    StructField("InvoiceDate", StringType, true),
    StructField("UnitPrice", DoubleType, true),
    StructField("CustomerID", IntegerType, true),
    StructField("Country", StringType, true)))

  //Read CSV file to DF and define scheme on the fly
  private val gdelt = sparkSession.read
    .option("header", "true")
    .option("delimiter", ";")
    .option("nullValue", "")
    .option("treatEmptyValuesAsNulls", "true")
    .schema(dataSetSchema)
    .csv(input)

  gdelt.createOrReplaceTempView("dataSetTable")

  //Find the most mentioned actors (persons)
  def getInvoiceNoAndStockCode(): DataFrame = {
    sparkSession.sql(
      " SELECT InvoiceNo, StockCode" +
        " FROM dataSetTable" +
        " WHERE InvoiceNo IS NOT NULL AND StockCode IS NOT NULL")
  }

  def getStockCodeAndDescription(): DataFrame = {
    sparkSession.sql(
      "SELECT DISTINCT StockCode, Description" +
        " FROM dataSetTable" +
        " WHERE StockCode IS NOT NULL AND Description IS NOT NULL")
  }
}