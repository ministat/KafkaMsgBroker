package net.local.example

import java.util.concurrent.TimeUnit

import com.google.common.collect.ImmutableList
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.{Schema, Type}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.joda.time.DateTime

object CreateFixTable {

  val fixSchema: Schema = {
    val columns = ImmutableList.of(
      new ColumnSchemaBuilder("transacttime", Type.INT64).key(true).build(),
      new ColumnSchemaBuilder("stocksymbol", Type.STRING).key(true).build(),
      new ColumnSchemaBuilder("clordid", Type.STRING).key(true).build(),
      new ColumnSchemaBuilder("msgtype", Type.STRING).key(false).build(),
      new ColumnSchemaBuilder("orderqty", Type.INT32).nullable(true).key(false).build(),
      new ColumnSchemaBuilder("leavesqty", Type.INT32).nullable(true).key(false).build(),
      new ColumnSchemaBuilder("cumqty", Type.INT32).nullable(true).key(false).build(),
      new ColumnSchemaBuilder("avgpx", Type.DOUBLE).nullable(true).key(false).build(),
      new ColumnSchemaBuilder("lastupdated", Type.INT64).key(false).build())
    new Schema(columns)
  }

  val schema =
    StructType(
      StructField("transacttime", LongType, false) ::
        StructField("stocksymbol", StringType, false) ::
        StructField("clordid", StringType, false) ::
        StructField("msgtype", StringType, false) ::
        StructField("orderqty", IntegerType, true) ::
        StructField("leavesqty", IntegerType, true) ::
        StructField("cumqty", IntegerType, true) ::
        StructField("avgpx", DoubleType, true) ::
        StructField("lastupdated", LongType, false) :: Nil)

  def createFixTable(
    kuduMaster: String,
    tableName: String,
    numberOfHashPartitions: Int,
    numberOfDays: Int,
    tabletReplicas: Int): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark-SQL kafka-kudu")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)
    if(kuduContext.tableExists(tableName )) {
      System.out.println("Deleting existing table with same name.")
      kuduContext.deleteTable(tableName)
    }

    val options = new CreateTableOptions()
      .setRangePartitionColumns(ImmutableList.of("transacttime"))
      .addHashPartitions(ImmutableList.of("stocksymbol"),numberOfHashPartitions)
      .setNumReplicas(tabletReplicas)
    val today = new DateTime().withTimeAtStartOfDay() //today at midnight
    val dayInMillis = TimeUnit.MILLISECONDS.convert(1,TimeUnit.DAYS) //1 day in millis
    for (i <- 0 until numberOfDays) {
      val lbMillis = today.plusDays(i).getMillis
      val upMillis = lbMillis+dayInMillis-1
      val lowerBound = fixSchema.newPartialRow()
      lowerBound.addLong("transacttime",lbMillis)
      val upperBound = fixSchema.newPartialRow()
      upperBound.addLong("transacttime",upMillis)
      options.addRangePartition(lowerBound,upperBound)
    }
    kuduContext.createTable(tableName, schema, Seq("transacttime","stocksymbol","clordid"),options)

    System.out.println("Created new Kudu table " + tableName + " with " + numberOfHashPartitions + " hash partitions and " + numberOfDays + " date partitions. ")
    spark.close()
  }
}
