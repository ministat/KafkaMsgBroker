package net.local.example

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KuduFixDataStreamer {

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

  def kuduFixDataStreamWriter(brokers: String,
                              topics: String,
                              kuduMaster: String,
                              tableName: String,
                              runLocal: Boolean): Unit = {

    var spark = SparkSession
      .builder()
      .appName("Kudu StockStreamer")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    if (runLocal) {
      spark = SparkSession
        .builder()
        .appName("Kudu StockStreamer")
        .config("spark.some.config.option", "some-value")
        .config("spark.master", "local")
        .getOrCreate()
    }
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val ssc = new StreamingContext(sc, Seconds(5))
    var kuduContext: KuduContext = new KuduContext(kuduMaster, sc)
    val broadcastSchema = sc.broadcast(schema)

    val topicSet = topics.split(",").toSet

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> s"kudu-stream-${System.currentTimeMillis()}"
    )
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicSet, kafkaParams))
    val parsed = messages.map(line => {
      parseFixEvent(line.value())
    })
    parsed.foreachRDD(rdd => {
      val df = sqlContext.createDataFrame(rdd,broadcastSchema.value)
      kuduContext.upsertRows(df,tableName)
    })

    sys.ShutdownHookThread {
      println("Gracefully stopping Spark Streaming Application")
      spark.close();
      ssc.stop(true, true)
      println("Application stopped")
    }

    // Start the computation
    ssc.checkpoint("./checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Takes a generated FIX Message string, parses into key-value pairs, and returns a Row
   *
   * @param eventString - string of key value pairs formatted FIX event
   * @return - Row representing a single FIX order or execution report
   */
  def parseFixEvent(eventString: String) : Row = {
    var fixElements = scala.collection.mutable.Map[String,String]()
    val pattern = """(\d*)=(.*?)(?:[\001])""".r
    pattern.findAllIn(eventString).matchData.foreach {
      m => fixElements. += (m.group(1) -> m.group(2))
    }
    try {
      Row(
        fixElements("60").toLong /* transacttime = 60: transacttime */,
        fixElements("55") /* stocksymbol = 55: symbol */,
        fixElements("11") /* clordid = 11: clordid */,
        fixElements("35") /* msgtype = 35: msgtype */,
        if (fixElements("35").equals("D")) fixElements("38").toInt else null /* orderqty = 38: orderqty */,
        if (fixElements.contains("151")) fixElements("151").toInt else null /* leavesqty = 151: leavesqty */,
        if (fixElements.contains("14")) fixElements("14").toInt else null /* cumqty = 14: cumqty */,
        if (fixElements.contains("6")) fixElements("6").toDouble else null /* avgpx = 6: avgpx */,
        System.currentTimeMillis()
      )
    } catch {
      case e:Exception => e.printStackTrace()
        null
    }
  }

}
