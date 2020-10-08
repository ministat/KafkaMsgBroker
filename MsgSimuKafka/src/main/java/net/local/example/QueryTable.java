package net.local.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.kudu.client.*;
import org.apache.kudu.spark.kudu.*;

import java.util.Iterator;

public class QueryTable {
    SparkSession _spark;
    String _sql = "SELECT stocksymbol, max(orderqty) AS max_order," +
    "CAST((CAST(transacttime/10000 AS int)*10000)/1000 as timestamp) AS 10_s_time_window FROM " +
    "kafka_kudu WHERE transacttime > (CAST(unix_timestamp(to_utc_timestamp(now(),'PDT'))/10 AS int)*10 - 600)*1000 " +
            "AND transacttime < (CAST(unix_timestamp(to_utc_timestamp(now(),'PDT'))/10 AS int)*10 - 10)*1000 " + "" +
            "GROUP BY stocksymbol, 10_s_time_window ORDER BY stocksymbol, 10_s_time_window";
    public QueryTable(String kuduMasters, String kuduTable) {
        _spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> df = _spark.read()
                .option("kudu.master", kuduMasters)
                .option("kudu.table", kuduTable)
                .option("kudu.scanLocality", "leader_only")
                .format("kudu").load();
        df.createOrReplaceTempView("kafka_kudu");
    }

    public String Query() {
        StringBuilder sb = new StringBuilder();
        Dataset<Row> result = _spark.sql(_sql);
        Dataset<String> formattedResult = result.map(
                (MapFunction<Row, String>)
                        row -> row.getString(0) + "," + row.getInt(1) + "," + row.getTimestamp(2),
                Encoders.STRING());
        sb.append("symbol,orderqty,timestamp");
        Iterator<String> it = formattedResult.toLocalIterator();
        while (it.hasNext()) {
            sb.append(it.next());
            sb.append(System.lineSeparator());
        }
        return sb.toString();
    }
}
