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
    String _sql_pat = "SELECT stocksymbol, max(orderqty) AS max_order," +
    "CAST((CAST(transacttime/10000 AS bigint)*10000)/1000 as timestamp) AS 10_s_time_window FROM " +
    "kafka_kudu WHERE transacttime > (CAST(unix_timestamp(to_utc_timestamp(now(),'PDT'))/10 AS bigint)*10 - %d)*1000 " +
            "AND transacttime < (CAST(unix_timestamp(to_utc_timestamp(now(),'PDT'))/10 AS bigint)*10 - 10)*1000 " + "" +
            "GROUP BY stocksymbol, 10_s_time_window ORDER BY stocksymbol, 10_s_time_window";
    public QueryTable(String kuduMasters, String kuduTable, String sparkMaster) {
        _spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .config("spark.master", sparkMaster)
                .getOrCreate();
        Dataset<Row> df = _spark.read()
                .option("kudu.master", kuduMasters)
                .option("kudu.table", kuduTable)
                .option("kudu.scanLocality", "leader_only")
                .format("kudu").load();
        df.createOrReplaceTempView("kafka_kudu");
    }

    public void Stop() {
        _spark.close();
    }

    private String getSql(int seconds) {
        return String.format(_sql_pat, seconds);
    }

    public String Query(int seconds) {
        StringBuilder sb = new StringBuilder();
        Dataset<Row> result = _spark.sql(getSql(seconds));
        Dataset<String> formattedResult = result.map(
                (MapFunction<Row, String>)
                        row -> {
                            if (row.size() > 0) {
                                return row.getString(0) + "," + row.getInt(1) + "," + row.getTimestamp(2);
                            } else {
                                return "";
                            }
                        },
                Encoders.STRING());
        sb.append("symbol,orderqty,timestamp").append(System.lineSeparator());
        Iterator<String> it = formattedResult.toLocalIterator();
        while (it.hasNext()) {
            sb.append(it.next());
            sb.append(System.lineSeparator());
        }
        return sb.toString();
    }
}
