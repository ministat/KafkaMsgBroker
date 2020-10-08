package net.local.example;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.apache.spark.sql.SparkSession;

public class FIXDataServlet extends HttpServlet {
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
    }


}
