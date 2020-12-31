package Spark_Sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import breeze.linalg.cholesky;

import static org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class JsonExample {

	public static void main(String[] args) throws AnalysisException
	{
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkSession spark = SparkSession.builder().appName("JSON").master("local[*]").getOrCreate();
		Dataset<Row> data = spark.read().json("src\\main\\java\\Exercises\\sample.json");
		data.createOrReplaceTempView("hi");
		
		Dataset<Row> test = data.select("name").where("age >= 30");
		test.show();
		
	}
}
