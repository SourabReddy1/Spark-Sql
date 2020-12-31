/*To create data by ourselves
 *using RowFactory and StructFields
 *Performing groups and aggregations
 *WOrking on DateFormatting
  */


package Spark_Sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Array;

public class InMemory {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession spark = SparkSession.builder().appName("Spark-Sql").master("local[*]")
												   .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
			                                       .getOrCreate();
		Logger.getLogger("org.apache").setLevel(Level.WARN);
										 
		/*List<Row> inMemory = new ArrayList<>();
		
		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

		
		StructField[] fields = new StructField[]{
			new StructField("level", DataTypes.StringType, false, Metadata.empty()),
			new StructField("date", DataTypes.StringType, false, Metadata.empty())
		};
		StructType schema = new StructType(fields );
		
		Dataset<Row> dataset = spark.createDataFrame(inMemory, schema );
		
		dataset.createOrReplaceTempView("logging_table");
		
		Dataset<Row> result = spark.sql("select level, date_format(date, 'MMMM') as month , count(*) as total from logging_table group by level, month");*/
		
		
		
		Dataset<Row> dataset = spark.read().option("header", "true").csv("src\\main\\java\\Exercises\\biglog.txt");
		
		dataset.createOrReplaceTempView("logging_table");
		
		/*Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(*) as total"
				+ " from logging_table group by level, month order by monthnum");*/
		
		Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(*) as total"
				+ " from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level");
		
		results.show(100);
		
	}

}
