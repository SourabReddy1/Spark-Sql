/*DataFrame is nothing but a dataset of rows
 * Java API for spark sql
 * Let's convert sql statement into java like functions
 * Pivot table is useful when we have two groupings along with some aggregations
 * A pivot can be thought of as translating rows into columns while applying one or more aggregations
 * 
 * */

package Spark_Sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

public class DataFrame {

	public static void main(String[] args)
	{
		SparkSession spark = SparkSession.builder().appName("Spark-Sql").master("local[*]")
				   .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        
        Dataset<Row> dataset = spark.read().option("header", "true").csv("src\\main\\java\\Exercises\\biglog.txt");
        
        //Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(*) as total"
			//+ " from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level");
        
        /*SimpleDateFormat input = new SimpleDateFormat("MMMM");//Month String
        SimpleDateFormat output = new SimpleDateFormat("M");//Month number
        
        spark.udf().register("monthnum", (String month)-> { 
        java.util.Date inputDate =	input.parse(month);
        return Integer.parseInt(output.format(inputDate));
        	}, DataTypes.IntegerType);
        	*/
        
        //Dataset<Row> results = spark.sql("select level, date_format(datetime, 'MMMM') as month, count(*) as total"
		//+ " from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level");
	 
        dataset = dataset.select(col("level"), date_format(col("datetime"), "MMMM").alias("month"), 
        						date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));
        
        dataset = dataset.groupBy(col("level"), col("month"), col("monthnum")).count();
        dataset = dataset.orderBy(col("monthnum"));
        dataset = dataset.drop(col("monthnum"));
        
        
        
        Object[] months = new Object[] {"January", "febraury", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
        List<Object> columns = Arrays.asList(months);
        
        
        //dataset = dataset.groupBy("level").pivot("month", columns).count();
        
        dataset.show();
        
        spark.close();
	
	
	}
}
