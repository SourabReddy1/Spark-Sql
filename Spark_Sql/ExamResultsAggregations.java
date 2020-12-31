/*To set the datatypes like how we wish we might use InferSchema
 * Using Inferschema would be costly as it would traverse over all the data once more
 * We could avoid inferschema by using the following way
 * We use Agg method to call all the aggregations
 * agg(max)
 * 
 */


package Spark_Sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class ExamResultsAggregations {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	 SparkSession spark = SparkSession.builder().appName("Spark-Sql").master("local[*]")
				   .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
             .getOrCreate();
     Logger.getLogger("org.apache").setLevel(Level.WARN);
     
     Dataset<Row> dataset = spark.read().option("header", "true").csv("src\\main\\java\\Exercises\\students.csv");
     
     
     
     
     /*dataset = dataset.groupBy("subject").agg(max(col("score").cast(DataTypes.IntegerType)).alias("max_Score"), 
											  min(col("score").cast(DataTypes.IntegerType)).alias("min_score"));
											  */
     
     //Practice
     //Create a pivot table using subject on the left and year column on the top with mean and stddev on the score
     
    dataset = dataset.groupBy("subject").pivot("year").agg(round(avg(col("score")), 2).alias("average"),
    														round(stddev(col("score")), 2).alias("stddev"));

     
     dataset.show();
	}

}
