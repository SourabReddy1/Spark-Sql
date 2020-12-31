/*How to use lambdas to add your own functions to the SQL syntax and DataFrame API
 * Lit is used to create a new column within("name", lit("values")
 * UDF has to be registered first using spark session
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

public class UserDefinedFunctions {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		 Logger.getLogger("org.apache").setLevel(Level.WARN);
		 SparkSession spark = SparkSession.builder().appName("Spark-Sql").master("local[*]")
				    								.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				    								.getOrCreate();
		
		 spark.udf().register("hasPassed", (String grade, String subject)-> {
			 if(subject.equals("Biology")) {
				 if(grade.startsWith("A"))
					 return true;
				 return false;
			 }
			 return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
			 }, DataTypes.BooleanType);
	
		 Dataset<Row> dataset = spark.read().option("header", "true").csv("src\\main\\java\\Exercises\\students.csv");
		 
		 dataset.show();
		 
		 //Will create a new column
		 dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject"))); //To add additional column in the dataset
		 
		 
		 dataset.show();

	}

}
