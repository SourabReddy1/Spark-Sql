package Spark_Sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.api.*;

public class Main {
	
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "c:/winutils");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		
		SparkSession spark = SparkSession.builder().appName("Testing-sql").master("local[*]")
											       .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
											       .getOrCreate();
		Dataset<Row> dataset = spark.read().option("header", "true").csv("src/main/java/Exercises/students.csv");
		
		dataset.show();
		
		long number = dataset.count();
		
		Row firstRow = dataset.first();
		
		//To get a specific column with an idex
		//String subject = firstRow.getString(2);
		
		//To get a specific column when we know the header
		String subject = firstRow.getAs("subject");
		
		//To get an Integer column
		int year = Integer.parseInt(firstRow.getAs("year"));
	
		
		System.out.println(subject.toString()+year);
		
		System.out.println(number);
		
		//Filter different ways
		//Dataset<Row> math = dataset.filter("subject='Math' and year = 2005 ");
		
		//Filter using Lambda
		//Dataset<Row> math = dataset.filter(row->row.getAs("subject").equals("Math") && Integer.parseInt(row.getAs("year"))==2005);
		
		//Third approach most used one
        //Usic functions and static methods	
		
		//Dataset<Row> math = dataset.filter(col("subject").equalTo("Math").and(col("year").geq(2005)));
		
		
		//Sql like statements by creating a tempview
		
		dataset.createOrReplaceTempView("student_table");
		
		Dataset<Row> math = spark.sql("select * from student_table where subject='Math'");
		
		
		math.show();
		spark.close();
	}

}
