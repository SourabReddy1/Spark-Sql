
import java.util.*;
import java.io.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class CreateDstextfile {

	
		// TODO Auto-generated method stub
public static class person implements Serializable{
	
	String name;
	int age;
	String Add;
	public String getAdd() {
		return Add;
	}
	public void setAdd(String add) {
		Add = add;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	

}
public static void main(String[] args) throws AnalysisException
{
	System.setProperty("hadoop.home.dir", "C:\\hadoop-winutils-2.6.0");
	SparkConf con=new SparkConf();
	JavaSparkContext jcon=new JavaSparkContext("local[2]","hi",con);
	SQLContext sqlct=new SQLContext(jcon);
	SparkSession spark=SparkSession.builder().config("log4j.rootCategory", "WARN").getOrCreate();
	//CreateDstextfile.person cp=new CreateDstextfile.person();
	Encoder<person> pencoder=Encoders.bean(person.class);
	JavaRDD<person> jp=spark.read().textFile("Spark-Sql/student.txt").javaRDD().map(line->{ String parts[]=line.split(",");
	CreateDstextfile.person cp=new CreateDstextfile.person();
	cp.setName(parts[0]);
	cp.setAge(Integer.parseInt(parts[1].trim()));
	cp.setAdd(parts[2]);
	return cp;
		
	});
	
	Dataset<Row> dr=spark.createDataFrame(jp,person.class);
	dr.createTempView("people");
	Dataset<Row> dp=sqlct.sql("select * from people  ");
	dp.show();
	
	
}
}
