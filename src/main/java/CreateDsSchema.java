
import java.util.*;
import java.io.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.joda.time.chrono.AssembledChronology.Fields;
import org.apache.spark.sql.types.StructType;

public class CreateDsSchema {
public static void main(String[] args) throws AnalysisException
 	{
	        System.setProperty("hadoop.home.dir", "C:\\hadoop-winutils-2.6.0");
		SparkConf con=new SparkConf();
		JavaSparkContext jsc=new JavaSparkContext("local[2]","hi-",con);
		SQLContext sqlct=new SQLContext(jsc);
		SparkSession spark=SparkSession.builder().config("log4j.rootCategory", "WARN").getOrCreate();
		JavaRDD<String> jdd=spark.sparkContext().textFile("Spark-Sql/student.txt",1).toJavaRDD();
		String Stringschema="name age address";
		List<StructField> fields=new ArrayList<>();
		for(String fieldName: Stringschema.split(" "))
		{
			StructField field=DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		
		JavaRDD<Row> jrd=jdd.map(l->{
			String[] att=l.split(",");
			return RowFactory.create(att[0],att[1],att[2]);
			
		}
				);
		Dataset<Row> dr=spark.createDataFrame(jrd, schema);
		dr.createOrReplaceTempView("kmit");
		Dataset<Row> fin=sqlct.sql("select * from kmit");
		fin.show();
		dr.printSchema();
  }
}
