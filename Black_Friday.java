package blackfriday;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static com.sun.deploy.perf.DeployPerfUtil.write;
import static org.apache.spark.sql.types.DataTypes.*;

public class Black_Friday {
    public static void main(String[] args) {
        //Accessing the cluster
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sc);
        //Storing data by creating RDD
        JavaRDD<String> RDD1 = sc.textFile("f:\\blackfriday.txt");

        // Applying Transfermation for creating dataframe.
        JavaRDD<Row> rowRDD = RDD1.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] words1 = s.split(",");


                return RowFactory.create((Integer.parseInt(words1[0])),
                        words1[1],
                        words1[2],
                        words1[3],
                        Integer.parseInt(words1[4]),
                        words1[5],
                        words1[6],
                        Integer.parseInt(words1[7]),
                        Integer.parseInt(words1[8]),
                        words1[9],
                        words1[10],
                        Integer.parseInt(words1[11]));


            }
        });

        //System.out.println("RDD of row : " + rowRDD.collect());


        StructType schema = createStructType(new StructField[]{
                createStructField("userid", IntegerType, true),
                createStructField("productid", StringType, true),
                createStructField("gender", StringType, true),
                createStructField("age", StringType, true),
                createStructField("occupation", IntegerType, true),
                createStructField("city", StringType, true),
                createStructField("year", StringType, true),
                createStructField("maritalstatus", IntegerType, true),
                createStructField("product1", IntegerType, true),
                createStructField("product2", StringType, true),
                createStructField("product3", StringType, true),
                createStructField("purchase", IntegerType, true),

        });

        // Creating DataSets
        Dataset<Row> df = sqlContext.createDataFrame(rowRDD, schema);

        df.createOrReplaceTempView("blackfriday");

//KPI(1) OF TOTAL PURCHASE AGAINST USERID
        System.out.println("KPI USER ID AGAINST DISTRUBUTION");
        Dataset<Row> result10 = sqlContext.sql("select  userid,purchase from blackfriday");
        result10.groupBy("userid").sum("purchase").show();//write().csv("f:\\file");



        //KPI(1) OF TOTAL PURCHASE AGAINST product1

        Dataset<Row> result5 = sqlContext.sql("select product1,purchase from blackfriday");
        result5.groupBy("product1").sum("purchase").show();//write().csv("f:\\file1");

        //KPI(1) OF TOTAL PURCHASE AGAINST product2
        Dataset<Row> result6 = sqlContext.sql("select product2,purchase from blackfriday");
        result6.groupBy("product2").sum("purchase").show();//write().csv("f:\\file2");


        //KPI(1) OF TOTAL PURCHASE AGAINST product3
        Dataset<Row> result7 = sqlContext.sql("select product3,purchase from blackfriday");
        result7.groupBy("product3").sum("purchase").show();//write().csv("f:\\file3");


        //KPI(2) OF TOTAL PURCHASE AGAINST CITY.
        Dataset<Row> result1 = sqlContext.sql("select  city,purchase from blackfriday");
        result1.groupBy("city").sum("purchase").show();//write().csv("f:\\file4");


        //KPI(3) OF TOTAL PURCHASE AGAINST CITY AND GENDER.
        Dataset<Row> result2 = sqlContext.sql("select  gender,city,purchase from blackfriday");
        result2.groupBy("gender", "city").sum("purchase").show();//write().csv("f:\\file5");

        //KPI(4) OF TOTAL PURCHASE AGAINST GENDER.
        Dataset<Row> result3 = sqlContext.sql("select  gender,purchase from blackfriday");
        result3.groupBy("gender").sum("purchase").show();//write().csv("f:\\file6");

        //KPI(5) OF TOTAL PURCHASE AGAINST AGE
        Dataset<Row> result4 = sqlContext.sql("select  age,purchase from blackfriday");
        result4.groupBy("age").sum("purchase").show();//write().csv("f:\\file7");



    }
}











