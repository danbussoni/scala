/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*  Autor: Danilo Bussoni
	Resource: https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/sql/SparkSQLExample.scala
	Description: Sumarizes a flat file to RDD to SQL table in spark enviroment. Used a local node and java 8 to emulate node. */


val input = sc.textFile("C:/spark/README.md")
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object SparkSQLExample {

	def main(args: Array[String]): Unit = {
	
		val spark = SparkSession
		.builder()
		.appName("SparkSQL from RDD Dataframe")
		.getOrCreate()
		
		import spark.implicits._
		runBasicDataFrameExample(spark)
		spark.stop()
  }
	/*made it public for debugging*/
	
	public def runBasicDataFrameExample(spark: SparkSession): Unit = {
	
		import org.apache.spark.sql.types._
		import org.apache.spark.storage.StorageLevel
		import scala.io.Source
		import scala.collection.mutable.HashMap
		import java.io.File
		import org.apache.spark.sql.Row
		import scala.collection.mutable.ListBuffer
		import org.apache.spark.util.IntParam
		import org.apache.spark.sql.functions
		import org.apache.spark.sql.types.StructType
		import org.apache.spark.sql.types.{StringType,IntegerType}
		
		/*Alternative call*/
		/*val schema = StructType(Array(StructField("a",IntegerType,true),StructField("b",StringType,true),StructField("c",StringType,true),StructField("d",StringType,true),StructField("e",StringType,true),StructField("f",StringType,true),StructField("g",StringType,true),StructField("h",StringType,true),StructField("i",StringType,true),StructField("j",StringType,true),StructField("k",StringType,true,StructField("l",StringType,true),StructField("m",StringType,true))))*/
		/*val schemaTD = new StructType().add("a", StringType, true).add("b", StringType, true).add("c", StringType, true).add("d", StringType, true).add("e", StringType, true).add("f", StringType, true).add("g", StringType, true).add("h", StringType, true).add("i", StringType, true).add("j", StringType, true).add("k", StringType, true).add("l", StringType, true).add("m", StringType, true)*/
		
		val schemaTD = StructType(List(
		StructField("a",StringType),
		StructField("b",StringType),
		StructField("c",StringType),
		StructField("d",StringType),
		StructField("e",StringType),
		StructField("f",StringType),
		StructField("g",StringType),
		StructField("h",StringType),
		StructField("i",StringType),
		StructField("j",StringType),
		StructField("k",StringType),
		StructField("l",StringType),
		StructField("m",StringType)))

		/*used this resource https://archive.ics.uci.edu/ml/datasets/Forest+Fires
		as pointed from https://veekaybee.github.io/2018/07/23/small-datasets/ 
		Source and resource must be in root of spark installation .bin*/
		val testeRDD = spark.sparkContext.textFile("forestfires.txt")
		
		
		/*Alternative call*/
		/*val rowRDD = testeRDD.map(line => line.split(","))
				 .schema(schemaTD)
                 .map(r => Row(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12)))*/
		val rowRDD = testeRDD.map(_.split(",")).map(attributes => Row(attributes(0),attributes(1),attributes(2),attributes(3),attributes(4),attributes(5),attributes(6),attributes(7),attributes(8),attributes(9),attributes(10),attributes(11),attributes(12).trim))
		
		
		val testeDF = spark.createDataFrame(rowRDD, schemaTD)
		
		testeDF.printSchema()
		testeDF.show
		
		/*new dataframe with correct datatypes
		thats because java.lang.RuntimeException: java.lang.String is not a valid external type for schema of int*/
		val testeDF_mod = testeDF.select($"a".cast(IntegerType),$"b".cast(IntegerType),$"c".cast(StringType),$"d".cast(StringType),$"e".cast(DoubleType))
		
		/*new schema*/
		testeDF_mod.printSchema()
		testeDF_mod.show
		
		testeDF.createOrReplaceTempView("teste_sql")
		
		/*spark.sql("SELECT * FROM teste_sql").show()*/
		spark.sql("SELECT a FROM teste_sql").show()
		
		testeDF_mod.createOrReplaceTempView("teste_sql_mod")
		
		/*this shows how actually the RDD in spark breaks into multiple threads. SQL below to stack results */
		spark.sql("SELECT SUM(a) AS SOMA FROM teste_sql_mod GROUP BY a").show()
		
		spark.sql("SELECT SOMA FROM(SELECT 1 as AGGR, SUM(a) AS SOMA FROM teste_sql_mod GROUP BY AGGR)").show()
		
	
	