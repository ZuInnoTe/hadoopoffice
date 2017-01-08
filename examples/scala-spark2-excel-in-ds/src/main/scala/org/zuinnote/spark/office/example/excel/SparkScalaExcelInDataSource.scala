/**
* Copyright 2016 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
**/
package org.zuinnote.spark.office.example.excel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.sql.Row
import org.apache.hadoop.conf._

   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
* Demonstrate the HadoopOfficeSpark Datasource API by loading an Excel file into a dataframe
*
*
*/

object SparkScalaExcelInDataSource {
   def main(args: Array[String]): Unit = {
	
   val sparkSession = SparkSession.builder.appName("Spark-Scala Excel Analytics (hadoopoffice) - Datasource API - In").getOrCreate()
   import sparkSession.implicits._
	val sc=sparkSession.sparkContext
val sqlContext = sparkSession.sqlContext
val df = sqlContext.read
    .format("org.zuinnote.spark.office.excel")
    .option("read.locale.bcp47", "de")  
    .load(args(0))
	val totalCount = df.count
	// print to screen
	println("Total number of rows in Excel: "+totalCount)	
	df.printSchema
	// print formattedValues
	df.show	
    }
}

