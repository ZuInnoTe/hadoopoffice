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
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf._

import scala.collection.mutable.WrappedArray

   
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
     import sqlContext.implicits._

    convertToCSV(sqlContext, args(0), args(1))
    sc.stop()
    }
   
   def convertToCSV(sqlContext: SQLContext, inputFile: String, outputFile: String): Unit = {
     val df = sqlContext.read
    .format("org.zuinnote.spark.office.excel")
         /** note this sets the locale to us-english, which means that numbers might be displayed differently then you expect. Change this to the locale of the Excel file **/

    .option("read.locale.bcp47", "us")  
    .load(inputFile)
     val rowsDF=df.select(explode(df("rows")).alias("rows"))
     df.select("rows.formattedValue").rdd.map(formattedValueRow => {
       val rowStrBuffer = new StringBuilder
       val theRow = formattedValueRow.getAs[WrappedArray[String]](0)
	      for ( x <- theRow) {
	        			rowStrBuffer.append(x+",")
	      }
       if (rowStrBuffer.length>0) {
		    rowStrBuffer.deleteCharAt(rowStrBuffer.length-1) // remove last comma
		    } 
		  rowStrBuffer.toString
     } ).repartition(1).saveAsTextFile(outputFile)
	
   }
}

