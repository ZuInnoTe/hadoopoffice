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
import org.apache.hadoop.conf._


   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
* Demonstrate the HadoopOfficeSpark Datasource API by writing a dataframe to an Excel file
*
*
*/


object SparkScalaExcelOutDataSource {
   def main(args: Array[String]): Unit = {
val sparkSession = SparkSession.builder.appName("Spark-Scala Excel Analytics (hadoopoffice) - Datasource API - Out").getOrCreate()
   import sparkSession.implicits._
	    saveAsExcelFile(sparkSession.sparkContext, args(0));
     sparkSession.sparkContext.stop
      }
   
    def saveAsExcelFile(sc: SparkContext,outputFile: String): Unit = {
        val sqlContext=new SQLContext(sc)
        import sqlContext.implicits._
          // create a simple data frame with
		// another row with SpreadSheetCellDAO with 1,2 (with comment),3
		// another row with SpreadSheetCellDAO with a formula combining A2+A3 (2+3=5)
	val sRdd = sc.parallelize(Seq(Seq("","","1","A1","Sheet1"),Seq("","This is a comment","2","A2","Sheet1"),Seq("","","3","A3","Sheet1"),Seq("","","A2+A3","B1","Sheet1"))).repartition(1)
	val df= sRdd.toDF()
	df.write
      .format("org.zuinnote.spark.office.excel")
           /** note this sets the locale to us-english, which means that numbers might be displayed differently then you expect. Change this to the locale of the Excel file **/
     
    .option("write.locale.bcp47", "us") 
    .save(outputFile)
	 
    }
}


