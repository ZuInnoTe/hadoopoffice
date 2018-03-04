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


import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.sql.Row
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.conf._


import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._

import org.zuinnote.hadoop.office.format.common.dao._
import org.zuinnote.hadoop.office.format.mapreduce._
   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
* Demonstrate the HadoopOffice library on Spark 1.x.
* Converts an Excel file to CSV
*
*
*/

object SparkScalaExcelIn {
   def main(args: Array[String]): Unit = {
 	  val conf = new SparkConf().setAppName("Spark-Scala Excel Analytics (hadoopoffice)")
	  val sc=new SparkContext(conf)
	  val sqlContext=new SQLContext(sc)
	  // example for configuration
	  val hadoopConf = new Configuration()
 	       /** note this sets the locale to us-english, which means that numbers might be displayed differently then you expect. Change this to the locale of the Excel file **/
   
	   hadoopConf.set("hadoopoffice.read.locale.bcp47","us");
	   convertToCSV(sc, hadoopConf, args(0), args(1));
	   sc.stop()
   }
   
   
   def convertToCSV(sc: SparkContext, hadoopConf: Configuration, inputFile: String, outputFile: String): Unit = {
     	// load using the new Hadoop API (mapreduce.*)
	val excelRDD = sc.newAPIHadoopFile(inputFile, classOf[ExcelFileInputFormat], classOf[Text], classOf[ArrayWritable], hadoopConf);
	// print the cell address and the formatted cell content
	excelRDD.map(hadoopKeyValueTuple => {
		// note see also org.zuinnote.hadoop.office.format.common.converter.ExcelConverterSimpleSpreadSheetCellDAO to convert from and to SpreadSheetCellDAO
		val rowStrBuffer = new StringBuilder
		var i=0;
		for (x <-hadoopKeyValueTuple._2.get) { // parse through the SpreadSheetCellDAO
				if (x!=null) {
					rowStrBuffer.append(x.asInstanceOf[SpreadSheetCellDAO].getAddress+":"+x.asInstanceOf[SpreadSheetCellDAO].getFormattedValue+",")
				} else {
					rowStrBuffer.append(",")
				}
		i+=1
		}
		if (rowStrBuffer.length>0) {
		  rowStrBuffer.deleteCharAt(rowStrBuffer.length-1) // remove last comma
		}
		rowStrBuffer.toString
	  }).repartition(1).saveAsTextFile(outputFile)
   }
}

