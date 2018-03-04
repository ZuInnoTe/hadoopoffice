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

import org.zuinnote.hadoop.office.format.common.util._
import org.zuinnote.hadoop.office.format.common.converter._
import org.zuinnote.hadoop.office.format.common.dao._
import org.zuinnote.hadoop.office.format.mapreduce._
   
import collection.JavaConverters._
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
* Demonstrate the HadoopOffice library on Spark 1.x.
* Converts a CSV file to Excel
*
*
*/

object SparkScalaExcelOut {
   def main(args: Array[String]): Unit = {
 	  val conf = new SparkConf().setAppName("Spark-Scala Excel Analytics (hadoopoffice)")
	  val sc=new SparkContext(conf)
	  val sqlContext=new SQLContext(sc)
	  // example for configuration
	  val hadoopConf = new Configuration()
 	       /** note this sets the locale to us-english, which means that numbers might be displayed differently then you expect. Change this to the locale of the Excel file **/
   
	   hadoopConf.set("hadoopoffice.read.locale.bcp47","us");
	   convertToExcel(sc, hadoopConf, args(0), args(1));
	   sc.stop()
   }
   
   
   def convertToExcel(sc: SparkContext, hadoopConf: Configuration, inputFile: String, outputFile: String): Unit = {
    	// load a text file using standard spark methods
     val textFile = sc.textFile(inputFile)
     // create a pair rdd (nullwritable, SpreadSheetCellDAO)
     val excelRdd = textFile
     .zipWithIndex() // create an index for each line  (to create the corresponding cells in Excel)
     .map{case (line, idx) => {
     	// read the cells and create an array for all cells
     	val cells = line.split(",")
     	var columnNum=0
     	var outputRow = new Array[Writable](cells.length)
     	for (cell <- cells) {
     	       //SpreadSheetCellDAO(String formattedValue, String comment, String formula, String address,String sheetName)
     		outputRow(columnNum)=new SpreadSheetCellDAO(cell, "","",MSExcelUtil.getCellAddressA1Format(idx.intValue,columnNum), "Sheet1")
     		columnNum+=1
     	}
     	val outputRowWritable = new SpreadSheetCellDAOArrayWritable()
     	outputRowWritable.set(outputRow)
     	outputRowWritable
     }}.map(spreadSheetCellArrayWritable => (NullWritable.get(),spreadSheetCellArrayWritable)) // create key/value pair for outputformat
     .saveAsNewAPIHadoopFile(outputFile,classOf[NullWritable], classOf[SpreadSheetCellDAOArrayWritable], classOf[ExcelRowFileOutputFormat], hadoopConf) // use new hadoopp api (mapreduce.*)
}
}

