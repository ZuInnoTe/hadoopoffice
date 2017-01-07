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
* Demonstrate the HadoopOffice library on Spark 1.x
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
	 hadoopConf.set("hadoopoffice.read.locale.bcp47","de");
	// load using the new Hadoop API (mapreduce.*)
	val excelRDD = sc.newAPIHadoopFile(args(0), classOf[ExcelFileInputFormat], classOf[Text], classOf[ArrayWritable], hadoopConf);
	// print the cell address and the formatted cell content
	excelRDD.flatMap(hadoopKeyValueTuple => {
		
		val rowArray = new Array[String](hadoopKeyValueTuple._2.get.length)
		var i=0;
		for (x <-hadoopKeyValueTuple._2.get) { // parse through the SpreadSheetCellDAO
				if (x!=null) {
					rowArray(i)=x.asInstanceOf[SpreadSheetCellDAO].getAddress+":"+x.asInstanceOf[SpreadSheetCellDAO].getFormattedValue
				} else {
					rowArray(i)=""
				}
		i+=1
		}
		rowArray
	}).foreach({ row=>println(row)
	})
    }
}

