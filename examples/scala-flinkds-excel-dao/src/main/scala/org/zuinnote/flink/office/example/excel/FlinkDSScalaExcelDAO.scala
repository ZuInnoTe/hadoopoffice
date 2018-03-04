/**
* Copyright 2018 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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
package org.zuinnote.flink.office.example.excel



import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.core.fs.Path
import java.util.Locale


import org.zuinnote.flink.office.excel.ExcelFlinkFileInputFormat

import org.zuinnote.flink.office.excel.ExcelFlinkFileOutputFormat
import org.zuinnote.hadoop.office.format.common._
import org.zuinnote.hadoop.office.format.common.dao._


   
/**
* Author: Jörn Franke <zuinnote@gmail.com>
*
*/

/**
* Demonstrate the Flink DataSource/DataSink of the HadoopOffice library
* Illustrate the usage of SpreadSheetCellDAOs to define formulas, comments etc.
*
*
*/

object FlinkDSScalaExcelDAO {
   val MIMETYPE_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
   val MIMETYPE_XLS = "application/vnd.ms-excel";
   
   def main(args: Array[String]): Unit = {
 		val  env = ExecutionEnvironment.getExecutionEnvironment
        val params: ParameterTool = ParameterTool.fromArgs(args)

        readwriteExcelDAODS(env,params.get("input"),params.get("output"))
		env.execute("Flink Scala DataSource/DataSink HadoopOffice read/write Excel files using DAO format (allows you define comments, formulas etc)")
   }
   
   
   def readwriteExcelDAODS(env: ExecutionEnvironment, inputFile: String, outputFile: String): Unit = {
        val hocr = new HadoopOfficeReadConfiguration()
        hocr.setLocale(new Locale.Builder().setLanguageTag("us").build())
      // load Excel file
      val useHeader = false // set to true if you want to skip the first row
      val inputFormat = new ExcelFlinkFileInputFormat(hocr, false)
      val excelData = env.readFile(inputFormat, inputFile)
      // add some comments to each cell
      val excelDataWithComment = excelData.map(spreadSheetRow => {
		  val rowWithComment = new Array[SpreadSheetCellDAO](spreadSheetRow.length)
	      for (i <- 0 to spreadSheetRow.length-1) {
	        // SpreadSheetCellDAO(String formattedValue, String comment, String formula, String address,String sheetName) 
	        // formattedValue: here you can set the text of an Excel (e.g. "Text")
	        // comment: here you can set a comment for the cell
	        // formula: here you can set the formula without equal sign, e.g. "A1+A2"
	        // address: here you can set the address in A1 format, e.g. "B2"
	        // sheetName :  here you can set the sheet where the cell should end up
	        if (spreadSheetRow(i)!=null) {
	        		rowWithComment(i) = new SpreadSheetCellDAO(spreadSheetRow(i).getFormattedValue(),"This is a comment", spreadSheetRow(i).getFormula(), spreadSheetRow(i).getAddress(), "Sheet2")
	        }
	      }
	      rowWithComment  // if you want to convert it to simple datatypes then check https://github.com/ZuInnoTe/hadoopoffice/wiki/Convert-spreadsheet-cells-to-simple-datatypes-and-vice-versa
      })
      // write Excel file
	  val howc = new HadoopOfficeWriteConfiguration(new Path(outputFile).getName())
	  howc.setMimeType(MIMETYPE_XLSX)
	  howc.setCommentAuthor("Jane Doe")
	  howc.setCommentHeight(1)  // units in terms of "cells",ie the comment box is 1 cell height
	  howc.setCommentWidth(3) // units in terms of "cells",ie the comment box is 3 cell width
	  val defaultSheetName = "Sheet1"
	  val header = null // write a header as first line in defaultSheetName
	  val outputFormat = new ExcelFlinkFileOutputFormat(howc, header,defaultSheetName)
	  excelDataWithComment.write(outputFormat, outputFile)
    }
}

