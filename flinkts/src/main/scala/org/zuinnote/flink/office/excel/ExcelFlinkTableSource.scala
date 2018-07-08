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
 */

package org.zuinnote.flink.office.excel

import org.apache.flink.types.Row
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation

import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration
import java.text.SimpleDateFormat
import java.text.DecimalFormat
import java.text.NumberFormat
import java.text.DateFormat
import org.apache.flink.table.sources.BatchTableSource
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericBooleanDataType
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDateDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericTimestampDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericBigDecimalDataType
import java.util.Date
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericByteDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericShortDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericIntegerDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericLongDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDoubleDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericFloatDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericStringDataType

import org.zuinnote.flink.office.excel.RowSimpleExcelFlinkFileInputFormat
import org.apache.flink.table.api.Types

import scala.collection.mutable
import java.util.Locale

class ExcelFlinkTableSource(
  val path:          String,
  val fieldNames:    Array[String],
  val fieldTypes:    Array[TypeInformation[_]],
  val hocr:          HadoopOfficeReadConfiguration,
  val dateFormat:    SimpleDateFormat,
  val decimalFormat: DecimalFormat)
  extends BatchTableSource[Row] {

  private val returnType: RowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames)

/***
   * Returns the dataSet from the Excel file. Note: Currently only the types string, date, timestamp, decimal, byte,short,integer,long,double,float are supported
   * 
   * @param execEnv executionEnvironment 
   * 
   */
  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    // we do not infer any data, because fieldnames and types are given

    val inputFormat = new RowSimpleExcelFlinkFileInputFormat(hocr, 0, dateFormat, decimalFormat, fieldTypes)
    // set custom schema (= automated inference is deactivated)
    val customSchema: Array[GenericDataType] = new Array[GenericDataType](fieldTypes.length)
    var i = 0
    for (ct <- fieldTypes) {
      if (ct.equals(Types.BOOLEAN)) {
        customSchema(i) = new GenericBooleanDataType
      } else if (ct.equals(Types.STRING)) {
        customSchema(i) = new GenericStringDataType()
      } else if (ct.equals(Types.SQL_DATE)) {
        customSchema(i) = new GenericDateDataType()
      } else if (ct.equals(Types.SQL_TIMESTAMP)) {
        customSchema(i) = new GenericTimestampDataType()
      } else if (ct.equals(Types.DECIMAL)) {
        customSchema(i) = new GenericBigDecimalDataType(10, 0) // note: the precision and scale are ignored and always the correct precision/scale are returned based on the data
      } else if (ct.equals(Types.BYTE)) {
        customSchema(i) = new GenericByteDataType()
      } else if (ct.equals(Types.SHORT)) {
        customSchema(i) = new GenericShortDataType()
      } else if (ct.equals(Types.INT)) {
        customSchema(i) = new GenericIntegerDataType()
      } else if (ct.equals(Types.LONG)) {
        customSchema(i) = new GenericLongDataType()
      } else if (ct.equals(Types.DOUBLE)) {
        customSchema(i) = new GenericDoubleDataType()
      } else if (ct.equals(Types.FLOAT)) {
        customSchema(i) = new GenericFloatDataType()
      } else {
        throw new RuntimeException("Type not supported " + ct.toString())
      }
      i += 1
    }
    inputFormat.setSchema(customSchema)
    inputFormat.setFilePath(path)
    execEnv.createInput(inputFormat, returnType).name(explainSource())

  }

  /** Returns the [[RowTypeInfo]] for the return type of the [[ExcelTableSource]]. */
  override def getReturnType: RowTypeInfo = returnType

/***
   * Explains the table source
   * 
   */
  override def explainSource(): String = {
    s"ExcelFlinkTableSource(" +
      s"read fields: ${getReturnType.getFieldNames.mkString(", ")})"
  }
}

object ExcelFlinkTableSource {

  /**
   * A builder for creating [[ExcelFlinkTableSource]] instances.
   *
   * Example:
   *
   * {{{
   *   val source: ExcelFlinkTableSource = new ExcelFlinkTableSource.builder()
   *     .path("/path/to/your/file.xlsx")
   *     .field("column1", Types.STRING)
   *     .field("column2", Types.SQL_DATE)
   *     .hocr(new HadoopOfficeReadConfiguration())
   *     .decimalFormat(NumberFormat.getInstance(Locale.US))
   *     .build()
   * }}}
   *
   */
  class Builder {

    private val schema: mutable.LinkedHashMap[String, TypeInformation[_]] =
      mutable.LinkedHashMap[String, TypeInformation[_]]()
    private var path: String = _
    private var hocr: HadoopOfficeReadConfiguration = _
    private var dateFormat: SimpleDateFormat = DateFormat.getDateInstance(DateFormat.SHORT, Locale.US).asInstanceOf[SimpleDateFormat]
    private var decimalFormat: DecimalFormat = NumberFormat.getInstance().asInstanceOf[DecimalFormat]

    /**
     * Sets the path to the CSV file. Required.
     *
     * @param path the path to the CSV file
     */
    def path(path: String): Builder = {
      this.path = path
      this
    }

    /**
     * Adds a field with the field name and the type information. Required.
     * This method can be called multiple times. The call order of this method defines
     * also the order of the fields in a row.
     *
     * @param fieldName the field name
     * @param fieldType the type information of the field
     */
    def field(fieldName: String, fieldType: TypeInformation[_]): Builder = {
      if (schema.contains(fieldName)) {
        throw new IllegalArgumentException(s"Duplicate field name $fieldName.")
      }
      schema += (fieldName -> fieldType)
      this
    }



    /**
     * Defines the configuration for reading office files (e.g. decryption, linked documents, signature verification etc.)
     *
     * @param conf the configuration
     */

    def conf(conf: HadoopOfficeReadConfiguration): Builder = {
      this.hocr = conf
      this
    }

    /**
     * Defines the dateFormat to use when reading office files. Note: for some office files, such as Excel the default "US" makes sense, because even for other regions they store it internally as US
     *
     * @param dateFormat DateFormat
     */
    def dateFormat(dateFormat: SimpleDateFormat): Builder = {
      this.dateFormat = dateFormat
      this
    }

/***
     * Defines the decimalFormat when reading office files. 
     * 
     * @param decimalFormat decimal format to use
     */
    def decimalFormat(decimalFormat: DecimalFormat): Builder = {
      this.decimalFormat = decimalFormat
      this
    }

    /**
     * Apply the current values and constructs a newly-created [[CsvTableSource]].
     *
     * @return a newly-created [[CsvTableSource]].
     */
    def build(): ExcelFlinkTableSource = {
      if (path == null) {
        throw new IllegalArgumentException("Path must be defined.")
      }
      if (schema.isEmpty) {
        throw new IllegalArgumentException("Fields can not be empty.")
      }
      if (hocr == null) {
        throw new IllegalArgumentException("Configuration must be provided")
      }
      new ExcelFlinkTableSource(
        path,
        schema.keys.toArray,
        schema.values.toArray,
        hocr,
        dateFormat,
        decimalFormat)
    }

  }
  /**
   * Return a new builder that builds a [[ExcelFlinkTableSource]].
   *
   * For example:
   *
   * {{{
   *   val source: ExcelFlinkTableSource = new ExcelFlinkTableSource.builder()
   *     .path("/path/to/your/file.xlsx")
   *     .field("column1", Types.STRING)
   *     .field("column2", Types.SQL_DATE)
   *     .hocr(new HadoopOfficeReadConfiguration())
   *     .decimalFormat(NumberFormat.getInstance(Locale.US))
   *     .build()
   * }}}
   * @return a new builder to build a [[ExcelFLinkTableSource]]
   */
  def builder(): Builder = new Builder
}