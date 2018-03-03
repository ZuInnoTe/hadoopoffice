package org.zuinnote.flink.office.excel

import org.apache.flink.types.Row
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation

import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration
import java.text.SimpleDateFormat
import java.text.DecimalFormat
import org.apache.flink.table.sources.BatchTableSource
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDataType
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericBooleanDataType
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDateDataType
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

class ExcelFlinkTableSource(
  val path:          String,
  val fieldNames:    Array[String],
  val fieldTypes:    Array[TypeInformation[_]],
  val useHeader:     Boolean,
  val hocr:          HadoopOfficeReadConfiguration,
  val dateFormat:    SimpleDateFormat,
  val decimalFormat: DecimalFormat)
  extends BatchTableSource[Row] {

  private val returnType: RowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames)

  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    // we do not infer any data, because fieldnames and types are given
    val inputFormat = new RowSimpleExcelFlinkFileInputFormat(hocr, 0, useHeader, dateFormat, decimalFormat, fieldTypes)
    // set custom schma
    val customSchema: Array[GenericDataType] = new Array[GenericDataType](fieldTypes.length)
    var i = 0
    for (ct <- fieldTypes) {
      if (!ct.isBasicType()) {
        throw new RuntimeException("Cannot process complex types")
      }
      if (ct.equals(Types.BOOLEAN)) {
        customSchema(i) = new GenericBooleanDataType
      } else if (ct.equals(Types.STRING)) {
        customSchema(i) = new GenericStringDataType()
      } else if (ct.equals(Types.DATE)) {
        customSchema(i) = new GenericDateDataType()
      } else if (ct.equals(Types.DECIMAL)) {
        new GenericBigDecimalDataType(10, 0)
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
        customSchema(i) = new GenericStringDataType()
      }
      i += 1
    }
    inputFormat.setSchema(customSchema)
    execEnv.createInput(inputFormat, returnType).name(explainSource())

  }

  /** Returns the [[RowTypeInfo]] for the return type of the [[ExcelTableSource]]. */
  override def getReturnType: RowTypeInfo = returnType

  override def explainSource(): String = {
    s"ExcelTableSource(" +
      s"read fields: ${getReturnType.getFieldNames.mkString(", ")})"
  }

}