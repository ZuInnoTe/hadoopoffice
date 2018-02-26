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
package org.zuinnote.hadoop.excel.hive.serde;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.zuinnote.hadoop.office.format.common.converter.ExcelConverterSimpleSpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericBigDecimalDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericBooleanDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericByteDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDateDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericDoubleDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericFloatDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericIntegerDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericLongDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericShortDataType;
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericStringDataType;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAOArrayWritable;
import org.zuinnote.hadoop.office.format.common.util.MSExcelUtil;

/**
 * SerDe for Excel files (.xls,.xlsx) to retrieve them as rows. Note:
 *
 */
public class ExcelSerde extends AbstractSerDe {
	public static final String CONF_DATEFORMAT = "office.hive.dateFormat";
	public static final String CONF_DECIMALFORMAT = "office.hive.decimalFormat";
	public static final String CONF_WRITEHEADER = "office.hive.write.header";
	public static final String CONF_DEFAULTSHEETNAME = "office.hive.write.defaultSheetName";
	public static final Boolean DEFAULT_WRITEHEADER = false;
	public static final String DEFAULT_DATEFORMAT = "";
	public static final String DEFAULT_DECIMALFORMAT = "";
	public static final String DEFAULT_DEFAULTSHEETNAME = "Sheet1";
	public static final String HOSUFFIX = "hadoopoffice.";
	private static final Log LOG = LogFactory.getLog(ExcelSerde.class.getName());
	private ObjectInspector oi;
	private boolean writeHeader=ExcelSerde.DEFAULT_WRITEHEADER;
	private String defaultSheetName=ExcelSerde.CONF_DEFAULTSHEETNAME;
	private List<String> columnNames;
	private List<TypeInfo> columnTypes;
	private Object[] nullRow;
	private Object[] outputRow;
	private int currentWriteRow;
	private ExcelConverterSimpleSpreadSheetCellDAO converter;

	@Override
	public Object deserialize(Writable arg0) throws SerDeException {
		if ((arg0==null) || (arg0 instanceof NullWritable)) {
			return this.nullRow;
		}
		return this.converter.getDataAccordingToSchema((SpreadSheetCellDAO[]) ((ArrayWritable)arg0).get());
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return this.oi;
	}

	@Override
	public SerDeStats getSerDeStats() {
		// no statistics supported
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return ArrayWritable.class;
	}

	@Override
	public void initialize(Configuration conf, Properties prop) throws SerDeException {
		this.initialize(conf, prop, null);
	}

	@Override
	public void initialize(Configuration conf, Properties prop, Properties partionProperties) throws SerDeException {
		LOG.debug("Initializing Excel Hive Serde");
		LOG.debug("Configuring Hive-only options");
		// configure hadoopoffice specific hive options
		String writeHeaderStr=prop.getProperty(ExcelSerde.CONF_WRITEHEADER);
		if (writeHeaderStr!=null) {
			this.writeHeader=Boolean.valueOf(writeHeaderStr);
		}
		String defaultSheetNameStr=prop.getProperty(ExcelSerde.CONF_DEFAULTSHEETNAME);
		if (defaultSheetNameStr!=null) {
			this.defaultSheetName=defaultSheetNameStr;
		}
		String dateFormatStr=prop.getProperty(ExcelSerde.CONF_DATEFORMAT);
		if (dateFormatStr==null) {
			dateFormatStr=ExcelSerde.DEFAULT_DATEFORMAT;
		}
		Locale datelocale = Locale.getDefault();
		if (!"".equals(dateFormatStr)) {
			 datelocale = new Locale.Builder().setLanguageTag(dateFormatStr).build();
		}
		SimpleDateFormat dateFormat = (SimpleDateFormat) DateFormat.getDateInstance(DateFormat.SHORT, datelocale);
		String decimalFormatString=prop.getProperty(ExcelSerde.CONF_DECIMALFORMAT);
		if (decimalFormatString==null) {
			decimalFormatString=ExcelSerde.DEFAULT_DECIMALFORMAT;
		}
		Locale decimallocale = Locale.getDefault();
		if (!"".equals(decimalFormatString)) {
			decimallocale = new Locale.Builder().setLanguageTag(decimalFormatString).build();
		}
		DecimalFormat decimalFormat = (DecimalFormat) NumberFormat.getInstance(decimallocale);
		// copy hadoopoffice options
		LOG.debug("Configuring HadoopOffice Format");
		Set<Entry<Object, Object>> entries = prop.entrySet();
		for (Entry<Object, Object> entry : entries) {
			if ((entry.getKey() instanceof String)
					&& ((String) entry.getKey()).startsWith(ExcelSerde.HOSUFFIX)) {
				if (("TRUE".equalsIgnoreCase((String)entry.getValue())) || ("FALSE".equalsIgnoreCase(((String)entry.getValue())))) {
					conf.setBoolean((String) entry.getKey(), Boolean.valueOf((String)entry.getValue()));
				} else {
					conf.set((String) entry.getKey(), (String) entry.getValue());
				}
			}
		}
		// create object inspector (always a struct = row)
		LOG.debug("Creating object inspector");
	    this.columnNames = Arrays.asList(prop.getProperty(serdeConstants.LIST_COLUMNS).split(","));
	    this.columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(prop.getProperty(serdeConstants.LIST_COLUMN_TYPES));
	    final List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
		for (TypeInfo currentColumnType: columnTypes) {
			columnOIs.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(currentColumnType));
		}
		this.oi=ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
		// create converter
		LOG.debug("Creating converter");
		this.converter = new ExcelConverterSimpleSpreadSheetCellDAO(dateFormat,decimalFormat);
		GenericDataType[] columnsGD = new GenericDataType[columnNames.size()];
		for (int i=0;i<columnOIs.size();i++) {
		ObjectInspector currentOI = columnOIs.get(i);
			if (currentOI instanceof BooleanObjectInspector) {
				columnsGD[i] = new GenericBooleanDataType();
			} else if (currentOI instanceof DateObjectInspector) {
				columnsGD[i] = new GenericDateDataType();
			} else if (currentOI instanceof ByteObjectInspector) {
				columnsGD[i] = new GenericByteDataType();
			}
			else if (currentOI instanceof ShortObjectInspector) {
				columnsGD[i] = new GenericShortDataType();
			}
			else if (currentOI instanceof IntObjectInspector) {
				columnsGD[i] = new GenericIntegerDataType();
			}
			else if (currentOI instanceof LongObjectInspector) {
				columnsGD[i] = new GenericLongDataType();
			}
			else if (currentOI instanceof DoubleObjectInspector) {
				columnsGD[i] = new GenericDoubleDataType();
			}
			else if (currentOI instanceof FloatObjectInspector) {
				columnsGD[i] = new GenericFloatDataType();
			}
			else if (currentOI instanceof HiveDecimalObjectInspector) {
				HiveDecimalObjectInspector currentOIHiveDecimalOI = (HiveDecimalObjectInspector) currentOI;
				columnsGD[i] = new GenericBigDecimalDataType(currentOIHiveDecimalOI.precision(),currentOIHiveDecimalOI.scale());
			} else if (currentOI instanceof StringObjectInspector) {
				columnsGD[i] = new GenericStringDataType();
			} else {
				LOG.warn("Could not detect desired datatype for column "+i+". Type "+currentOI.getTypeName()+". Using String");
				columnsGD[i] = new GenericStringDataType();
			}
		}
		this.converter.setSchemaRow(columnsGD);
		// create nullrow
		this.nullRow=new Object[this.columnNames.size()];
		// set writerow
		this.currentWriteRow=0;
		// set outputrow
		this.outputRow=new Object[this.columnNames.size()];
		LOG.debug("Finished Initialization");
	}

	@Override
	public Writable serialize(Object arg0, ObjectInspector arg1) throws SerDeException {
		SpreadSheetCellDAO[] resultHeader = new SpreadSheetCellDAO[0];
		if (this.writeHeader) {
			this.writeHeader=false;
			resultHeader = new SpreadSheetCellDAO[this.columnNames.size()];
			for (int i=0;i<this.columnNames.size();i++) {
				resultHeader[i] = new SpreadSheetCellDAO(columnNames.get(i),"","",MSExcelUtil.getCellAddressA1Format(0, i),this.defaultSheetName);
			}
			this.currentWriteRow++;
		}
		if (arg0==null) {
			if (resultHeader.length>0) {
				// write header
				SpreadSheetCellDAOArrayWritable resultWritable = new SpreadSheetCellDAOArrayWritable();
				resultWritable.set(resultHeader);
				return resultWritable;
			} else { // write empty row
				this.currentWriteRow++;
				return null;
			}
		}
		if (!(arg1 instanceof StructObjectInspector)) {
			throw new SerDeException("Expect a row of primitive datatypes for serialization");
		}
		final StructObjectInspector outputOI = (StructObjectInspector) arg1;
		final List<? extends StructField> outputFields = outputOI.getAllStructFieldRefs();
		if (outputFields.size() != this.columnNames.size()) {
		      throw new SerDeException("Cannot serialize the object because there are "
		          + outputFields.size() + " fields but the table has " + this.columnNames.size() + " columns.");
		}
		SpreadSheetCellDAO[] result = new SpreadSheetCellDAO[resultHeader.length+this.columnNames.size()];
		// copy header, if available
		for (int i=0;i<resultHeader.length;i++) {
			result[i]=resultHeader[i];
		}
		// get field data
		for (int i=0;i<this.columnNames.size();i++) {
			final Object fieldObject = outputOI.getStructFieldData(arg0, outputFields.get(i));
			final ObjectInspector fieldOI = outputFields.get(i).getFieldObjectInspector();
			this.outputRow[i]=((PrimitiveObjectInspector)fieldOI).getPrimitiveJavaObject(fieldObject);
		}
		// convert to spreadsheetcell
		SpreadSheetCellDAO[] convertedRow = this.converter.getSpreadSheetCellDAOfromSimpleDataType(this.outputRow, this.defaultSheetName, this.currentWriteRow);
		// write into result
		for (int i=0;i<convertedRow.length;i++) {
			result[resultHeader.length+i]=convertedRow[i];
		}
		this.currentWriteRow++;
		SpreadSheetCellDAOArrayWritable resultWritable = new SpreadSheetCellDAOArrayWritable();
		resultWritable.set(result);
		return resultWritable;
	}

}
