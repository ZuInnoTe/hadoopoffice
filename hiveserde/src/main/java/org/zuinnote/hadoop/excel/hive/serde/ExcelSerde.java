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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
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
import org.zuinnote.hadoop.office.format.common.converter.datatypes.GenericTimestampDataType;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAOArrayWritable;
import org.zuinnote.hadoop.office.format.common.util.msexcel.MSExcelUtil;

/**
 * SerDe for Excel files (.xls,.xlsx) to retrieve them as rows and store them in Excel.  Note: you need to specify a schema according to which the data in the Excel should be converted.
 *
 */

public class ExcelSerde extends AbstractSerDe {

	public static final String CONF_DEFAULTSHEETNAME = "office.hive.write.defaultSheetName";


	public static final String DEFAULT_DEFAULTSHEETNAME = "Sheet1";
	public static final String HOSUFFIX = "hadoopoffice.";
	private static final Log LOG = LogFactory.getLog(ExcelSerde.class.getName());
	private ObjectInspector oi;
	private String defaultSheetName = ExcelSerde.CONF_DEFAULTSHEETNAME;
	private List<String> columnNames;
	private List<TypeInfo> columnTypes;
	private Object[] nullRow;
	private Object[] outputRow;
	private int currentWriteRow;
	private ExcelConverterSimpleSpreadSheetCellDAO readConverter;
	private ExcelConverterSimpleSpreadSheetCellDAO writeConverter;
	private boolean writeHeader;

	/**
	 * Initializes the Serde
	 *
	 * @see #initialize(Configuration, Properties, Properties)
	 *
	 * @param conf Hadoop Configuration
	 * @param prop table properties
	 *
	 *
	 */
	@Override
	public void initialize(Configuration conf, Properties prop) throws SerDeException {
		this.initialize(conf, prop, null);
	}

	/**
	 * Initializes the SerDe \n
	 * You can define in the table properties (additionally to the standard Hive properties) the following options \n
	 * office.hive.write.defaultSheetName: The sheetname to which data should be written (note: as an input any sheets can be read or selected sheets according to HadoopOffice configuration values) \n
	 * Any of the HadoopOffice options (hadoopoffice.*), such as encryption, signing, low footprint mode, linked workbooks, can be defined in the table properties @see <a href="https://github.com/ZuInnoTe/hadoopoffice/wiki/Hadoop-File-Format">HadoopOffice configuration</a>\n
	 * @param conf Hadoop Configuration
	 * @param prop table properties.
	 * @param partitionProperties ignored. Partitions are not supported.
	 */

	@Override
	public void initialize(Configuration conf, Properties prop, Properties partitionProperties) throws SerDeException {
		LOG.debug("Initializing Excel Hive Serde");
		LOG.debug("Configuring Hive-only options");
		// configure hadoopoffice specific hive options

		String defaultSheetNameStr = prop.getProperty(ExcelSerde.CONF_DEFAULTSHEETNAME);
		if (defaultSheetNameStr != null) {
			this.defaultSheetName = defaultSheetNameStr;
		}
	// copy hadoopoffice options
		LOG.debug("Configuring HadoopOffice Format");
		Set<Entry<Object, Object>> entries = prop.entrySet();
		for (Entry<Object, Object> entry : entries) {
			if ((entry.getKey() instanceof String) && ((String) entry.getKey()).startsWith(ExcelSerde.HOSUFFIX)) {
				if (("TRUE".equalsIgnoreCase((String) entry.getValue()))
						|| ("FALSE".equalsIgnoreCase(((String) entry.getValue())))) {
					conf.setBoolean((String) entry.getKey(), Boolean.valueOf((String) entry.getValue()));
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
		for (TypeInfo currentColumnType : columnTypes) {
			columnOIs.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(currentColumnType));
		}
		this.oi = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
		// create converter
		LOG.debug("Creating converter");
		HadoopOfficeReadConfiguration hocr = new HadoopOfficeReadConfiguration(conf);
		this.readConverter = new ExcelConverterSimpleSpreadSheetCellDAO(hocr.getSimpleDateFormat(), hocr.getSimpleDecimalFormat(), hocr.getSimpleDateTimeFormat());
		HadoopOfficeWriteConfiguration howc = new HadoopOfficeWriteConfiguration(conf,"");
		this.writeConverter = new ExcelConverterSimpleSpreadSheetCellDAO(howc.getSimpleDateFormat(), howc.getSimpleDecimalFormat(), howc.getSimpleDateTimeFormat());
		// configure writing of header
		this.writeHeader=howc.getWriteHeader();
		GenericDataType[] columnsGD = new GenericDataType[columnNames.size()];
		for (int i = 0; i < columnOIs.size(); i++) {
			ObjectInspector currentOI = columnOIs.get(i);
			if (currentOI instanceof BooleanObjectInspector) {
				columnsGD[i] = new GenericBooleanDataType();
			} else if (currentOI instanceof DateObjectInspector) {
				columnsGD[i] = new GenericDateDataType();
			} else if (currentOI instanceof TimestampObjectInspector) {
				columnsGD[i] = new GenericTimestampDataType();
			}
			else if (currentOI instanceof ByteObjectInspector) {
				columnsGD[i] = new GenericByteDataType();
			} else if (currentOI instanceof ShortObjectInspector) {
				columnsGD[i] = new GenericShortDataType();
			} else if (currentOI instanceof IntObjectInspector) {
				columnsGD[i] = new GenericIntegerDataType();
			} else if (currentOI instanceof LongObjectInspector) {
				columnsGD[i] = new GenericLongDataType();
			} else if (currentOI instanceof DoubleObjectInspector) {
				columnsGD[i] = new GenericDoubleDataType();
			} else if (currentOI instanceof FloatObjectInspector) {
				columnsGD[i] = new GenericFloatDataType();
			} else if (currentOI instanceof HiveDecimalObjectInspector) {
				HiveDecimalObjectInspector currentOIHiveDecimalOI = (HiveDecimalObjectInspector) currentOI;
				columnsGD[i] = new GenericBigDecimalDataType(currentOIHiveDecimalOI.precision(),
						currentOIHiveDecimalOI.scale());
			} else if (currentOI instanceof StringObjectInspector) {
				columnsGD[i] = new GenericStringDataType();
			} else {
				LOG.warn("Could not detect desired datatype for column " + i + ". Type " + currentOI.getTypeName()
						+ ". Using String");
				columnsGD[i] = new GenericStringDataType();
			}
		}
		this.readConverter.setSchemaRow(columnsGD);
		this.writeConverter.setSchemaRow(columnsGD);
		// create nullrow
		this.nullRow = new Object[this.columnNames.size()];
		// set writerow
		this.currentWriteRow = 0;
		// set outputrow
		this.outputRow = new Object[this.columnNames.size()];
		LOG.debug("Finished Initialization");
	}

	/**
	 * The object inspector returned is always of type StructObjectInspector
	 *
	 * @return StructObjectInspector
	 */
	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return this.oi;
	}

	/**
	 * Returns null because SerdeStata are not supported
	 *
	 * @return null
	 */
	@Override
	public SerDeStats getSerDeStats() {
		// no statistics supported
		return null;
	}

	/**
	 * Returns the class in which information is serialized.
	 * It is an ArrayWritable (@see org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAOArrayWritable) containing objects of type SpreadSheetCellDAO (@see org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO)
	 *
	 * @return SpreadSheetCellDAOArrayWritable.class
	 */
	@Override
	public Class<? extends Writable> getSerializedClass() {
		return SpreadSheetCellDAOArrayWritable.class;
	}

	/**
	 * Deserializes an object of type @see #getSerializedClass()
	 * Note: Some Java types, such as Decimal, are converted to Hive specific datatypes.
	 *
	 * @param arg0 object of type @see #getSerializedClass()
	 * @return Array containing objects of type primitive Java (e.g. string, byte, integer)/Hive (e.g HiveDecimal, HiveVarChar)
	 *
	 */
	@Override
	public Object deserialize(Writable arg0) throws SerDeException {
		if ((arg0 == null) || (arg0 instanceof NullWritable)) {
			return this.nullRow;
		}
		Object[] primitiveRow = this.readConverter
				.getDataAccordingToSchema((SpreadSheetCellDAO[]) ((ArrayWritable) arg0).get());
		// check if supported type and convert to hive type, if necessary
		for (int i = 0; i < primitiveRow.length; i++) {
			PrimitiveTypeInfo ti = (PrimitiveTypeInfo) this.columnTypes.get(i);
			switch (ti.getPrimitiveCategory()) {
			case STRING:
				primitiveRow[i] = primitiveRow[i];
				break;
			case BYTE:
				primitiveRow[i] = primitiveRow[i];
				break;
			case SHORT:
				primitiveRow[i] = primitiveRow[i];
				break;
			case INT:
				primitiveRow[i] = primitiveRow[i];
				break;
			case LONG:
				primitiveRow[i] = primitiveRow[i];
				break;
			case FLOAT:
				primitiveRow[i] = primitiveRow[i];
				break;
			case DOUBLE:
				primitiveRow[i] = primitiveRow[i];
				break;
			case BOOLEAN:
				primitiveRow[i] = primitiveRow[i];
				break;
			case TIMESTAMP:
				primitiveRow[i] = primitiveRow[i];
				break;
			case DATE:
				if (primitiveRow[i] != null) {
					boolean hiveNewDateClass=true;
					try {
						Class.forName("org.apache.hadoop.hive.common.type.Date");
					} catch(ClassNotFoundException e) {
						hiveNewDateClass=false;
					}
					if (hiveNewDateClass) {
						SimpleDateFormat hiveDateFormat = new SimpleDateFormat("yyyy-MM-dd");
						primitiveRow[i]=org.apache.hadoop.hive.common.type.Date.valueOf(hiveDateFormat.format((Date) primitiveRow[i]));
					} else 	{
						primitiveRow[i] = new java.sql.Date(((Date) primitiveRow[i]).getTime());
						}
				}
				break;
			case DECIMAL:
				if (primitiveRow[i] != null) {
					primitiveRow[i] = HiveDecimal.create((BigDecimal) primitiveRow[i]);
				}
				break;
			case CHAR:
				if (primitiveRow[i] != null) {
					primitiveRow[i] = new HiveChar((String) primitiveRow[i], ((CharTypeInfo) ti).getLength());
				}
				break;
			case VARCHAR:
				if (primitiveRow[i] != null) {
					primitiveRow[i] = new HiveVarchar((String) primitiveRow[i], ((VarcharTypeInfo) ti).getLength());
				}
				break;
			default:
				throw new SerDeException("Unsupported type " + ti);
			}
		}
		if (this.columnNames.size()>primitiveRow.length) { // can happen in rare cases where a row does not contain all columns
			Object[] tempRow = new Object[this.columnNames.size()];
			for (int i=0;i<primitiveRow.length;i++) {
				tempRow[i]=primitiveRow[i];
			}
			primitiveRow=tempRow;
		}
		return primitiveRow;
	}


/***
 *  Serializes an array of primitive (Hive) data types to a objects of type @see #getSerializedClass()
 *  Note: For HiveDecimals we add trailing zeros
 *
 *  @param arg0 Array of objects of primitive (Hive) data types.
 *  @param arg1 ObjectInspector to be able to parse the object given in arg0
 *
 *  @return object of type @see #getSerializedClass()
 *
 */
	@Override
	public Writable serialize(Object arg0, ObjectInspector arg1) throws SerDeException {
		SpreadSheetCellDAO[] resultHeader = new SpreadSheetCellDAO[0];
		if (this.writeHeader) {
			this.writeHeader = false;
			resultHeader = new SpreadSheetCellDAO[this.columnNames.size()];
			for (int i = 0; i < this.columnNames.size(); i++) {
				resultHeader[i] = new SpreadSheetCellDAO(columnNames.get(i), "", "",
						MSExcelUtil.getCellAddressA1Format(0, i), this.defaultSheetName);
			}
			this.currentWriteRow++;
		}
		if (arg0 == null) {
			if (resultHeader.length > 0) {
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
			throw new SerDeException("Cannot serialize the object because there are " + outputFields.size()
					+ " fields but the table has " + this.columnNames.size() + " columns.");
		}
		SpreadSheetCellDAO[] result = new SpreadSheetCellDAO[resultHeader.length + this.columnNames.size()];
		// copy header, if available
		for (int i = 0; i < resultHeader.length; i++) {
			result[i] = resultHeader[i];
		}
		// get field data
		for (int i = 0; i < this.columnNames.size(); i++) {
			final Object fieldObject = outputOI.getStructFieldData(arg0, outputFields.get(i));
			final ObjectInspector fieldOI = outputFields.get(i).getFieldObjectInspector();
			this.outputRow[i] = ((PrimitiveObjectInspector) fieldOI).getPrimitiveJavaObject(fieldObject);
			PrimitiveTypeInfo ti = (PrimitiveTypeInfo) this.columnTypes.get(i);
			switch (ti.getPrimitiveCategory()) {
			case STRING:
				outputRow[i] = outputRow[i];
				break;
			case BYTE:
				outputRow[i] = outputRow[i];
				break;
			case SHORT:
				outputRow[i] = outputRow[i];
				break;
			case INT:
				outputRow[i] = outputRow[i];
				break;
			case LONG:
				outputRow[i] = outputRow[i];
				break;
			case FLOAT:
				outputRow[i] = outputRow[i];
				break;
			case DOUBLE:
				outputRow[i] = outputRow[i];
				break;
			case BOOLEAN:
				outputRow[i] = outputRow[i];
				break;
			case TIMESTAMP:
				outputRow[i] = outputRow[i];
				break;
			case DATE:
			boolean hiveNewDateClass=true;
			try {
				Class.forName("org.apache.hadoop.hive.common.type.Date");
			} catch(ClassNotFoundException e) {
				hiveNewDateClass=false;
			}
			if (hiveNewDateClass) {
				SimpleDateFormat hiveDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				try {
				if (outputRow[i]!=null) {
				outputRow[i]=hiveDateFormat.parse(((org.apache.hadoop.hive.common.type.Date)outputRow[i]).toString());
				}
			} catch (ParseException e) {
				this.LOG.error ("Internal Error converting Hive Date to Date");
				this.LOG.error(e);
			}
			}
			else {
				 outputRow[i] = outputRow[i];
			 }
				break;
			case DECIMAL:
				if (outputRow[i] != null) {
					// add trailing zeros, if necessary, Hive removes them by default, but for certain scenarios we might still need them in Excel
					outputRow[i] = ((HiveDecimal) outputRow[i]).bigDecimalValue().setScale(((DecimalTypeInfo)ti).scale());
				}
				break;
			case CHAR:
				if (outputRow[i] != null) {
					outputRow[i] = ((HiveChar) outputRow[i]).getStrippedValue();
				}
				break;
			case VARCHAR:
				if (outputRow[i] != null) {
					outputRow[i] = ((HiveVarchar) outputRow[i]).toString();
				}
				break;
			default:
				throw new SerDeException("Unsupported type " + ti);
			}
		}
		// convert to spreadsheetcell
		SpreadSheetCellDAO[] convertedRow = this.writeConverter.getSpreadSheetCellDAOfromSimpleDataType(this.outputRow,
				this.defaultSheetName, this.currentWriteRow);
		// write into result
		for (int i = 0; i < convertedRow.length; i++) {
			result[resultHeader.length + i] = convertedRow[i];
		}
		this.currentWriteRow++;
		SpreadSheetCellDAOArrayWritable resultWritable = new SpreadSheetCellDAOArrayWritable();
		resultWritable.set(result);
		return resultWritable;
	}

}
