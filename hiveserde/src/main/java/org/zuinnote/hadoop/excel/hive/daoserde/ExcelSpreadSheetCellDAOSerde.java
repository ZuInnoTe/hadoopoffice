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
package org.zuinnote.hadoop.excel.hive.daoserde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.zuinnote.hadoop.excel.hive.serde.ExcelSerde;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;

/**
 * SerDe for Excel files (.xls,.xlsx) to retrieve each Cell in an Excel as a
 * dedicated row in Hive containing the information formattedValue, formula,
 * comment, cellAddress, Sheet. Writing is possible by providing one cell / row
 * in Hive (it does not prevent you having multiple cells in the same row in
 * Excel, you just use the right cell address, e.g. first row in Hive as cell
 * address A1 and second row in Hive cell address B1 then both entries are in the
 * same row in Excel)
 *
 */
public class ExcelSpreadSheetCellDAOSerde extends AbstractSerDe {
	private static final Log LOG = LogFactory.getLog(ExcelSpreadSheetCellDAOSerde.class.getName());
	private static final String[] columnNames = new String[]{"formattedValue","comment","formula","address","sheetName"};
	private ObjectInspector oi;
	
	@Override
	public void initialize(Configuration conf, Properties tbl) throws SerDeException {
		this.initialize(conf, tbl, null);

	}

	/**
	 * Initializes the SerDe \n You can define in the table properties (additionally
	 * to the standard Hive properties) the following options \n Any of the
	 * HadoopOffice options (hadoopoffice.*), such as encryption, signing, low
	 * footprint mode, linked workbooks, can be defined in the table properties @see
	 * <a href=
	 * "https://github.com/ZuInnoTe/hadoopoffice/wiki/Hadoop-File-Format">HadoopOffice
	 * configuration</a>\n
	 * 
	 * @param conf                Hadoop Configuration
	 * @param prop                table properties.
	 * @param partitionProperties ignored. Partitions are not supported.
	 */

	@Override
	public void initialize(Configuration conf, Properties prop, Properties partitionProperties) throws SerDeException {
		LOG.debug("Initializing Excel Hive Serde");
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
		LOG.debug("Configuring Object inspector");
		final List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(ExcelSpreadSheetCellDAOSerde.columnNames.length);
		for (int i=0;i<ExcelSpreadSheetCellDAOSerde.columnNames.length;i++) {
			columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		}
		this.oi = ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList(ExcelSpreadSheetCellDAOSerde.columnNames), columnOIs);
		LOG.debug("Finished Initialization");
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return SpreadSheetCellDAO.class;
	}

	/** 
	 * Writes a row in Hive containing exactly 5 elements (String) to a SpreadSheetCellDAO. Order: "formattedValue","comment","formula","address","sheetName"
	 * 
	 */
	@Override
	public Writable serialize(Object arg0, ObjectInspector arg1) throws SerDeException {
		if (!(arg1 instanceof StructObjectInspector)) {
			throw new SerDeException("Expect a row of Strings for serialization");
		}
		final StructObjectInspector outputOI = (StructObjectInspector) arg1;
		final List<? extends StructField> outputFields = outputOI.getAllStructFieldRefs();
		if (outputFields.size()!=ExcelSpreadSheetCellDAOSerde.columnNames.length) {
			throw new SerDeException("Expected "+ExcelSpreadSheetCellDAOSerde.columnNames.length+" fields characterizing a cell: \"formattedValue\",\"comment\",\"formula\",\"address\",\"sheetName\", but found "+outputFields.size()+" fields");
		}
		if (arg0==null) {
			return null;
		}
		// get field data
		// formattedValue
		int columnNum=0;
		final Object foFormattedValue = outputOI.getStructFieldData(arg0, outputFields.get(columnNum));
		final ObjectInspector oiFormattedValue = outputFields.get(columnNum).getFieldObjectInspector();
		String formattedValue = String.valueOf(((PrimitiveObjectInspector) oiFormattedValue).getPrimitiveJavaObject(foFormattedValue));
		// comment
		columnNum=1;
		final Object foComment= outputOI.getStructFieldData(arg0, outputFields.get(columnNum));
		final ObjectInspector oiComment = outputFields.get(columnNum).getFieldObjectInspector();
		String comment = String.valueOf(((PrimitiveObjectInspector) oiComment).getPrimitiveJavaObject(foComment));
		// formula
		columnNum=2;
		final Object foFormula= outputOI.getStructFieldData(arg0, outputFields.get(columnNum));
		final ObjectInspector oiFormula = outputFields.get(columnNum).getFieldObjectInspector();
		String formula = String.valueOf(((PrimitiveObjectInspector) oiFormula).getPrimitiveJavaObject(foFormula));
		// address
		columnNum=3;
		final Object foAddress= outputOI.getStructFieldData(arg0, outputFields.get(columnNum));
		final ObjectInspector oiAddress = outputFields.get(columnNum).getFieldObjectInspector();
		String address = String.valueOf(((PrimitiveObjectInspector) oiAddress).getPrimitiveJavaObject(foAddress));
		// sheetName
		columnNum=4;
		final Object foSheetName= outputOI.getStructFieldData(arg0, outputFields.get(columnNum));
		final ObjectInspector oiSheetName = outputFields.get(columnNum).getFieldObjectInspector();
		String sheetName = String.valueOf(((PrimitiveObjectInspector) oiSheetName).getPrimitiveJavaObject(foSheetName));
		return new SpreadSheetCellDAO(formattedValue,comment,formula,address,sheetName);
	}

	@Override
	public SerDeStats getSerDeStats() {
		// no statistics supported
		return null;
	}

	/**
	 * Returns one cell in an Excel as a row in Hive containing 5 elements:  "formattedValue","comment","formula","address","sheetName"
	 */
	
	@Override
	public Object deserialize(Writable arg0) throws SerDeException {
		// check for null
		if ((arg0 == null) || (arg0 instanceof NullWritable)) {
			return null;
		}
		if (!(arg0 instanceof SpreadSheetCellDAO)) {
			throw new SerDeException("Table does not contain objects of type SpreadSheetCellDAO. Did you use the ExcelCellInputFormat of HadoopOffice?");
		}
		String[] spreadSheetCellRow = new String[5];
		SpreadSheetCellDAO obj = (SpreadSheetCellDAO)arg0;
		spreadSheetCellRow[0] = obj.getFormattedValue();
		spreadSheetCellRow[1] = obj.getComment();
		spreadSheetCellRow[2] = obj.getFormula();
		spreadSheetCellRow[3] = obj.getAddress();
		spreadSheetCellRow[4] = obj.getSheetName();
		return spreadSheetCellRow;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return this.oi;
	}

}
