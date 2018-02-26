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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.junit.jupiter.api.Test;

/**
 *
 *
 */
public class ExcelTextSerdeTest {
	
	@Test
	public void checkTestExcel2003SingleSheetAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003test.xls";
		String fileNameSpreadSheet = classLoader.getResource("testdata/"+fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013SingleSheetAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource("testdata/"+fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}
	
	@Test
	public void checkTestExcel2013SimpleAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "testsimple.xlsx";
		String fileNameSpreadSheet = classLoader.getResource("testdata/"+fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void initializePositive() throws SerDeException {
		ExcelSerde testSerde = new ExcelSerde();
		Configuration conf = new Configuration();
		Properties tblProperties = new Properties();
		tblProperties.setProperty(ExcelSerde.CONF_DATEFORMAT, "de");
		tblProperties.setProperty(ExcelSerde.CONF_DECIMALFORMAT, "de");
		tblProperties.setProperty(ExcelSerde.CONF_DEFAULTSHEETNAME, "Sheet2");
		tblProperties.setProperty(ExcelSerde.CONF_WRITEHEADER, "true");
		tblProperties.setProperty("hadoopoffice.read.locale.bcp47", "de");
		tblProperties.setProperty("hadoopoffice.read.linkedworkbooks", "true");
		tblProperties.setProperty(serdeConstants.LIST_COLUMNS, "column1,column2");
		tblProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,string");
		testSerde.initialize(conf, tblProperties);
		assertEquals("de",conf.get("hadoopoffice.read.locale.bcp47","us"),"HadoopOffice Hadoop configuration option set");
		assertTrue(conf.getBoolean("hadoopoffice.read.linkedworkbooks",false),"HaodoopOffice Hadoop configuration option set boolean");
		
	}
	
	@Test
	public void deserializeSimple() {
		
	}
	
	
	@Test
	public void serializeSimple() {
		
	}

}
