/**
* Copyright 2017 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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
package org.zuinnote.hadoop.office.format.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class OfficeCheckTestDataAvailable {
	
	@BeforeAll
    public static void oneTimeSetUp() throws IOException {
      // one-time initialization code   

    }

    @AfterAll
    public static void oneTimeTearDown() {
        // one-time cleanup code
      }

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void tearDown() {  
    }
	
	@Test
	public void checkTestExcel2003EmptySheetAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003empty.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013EmptySheetAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013empty.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2003SingleSheetAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003test.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013SingleSheetAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013test.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013MultiSheetAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013testmultisheet.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013CommentAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013comment.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013LinkedWorkbooksAvailable() {
		// depends on file excel2013test.xslx
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013linkedworkbooks.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013LinkedWorkbooksLink1Available() {
		// depends on file excel2013test.xslx
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013linkedworkbookslink1.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013LinkedWorkbooksLink2Available() {
		// depends on file excel2013test.xslx
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013linkedworkbookslink2.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013MultiSheetGzipAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013testmultisheet.xlsx.gz";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2003EmptyRows() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003testemptyrows.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013EmptyRows() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013testemptyrows.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013MainWorkbook() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013linkedworkbooks.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013LinkedWorkbook2() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013linkedworkbookslink1.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013LinkedWorkbook1() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013linkedworkbookslink2.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2003MainWorkbook() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003linkedworkbooks.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2003LinkedWorkbook1() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003linkedworkbookslink1.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2003LinkedWorkbook2() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2003linkedworkbookslink2.xls";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013TemplateAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "templatetest1.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013TemplateEncryptedAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "templatetest1encrypt.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013MultiSheetBzip2Available() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "excel2013testmultisheet.xlsx.bz2";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013MultiSheetHeaderAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "multisheetheader.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}

	@Test
	public void checkTestExcel2013SkipSheetvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName = "skipsheet.xlsx";
		String fileNameSpreadSheet = classLoader.getResource(fileName).getFile();
		assertNotNull(fileNameSpreadSheet, "Test Data File \"" + fileName + "\" is not null in resource path");
		File file = new File(fileNameSpreadSheet);
		assertTrue(file.exists(), "Test Data File \"" + fileName + "\" exists");
		assertFalse(file.isDirectory(), "Test Data File \"" + fileName + "\" is not a directory");
	}



}
