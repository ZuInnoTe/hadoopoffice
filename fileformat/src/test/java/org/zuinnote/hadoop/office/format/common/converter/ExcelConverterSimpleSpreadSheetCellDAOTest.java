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
package org.zuinnote.hadoop.office.format.common.converter.datatypes;

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

/**
 * @author jornfranke
 *
 */
public class ExcelConverterSimpleSpreadSheetCellDAOTest {

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
	    public void checkTestExcelSimpleSheetAvailable() {
			ClassLoader classLoader = getClass().getClassLoader();
			String fileName="testsimple.xlsx";
			String fileNameSpreadSheet=classLoader.getResource(fileName).getFile();	
			assertNotNull(fileNameSpreadSheet,"Test Data File \""+fileName+"\" is not null in resource path");
			File file = new File(fileNameSpreadSheet);
			assertTrue(file.exists(),"Test Data File \""+fileName+"\" exists");
			assertFalse(file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
	    }
	    

	    @Test
	    public void convertCaseTestSimple() {
	    }

}
