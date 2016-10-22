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

package org.zuinnote.hadoop.office.format.parser;

import java.io.InputStream;
import java.io.IOException;

import java.util.Locale;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory; 
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.util.CellRangeAddress;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.zuinnote.hadoop.office.format.dao.SpreadSheetCellDAO;

/*
*
* This class is responsible for parsing Excel content in OOXML format and old excel format
*
*/

public class MSExcelParser implements OfficeReaderParserInterface {
private static final Log LOG = LogFactory.getLog(MSExcelParser.class.getName());
private FormulaEvaluator formulaEvaluator;
private InputStream in;
private DataFormatter useDataFormatter=null;
private String[] sheets=null;
private Workbook currentWorkbook=null;
private int currentSheet=0; // current sheet where we are
private int sheetsIndex=0; // current index of sheets, if specified
private int currentRow=0;

	/*
	* In the default case all sheets are parsed one after the other.
	*
	* @param useLocale Locale to use (if null then default locale will be used), see java.util.Locale
	*
	*/

	public MSExcelParser(Locale useLocale) {
		this.sheets=null;
		if (useLocale==null)  {
			useDataFormatter=new DataFormatter(); // use default locale
		} else {
			useDataFormatter=new DataFormatter(useLocale);
		}		
	}

	/*
	*
	* Only process selected sheets (one after the other)
	*
	* @param useLocale Locale to use (if null then default locale will be used), see java.util.Locale
	* @param sheets Set of sheets to be read
	*
	*/
	public MSExcelParser(Locale useLocale, String[] sheets) {
		this.sheets=sheets;
		if (useLocale==null)  {
			useDataFormatter=new DataFormatter(); // use default locale
		} else {
			useDataFormatter=new DataFormatter(useLocale);
		}
	}

	/*
	*
	* Parses the given InputStream containing Excel data. The type of InputStream (e.g. FileInputStream, BufferedInputStream etc.) does not matter here, but it is recommended to use an appropriate
	* type to avoid performance issues. 
	*
	* @param in InputStream containing Excel data
	*
	*/
	public void parse(InputStream in) throws IOException,FormatNotUnderstoodException {
		this.in=in;
		// read xls
		try {
			this.currentWorkbook=WorkbookFactory.create(in);
		} catch (InvalidFormatException e) {
			throw new FormatNotUnderstoodException(e.toString());
		}
		finally 
		{
			this.in.close();
			this.in=null;
		}
		 this.formulaEvaluator = this.currentWorkbook.getCreationHelper().createFormulaEvaluator();
	}

	/*
	* Returns the next row in the set of sheets. If sheets==null then all available sheets are returned in the order as specified in the document. If sheets contains specific sheets then rows of the specific sheets are returned in order of the sheets specified.
	*
	* @return column values for given row as Objects (use instanceof or similar to determine the type, currently only String objects or null if cell does not have a value), null if no further rows exist
	* 
	*/
	public Object[] getNext() {
		SpreadSheetCellDAO[] result=null;
		// all sheets?
		if (this.sheets==null) { //  go on with all sheets
				if (this.currentRow>this.currentWorkbook.getSheetAt(this.currentSheet).getLastRowNum()) { // end of row reached? => next sheet
					this.currentSheet++;
					this.currentRow=0;
					if (this.currentSheet>=this.currentWorkbook.getNumberOfSheets()) return result; // no more sheets available?
				}

		} else { // go on with specified sheets
			// go through sheets specified until one found
			boolean sheetFound=false;
			while((this.sheetsIndex!=this.sheets.length) && sheetFound==false) {
				if (this.currentWorkbook.getSheet(this.sheets[this.sheetsIndex])==null) { // log only if sheet not found
					LOG.warn("Sheet \""+this.sheets[this.sheetsIndex]+"\" not found");
				} else { // sheet found, check number of rows
				   if (this.currentRow>this.currentWorkbook.getSheet(this.sheets[this.sheetsIndex]).getLastRowNum()) {
					// reset rows
					this.currentRow=0;
				   } else { // we have a sheet where we still need to process rows
					this.currentSheet=this.currentWorkbook.getSheetIndex(this.currentWorkbook.getSheet(this.sheets[this.sheetsIndex]));
					sheetFound=true;
					break;
				   }
				}
				this.sheetsIndex++;
			}
			if (this.sheetsIndex==this.sheets.length) return result; // all sheets processed
		}
		// read row from the sheet currently to be processed
		Sheet rSheet = this.currentWorkbook.getSheetAt(this.currentSheet);
		Row rRow = rSheet.getRow(this.currentRow);
		if (rRow==null) return result;
		result = new SpreadSheetCellDAO[rRow.getLastCellNum()];
		for (int i=0;i<rRow.getLastCellNum();i++) {
			Cell currentCell=rRow.getCell(i);
			if (currentCell==null) {
				result[i]=null;
			} else {	
				String formattedValue=useDataFormatter.formatCellValue(currentCell,this.formulaEvaluator);
				String formula = "";
				if (currentCell.getCellTypeEnum()==CellType.FORMULA)  {
					formula = currentCell.getCellFormula();
				}
				Comment currentCellComment = currentCell.getCellComment();
				String comment = "";
				if (currentCellComment!=null) {
					comment = currentCellComment.getString().getString();
				}
				String address = currentCell.getAddress().toString();
				String sheetName = currentCell.getSheet().getSheetName();
				SpreadSheetCellDAO mySpreadSheetCellDAO = new SpreadSheetCellDAO(formattedValue,comment,formula,address,sheetName);
				result[i]=mySpreadSheetCellDAO;
			}
		}
		
		// increase rows
		this.currentRow++;
		return result;
	}

	public void close() throws IOException {
		if (this.in!=null) {
			in.close();
		}
		if (this.currentWorkbook!=null) {
			this.currentWorkbook.close();
		}
	
	}

}
