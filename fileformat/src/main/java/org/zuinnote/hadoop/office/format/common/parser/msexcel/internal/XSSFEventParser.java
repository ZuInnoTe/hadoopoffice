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

package org.zuinnote.hadoop.office.format.common.parser.msexcel.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;



/** Adapted from the Apache POI HowTos 
 * https://poi.apache.org/spreadsheet/how-to.html
 * 
 * **/
//https://svn.apache.org/repos/asf/poi/trunk/src/examples/src/org/apache/poi/xssf/streaming/examples/HybridStreaming.java
public class XSSFEventParser implements SheetContentsHandler {
	private Map<Integer,List<SpreadSheetCellDAO[]>> spreadSheetCellDAOCache; 
	private ArrayList<SpreadSheetCellDAO> spreadSheetCellDAOCurrentRow;
	private String sheetName;
	private Integer currentSheet;

	private int currentRow;
	private int currentColumn;
	
	public XSSFEventParser(Integer currentSheet,String sheetName, Map<Integer,List<SpreadSheetCellDAO[]>> spreadSheetCellDAOCache) {
		this.currentSheet=currentSheet;
		this.spreadSheetCellDAOCache=spreadSheetCellDAOCache;
		this.spreadSheetCellDAOCache.put(currentSheet, new ArrayList<SpreadSheetCellDAO[]>());
		this.sheetName=sheetName;
		this.currentRow=-1;
	}
	
	
	@Override
	public void startRow(int rowNum) {
		if (rowNum>currentRow+1) {
			// create empty rows
			while (rowNum-1!=currentRow) {
				this.spreadSheetCellDAOCache.get(this.currentSheet).add(new SpreadSheetCellDAO[0]);
				this.currentRow++;
			}
		}
			// create for current Row temporary storage
			this.spreadSheetCellDAOCurrentRow=new ArrayList<SpreadSheetCellDAO>();
			this.currentColumn=0;
	}
	
	@Override
	public void endRow(int rowNum) {
		currentRow+=1;
		// store row
		SpreadSheetCellDAO[] currentRowDAO = new SpreadSheetCellDAO[this.spreadSheetCellDAOCurrentRow.size()];
		currentRowDAO=this.spreadSheetCellDAOCurrentRow.toArray(currentRowDAO);
		this.spreadSheetCellDAOCache.get(this.currentSheet).add(currentRowDAO);
		
	}
	@Override
	public void cell(String cellReference, String formattedValue, XSSFComment comment) {
		// create empty column, if needed
		
		CellAddress currentCellAddress = new CellAddress(cellReference);
		for (int i=this.currentColumn;i<currentCellAddress.getColumn();i++) {
			this.spreadSheetCellDAOCurrentRow.add(null);
			this.currentColumn++;
		}
		// add column
		SpreadSheetCellDAO currentDAO = null;
		if (comment!=null) {
			currentDAO = new SpreadSheetCellDAO(formattedValue,comment.getString().getString(), "", cellReference,this.sheetName);
		} else {
			currentDAO = new SpreadSheetCellDAO(formattedValue,"", "", cellReference,this.sheetName);
		}
		this.currentColumn++;
		this.spreadSheetCellDAOCurrentRow.add(currentDAO);
	}
	@Override
	public void headerFooter(String text, boolean isHeader, String tagName) {
		// we do not care about header/footer
		
		
	}
	
	
	
}
