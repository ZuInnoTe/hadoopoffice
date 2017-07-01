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
package org.zuinnote.hadoop.office.format.common.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.EmptyFileException;
import org.apache.poi.hssf.eventusermodel.HSSFEventFactory;
import org.apache.poi.hssf.eventusermodel.HSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.hssf.record.BOFRecord;
import org.apache.poi.hssf.record.BoundSheetRecord;
import org.apache.poi.hssf.record.FormulaRecord;
import org.apache.poi.hssf.record.LabelSSTRecord;
import org.apache.poi.hssf.record.NumberRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.RowRecord;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.hssf.record.StringRecord;
import org.apache.poi.poifs.filesystem.DocumentFactoryHelper;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import org.apache.poi.util.IOUtils;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.util.MSExcelUtil;

/*
*
* This class is responsible for parsing Excel content in OOXML format and old excel format using a low resource footprint (CPU, memory)
*
*/
public class MSExcelLowFootprintParser implements OfficeReaderParserInterface  {
	/*
	* In the default case all sheets are parsed one after the other.
	* @param hocr HadoopOffice configuration for reading files:
	* locale to use (if null then default locale will be used), see java.util.Locale
	* filename Filename of the document
	* password Password of this document (null if no password)
	*
	*/
	public final static int FORMAT_UNSUPPORTED=-1;
	public final static int FORMAT_OLDEXCEL=0;
	public final static int FORMAT_OOXML=1;
	private static final Log LOG = LogFactory.getLog(MSExcelLowFootprintParser.class.getName());
	private ArrayList<SpreadSheetCellDAO[]> spreadSheetCellDAOCache;
	private InputStream in;
	private String[] sheets=null;
	private HadoopOfficeReadConfiguration hocr;
	private int format;
	private int currentRow;
	
	public MSExcelLowFootprintParser(HadoopOfficeReadConfiguration hocr) {
		this(hocr, null);
	}

	/*
	*
	* Only process selected sheets (one after the other)
	*
	* @param hocr HadoopOffice configuration for reading files:
	* password Password of this document (null if no password)
	* metadataFilter filter on metadata. The name is the metadata attribute name and the property is a filter which contains a regular expression. Currently the following are supported for .xlsx documents: category,contentstatus, contenttype,created,creator,description,identifier,keywords,lastmodifiedbyuser,lastprinted,modified,revision,subject,title. Additionally all custom.* are defined as custom properties. Example custom.myproperty. Finally, matchAll can be set to true (all metadata needs to be matched), or false (at least one of the metadata item needs to match).
 Currently the following are supported for .xls documents: applicationname,author,charcount, comments, createdatetime,edittime,keywords,lastauthor,lastprinted,lastsavedatetime,pagecount,revnumber,security,subject,template,title,wordcount. Finally, matchAll can be set to true (all metadata needs to be matched), or false (at least one of the metadata item needs to match).
	* @param sheets selecrted sheets
	*
	*/
	public MSExcelLowFootprintParser(HadoopOfficeReadConfiguration hocr, String[] sheets) {
		this.sheets=sheets;
		this.hocr=hocr;
		this.format=MSExcelLowFootprintParser.FORMAT_UNSUPPORTED; // will be detected when calling parse
		this.spreadSheetCellDAOCache=new ArrayList<SpreadSheetCellDAO[]>();
	}
	
	/*
	*
	* Parses the given InputStream containing Excel data. The type of InputStream (e.g. FileInputStream, BufferedInputStream etc.) does not matter here, but it is recommended to use an appropriate
	* type to avoid performance issues. 
	*
	* @param in InputStream containing Excel data
	*
	* @throws org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException in case there are issues reading from the Excel file, e.g. wrong password or unknown format
	*
	*/
	@Override
	public void parse(InputStream in) throws FormatNotUnderstoodException {
		this.currentRow=0;
		// detect workbook type (based on Workbookfactory code in Apache POI
		// If clearly doesn't do mark/reset, wrap up
		if(!in.markSupported()) {
					in = new PushbackInputStream(in, 8);
				}
		 try {
			byte[] header8 = IOUtils.peekFirst8Bytes(in);
		 
				if(NPOIFSFileSystem.hasPOIFSHeader(header8)) {
					 // use event model API for old Excel files
					this.format=MSExcelLowFootprintParser.FORMAT_OLDEXCEL;
					  NPOIFSFileSystem poifs = new NPOIFSFileSystem(in);
					  InputStream din = poifs.createDocumentInputStream("Workbook");
					  HSSFRequest req = new HSSFRequest();
					  req.addListenerForAllRecords(new HSSFEventParser(this.spreadSheetCellDAOCache,this.sheets));
					  HSSFEventFactory factory = new HSSFEventFactory();
					  factory.processEvents(req, din);
					  din.close();
				} else
				if(DocumentFactoryHelper.hasOOXMLHeader(in)) {
							// use event model API for new Excel files
					this.format=MSExcelLowFootprintParser.FORMAT_OOXML;
				} else {
					this.format=MSExcelLowFootprintParser.FORMAT_UNSUPPORTED;
					throw new FormatNotUnderstoodException("Could not detect Excel format in low footprint reading mode");
				}
		 } 
			
				catch (EmptyFileException | IOException e) {
					LOG.error(e);
					throw new FormatNotUnderstoodException("Could not detect format in Low footprint reading mode");
				}
		 finally {
		 	  if (this.in!=null) {
		 		  try {
					this.in.close();
				} catch (IOException e) {
					LOG.error(e);
					throw new FormatNotUnderstoodException("Error closing inputstream");
				}
		 	  }
		 }
	}

	@Override
	public long getCurrentRow() {
		return this.currentRow;
	}

	@Override
	public String getCurrentSheetName() {
		// check in currentRow if it is there?
		SpreadSheetCellDAO[] currentRowDAO = this.spreadSheetCellDAOCache.get(this.currentRow);
		if ((currentRowDAO!=null) && (currentRowDAO.length>0)) {
			return currentRowDAO[0].getSheetName();
		} else if (this.currentRow<this.spreadSheetCellDAOCache.size()) {
			int i = this.currentRow;
			while (i<this.spreadSheetCellDAOCache.size()) {
				currentRowDAO = this.spreadSheetCellDAOCache.get(i);
				if ((currentRowDAO!=null) && (currentRowDAO.length>0)) {
					return currentRowDAO[0].getSheetName();
				}
			}
			
		} 
		return "";
	}

	@Override
	public boolean addLinkedWorkbook(String name, InputStream inputStream, String password)
			throws FormatNotUnderstoodException {
		throw new FormatNotUnderstoodException("Workbooks are not supported in low footprint mode");
	}

	@Override
	public List<String> getLinkedWorkbooks() {
		return new ArrayList<>();
	}

	@Override
	public Object[] getNext() {
		if (this.currentRow<this.spreadSheetCellDAOCache.size()) {
			return this.spreadSheetCellDAOCache.get(this.currentRow++);
		}
		return null;
	}

	@Override
	public boolean getFiltered() {
		return false;
	}

	@Override
	public void close() throws IOException {
 	  if (this.in!=null) {
 		  this.in.close();
 	  }
		
	}
	
	private class HSSFEventParser implements HSSFListener {
		private ArrayList<SpreadSheetCellDAO[]> spreadSheetCellDAOCache;
		private String currentSheet;
		private SpreadSheetCellDAO[] currentRow;
		private String[] sheets;
		private int currentRowNum;
		private int currentCellNum;
		private boolean readCachedFormulaResult;
		private boolean currentSheetIgnore;
		private SSTRecord currentSSTrecord;

		public HSSFEventParser(ArrayList<SpreadSheetCellDAO[]> spreadSheetCellDAOCache, String[] sheets) {
			this.spreadSheetCellDAOCache=spreadSheetCellDAOCache;
			this.currentSheet="";
			this.sheets=sheets;
			this.currentRow=null;
			this.currentRowNum=-1;
			this.currentCellNum=0;
			this.currentSheetIgnore=false;
			this.readCachedFormulaResult=false;
		}
		
		@Override
		public void processRecord(Record record) {
			switch (record.getSid())
	        {
	            // the BOFRecord can represent either the beginning of a sheet or the workbook
	            case BOFRecord.sid:
	                BOFRecord bof = (BOFRecord) record;
	                if (bof.getType() == bof.TYPE_WORKBOOK)
	                {
	                    // ignored
	                } else if (bof.getType() == bof.TYPE_WORKSHEET)
	                {
	                    // ignored
	                }
	                break;
	            case BoundSheetRecord.sid:
	                BoundSheetRecord bsr = (BoundSheetRecord) record;
	                this.currentSheet=bsr.getSheetname();
	                if ((this.sheets!=null) && (this.currentSheet!=null)) { // check if sheet should be ignored
	                	boolean found=false;
	                    for(int i=0;i<this.sheets.length;i++) {
	                    	if (this.currentSheet.equals(this.sheets[i])) {
	                    		found=true;
	                    		break;
	                    	}
	                    }
	                    this.currentSheetIgnore=found;
	                } 
	                break;
	            case RowRecord.sid:
	            	if (!this.currentSheetIgnore) { // only create a new row record if sheet should not be ignored
		                RowRecord rowRec = (RowRecord) record;
		                if (currentCellNum==this.currentRow.length) { // create a new row
		                	this.currentRow=null;
	            		}
		                // create new Row
		                if (this.currentRow==null) {
		                	this.currentRow=new SpreadSheetCellDAO[rowRec.getLastCol()];
		                	this.currentRowNum++;
		                } else {
		                	LOG.error("Unexpected new row in file.");
		                }
		                this.currentCellNum=0;
	            	}
	                break;
	            case FormulaRecord.sid:
	            	// check if formula has a cached value
	            	FormulaRecord formRec=(FormulaRecord) record;
	            	if (formRec.hasCachedResultString()) {
	            		this.readCachedFormulaResult=true;
	            	} else {
	            		// try to read the result
	            		if (this.currentCellNum>=this.currentRow.length) {
	            			LOG.error("More cells in row than expected. Cell not added");
	            		} else {
	             			this.currentRow[this.currentCellNum] = new SpreadSheetCellDAO(String.valueOf(formRec.getValue()),"","",MSExcelUtil.getCellAddressA1Format(this.currentRowNum, this.currentCellNum),this.currentSheet);
	            			this.currentCellNum++;
	            		}
	            	}
	            	break;
	            case StringRecord.sid:  // read cached formula results, if available
	            	StringRecord strRec=(StringRecord) record;
	            	if (this.readCachedFormulaResult) {
	            		if (this.currentCellNum>=this.currentRow.length) {
	            			LOG.error("More cells in row than expected. Cell not added");
	            		} else {
	             			this.currentRow[this.currentCellNum] = new SpreadSheetCellDAO(strRec.getString(),"","",MSExcelUtil.getCellAddressA1Format(this.currentRowNum, this.currentCellNum),this.currentSheet);
	            			this.readCachedFormulaResult=false;
	            			this.currentCellNum++;
	            		}
	            	
	            	}
	            	break;
	            case NumberRecord.sid: // read number result
	                NumberRecord numrec = (NumberRecord) record;
	                if (this.currentCellNum>=this.currentRow.length) {
            			LOG.error("More cells in row than expected. Cell not added");
            		} else {
             			this.currentRow[this.currentCellNum] = new SpreadSheetCellDAO(String.valueOf(numrec.getValue()),"","",MSExcelUtil.getCellAddressA1Format(this.currentRowNum, this.currentCellNum),this.currentSheet);
            		
            			this.currentCellNum++;
            		}
	                break;
	                // SSTRecords store a array of unique strings used in Excel.
	            case SSTRecord.sid:
	                this.currentSSTrecord=(SSTRecord) record;
	                break;
	            case LabelSSTRecord.sid: // get the string out of unique string value table 
	                LabelSSTRecord lrec = (LabelSSTRecord) record;
	                if (this.currentCellNum>=this.currentRow.length) {
            			LOG.error("More cells in row than expected. Cell not added");
            		} else {
            			if ((lrec.getSSTIndex()<0) || (lrec.getSSTIndex()>=this.currentSSTrecord.getNumUniqueStrings())) {
            				LOG.error("Invalid SST record index. Cell ignored");
            			} else {
            				this.currentRow[this.currentCellNum] = new SpreadSheetCellDAO(this.currentSSTrecord.getString(lrec.getSSTIndex()).getString(),"","",MSExcelUtil.getCellAddressA1Format(this.currentRowNum, this.currentCellNum),this.currentSheet);
            				this.currentCellNum++;
            			}
            		}
	                break;
	              
	        }
			
		}
		
	}
	
}
