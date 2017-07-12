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
package org.zuinnote.hadoop.office.format.common.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.optim.linear.LinearObjectiveFunction;
import org.apache.poi.EmptyFileException;
import org.apache.poi.hssf.eventusermodel.EventWorkbookBuilder.SheetRecordCollectingListener;
import org.apache.poi.hssf.eventusermodel.HSSFEventFactory;
import org.apache.poi.hssf.eventusermodel.HSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.hssf.eventusermodel.MissingRecordAwareHSSFListener;
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingRowDummyRecord;
import org.apache.poi.hssf.model.HSSFFormulaParser;
import org.apache.poi.hssf.record.BOFRecord;
import org.apache.poi.hssf.record.BoundSheetRecord;
import org.apache.poi.hssf.record.ExtendedFormatRecord;
import org.apache.poi.hssf.record.FormatRecord;
import org.apache.poi.hssf.record.FormulaRecord;
import org.apache.poi.hssf.record.LabelSSTRecord;
import org.apache.poi.hssf.record.NameCommentRecord;
import org.apache.poi.hssf.record.NameRecord;
import org.apache.poi.hssf.record.NumberRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.RowRecord;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.hssf.record.StringRecord;
import org.apache.poi.hssf.record.aggregates.FormulaRecordAggregate;
import org.apache.poi.hssf.record.crypto.Biff8EncryptionKey;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.crypt.Decryptor;
import org.apache.poi.poifs.filesystem.DocumentFactoryHelper;
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.util.IOUtils;
import org.apache.poi.util.SAXHelper;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler;
import org.apache.poi.xssf.model.CommentsTable;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
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

	private DataFormatter useDataFormatter=null;
	private static final Log LOG = LogFactory.getLog(MSExcelLowFootprintParser.class.getName());
	private Map<Integer,List<SpreadSheetCellDAO[]>> spreadSheetCellDAOCache;
	private List<String> sheetNameList;
	private InputStream in;
	private String[] sheets=null;
	private HadoopOfficeReadConfiguration hocr;
	private int currentSheet;
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
		if (hocr.getLocale()==null)  {
			useDataFormatter=new DataFormatter(); // use default locale
		} else {
			useDataFormatter=new DataFormatter(hocr.getLocale());
		}
		this.spreadSheetCellDAOCache=new HashMap<>();
		this.sheetNameList=new ArrayList<>();
		this.currentRow=0;
		this.currentSheet=0;
		// check not supported things and log
		if ((this.hocr.getReadLinkedWorkbooks()) || (this.hocr.getIgnoreMissingLinkedWorkbooks())) {
			LOG.warn("Linked workbooks not supported in low footprint parsing mode");
		}
		if ((this.hocr.getMetaDataFilter()!=null) && (this.hocr.getMetaDataFilter().size()>0))  {
			LOG.warn("Metadata filtering is not supported in low footprint parsing mode");
		}
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
					NPOIFSFileSystem poifs = new NPOIFSFileSystem(in);
					// check if we need to decrypt a new Excel file
					if (poifs.getRoot().hasEntry(Decryptor.DEFAULT_POIFS_ENTRY)) {
						in = DocumentFactoryHelper.getDecryptedStream(poifs, this.hocr.getPassword());
						OPCPackage pkg;
						try {
							pkg = OPCPackage.open(in);
							this.processOPCPackage(pkg);
						} catch (InvalidFormatException e) {
							LOG.error(e);
							throw new FormatNotUnderstoodException("Error cannot read new Excel file (.xlsx) in low footprint mdoe");
						}
						
					}
					// else we need to 
					LOG.info("Low footprint parsing of old Excel files (.xls)");
					 // use event model API for old Excel files
					if (this.hocr.getPassword()!=null) {
						Biff8EncryptionKey.setCurrentUserPassword(this.hocr.getPassword());
					}
					
					InputStream din = poifs.createDocumentInputStream("Workbook");
					try {
					  HSSFRequest req = new HSSFRequest();
					  HSSFEventParser parser = new HSSFEventParser(this.sheetNameList,this.useDataFormatter,this.spreadSheetCellDAOCache,this.sheets);
					  SheetRecordCollectingListener listener = new SheetRecordCollectingListener(new MissingRecordAwareHSSFListener(parser));
					  parser.setSheetRecordCollectingListener(listener);
					  req.addListenerForAllRecords(listener);
					  HSSFEventFactory factory = new HSSFEventFactory();
					  factory.processEvents(req, din);
					}
					  finally {

						  Biff8EncryptionKey.setCurrentUserPassword(null);
						  din.close();
						  poifs.close();
					  }
				} else
				if(DocumentFactoryHelper.hasOOXMLHeader(in)) { // use event model API for uncrypted new Excel files
					LOG.info("Low footprint parsing of new Excel files (.xlsx)");
					// this is unencrypted
					
					try {
						OPCPackage pkg = OPCPackage.open(in);
						this.processOPCPackage(pkg);
					} catch (InvalidFormatException e) {
						LOG.error(e);
						throw new FormatNotUnderstoodException("Error cannot read new Excel file (.xlsx)");
					}
							
				} else {
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

	
	/**
	 * Processes a OPCPackage (new Excel format, .xlsx) in Streaming Mode
	 * 
	 * @param pkg
	 * @throws OpenXML4JException 
	 * @throws IOException 
	 */
	private void processOPCPackage(OPCPackage pkg) throws FormatNotUnderstoodException {
		LOG.debug("Processing OPCPackage in low footprint mode");
		XSSFReader r;
		try {
			r = new XSSFReader( pkg );
		} catch (IOException | OpenXML4JException e) {
			LOG.error(e);
			throw new FormatNotUnderstoodException("Error cannot parse new Excel file (.xlsx)");
		}
		try {
			
			ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
			
			StylesTable styles = r.getStylesTable();
			XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator)r.getSheetsData();
			int sheetNumber = 0;
			while (iter.hasNext()) {
				
				// check if we need to parse this sheet?
				boolean parse=false;
				if (this.sheets!=null) {
					for (int i=0;i<this.sheets.length;i++) {
						if (iter.getSheetName().equals(this.sheets[i])) {
							parse=true;
							break;
						}
					}
				} else {
					parse=true;
				}
				// sheet is supposed to be parsed
				if (parse) {
					
					InputStream rawSheetInputStream = iter.next();
					this.sheetNameList.add(iter.getSheetName());
					InputSource rawSheetInputSource = new InputSource(rawSheetInputStream);
					XMLReader sheetParser = SAXHelper.newXMLReader();
					XSSFEventParser xssfp = new XSSFEventParser(sheetNumber,iter.getSheetName(), this.spreadSheetCellDAOCache);
					
		            ContentHandler handler = new XSSFSheetXMLHandler(
		                  styles, iter.getSheetComments(), strings, xssfp, this.useDataFormatter, false);
		            sheetParser.setContentHandler(handler);
		            sheetParser.parse(rawSheetInputSource);
		            sheetNumber++;

				}
						}
		} catch (InvalidFormatException | IOException e) {
			LOG.error(e);
			throw new FormatNotUnderstoodException("Error cannot parse new Excel file (.xlsx)");
		} catch (SAXException e) {
			LOG.error(e);
			throw new FormatNotUnderstoodException("Parsing Excel sheet in .xlsx format failed. Cannot read XML content");
		} catch (ParserConfigurationException e) {
			LOG.error(e);
			throw new FormatNotUnderstoodException("Parsing Excel sheet in .xlsx format failed. Cannot read XML content");
		}
		
		
	
	}
	
	@Override
	public long getCurrentRow() {
		return this.currentRow;
	}

	@Override
	public String getCurrentSheetName() {
		if (this.currentSheet>=this.sheetNameList.size()) {
			return this.sheetNameList.get(this.sheetNameList.size()-1);
		}
		return this.sheetNameList.get(this.currentSheet);
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
		SpreadSheetCellDAO[] result = null;
		if (this.currentRow<this.spreadSheetCellDAOCache.get(this.currentSheet).size()) {
			result=this.spreadSheetCellDAOCache.get(this.currentSheet).get(this.currentRow++);
		} 
		if (this.currentRow==this.spreadSheetCellDAOCache.get(this.currentSheet).size()) { // next sheet
			this.spreadSheetCellDAOCache.remove(this.currentSheet);
			this.currentSheet++;
			this.currentRow=0;
			
		}
		
		return result;
	}

	@Override
	public boolean getFiltered() {
		return true;
	}

	@Override
	public void close() throws IOException {
 	  if (this.in!=null) {
 		  this.in.close();
 	  }
		
	}
	
	/** Adapted from the Apache POI HowTos 
	 * https://poi.apache.org/spreadsheet/how-to.html
	 * 
	 * **/
	//https://svn.apache.org/repos/asf/poi/trunk/src/examples/src/org/apache/poi/xssf/streaming/examples/HybridStreaming.java
	private static class XSSFEventParser implements SheetContentsHandler {
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
				while (rowNum!=currentRow) {
					this.spreadSheetCellDAOCache.get(this.currentSheet).add(new SpreadSheetCellDAO[0]);
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
	
	
	/** Adapted the Apache POI HowTos 
	 * https://poi.apache.org/spreadsheet/how-to.html
	 * 
	 * **/
	private class HSSFEventParser implements HSSFListener {
		private Map<Integer,List<SpreadSheetCellDAO[]>> spreadSheetCellDAOCache; 
		private List<String> sheetList;
		private Map<Integer,Boolean> sheetMap;
		private Map<Integer,Long> sheetSizeMap;
		private List<Integer> extendedRecordFormatIndexList;
		private Map<Integer,String> formatRecordIndexMap;
		private DataFormatter useDataFormatter;
		private int currentSheet;
		private String[] sheets;
		private long currentCellNum;
		private boolean readCachedFormulaResult;
		private int cachedRowNum;
		private short cachedColumnNum;
		private boolean currentSheetIgnore;
		private SSTRecord currentSSTrecord;
		private SheetRecordCollectingListener workbookBuildingListener;
		private HSSFWorkbook stubWorkbook;

		public HSSFEventParser(List<String> sheetNameList,DataFormatter useDataFormatter, Map<Integer,List<SpreadSheetCellDAO[]>> spreadSheetCellDAOCache, String[] sheets) {
			this.spreadSheetCellDAOCache=spreadSheetCellDAOCache;
			this.sheets=sheets;
			this.currentCellNum=0L;
			this.currentSheetIgnore=false;
			this.readCachedFormulaResult=false;
			this.cachedRowNum=0;
			this.cachedColumnNum=0;
			this.sheetList=new ArrayList<>();
			this.sheetMap=new HashMap<>();
			this.sheetSizeMap=new HashMap<>();
			this.currentSheet=0;
			this.extendedRecordFormatIndexList=new ArrayList<>();
			this.formatRecordIndexMap=new HashMap<>();
			this.sheetList=sheetNameList;
			this.useDataFormatter=useDataFormatter;
		}
		
		public void setSheetRecordCollectingListener(SheetRecordCollectingListener listener) {
			this.workbookBuildingListener=listener;
			
		}

		@Override
		public void processRecord(Record record) {
			switch (record.getSid()) // one should note that these do not arrive necessary in linear order. First all the sheets are processed. Then all the rows of the sheets
	        {
	            // the BOFRecord can represent either the beginning of a sheet or the workbook
	            case BOFRecord.sid:
	                BOFRecord bof = (BOFRecord) record;
	                if (bof.getType() == bof.TYPE_WORKBOOK)
	                {
	                    // ignored
	                	if ((stubWorkbook==null) && (workbookBuildingListener!=null)){
	                		stubWorkbook= workbookBuildingListener.getStubHSSFWorkbook();
	                	} else {
	                		LOG.error("Cannot create stub network. Formula Strings cannot be parsed");
	                	}
	                } else if (bof.getType() == bof.TYPE_WORKSHEET)
	                {
	                    // ignored
	                }
	                break;
	            case BoundSheetRecord.sid:
	                BoundSheetRecord bsr = (BoundSheetRecord) record;
	                String currentSheet=bsr.getSheetname();
	                LOG.debug("Sheet found: "+currentSheet);
	                if (this.sheets==null) { // no sheets filter
	                	// ignore the filter
	                	this.sheetMap.put(this.sheetList.size(), true);
	                	this.sheetSizeMap.put(this.sheetList.size(), 0L);
                    	this.sheetList.add(currentSheet);
	                } else
	                if (currentSheet!=null) { // sheets filter
	                 	boolean found=false;
	                    for(int i=0;i<this.sheets.length;i++) {
	                    	if (currentSheet.equals(this.sheets[i])) {
	                    		found=true;
	                    		break;
	                    	}
	                    }
	                    this.sheetMap.put(this.sheetList.size(), found);
	                    this.sheetList.add(currentSheet);
	                } 
	                if (this.sheetMap.get(this.sheetList.size()-1)) { // create sheet
	                	 this.spreadSheetCellDAOCache.put(this.sheetList.size()-1, new ArrayList<SpreadSheetCellDAO[]>());
	                }
	                break;
	            case RowRecord.sid:
	            	  RowRecord rowRec = (RowRecord) record;
		              LOG.debug("Row found. Number of Cells: "+rowRec.getLastCol());
		              if ((this.currentSheet==0) && (rowRec.getRowNumber()==0)) { // first sheet
		            	  // special handling first sheet
		            	  this.currentSheet++;
		            	  this.currentCellNum=0;
		              } else
		              if ((this.currentSheet>0) && (rowRec.getRowNumber()==0)) { // new sheet
		            	  LOG.debug("Sheet number : "+this.currentSheet+" total number of cells "+this.currentCellNum);
		            	  this.sheetSizeMap.put(this.currentSheet-1, this.currentCellNum);
		            	  this.currentSheet++; // start processing next sheet
		            	  this.currentCellNum=0;
		              }
		              // create row if this sheet is supposed to be parsed
		              if (this.sheetMap.get(this.currentSheet-1)) {
		            	  this.spreadSheetCellDAOCache.get(this.currentSheet-1).add(new SpreadSheetCellDAO[rowRec.getLastCol()]);
		              }
		              this.currentCellNum+=rowRec.getLastCol();
	                break;
	            case FormulaRecord.sid:
	            	LOG.debug("Formula Record found");
	            	// check if formula has a cached value
	            	FormulaRecord formRec=(FormulaRecord) record;
	            	
	            	/** check if this one should be parsed **/
	            	if (!this.sheetMap.get(this.currentSheet-1)) {// if not then do nothing
	            		break;
	            	}
	            	/** **/
	            	String formulaString = "";
	            	if (this.stubWorkbook!=null) {
	            		formulaString=HSSFFormulaParser.toFormulaString(stubWorkbook, formRec.getParsedExpression());
	            	}
	            	if (formRec.hasCachedResultString()) {
	            		this.readCachedFormulaResult=true;
	            		this.cachedColumnNum=formRec.getColumn();
	            		this.cachedRowNum=formRec.getRow();
	            	} else {
	            		// try to read the result
	            		if (formRec.getColumn()>=this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(formRec.getRow()).length) {
	            			LOG.error("More cells in row than expected. Row number:"+(formRec.getRow())+"Column number: "+formRec.getColumn()+"row length "+this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(formRec.getRow()).length);
	                    	
	            		} else {

	            			int formatIndex= this.extendedRecordFormatIndexList.get(formRec.getXFIndex());
	            			String theNumber=this.useDataFormatter.formatRawCellContents(formRec.getValue(), formatIndex, this.formatRecordIndexMap.get(formatIndex));
	            			this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(formRec.getRow())[formRec.getColumn()]=new SpreadSheetCellDAO(theNumber,"",formulaString,MSExcelUtil.getCellAddressA1Format(formRec.getRow(), formRec.getColumn()),this.sheetList.get(this.currentSheet-1));          			
	            		}
	            	}
	            	break;
	            case StringRecord.sid:  // read cached formula results, if available
	            	LOG.debug("String Record found");
	            	StringRecord strRec=(StringRecord) record;
	            	/** check if this one should be parsed **/
	               	if (!this.sheetMap.get(this.currentSheet-1)) {// if not then do nothing
	            		break;
	            	}
	            	/** **/
	              this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(this.cachedRowNum)[this.cachedColumnNum]=new SpreadSheetCellDAO(strRec.getString(),"","",MSExcelUtil.getCellAddressA1Format(this.cachedRowNum,this.cachedColumnNum),this.sheetList.get(this.currentSheet-1));          			
	    	         this.readCachedFormulaResult=false;	
	            	
	            	break;
	            case NumberRecord.sid: // read number result
	            	LOG.debug("Number Record found");
	            	
	                NumberRecord numrec = (NumberRecord) record;
	           
	            
	                /** check if this one should be parsed **/
	               	if (!this.sheetMap.get(this.currentSheet-1)) {// if not then do nothing
	            		break;
	            	}
	            	/** **/
	            	// try to read the result
            		if (numrec.getColumn()>=this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(numrec.getRow()).length) {
            			LOG.error("More cells in row than expected. Row number:"+(numrec.getRow())+"Column number: "+numrec.getColumn()+"row length "+this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(numrec.getRow()).length);
            		} else {
            			// convert the number in the right format (can be date etc.)
            			int formatIndex= this.extendedRecordFormatIndexList.get(numrec.getXFIndex());
            			String theNumber=this.useDataFormatter.formatRawCellContents(numrec.getValue(), formatIndex, this.formatRecordIndexMap.get(formatIndex));
            			   this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(numrec.getRow())[numrec.getColumn()]=new SpreadSheetCellDAO(theNumber,"","",MSExcelUtil.getCellAddressA1Format(numrec.getRow(),numrec.getColumn()),this.sheetList.get(this.currentSheet-1));          		
            		}
	                break;
	                // SSTRecords store a array of unique strings used in Excel. (one per sheet?)
	            case SSTRecord.sid:
	            	LOG.debug("SST record found");
	          
	                this.currentSSTrecord=(SSTRecord) record;
	                break;
	            case LabelSSTRecord.sid: // get the string out of unique string value table 
	            	LOG.debug("Label found");
	                LabelSSTRecord lrec = (LabelSSTRecord) record;
	              	/** check if this one should be parsed **/
	               	if (!this.sheetMap.get(this.currentSheet-1)) {// if not then do nothing
	            		break;
	            	}
	            	/** **/
	            	if (lrec.getColumn()>=this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(lrec.getRow()).length) {
	            		LOG.error("More cells in row than expected. Row number:"+(lrec.getRow())+"Column number: "+lrec.getColumn()+"row length "+this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(lrec.getRow()).length);
	                	
            		} else {
            			if ((lrec.getSSTIndex()<0) || (lrec.getSSTIndex()>=this.currentSSTrecord.getNumUniqueStrings())) {
            				LOG.error("Invalid SST record index. Cell ignored");
            			} else {
            				   this.spreadSheetCellDAOCache.get(this.currentSheet-1).get(lrec.getRow())[lrec.getColumn()]=new SpreadSheetCellDAO(this.currentSSTrecord.getString(lrec.getSSTIndex()).getString(),"","",MSExcelUtil.getCellAddressA1Format(lrec.getRow(),lrec.getColumn()),this.sheetList.get(this.currentSheet-1));          		
            	            	
            				
            			}
            		}
	                break;
	            case ExtendedFormatRecord.sid:
	            	LOG.debug("Found extended format record");
	            	ExtendedFormatRecord nfir = (ExtendedFormatRecord)record;
	            	this.extendedRecordFormatIndexList.add((int)nfir.getFormatIndex());
	               	
	            	
	            	break;
	            case FormatRecord.sid:
	            	LOG.debug("Found format record");
	            	FormatRecord frec = (FormatRecord)record;
	            	this.formatRecordIndexMap.put(frec.getIndexCode(),frec.getFormatString());
	            	
	            	break;
	            	
	          default:
	        	  //LOG.debug("Ignored record: "+record.getSid());
	        	  break;    
	        }
			if (record instanceof MissingRowDummyRecord) { // this is an empty row in the Excel
				MissingRowDummyRecord emptyRow = (MissingRowDummyRecord)record;
				 LOG.debug("Detected Empty row");
	              if ((this.currentSheet==0) && (emptyRow.getRowNumber()==0)) { // first sheet
	            	  // special handling first sheet
	            	  this.currentSheet++;
	            	  this.currentCellNum=0;
	              } else
	              if ((this.currentSheet>0) && (emptyRow.getRowNumber()==0)) { // new sheet
	            	  LOG.debug("Sheet number : "+this.currentSheet+" total number of cells "+this.currentCellNum);
	            	  this.sheetSizeMap.put(this.currentSheet-1, this.currentCellNum);
	            	  this.currentSheet++; // start processing next sheet
	            	  this.currentCellNum=0;
	              }
	              // create empty row if this sheet is supposed to be parsed
	              if (this.sheetMap.get(this.currentSheet-1)) {
	            	  this.spreadSheetCellDAOCache.get(this.currentSheet-1).add(new SpreadSheetCellDAO[0]);
	              }
			}
			
			
		}
		
	}
	
}
