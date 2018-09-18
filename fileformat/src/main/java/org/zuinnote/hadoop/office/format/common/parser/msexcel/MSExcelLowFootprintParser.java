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
package org.zuinnote.hadoop.office.format.common.parser.msexcel;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.EmptyFileException;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.hssf.eventusermodel.EventWorkbookBuilder.SheetRecordCollectingListener;
import org.apache.poi.hssf.eventusermodel.HSSFEventFactory;

import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.hssf.eventusermodel.MissingRecordAwareHSSFListener;

import org.apache.poi.hssf.record.crypto.Biff8EncryptionKey;
import org.apache.poi.ooxml.util.SAXHelper;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackagePart;
import org.apache.poi.poifs.crypt.ChainingMode;
import org.apache.poi.poifs.crypt.CipherAlgorithm;
import org.apache.poi.poifs.crypt.Decryptor;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.dsig.SignatureConfig;
import org.apache.poi.poifs.crypt.dsig.SignatureInfo;
import org.apache.poi.poifs.crypt.dsig.SignaturePart;
import org.apache.poi.poifs.filesystem.FileMagic;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.DataFormatter;

import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;

import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFRelation;
import org.apache.xmlbeans.XmlException;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTWorkbook;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTWorkbookPr;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.WorkbookDocument;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.parser.OfficeReaderParserInterface;
import org.zuinnote.hadoop.office.format.common.parser.msexcel.internal.EncryptedCachedDiskStringsTable;
import org.zuinnote.hadoop.office.format.common.parser.msexcel.internal.HSSFEventParser;
import org.zuinnote.hadoop.office.format.common.parser.msexcel.internal.XSSFEventParser;
import org.zuinnote.hadoop.office.format.common.parser.msexcel.internal.XSSFPullParser;
import org.zuinnote.hadoop.office.format.common.util.CertificateChainVerificationUtil;


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
	private String[] header;
	private int currentSkipLine=0;
	private boolean firstSheetSkipped=false;
	private boolean event=true;
	private List<InputStream> pullSheetInputList;
	private List<String> pullSheetNameList;
	private XSSFPullParser currentPullParser;
	private EncryptedCachedDiskStringsTable pullSST;
	private ReadOnlySharedStringsTable pushSST;
	private CipherAlgorithm ca;
	private ChainingMode cm;
	private StylesTable styles;
	private boolean isDate1904;
	private boolean headerParsed;
	
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
		this.pullSheetInputList=new ArrayList<>();
		this.pullSheetNameList=new ArrayList<>();
		this.headerParsed=false;
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

		 try {
			InputStream nin = FileMagic.prepareToCheckMagic(in);
			FileMagic fm = FileMagic.valueOf(nin);
				if(fm==FileMagic.OLE2) { // 
					LOG.debug("Paersing OLE2 container");
					POIFSFileSystem poifs = new POIFSFileSystem(nin);
					// check if we need to decrypt a new Excel file
					if (poifs.getRoot().hasEntry(Decryptor.DEFAULT_POIFS_ENTRY)) {
						LOG.info("Low footprint parsing of new Excel files (.xlsx) - encrypted file");
							EncryptionInfo info = new EncryptionInfo(poifs);
							
							Decryptor d = Decryptor.getInstance(info);
						
							this.ca = d.getEncryptionInfo().getHeader().getCipherAlgorithm();
							this.cm = d.getEncryptionInfo().getHeader().getChainingMode();
							try {
								if (!d.verifyPassword(this.hocr.getPassword())) {
									throw new FormatNotUnderstoodException("Error: Cannot decrypt new Excel file (.xlsx) in low footprint mode: wrong password");
								}
								in = d.getDataStream(poifs);
							} catch (GeneralSecurityException e) {
								
								LOG.error(e);
								throw new FormatNotUnderstoodException("Error: Cannot decrypt new Excel file (.xlsx) in low footprint mode");
							}
						
						OPCPackage pkg;
						try {
							pkg = OPCPackage.open(in);
							this.processOPCPackage(pkg);
							
						} catch (InvalidFormatException e) {
							LOG.error(e);
							throw new FormatNotUnderstoodException("Error: Cannot read new Excel file (.xlsx) in low footprint mode");
						}
						return;
						
					}
					// else we need to 
					LOG.info("Low footprint parsing of old Excel files (.xls)");
					this.event=true;
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
					} catch (EncryptedDocumentException e) {
						LOG.error(e);
						throw new FormatNotUnderstoodException("Cannot decrypt document");
					}
					  finally {

						  Biff8EncryptionKey.setCurrentUserPassword(null);
						  din.close();
						  poifs.close();
					  }
				} else
				if(fm==FileMagic.OOXML) { // use event model API for uncrypted new Excel files
					LOG.info("Low footprint parsing of new Excel files (.xlsx) - not encrypted file");
					// this is unencrypted
					
					try {
						OPCPackage pkg = OPCPackage.open(nin);
						
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
		// check if signature should be verified
		if (this.hocr.getVerifySignature()) {
				LOG.info("Verifying signature of document");
				SignatureConfig sic = new SignatureConfig();
				sic.setOpcPackage(pkg);
				SignatureInfo si = new SignatureInfo();
				si.setSignatureConfig(sic);
				if (!si.verifySignature()) {
						throw new FormatNotUnderstoodException("Cannot verify signature of OOXML (.xlsx) file: "+this.hocr.getFileName());
				} else {
					LOG.info("Successfully verifed first part signature of OXXML (.xlsx) file: "+this.hocr.getFileName());
				}
				Iterator<SignaturePart> spIter = si.getSignatureParts().iterator();
				 while (spIter.hasNext()) {
					 SignaturePart currentSP = spIter.next();
					 if (!(currentSP.validate())) {
						 throw new FormatNotUnderstoodException("Could not validate all signature parts for file: "+this.hocr.getFileName());
					 } else {
						 X509Certificate currentCertificate = currentSP.getSigner();
						 try {
							if ((this.hocr.getX509CertificateChain().size()>0) && (!CertificateChainVerificationUtil.verifyCertificateChain(currentCertificate, this.hocr.getX509CertificateChain()))) {
								 throw new FormatNotUnderstoodException("Could not validate signature part for principal \""+currentCertificate.getSubjectX500Principal().getName()+"\" : "+this.hocr.getFileName());
							 }
						} catch (CertificateException | NoSuchAlgorithmException | NoSuchProviderException
								| InvalidAlgorithmParameterException e) {
							LOG.error("Could not validate signature part for principal \""+currentCertificate.getSubjectX500Principal().getName()+"\" : "+this.hocr.getFileName(), e);
							 throw new FormatNotUnderstoodException("Could not validate signature part for principal \""+currentCertificate.getSubjectX500Principal().getName()+"\" : "+this.hocr.getFileName());
								
						}
					 }
				 }
				 LOG.info("Successfully verifed all signatures of OXXML (.xlsx) file: "+this.hocr.getFileName());
		}
		// continue in lowfootprint mode
		XSSFReader r;
		try {
			r = new XSSFReader( pkg );
		} catch (IOException | OpenXML4JException e) {
			LOG.error(e);
			throw new FormatNotUnderstoodException("Error cannot parse new Excel file (.xlsx)");
		}
		try {
			// read date format
			InputStream workbookDataXML = r.getWorkbookData();
			WorkbookDocument wd = WorkbookDocument.Factory.parse(workbookDataXML);
			this.isDate1904 = wd.getWorkbook().getWorkbookPr().getDate1904();

			
			// read shared string tables
			if (HadoopOfficeReadConfiguration.OPTION_LOWFOOTPRINT_PARSER_SAX.equalsIgnoreCase(this.hocr.getLowFootprintParser())) {
				this.pushSST = new ReadOnlySharedStringsTable(pkg);
			} else if (HadoopOfficeReadConfiguration.OPTION_LOWFOOTPRINT_PARSER_STAX.equalsIgnoreCase(this.hocr.getLowFootprintParser())) {
				List<PackagePart> pkgParts = pkg.getPartsByContentType(XSSFRelation.SHARED_STRINGS.getContentType());
				if (pkgParts.size()>0) {
					this.pullSST = new EncryptedCachedDiskStringsTable(pkgParts.get(0), this.hocr.getSstCacheSize(), this.hocr.getCompressSST(), this.ca, this.cm);			
				}
			}
			this.styles = r.getStylesTable();
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
					if (HadoopOfficeReadConfiguration.OPTION_LOWFOOTPRINT_PARSER_SAX.equalsIgnoreCase(this.hocr.getLowFootprintParser())) {
						this.event=true;
						LOG.info("Using SAX parser for low footprint Excel parsing");
						XMLReader sheetParser = SAXHelper.newXMLReader();
						XSSFEventParser xssfp = new XSSFEventParser(sheetNumber,iter.getSheetName(), this.spreadSheetCellDAOCache);
						
			            ContentHandler handler = new XSSFSheetXMLHandler(
			                  this.styles, iter.getSheetComments(), this.pushSST, xssfp, this.useDataFormatter, false);
			            sheetParser.setContentHandler(handler);
			            sheetParser.parse(rawSheetInputSource);
			            sheetNumber++;
					} else if (HadoopOfficeReadConfiguration.OPTION_LOWFOOTPRINT_PARSER_STAX.equalsIgnoreCase(this.hocr.getLowFootprintParser())) {
						LOG.info("Using STAX parser for low footprint Excel parsing");
						this.event=false;
						this.pullSheetInputList.add(rawSheetInputStream);
						this.pullSheetNameList.add(iter.getSheetName());
						// make shared string table available
						
						// everything else is in the getNext method
					} else {
						LOG.error("Unknown XML parser configured for low footprint mode: \""+this.hocr.getLowFootprintParser()+"\"");
						throw new FormatNotUnderstoodException("Unknown XML parser configured for low footprint mode: \""+this.hocr.getLowFootprintParser()+"\"");
					}

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
		} catch (XmlException e) {
			LOG.error(e);
			throw new FormatNotUnderstoodException("Parsing Excel sheet in .xlsx format failed. Cannot read XML content");
		}
		 // check skipping of additional lines
		for (int i=0;i<this.hocr.getSkipLines();i++) {
			this.getNext();
		}
		 // check header
		 if (this.hocr.getReadHeader()) {

			 LOG.debug("Reading header...");
			 Object[] firstRow = this.getNext();
			 if (firstRow!=null) {
				 this.header=new String[firstRow.length];
				 for (int i=0;i<firstRow.length;i++) {
					 if ((firstRow[i]!=null) && (!"".equals(((SpreadSheetCellDAO)firstRow[i]).getFormattedValue()))) {
						 this.header[i]=((SpreadSheetCellDAO)firstRow[i]).getFormattedValue();
					 }
				 }
				 this.header=MSExcelParser.sanitizeHeaders(this.header, this.hocr.getColumnNameRegex(), this.hocr.getColumnNameReplace());
			 } else {
				 this.header=new String[0];
			 }
		 }
		 this.headerParsed=true;
		
	}
	
	@Override
	public long getCurrentRow() {
		if (this.currentRow==0) { // to be checked this is a fix for HSSF in low footprint mode (if and only if) the HSSF is encrypted
			return 1;
		}
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
		Object[] result=null;
		if (event) {
			result=this.getNextEvent();
		} else {
			LOG.info("Using STAX parser for low footprint Excel parsing");
			// everything else is in the getNextMethod
			try {
				result=this.getNextPull();
			} catch (XMLStreamException | FormatNotUnderstoodException e) {
				LOG.error(e);
			}
		} 
		return result;
	}
	
	private Object[] getNextPull() throws XMLStreamException, FormatNotUnderstoodException {
		Object[] result=null;
		// check if currentPullParser == null
		if ((this.currentPullParser==null) || (!this.currentPullParser.hasNext())) {
			if (this.pullSheetInputList.size()>0) {
					try {
						this.currentPullParser=new XSSFPullParser(this.pullSheetNameList.get(0),this.pullSheetInputList.get(0),this.pullSST,this.styles, this.useDataFormatter, this.isDate1904);
						this.pullSheetNameList.remove(0);
						this.pullSheetInputList.remove(0);
						// check if we need to skip lines
						if ((this.hocr.getSkipLinesAllSheets()) && (this.headerParsed)) {
							for (int i=0;i<this.hocr.getSkipLines();i++) {
								if (this.currentPullParser.hasNext()) {
									this.currentPullParser.getNext(); // skip line
								}
								this.currentRow++;
							}
						}
						// check if we need to skip header
						if ((this.hocr.getIgnoreHeaderInAllSheets()) && (this.headerParsed)) {
							if (this.currentPullParser.hasNext()) {
								this.currentPullParser.getNext(); // skip header
							}
							this.currentRow++;
						}
					} catch (XMLStreamException e) {
						LOG.error(e);

					}
	
			} else {
				return result;
			}
			
		}
		result= this.currentPullParser.getNext();
		return result;
	}
	
	private Object[] getNextEvent() {
		SpreadSheetCellDAO[] result = null;
		if (this.spreadSheetCellDAOCache.size()==0) {
			return result;
		}
		if (this.spreadSheetCellDAOCache.get(this.currentSheet).size()>0) {
			result=this.spreadSheetCellDAOCache.get(this.currentSheet).remove(0);
			this.currentRow++;
		} 
		while (this.spreadSheetCellDAOCache.get(this.currentSheet).size()<=0) { // next sheet
			this.spreadSheetCellDAOCache.remove(this.currentSheet);
			if (this.spreadSheetCellDAOCache.size()==0) {
				return result;
			}
			this.currentSheet++;
			this.currentRow=0;
			// check if we need to skip lines
			if (this.hocr.getSkipLinesAllSheets()) {
				for (int i=0;i<this.hocr.getSkipLines();i++) {
					if (this.spreadSheetCellDAOCache.get(this.currentSheet).size()>0) {
						this.spreadSheetCellDAOCache.get(this.currentSheet).remove(0);
					}
					this.currentRow++;
				}
			}
			// check if we need to skip header
			if (this.hocr.getIgnoreHeaderInAllSheets()) {
				if (this.spreadSheetCellDAOCache.get(this.currentSheet).size()>0) {
					this.spreadSheetCellDAOCache.get(this.currentSheet).remove(0);
				}
				this.currentRow++;
			}
			
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
	  if (this.pullSST!=null) {
		  this.pullSST.close();
	  }
	}

	


	@Override
	public void setCurrentRow(long row) {
		this.currentRow=(int) row;
		
	}

	@Override
	public void setCurrentSheet(long sheet) {
		this.currentSheet=(int) sheet;
	}

	@Override
	public long getCurrentSheet() {
		return this.currentSheet;
	}

	@Override
	public String[] getHeader() {
		return this.header;
	}
	
}
