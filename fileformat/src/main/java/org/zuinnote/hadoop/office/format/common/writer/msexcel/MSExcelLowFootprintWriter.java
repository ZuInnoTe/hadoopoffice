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
package org.zuinnote.hadoop.office.format.common.writer.msexcel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.crypto.MarshalException;
import javax.xml.crypto.dsig.XMLSignatureException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.util.ZipEntrySource;
import org.apache.poi.poifs.crypt.ChainingMode;
import org.apache.poi.poifs.crypt.CipherAlgorithm;
import org.apache.poi.poifs.crypt.CryptoFunctions;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.EncryptionMode;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.crypt.HashAlgorithm;
import org.apache.poi.poifs.crypt.dsig.SignatureConfig;
import org.apache.poi.poifs.crypt.dsig.SignatureInfo;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.SpreadsheetVersion;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.ss.util.WorkbookUtil;
import org.apache.poi.util.TempFile;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;
import org.zuinnote.hadoop.office.format.common.util.msexcel.MSExcelOOXMLSignUtil;
import org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException;
import org.zuinnote.hadoop.office.format.common.writer.OfficeSpreadSheetWriterInterface;
import org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException;
import org.zuinnote.hadoop.office.format.common.writer.msexcel.internal.EncryptedTempData;
import org.zuinnote.hadoop.office.format.common.writer.msexcel.internal.SecureSXSSFWorkbook;

/**
 * @author Jörn Franke (zuinnote@gmail.com)
 *
 */
public class MSExcelLowFootprintWriter implements OfficeSpreadSheetWriterInterface {
	private static final Log LOG = LogFactory.getLog(MSExcelLowFootprintWriter.class.getName());
	private SecureSXSSFWorkbook currentWorkbook;
	private String format;
	private HadoopOfficeWriteConfiguration howc;
	private CipherAlgorithm encryptAlgorithmCipher;
	private HashAlgorithm hashAlgorithmCipher;
	private EncryptionMode encryptionModeCipher;
	private ChainingMode chainModeCipher;
	private OutputStream osStream;
	
	private Map<String,Drawing> mappedDrawings;

	private MSExcelOOXMLSignUtil signUtil;
	private FormulaEvaluator currentFormulaEvaluator;
	
public MSExcelLowFootprintWriter(String excelFormat, HadoopOfficeWriteConfiguration howc) throws InvalidWriterConfigurationException {
	boolean formatFound=MSExcelWriter.isSupportedFormat(excelFormat);
	if (!(formatFound)) {
		 LOG.error("Unknown Excel format: "+this.format);
		 throw new InvalidWriterConfigurationException("Unknown Excel format: "+this.format);
	}
	this.format=excelFormat;
	this.howc=howc;
	if (this.howc.getPassword()!=null) {
		this.encryptAlgorithmCipher=MSExcelWriter.getAlgorithmCipher(this.howc.getEncryptAlgorithm());
		this.hashAlgorithmCipher=MSExcelWriter.getHashAlgorithm(this.howc.getHashAlgorithm());
		this.encryptionModeCipher=MSExcelWriter.getEncryptionModeCipher(this.howc.getEncryptMode());
		this.chainModeCipher=MSExcelWriter.getChainMode(this.howc.getChainMode());
	}
}


	@Override
	public void create(OutputStream osStream, Map<String, InputStream> linkedWorkbooks,
			Map<String, String> linkedWorkbooksPasswords, InputStream template) throws OfficeWriterException {
		if ((linkedWorkbooks!=null) && (linkedWorkbooks.size()>0)) {
			throw new OfficeWriterException("Linked Workbooks are not supported in low footprint write mode");
		}
		if (template!=null) {
			throw new OfficeWriterException("Templates are not supported in low footprint write mode");
		}
		this.osStream=osStream;
		this.currentWorkbook=new SecureSXSSFWorkbook(this.howc.getLowFootprintCacheRows(),this.encryptAlgorithmCipher,this.chainModeCipher);
		this.currentFormulaEvaluator=this.currentWorkbook.getCreationHelper().createFormulaEvaluator();
		this.mappedDrawings=new HashMap<>();	
		if (this.howc.getSigKey()!=null) { // create temp file
			LOG.info("Creating tempfile for signing");
			// 
			try {
				this.signUtil= new MSExcelOOXMLSignUtil(this.osStream);
			} catch (IOException e) {
				LOG.error("Cannot create sign utilities "+e);
				throw new OfficeWriterException(e.toString());
			}
		}
	}

	@Override
	public void write(Object newDAO) throws OfficeWriterException {
		if (newDAO!=null) {
			SpreadSheetCellDAO sscd = MSExcelWriter.checkSpreadSheetCellDAO(newDAO);
			String safeSheetName=WorkbookUtil.createSafeSheetName(sscd.getSheetName());
			SXSSFSheet currentSheet=this.currentWorkbook.getSheet(safeSheetName);
			if (currentSheet==null) {// create sheet if it does not exist yet
				currentSheet=this.currentWorkbook.createSheet(safeSheetName);
				if (!(safeSheetName.equals(sscd.getSheetName()))) {
					LOG.warn("Sheetname modified from \""+sscd.getSheetName()+"\" to \""+safeSheetName+"\" to correspond to Excel conventions.");
				}
				// create drawing anchor (needed for comments...)
				this.mappedDrawings.put(safeSheetName,currentSheet.createDrawingPatriarch());
			}
			// check if cell exist
			CellAddress currentCA = new CellAddress(sscd.getAddress());
			// spill over check 
						// if we need to do spillover
						if (this.howc.getSimpleSpillOver()) {
							long maxRows=-1;
							switch (this.format) {
							case MSExcelWriter.FORMAT_OOXML:
								maxRows=SpreadsheetVersion.EXCEL2007.getLastRowIndex();
								break;
							case MSExcelWriter.FORMAT_OLD:
								maxRows=SpreadsheetVersion.EXCEL97.getLastRowIndex();
								break;
							default: 
								LOG.warn("Could not detect maximum number of rows / sheet");
								break;
							}
							
							if (maxRows>-1) {
								if (currentCA.getRow()>maxRows) {
									LOG.info("Maximum number of rows reached. Spilling over to additional sheet");
									// get the sheet number
									int sheetNum=(int) (currentCA.getRow()/maxRows) + 1;
									// get the row in the destination sheet
									int newRowNum=(int) (currentCA.getRow()%(maxRows+1));
									// create sheet if not exist
									String spillOverSheetName=WorkbookUtil.createSafeSheetName(sscd.getSheetName()+String.valueOf(sheetNum));
									currentSheet=this.currentWorkbook.getSheet(spillOverSheetName);
									if (currentSheet==null) {// create sheet if it does not exist yet
										currentSheet=this.currentWorkbook.createSheet(spillOverSheetName);
										// create drawing anchor (needed for comments...)
										this.mappedDrawings.put(spillOverSheetName,currentSheet.createDrawingPatriarch());
									}
									// fix address
									currentCA = new CellAddress(newRowNum,currentCA.getColumn());
									
								}
							}
						}
			SXSSFRow currentRow = currentSheet.getRow(currentCA.getRow());
			if (currentRow==null) { // row does not exist? => create it
				currentRow=currentSheet.createRow(currentCA.getRow());
			}
			SXSSFCell currentCell = currentRow.getCell(currentCA.getColumn());
			if ((currentCell!=null)) { // cell already exists and no template loaded ? => throw exception
				throw new OfficeWriterException("Invalid cell specification: cell already exists at "+currentCA);
			}
			// create cell if no template is loaded or cell not available in template
				currentCell=currentRow.createCell(currentCA.getColumn());		
			// set the values accordingly
			if (!("".equals(sscd.getFormula()))) { // if formula exists then use formula

				currentCell.setCellFormula(sscd.getFormula());
				this.currentFormulaEvaluator.evaluateFormulaCell(currentCell);
				
			} else {	
			// else use formattedValue
				currentCell.setCellValue(sscd.getFormattedValue());
			}
			// set comment
			if ((sscd.getComment()!=null) && (!("".equals(sscd.getComment())))) {
				/** the following operations are necessary to create comments **/
				/** Define size of the comment window **/
				    ClientAnchor anchor = this.currentWorkbook.getCreationHelper().createClientAnchor();
		    		    anchor.setCol1(currentCell.getColumnIndex());
		    		    anchor.setCol2(currentCell.getColumnIndex()+this.howc.getCommentWidth());
		    		    anchor.setRow1(currentRow.getRowNum());
		    		    anchor.setRow2(currentRow.getRowNum()+this.howc.getCommentHeight());
				/** create comment **/
				   Comment currentComment = mappedDrawings.get(safeSheetName).createCellComment(anchor);
		    		    currentComment.setString(this.currentWorkbook.getCreationHelper().createRichTextString(sscd.getComment()));
		    		    currentComment.setAuthor(this.howc.getCommentAuthor());
				    currentCell.setCellComment(currentComment);
	
			}
		}
		}

	@Override
	public void close() throws IOException {
		
		// store unencrypted
		if (this.howc.getPassword()==null) {
			if (this.signUtil!=null) {
				this.currentWorkbook.write(this.signUtil.getTempOutputStream());
			} else {
	
				this.currentWorkbook.write(this.osStream);
				if (this.osStream!=null) {
					this.osStream.close();
				}
			}
			
		
		} else {
			// encrypt if needed
	
			POIFSFileSystem fs = new POIFSFileSystem();
			EncryptionInfo info = new EncryptionInfo(this.encryptionModeCipher, this.encryptAlgorithmCipher, this.hashAlgorithmCipher, -1, -1, this.chainModeCipher);
			Encryptor enc = info.getEncryptor();
			
			enc.confirmPassword(this.howc.getPassword());
			try {
				OutputStream os = enc.getDataStream(fs);
				if (os!=null) {
					this.currentWorkbook.write(os);
				}
				if (os!=null) {
					os.close();
				}
			} catch (GeneralSecurityException e) {
				
				LOG.error(e);
				throw new IOException(e);
			}
			if (this.signUtil!=null) {
				fs.writeFilesystem(this.signUtil.getTempOutputStream());
			} else {
				fs.writeFilesystem(this.osStream);
				if (this.osStream!=null) {
					this.osStream.close();
				}
			}
			

			if (fs!=null) {
				fs.close();
			}
		}
		
		this.currentWorkbook.dispose(); // this is needed to remove tempfiles
		
			try {
				// do we need to sign => sign
				if (this.signUtil!=null) {
					// sign
					LOG.info("Signing document \""+this.howc.getFileName()+"\"");
					if (this.howc.getSigCertificate()==null) {
						LOG.error("Cannot sign document \""+this.howc.getFileName()+"\". No certificate for key provided");
					} else {
					try {
						ArrayList<X509Certificate> certList = new ArrayList<>();
						certList.add(this.howc.getSigCertificate());
						this.signUtil.sign(this.howc.getSigKey(), certList, this.howc.getPassword(), MSExcelWriter.getHashAlgorithm(this.howc.getSigHash()));
						} catch (XMLSignatureException|MarshalException|IOException|FormatNotUnderstoodException e) {
						LOG.error("Cannot sign document \""+this.howc.getFileName()+"\" "+e);
					}
						
					}
					
				}
				} finally {
					if (this.signUtil!=null) {
						this.signUtil.close();
					}
				}
		
	}
	


	
	
	
	

}
