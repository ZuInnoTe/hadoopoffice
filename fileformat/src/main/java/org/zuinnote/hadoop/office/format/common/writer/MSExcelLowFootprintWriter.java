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
package org.zuinnote.hadoop.office.format.common.writer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
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
import org.apache.poi.poifs.filesystem.NPOIFSFileSystem;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.ss.util.WorkbookUtil;
import org.apache.poi.util.TempFile;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.dao.SpreadSheetCellDAO;

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
		this.mappedDrawings=new HashMap<>();	
	}

	@Override
	public void write(Object newDAO) throws OfficeWriterException {
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

	@Override
	public void close() throws IOException {
		// store unencrypted
		if (this.howc.getPassword()==null) {
			this.currentWorkbook.write(this.osStream);
		} else {
			// encrypt if needed
	
			POIFSFileSystem fs = new POIFSFileSystem();
			EncryptionInfo info = new EncryptionInfo(this.encryptionModeCipher, this.encryptAlgorithmCipher, this.hashAlgorithmCipher, -1, -1, this.chainModeCipher);
			Encryptor enc = info.getEncryptor();
			
			enc.confirmPassword(this.howc.getPassword());
			try {
				this.currentWorkbook.write(enc.getDataStream(fs));
			} catch (GeneralSecurityException e) {
				
				LOG.error(e);
				throw new IOException(e);
			}
			fs.writeFilesystem(this.osStream);
			if (this.osStream!=null) {
				this.osStream.close();
			}
		}
		
		this.currentWorkbook.dispose(); // this is needed to remove tempfiles
		
	}
	/**
	 * 
	 * This class is inspired by https://bz.apache.org/bugzilla/show_bug.cgi?id=60321 to create - in case of encrypted excel - also use encrypted and compressed temporary files
	 *
	 * The underlying ideas of the examples have been enhanced, e.g. temporary data is encrypted with the same algorithm as the output data. The examples uses only AES128, which may not be sufficient for all cases of confidential data
	 * 
	 */
	public class SecureSXSSFWorkbook extends SXSSFWorkbook {
		private CipherAlgorithm ca;
		private ChainingMode cm;
		
		public SecureSXSSFWorkbook(int cacherows, CipherAlgorithm ca, ChainingMode cm) {
			super(cacherows);
			setCompressTempFiles(true);
			this.ca=ca;
			this.cm=cm;
		}
		
		@Override
		public void write(OutputStream stream) throws IOException {
			
			this.flushSheets();
			EncryptedTempData tempData = new EncryptedTempData(this.ca,this.cm);
			ZipEntrySource source = null;
			
				OutputStream os = tempData.getOutputStream();
				try {
					getXSSFWorkbook().write(os);
				} finally {
					IOUtils.closeQuietly(os);
				}
				// provide ZipEntrySoruce to poi to decrypt on the fly (if necessary)
				source = new EncryptedZipEntrySource(this.ca,this.cm);
				((EncryptedZipEntrySource)source).setInputStream(tempData.getInputStream());
				injectData(source,stream);
		
				tempData.dispose();
		
			
		}
	}
	
	public class EncryptedTempData {
		private CipherAlgorithm ca;
		private ChainingMode cm;
		private Cipher ciEncrypt;
		private Cipher ciDecrypt;
		private File tempFile;
		
		public EncryptedTempData(CipherAlgorithm ca, ChainingMode cm) throws IOException {
			// generate random key for temnporary files
			if (ca!=null) {
						SecureRandom sr = new SecureRandom();
						byte[] iv = new byte[ca.blockSize];
						byte[] key = new byte[ca.defaultKeySize/8];
						sr.nextBytes(iv);
						sr.nextBytes(key);
						SecretKeySpec skeySpec = new SecretKeySpec(key,ca.jceId);
						this.ca = ca;
						this.cm = cm;
						this.ciEncrypt=CryptoFunctions.getCipher(skeySpec, ca, cm, iv, Cipher.ENCRYPT_MODE,"PKCS5Padding");
						this.ciDecrypt=CryptoFunctions.getCipher(skeySpec, ca, cm, iv, Cipher.DECRYPT_MODE,"PKCS5Padding");
			}
					this.tempFile=TempFile.createTempFile("hadooffice-poi-temp-data",".tmp");
		}
		
		public OutputStream getOutputStream() throws FileNotFoundException {
			if (this.ciEncrypt!=null) { // encrypted tempdata
				LOG.debug("Returning encrypted OutputStream for "+this.tempFile.getAbsolutePath());
				
				return new CipherOutputStream(new FileOutputStream(this.tempFile),this.ciEncrypt);
			}
			return new FileOutputStream(this.tempFile);
		}
		
		public InputStream getInputStream() throws FileNotFoundException {
			if (this.ciDecrypt!=null) { // decrypt tempdata
				LOG.debug("Returning decrypted InputStream for "+this.tempFile.getAbsolutePath());
				LOG.debug("Size of temp file: "+this.tempFile.length());
				return new CipherInputStream(new FileInputStream(this.tempFile),this.ciDecrypt);
			}
			return new FileInputStream(this.tempFile);
		}
		
		public void dispose() {
			this.tempFile.delete();
		}
	}
	
	public class EncryptedZipEntrySource implements ZipEntrySource {

		private ZipFile zipFile;
		private CipherAlgorithm ca;
		private ChainingMode cm;
		private Cipher ciEncoder;
		private Cipher ciDecoder;
		private File tmpFile;
		private boolean closed;

		public EncryptedZipEntrySource( CipherAlgorithm ca, ChainingMode cm) throws ZipException, IOException {
			// generate random key for temporary files
			if (ca!=null) { // encrypted files
				SecureRandom sr = new SecureRandom();
				byte[] iv = new byte[ca.blockSize];
				byte[] key = new byte[ca.defaultKeySize/8];
				sr.nextBytes(iv);
				sr.nextBytes(key);
				SecretKeySpec skeySpec = new SecretKeySpec(key,ca.jceId);
				this.ca = ca;
				this.cm = cm;
				this.ciEncoder=CryptoFunctions.getCipher(skeySpec, ca, cm, iv, Cipher.ENCRYPT_MODE,"PKCS5Padding");
				this.ciDecoder=CryptoFunctions.getCipher(skeySpec, ca, cm, iv, Cipher.DECRYPT_MODE,"PKCS5Padding");
			}
			
			
			this.closed=false;
		
		}
		
		@Override
		public Enumeration<? extends ZipEntry> getEntries() {
			return zipFile.entries();
		}
		
		public void setInputStream(InputStream is) throws IOException {
			this.tmpFile = TempFile.createTempFile("hadoopoffice-protected", ".zip");
			
			ZipInputStream zis = new ZipInputStream(is);
			FileOutputStream fos = new FileOutputStream(tmpFile);
			ZipOutputStream zos = new ZipOutputStream(fos);
			ZipEntry ze;
			while ((ze = zis.getNextEntry()) !=null) {
				// rewrite zip entries to match the size of the encrypted data (with padding)
				ZipEntry zeNew = new ZipEntry(ze.getName());
				zeNew.setComment(ze.getComment());
				zeNew.setExtra(ze.getExtra());
				zeNew.setTime(ze.getTime());
				zos.putNextEntry(zeNew);
				FilterOutputStream fos2 = new FilterOutputStream(zos) {
					// do not close underlyzing ZipOutputStream
					@Override
					public void close() {}
				};
				OutputStream nos;
				if (this.ciEncoder!=null) { // encrypt if needed
					nos= new CipherOutputStream(fos2,this.ciEncoder);
				} else { // do not encrypt
					nos = fos2;
				}
				IOUtils.copy(zis, nos);
				nos.close();
				if (fos2!=null) {
					fos2.close();
				}
				zos.closeEntry();
				zis.closeEntry();
			}
			zos.close();
			fos.close();
			zis.close();
			IOUtils.closeQuietly(is);
			this.zipFile=new ZipFile(this.tmpFile);
			
		}

		@Override
		public InputStream getInputStream(ZipEntry entry) throws IOException {
			InputStream is = this.zipFile.getInputStream(entry);
			if (this.ciDecoder!=null) {
				return new CipherInputStream(is,this.ciDecoder);
			}
			return is;
		}

		@Override
		public void close() throws IOException {
			if (!this.closed) {
				this.zipFile.close();
				if (this.tmpFile!=null) {
					
					this.tmpFile.delete();
				}
			}
			this.closed=true;
		}

		@Override
		public boolean isClosed() {
			
			return this.closed;
		}
		
	}
	

}
