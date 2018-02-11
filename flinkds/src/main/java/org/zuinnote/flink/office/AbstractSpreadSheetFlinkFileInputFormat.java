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
package org.zuinnote.flink.office;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.zuinnote.flink.office.common.FlinkFileReader;
import org.zuinnote.flink.office.common.FlinkKeyStoreManager;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeReadConfiguration;
import org.zuinnote.hadoop.office.format.common.OfficeReader;
import org.zuinnote.hadoop.office.format.common.parser.FormatNotUnderstoodException;

/**
 * @author jornfranke
 *
 */
public abstract class AbstractSpreadSheetFlinkFileInputFormat<E> extends FileInputFormat<E>
implements CheckpointableInputFormat<FileInputSplit, Tuple2<Long, Long>> {
	/**
	 * 
	 */
	public static final String MIMETYPE_EXCEL="ms-excel";
	private static final long serialVersionUID = -2738109984301195074L;
	private static final Log LOG = LogFactory.getLog(AbstractSpreadSheetFlinkFileInputFormat.class.getName());
	private HadoopOfficeReadConfiguration hocr;
	private OfficeReader officeReader = null;
	private FlinkFileReader currentFFR;
	private long sheetNum;
	private long rowNum;
	private boolean reachedEnd;

	
	private AbstractSpreadSheetFlinkFileInputFormat() {
		// not supported
	}



	/**
	 * Initialize the ExcelFlinkInputFormat with a given configuration for the
	 * HadoopOffice library
	 * 
	 * @param hocr
	 *            HadoopOffice read configuration
	 */

	public AbstractSpreadSheetFlinkFileInputFormat(HadoopOfficeReadConfiguration hocr) {
		this.hocr = hocr;
		this.unsplittable=true;
	}


	

	
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		this.sheetNum = 0;
		this.rowNum = 0;
		this.reachedEnd=false;
		this.hocr.setFileName(split.getPath().getName());
		try {
			this.readKeyStore();
		} catch (FormatNotUnderstoodException e) {
			LOG.error("Could not read keystore. Exception: ",e);
		}
		 try {
			this.readTrustStore();
		} catch (FormatNotUnderstoodException e) {
			LOG.error("Could not read truststore. Exception: ",e);
		}
		this.officeReader = new OfficeReader(this.stream, this.hocr); 
		 // initialize reader
	    try {
			this.officeReader.parse();
		} catch (FormatNotUnderstoodException e1) {
			LOG.error("Could not read file "+this.hocr.getFileName(),e1);
		}
	    // read linked workbooks
	    if (this.hocr.getReadLinkedWorkbooks()) {
		// get current path
		Path currentPath = split.getPath();
		Path parentPath = currentPath.getParent();
		// read linked workbook filenames
		List<String> linkedWorkbookList=this.officeReader.getCurrentParser().getLinkedWorkbooks();
		LOG.debug(linkedWorkbookList.size());
		this.currentFFR = new FlinkFileReader();
		for (String listItem: linkedWorkbookList) {
			LOG.info("Adding linked workbook \""+listItem+"\"");
			String sanitizedListItem = new Path(listItem).getName();
			// read file from hadoop file
			Path currentFile=new Path(parentPath,sanitizedListItem);
			InputStream currentIn=this.currentFFR.openFile(currentFile);
			try {
				this.officeReader.getCurrentParser().addLinkedWorkbook(listItem,currentIn,this.hocr.getLinkedWBCredentialMap().get(sanitizedListItem));
			} catch (FormatNotUnderstoodException e) {
				LOG.error("Could not read linked workbook: "+listItem);
				LOG.error("Exception: ",e);
			}
		}
	    }
		
	}


	@Override
	public Tuple2<Long, Long> getCurrentState() throws IOException {
		return new Tuple2<>(this.sheetNum, this.rowNum);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return this.reachedEnd;
	}
	
	/**
	 * Get office reader
	 * 
	 * @return
	 */
		public  OfficeReader getOfficeReader() {
			return this.officeReader;
		}
	
	public Object[] readNextRow() {
		Object[] nextRow = this.officeReader.getNext();
		if (nextRow==null) {
			this.reachedEnd=true;
		}
		this.sheetNum=this.officeReader.getCurrentParser().getCurrentSheet();
		this.rowNum=this.officeReader.getCurrentParser().getCurrentRow();
		return nextRow;
	}
	

	/***
	 * 
	 * Reopens the stream. Note that it still needs to parse the full Excel file,
	 * but it resumes for nextRecord from the current sheet and row number
	 * 
	 * @param split
	 * @param state
	 * @throws IOException
	 */

	@Override
	public void reopen(FileInputSplit split, Tuple2<Long, Long> state) throws IOException {
		this.open(split);
		this.sheetNum = state.f0;
		this.rowNum = state.f1;
		this.officeReader.getCurrentParser().setCurrentSheet(this.sheetNum);
		this.officeReader.getCurrentParser().setCurrentRow(this.rowNum);

	}

	
	/**
	 * Reads the keystore to obtain credentials
	 * 
	 * @throws IOException
	 * @throws FormatNotUnderstoodException
	 * 
	 */
	private void readKeyStore() throws IOException, FormatNotUnderstoodException {
		if ((this.hocr.getCryptKeystoreFile() != null) && (!"".equals(this.hocr.getCryptKeystoreFile()))) {
			LOG.info("Using keystore to obtain credentials instead of passwords");
			FlinkKeyStoreManager fksm = new FlinkKeyStoreManager();
			try {
				fksm.openKeyStore(new Path(this.hocr.getCryptKeystoreFile()), this.hocr.getCryptKeystoreType(),
						this.hocr.getCryptKeystorePassword());
				String pw = "";
				if ((this.hocr.getCryptKeystoreAlias() != null) && (!"".equals(this.hocr.getCryptKeystoreAlias()))) {
					pw = fksm.getPassword(this.hocr.getCryptKeystoreAlias(), this.hocr.getCryptKeystorePassword());
				} else {
					pw = fksm.getPassword(this.hocr.getFileName(), this.hocr.getCryptKeystorePassword());
				}
				this.hocr.setPassword(pw);
			} catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IllegalArgumentException
					| UnrecoverableEntryException | InvalidKeySpecException e) {
				LOG.error("Exception: ",e);
				throw new FormatNotUnderstoodException(
						"Cannot read keystore to obtain credentials to access encrypted documents " + e);
			}

		}
	}

	/***
	 * Read truststore for establishing certificate chain for signature validation
	 * 
	 * @param conf
	 * @throws IOException
	 * @throws FormatNotUnderstoodException
	 */
	private void readTrustStore() throws IOException, FormatNotUnderstoodException {
		if (((this.hocr.getSigTruststoreFile() != null) && (!"".equals(this.hocr.getSigTruststoreFile())))) {
			LOG.info("Reading truststore to validate certificate chain for signatures");
			FlinkKeyStoreManager fksm = new FlinkKeyStoreManager();
			try {
				fksm.openKeyStore(new Path(this.hocr.getSigTruststoreFile()), this.hocr.getSigTruststoreType(),
						this.hocr.getSigTruststorePassword());
				this.hocr.setX509CertificateChain(fksm.getAllX509Certificates());
			} catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IllegalArgumentException e) {
				LOG.error("Exception: ",e);
				throw new FormatNotUnderstoodException(
						"Cannot read truststore to establish certificate chain for signature validation " + e);
			}

		}
	}

}
