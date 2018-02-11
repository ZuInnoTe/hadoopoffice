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
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.core.fs.Path;
import org.zuinnote.flink.office.common.FlinkFileReader;
import org.zuinnote.flink.office.common.FlinkKeyStoreManager;
import org.zuinnote.hadoop.office.format.common.HadoopOfficeWriteConfiguration;
import org.zuinnote.hadoop.office.format.common.OfficeWriter;
import org.zuinnote.hadoop.office.format.common.writer.InvalidWriterConfigurationException;
import org.zuinnote.hadoop.office.format.common.writer.OfficeWriterException;

/**
 * @author jornfranke
 *
 */
public abstract class AbstractSpreadSheetFlinkFileOutputFormat<E> extends FileOutputFormat<E>  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7915649174228493076L;
	private static final Log LOG = LogFactory.getLog(AbstractSpreadSheetFlinkFileOutputFormat.class.getName());
	private HadoopOfficeWriteConfiguration howc;
	private OfficeWriter officeWriter;
	private FlinkFileReader currentReader;
	private Map<String,InputStream> linkedWorkbooksMap;
	
	public AbstractSpreadSheetFlinkFileOutputFormat(HadoopOfficeWriteConfiguration howc) {
		this.howc=howc;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		// read keystore and signign key+certificate
		try {
			this.readKeyStore();
		} catch (OfficeWriterException e) {
			LOG.error("Could not read keystore for credentials. Exception: ",e);
		}
		try {
			this.readSigningKeyAndCertificate();
		} catch (OfficeWriterException e) {
			LOG.error("Could not read signing key and certificate for signature. Exception: ",e);
		}
		   this.currentReader= new FlinkFileReader();
		      this.linkedWorkbooksMap=this.currentReader.loadLinkedWorkbooks(this.howc.getLinkedWorkbooksName());
		     // create OfficeWriter 
		       try {
				this.officeWriter=new OfficeWriter(this.howc);
			} catch (InvalidWriterConfigurationException e1) {
				LOG.error("Could not create OfficeWriter. Exception: ",e1);
			}
		       InputStream templateInputStream=null;
		       if ((this.howc.getTemplate()!=null) && (!"".equals(this.howc.getTemplate()))) {
		    	   templateInputStream=this.currentReader.loadTemplate(this.howc.getTemplate());
		       }
		      try {
				this.officeWriter.create(stream,this.linkedWorkbooksMap,this.howc.getLinkedWBCredentialMap(), templateInputStream);
			} catch (OfficeWriterException e) {
				LOG.error("Could not create office file for writing. Exception: ",e);
			}
	}
	 
	@Override
	public void close() throws IOException {
		try {
			this.officeWriter.close();
		}  finally {
			if (this.stream!=null) {
				this.stream.close();
			}
			
		}
	}
	
	/**
	 * 
	 * @return
	 */
	public OfficeWriter getOfficeWriter() {
		return this.officeWriter;
	}
	
	/**
	 * Reads the keystore to obtain credentials
	 * 
	 * @throws IOException 
	 * @throws OfficeWriterException
	 * 
	 */
	private void readKeyStore() throws IOException, OfficeWriterException {
		if ((this.howc.getCryptKeystoreFile()!=null) && (!"".equals(this.howc.getCryptKeystoreFile()))) {
			LOG.info("Using keystore to obtain credentials instead of passwords");
			FlinkKeyStoreManager fksm = new FlinkKeyStoreManager();
			try {
				fksm.openKeyStore(new Path(this.howc.getCryptKeystoreFile()), this.howc.getCryptKeystoreType(), this.howc.getCryptKeystorePassword());
				String password="";
				if ((this.howc.getCryptKeystoreAlias()!=null) && (!"".equals(this.howc.getCryptKeystoreAlias()))) {
					password=fksm.getPassword(this.howc.getCryptKeystoreAlias(), this.howc.getCryptKeystorePassword());
				} else {
					password=fksm.getPassword(this.howc.getFileName(), this.howc.getCryptKeystorePassword());
				}
				this.howc.setPassword(password);
			} catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IllegalArgumentException | UnrecoverableEntryException | InvalidKeySpecException e) {
				LOG.error("Cannopt read keystore. Exception: ",e);
				throw new OfficeWriterException("Cannot read keystore to obtain credentials used to encrypt office documents "+e);
			}
			
		}
	}


	/***
	 * Reads the  (private) key and certificate from keystore to sign
	 * 
	 * @throws OfficeWriterException
	 * @throws IOException
	 */
	private void readSigningKeyAndCertificate() throws OfficeWriterException, IOException {
		if ((this.howc.getSigKeystoreFile()!=null) && (!"".equals(this.howc.getSigKeystoreFile()))) {
			LOG.info("Signing document");
			if ((this.howc.getSigKeystoreAlias()==null) || ("".equals(this.howc.getSigKeystoreAlias()))) {
					LOG.error("Keystore alias for signature keystore not defined. Cannot sign document");
					throw new OfficeWriterException("Keystore alias for signature keystore not defined. Cannot sign document");
			}
			if ((this.howc.getSigKeystoreType()==null) || ("".equals(this.howc.getSigKeystoreType()))) {
				LOG.error("Keystore type for signature keystore not defined. Cannot sign document");
				throw new OfficeWriterException("Keystore type for signature keystore not defined. Cannot sign document");
			}
			LOG.info("Reading keystore");
			FlinkKeyStoreManager fksm = new FlinkKeyStoreManager();
			try {
				fksm.openKeyStore(new Path(this.howc.getSigKeystoreFile()), this.howc.getSigKeystoreType(), this.howc.getSigKeystorePassword());
				this.howc.setSigKey(fksm.getPrivateKey(this.howc.getSigKeystoreAlias(), this.howc.getSigKeystorePassword()));
				this.howc.setSigCertificate((X509Certificate) fksm.getCertificate(this.howc.getSigKeystoreAlias()));
			} catch (NoSuchAlgorithmException | CertificateException | KeyStoreException | IllegalArgumentException | UnrecoverableKeyException  e) {
				LOG.error("Cannopt read signing certificate. Exception: ",e);
				throw new OfficeWriterException("Cannot read keystore to obtain key and certificate for signing "+e);
			}
			
			
		}
	}
}
