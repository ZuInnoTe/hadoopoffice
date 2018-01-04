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
package org.zuinnote.hadoop.office.format.common;

import java.security.Key;
import java.security.cert.X509Certificate;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * read the configuration for writing office files from a Hadoop configuration
 * 
 * @author Jörn Franke (zuinnote@gmail.com)
 *
 */
public class HadoopOfficeWriteConfiguration {
	public static final String CONF_MIMETYPE="hadoopoffice.write.mimeType";
	public static final String CONF_LOCALE="hadoopoffice.write.locale.bcp47";
	public static final String CONF_LINKEDWB="hadoopoffice.write.linkedworkbooks";
	public static final String CONF_IGNOREMISSINGWB="hadoopoffice.write.ignoremissinglinkedworkbooks";
	public static final String CONF_COMMENTAUTHOR="hadoopoffice.write.comment.author";
	public static final String CONF_COMMENTWIDTH="hadoopoffice.write.comment.width";
	public static final String CONF_COMMENTHEIGHT="hadoopoffice.write.comment.height";
	public static final String CONF_SECURITYCRED="hadoopoffice.write.security.crypt.password";
	public static final String CONF_SECURITYALGORITHM="hadoopoffice.write.security.crypt.encrypt.algorithm";
	public static final String CONF_SECURITYMODE="hadoopoffice.write.security.crypt.encrypt.mode";
	public static final String CONF_CHAINMODE="hadoopoffice.write.security.crypt.chain.mode";
	public static final String CONF_HASHALGORITHM="hadoopoffice.write.security.crypt.hash.algorithm";
	public static final String CONF_DECRYPTLINKEDWBBASE="hadoopoffice.write.security.crypt.linkedworkbooks.";
	public static final String CONF_METADATA="hadoopoffice.write.metadata."; // base: all these properties (e.g. hadoopoffice.write.metadata.author) will be handed over to the corresponding writer
	public static final String CONF_TEMPLATE="hadoopoffice.write.template.file";
	public static final String CONF_TEMPLATEPW="hadoopoffice.write.template.password";

	public static final String CONF_LOWFOOTPRINT="hadoopoffice.write.lowFootprint";
	public static final String CONF_LOWFOOTPRINT_CACHEROWS="hadoopoffice.write.lowFootprint.cacherows";
	public static final String CONF_CRYKEYSTOREFILE = "hadoopoffice.write.security.crypt.credential.keystore.file";
	public static final String CONF_CRYKEYSTORETYPE = "hadoopoffice.write.security.crypt.credential.keystore.type";
	public static final String CONF_CRYKEYSTOREPW = "hadoopoffice.write.security.crypt.credential.keystore.password";
	public static final String CONF_CRYKEYSTOREALIAS = "hadoopoffice.write.security.crypt.credential.keystore.alias";
	
	public static final String CONF_SIGKEYSTOREFILE = "hadoopoffice.write.security.sign.keystore.file";
	public static final String CONF_SIGKEYSTORETYPE = "hadoopoffice.write.security.sign.keystore.type";
	public static final String CONF_SIGKEYSTOREPW = "hadoopoffice.write.security.sign.keystore.password";
	public static final String CONF_SIGKEYSTOREALIAS = "hadoopoffice.write.security.sign.keystore.alias";
	public static final String CONF_SIGHASH = "hadoopoffice.write.security.sign.hash.algorithm";

	public static final String DEFAULT_MIMETYPE="";
	public static final String DEFAULT_LOCALE="";
	public static final String DEFAULT_LINKEDWB="";
	public static final boolean DEFAULT_IGNOREMISSINGLINKEDWB=false;
	public static final String DEFAULT_AUTHOR="hadoopoffice";
	public static final int DEFAULT_COMMENTWIDTH=1;
	public static final int DEFAULT_COMMENTHEIGHT=3;
	public static final String DEFAULT_PASSWORD=null;
	public static final String DEFAULT_ALGORITHM="aes256";
	public static final String DEFAULT_TEMPLATE ="";
	public static final String DEFAULT_TEMPLATEPW ="";

	public static final boolean DEFAULT_LOWFOOTPRINT=false;
	public static final int DEFAULT_LOWFOOTPRINT_CACHEROWS=1000;
	public static final String DEFAULT_CRYKEYSTOREFILE="";
	public static final String DEFAULT_CRYKEYSTORETYPE = "JCEKS";
	public static final String DEFAULT_CRYKEYSTOREPW="";
	public static final String DEFAULT_CRYKEYSTOREALIAS="";
	
	public static final String DEFAULT_SIGKEYSTOREFILE = "";
	public static final String DEFAULT_SIGKEYSTORETYPE = "PKCS12";
	public static final String DEFAULT_SIGKEYSTOREPW = "";
	public static final String DEFAULT_SIGKEYSTOREALIAS = "";
	public static final String DEFAULT_SIGHASH = "sha512";
	
	private String[] linkedWorkbooksName;
	private String fileName;
	private String mimeType;
	private Locale locale;
	private boolean ignoreMissingLinkedWorkbooks;
	private String commentAuthor;
	private int commentWidth;
	private int commentHeight;
	private String password;
	private String encryptAlgorithm;
	private String hashAlgorithm;
	private String encryptMode;
	private String chainMode;
	private String template;
	private String templatePassword;
	private Map<String,String> linkedWBCredentialMap;
	private Map<String,String> metadata;
	private boolean lowFootprint;
	private int lowFootprintCacheRows;
	private String cryptKeystoreFile;
	private String cryptKeystoreType;
	private String cryptKeystorePassword;
	private String cryptKeystoreAlias;
	
	private String sigKeystoreFile;
	private String sigKeystoreType;
	private String sigKeystorePassword;
	private String sigKeystoreAlias;
	private String sigHash;
	private X509Certificate sigCertificate;
	private Key sigKey;
/*
 * 	Read the configuration for writing office files from a Hadoop configuration
 * 
 * @conf Hadoop configuration
 *  hadoopoffice.write.mimeType: Mimetype of the document
* hadoopoffice.write.locale: Locale of the document (e.g. needed for interpreting spreadsheets) in the BCP47 format (cf. https://tools.ietf.org/html/bcp47). If not specified then default system locale will be used.
* hadoopoffice.write.linkedworkbooks a []: separated list of existing linked workbooks. Example: [hdfs:///home/user/excel/linkedworkbook1.xls]:[hdfs:///home/user/excel/linkedworkbook2.xls]. Note: these workbooks are loaded during writing the current workbook. This means you may need a lot of memory on the node writing the file. Furthermore, you can only specify files and NOT directories.
* hadoopoffice.write.ignoremissinglinkedworkbooks: if you have specified linkedworkbooks then they are not read during writing. This implies also that the written document does NOT have a cached value. Value is ignored if you did not specify linkedworkbooks. Default: false. 
* hadoopoffice.write.security.crypt.password: use password to encrypt the document. Note: There is no security check of strongness of password. This is up to the application developer.
* hadoopoffice.write.security.crypt.encrypt.algorithm: use the following algorithm to encrypt. Note that some writers do not support all algorithms and an exception will be thrown if the algorithm is not supported. See corresponding writer documentation for supported algorithms.
* hadoopoffice.write.security.crypt.hash.algorithm: use the following algorithm to hash. Note that some writers do not support all algorithms and an exception will be thrown if the algorithm is not supported. See corresponding writer documentation for supported algorithms.
* hadoopoffice.write.security.crypt.encrypt.mode: use the following mode to encrypt. Note that some writers do not support all modes and an exception will be thrown if the mode is not supported. See corresponding writer documentation for supported algorithms.
* hadoopoffice.write.security.crypt.chain.mode: use the following mode to chain. Note that some writers do not support all modes and an exception will be thrown if the mode is not supported. See corresponding writer documentation for supported algorithms.
* hadoopoffice.write.security.crypt.linkedworkbooks.*: if set then hadoopoffice will try to decrypt all the linked workbooks where a password has been specified. If no password is specified then it is assumed that the linked workbook is not encrypted. Example: Property key for file "linkedworkbook1.xlsx" is  "hadoopoffice.read.security.crypt.linkedworkbooks.linkedworkbook1.xslx". Value is the password. You must not include path or protocol information in the filename 
* hadoopoffice.write.metadata.*: Write metadata properties of the document. All properties belonging to the base (e.g. hadoopoffice.write.metadata.author for author) will be handed over to the corresponding writer. See writer documentation which properties are supported
* hadoopoffice.write.template.file: Use a template as input to modify selected cells of it
* hadoopoffice.write.lowFootprint: if true then a file is written in low footprint mode to save cpu/memory resources, false if it should be written in normal mode. Option is ignored for old Excel files (.xls). Note that if it is set to true then certain options are not available, such as formula evaluation. Default false. 
* hadoopoffice.write.security.crypt.credential.keystore.file: keystore file that is used to store credentials, such as passwords, for securing office documents. Note that the alias in the keystore needs to correspond to the filename (without the path)
* hadoopoffice.write.security.crypt.credential.keystore.type: keystore type. Default: JCEKS
* hadoopoffice.write.security.crypt.credential.keystore.password: keystore password: password of the keystore
* hadoopoffice.write.security.crypt.credential.keystore.alias: alias for the password if different from filename
* hadoopoffice.write.security.sign.keystore.file: keystore file that contains the private key used for signing a document
* hadoopoffice.write.security.sign.keystore.type: keystore type of the private key. Default PKCS12
* hadoopoffice.write.security.sign.keystore.password: keystore password for the private key.
* hadoopoffice.write.security.sign.keystore.alias: alias of private key in keystore
* hadoopoffice.write.security.sign.hash.algorithm: use the following algorithm to hash. Note that some writers do not support all algorithms and an exception will be thrown if the algorithm is not supported. See corresponding writer documentation for supported algorithms.

*
*
* @param fileName filename to write
 * 
 */
public HadoopOfficeWriteConfiguration(Configuration conf, String fileName) {
    this.setMimeType(conf.get(HadoopOfficeWriteConfiguration.CONF_MIMETYPE,HadoopOfficeWriteConfiguration.DEFAULT_MIMETYPE));
    String localeStrBCP47=conf.get(HadoopOfficeWriteConfiguration.CONF_LOCALE, HadoopOfficeWriteConfiguration.DEFAULT_LOCALE);
    if (!("".equals(localeStrBCP47))) { // create locale
	this.setLocale(new Locale.Builder().setLanguageTag(localeStrBCP47).build());
     }
     this.setFileName(fileName);
     this.setCommentAuthor(conf.get(HadoopOfficeWriteConfiguration.CONF_COMMENTAUTHOR,HadoopOfficeWriteConfiguration.DEFAULT_AUTHOR));
     this.setCommentWidth(conf.getInt(HadoopOfficeWriteConfiguration.CONF_COMMENTWIDTH,HadoopOfficeWriteConfiguration.DEFAULT_COMMENTWIDTH));
     this.setCommentHeight(conf.getInt(HadoopOfficeWriteConfiguration.CONF_COMMENTHEIGHT,HadoopOfficeWriteConfiguration.DEFAULT_COMMENTHEIGHT));
     String linkedWorkbooksStr=conf.get(HadoopOfficeWriteConfiguration.CONF_LINKEDWB,HadoopOfficeWriteConfiguration.DEFAULT_LINKEDWB);
     this.setLinkedWorkbooksName(HadoopUtil.parseLinkedWorkbooks(linkedWorkbooksStr));
     this.setIgnoreMissingLinkedWorkbooks(conf.getBoolean(HadoopOfficeWriteConfiguration.CONF_IGNOREMISSINGWB,HadoopOfficeWriteConfiguration.DEFAULT_IGNOREMISSINGLINKEDWB));
     this.setEncryptAlgorithm(conf.get(HadoopOfficeWriteConfiguration.CONF_SECURITYALGORITHM));
     this.setPassword(conf.get(HadoopOfficeWriteConfiguration.CONF_SECURITYCRED));
     this.setHashAlgorithm(conf.get(HadoopOfficeWriteConfiguration.CONF_HASHALGORITHM));
     this.setEncryptMode(conf.get(HadoopOfficeWriteConfiguration.CONF_SECURITYMODE));
     this.setChainMode(conf.get(HadoopOfficeWriteConfiguration.CONF_CHAINMODE));
     this.setMetadata(HadoopUtil.parsePropertiesFromBase(conf,HadoopOfficeWriteConfiguration.CONF_METADATA));
     this.setLinkedWBCredentialMap(HadoopUtil.parsePropertiesFromBase(conf,HadoopOfficeWriteConfiguration.CONF_DECRYPTLINKEDWBBASE));
     this.setFileName(fileName);
     this.setTemplate(conf.get(HadoopOfficeWriteConfiguration.CONF_TEMPLATE,HadoopOfficeWriteConfiguration.DEFAULT_TEMPLATE));

     this.setTemplatePassword(conf.get(HadoopOfficeWriteConfiguration.CONF_TEMPLATEPW,HadoopOfficeWriteConfiguration.DEFAULT_TEMPLATEPW));
     this.setLowFootprint(conf.getBoolean(HadoopOfficeWriteConfiguration.CONF_LOWFOOTPRINT,HadoopOfficeWriteConfiguration.DEFAULT_LOWFOOTPRINT));
     this.setLowFootprintCacheRows(conf.getInt(HadoopOfficeWriteConfiguration.CONF_LOWFOOTPRINT_CACHEROWS,HadoopOfficeWriteConfiguration.DEFAULT_LOWFOOTPRINT_CACHEROWS));
     

     this.setCryptKeystoreFile(conf.get(HadoopOfficeWriteConfiguration.CONF_CRYKEYSTOREFILE,HadoopOfficeWriteConfiguration.DEFAULT_CRYKEYSTOREFILE));
     this.setCryptKeystoreType(conf.get(HadoopOfficeWriteConfiguration.CONF_CRYKEYSTORETYPE,HadoopOfficeWriteConfiguration.DEFAULT_CRYKEYSTORETYPE));
     this.setCryptKeystorePassword(conf.get(HadoopOfficeWriteConfiguration.CONF_CRYKEYSTOREPW,HadoopOfficeWriteConfiguration.DEFAULT_CRYKEYSTOREPW));
     this.setCryptKeystoreAlias(conf.get(HadoopOfficeWriteConfiguration.CONF_CRYKEYSTOREALIAS,HadoopOfficeWriteConfiguration.DEFAULT_CRYKEYSTOREALIAS));
     
     this.setSigKeystoreFile(conf.get(HadoopOfficeWriteConfiguration.CONF_SIGKEYSTOREFILE,HadoopOfficeWriteConfiguration.DEFAULT_SIGKEYSTOREFILE));
     this.setSigKeystoreType(conf.get(HadoopOfficeWriteConfiguration.CONF_SIGKEYSTORETYPE,HadoopOfficeWriteConfiguration.DEFAULT_SIGKEYSTORETYPE));
     this.setSigKeystorePassword(conf.get(HadoopOfficeWriteConfiguration.CONF_SIGKEYSTOREPW,HadoopOfficeWriteConfiguration.DEFAULT_SIGKEYSTOREPW));
     this.setSigKeystoreAlias(conf.get(HadoopOfficeWriteConfiguration.CONF_SIGKEYSTOREALIAS,HadoopOfficeWriteConfiguration.DEFAULT_SIGKEYSTOREALIAS));
     this.setSigHash(conf.get(HadoopOfficeWriteConfiguration.CONF_SIGHASH,HadoopOfficeWriteConfiguration.DEFAULT_SIGHASH));
}
public String[] getLinkedWorkbooksName() {
	return linkedWorkbooksName;
}
public void setLinkedWorkbooksName(String[] linkedWorkbooksName) {
	this.linkedWorkbooksName = linkedWorkbooksName;
}
public String getFileName() {
	return fileName;
}
public void setFileName(String fileName) {
	this.fileName = fileName;
}
public String getMimeType() {
	return mimeType;
}
public void setMimeType(String mimeType) {
	this.mimeType = mimeType;
}
public Locale getLocale() {
	return locale;
}
public void setLocale(Locale locale) {
	this.locale = locale;
}
public boolean getIgnoreMissingLinkedWorkbooks() {
	return ignoreMissingLinkedWorkbooks;
}
public void setIgnoreMissingLinkedWorkbooks(boolean ignoreMissingLinkedWorkbooks) {
	this.ignoreMissingLinkedWorkbooks = ignoreMissingLinkedWorkbooks;
}
public String getCommentAuthor() {
	return commentAuthor;
}
public void setCommentAuthor(String commentAuthor) {
	this.commentAuthor = commentAuthor;
}
public int getCommentWidth() {
	return commentWidth;
}
public void setCommentWidth(int commentWidth) {
	this.commentWidth = commentWidth;
}
public int getCommentHeight() {
	return commentHeight;
}
public void setCommentHeight(int commentHeight) {
	this.commentHeight = commentHeight;
}
public String getPassword() {
	return password;
}
public void setPassword(String password) {
	this.password = password;
}
public String getEncryptAlgorithm() {
	return encryptAlgorithm;
}
public void setEncryptAlgorithm(String encryptAlgorithm) {
	this.encryptAlgorithm = encryptAlgorithm;
}
public String getHashAlgorithm() {
	return hashAlgorithm;
}
public void setHashAlgorithm(String hashAlgorithm) {
	this.hashAlgorithm = hashAlgorithm;
}
public String getEncryptMode() {
	return encryptMode;
}
public void setEncryptMode(String encryptMode) {
	this.encryptMode = encryptMode;
}
public String getChainMode() {
	return chainMode;
}
public void setChainMode(String chainMode) {
	this.chainMode = chainMode;
}
public Map<String,String> getLinkedWBCredentialMap() {
	return linkedWBCredentialMap;
}
public void setLinkedWBCredentialMap(Map<String,String> linkedWBCredentialMap) {
	this.linkedWBCredentialMap = linkedWBCredentialMap;
}
public Map<String,String> getMetadata() {
	return metadata;
}
public void setMetadata(Map<String,String> metadata) {
	this.metadata = metadata;
}

public String getTemplate() {
	return template;
}

public void setTemplate(String template) {
	this.template=template;
}

public String getTemplatePassword() {
	return templatePassword;
}

public void setTemplatePassword(String templatePassword) {
	this.templatePassword=templatePassword;
}

public boolean getLowFootprint() {
	return lowFootprint;
}
public void setLowFootprint(boolean lowFootprint) {
	this.lowFootprint = lowFootprint;
}
public int getLowFootprintCacheRows() {
	return lowFootprintCacheRows;
}
public void setLowFootprintCacheRows(int lowFootprintCacheRows) {
	this.lowFootprintCacheRows = lowFootprintCacheRows;
}
public String getCryptKeystoreFile() {
	return cryptKeystoreFile;
}
public void setCryptKeystoreFile(String cryptKeystoreFile) {
	this.cryptKeystoreFile = cryptKeystoreFile;
}
public String getCryptKeystoreType() {
	return cryptKeystoreType;
}
public void setCryptKeystoreType(String cryptKeystoreType) {
	this.cryptKeystoreType = cryptKeystoreType;
}
public String getCryptKeystorePassword() {
	return cryptKeystorePassword;
}
public void setCryptKeystorePassword(String cryptKeystorePassword) {
	this.cryptKeystorePassword = cryptKeystorePassword;
}
public String getCryptKeystoreAlias() {
	return cryptKeystoreAlias;
}
public void setCryptKeystoreAlias(String cryptKeystoreAlias) {
	this.cryptKeystoreAlias = cryptKeystoreAlias;
}
public String getSigKeystoreFile() {
	return sigKeystoreFile;
}
public void setSigKeystoreFile(String sigKeystoreFile) {
	this.sigKeystoreFile = sigKeystoreFile;
}
public String getSigKeystoreType() {
	return sigKeystoreType;
}
public void setSigKeystoreType(String sigKeystoreType) {
	this.sigKeystoreType = sigKeystoreType;
}
public String getSigKeystorePassword() {
	return sigKeystorePassword;
}
public void setSigKeystorePassword(String sigKeystorePassword) {
	this.sigKeystorePassword = sigKeystorePassword;
}
public String getSigKeystoreAlias() {
	return sigKeystoreAlias;
}
public void setSigKeystoreAlias(String sigKeystoreAlias) {
	this.sigKeystoreAlias = sigKeystoreAlias;
}
public String getSigHash() {
	return sigHash;
}
public void setSigHash(String sigHash) {
	this.sigHash = sigHash;
}
public X509Certificate getSigCertificate() {
	return sigCertificate;
}
public void setSigCertificate(X509Certificate sigCertificate) {
	this.sigCertificate = sigCertificate;
}
public Key getSigKey() {
	return sigKey;
}
public void setSigKey(Key sigKey) {
	this.sigKey = sigKey;
}
}
