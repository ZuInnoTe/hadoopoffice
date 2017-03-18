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
	public static final String DEFAULT_MIMETYPE="";
	public static final String DEFAULT_LOCALE="";
	public static final String DEFAULT_LINKEDWB="";
	public static final boolean DEFAULT_IGNOREMISSINGLINKEDWB=false;
	public static final String DEFAULT_AUTHOR="hadoopoffice";
	public static final int DEFAULT_COMMENTWIDTH=1;
	public static final int DEFAULT_COMMENTHEIGHT=3;
	public static final String DEFAULT_PASSWORD=null;
	public static final String DEFAULT_ALGORITHM="aes256";
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
	private Map<String,String> linkedWBCredentialMap;
	private Map<String,String> metadata;
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

}
