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
 * read the configuration for reading office files from a Hadoop configuration
 * 
 * @author Jörn Franke (zuinnote@gmail.com)
 *
 */
public class HadoopOfficeReadConfiguration {
public static final String CONF_MIMETYPE="hadoopoffice.read.mimeType";
public static final String CONF_SHEETS="hadoopoffice.read.sheets";
public static final String CONF_LOCALE="hadoopoffice.read.locale.bcp47";
public static final String CONF_LINKEDWB="hadoopoffice.read.linkedworkbooks";
public static final String CONF_IGNOREMISSINGWB="hadoopoffice.read.ignoremissinglinkedworkbooks";
public static final String CONF_DECRYPT="hadoopoffice.read.security.crypt.password";
public static final String CONF_DECRYPTLINKEDWBBASE="hadoopoffice.read.security.crypt.linkedworkbooks.";
public static final String CONF_FILTERMETADATA = "hadoopoffice.read.filter.metadata."; // base: all these properties (e.g. hadoopoffice.read.filter.metadata.author) will be handed over to the corresponding reader which does the filtering!
public static final String CONF_LOWFOOTPRINT="hadoopoffice.read.lowFootprint";
public static final String DEFAULT_MIMETYPE="";
public static final String DEFAULT_LOCALE="";
public static final String DEFAULT_SHEETS="";
public static final boolean DEFAULT_LINKEDWB=false;
public static final boolean DEFAULT_IGNOREMISSINGLINKEDWB=false;

public static final boolean DEFAULT_LOWFOOTPRINT=false;

private String fileName;
private String mimeType=null;
private String localeStrBCP47=null;
private String sheets=null;
private Locale locale=null;
private boolean readLinkedWorkbooks=false;
private boolean ignoreMissingLinkedWorkbooks=false;	
private String password=null;
private Map<String,String> metadataFilter;
private Map<String,String> linkedWBCredentialMap;
private boolean lowFootprint;

/*
 * Create an empty configuration
 * 
 */
public HadoopOfficeReadConfiguration() {
	// create an empty configuration
}

/**
 *  Reasd HadoopOffice configuration from Hadoop configuration
 * @param conf
 * * hadoopoffice.read.mimeType: Mimetype of the document
* hadoopoffice.read.locale: Locale of the document (e.g. needed for interpreting spreadsheets) in the BCP47 format (cf. https://tools.ietf.org/html/bcp47). If not specified then default system locale will be used.
* hadoopoffice.read.sheets: A ":" separated list of sheets to be read. If not specified then all sheets will be read one after the other
* hadoopoffice.read.linkedworkbooks: true if linkedworkbooks should be fetched. They must be in the same folder as the main workbook. Linked Workbooks will be processed together with the main workbook on one node and thus it should be avoided to have a lot of linked workbooks. It does only read the linked workbooks that are directly linked to the main workbook. Default: false
* hadoopoffice.read.ignoremissinglinkedworkbooks: true if missing linked workbooks should be ignored. Default: false
* hadoopoffice.read.security.crypt.password: if set then hadoopoffice will try to decrypt the file
* hadoopoffice.read.security.crypt.linkedworkbooks.*: if set then hadoopoffice will try to decrypt all the linked workbooks where a password has been specified. If no password is specified then it is assumed that the linked workbook is not encrypted. Example: Property key for file "linkedworkbook1.xlsx" is  "hadoopoffice.read.security.crypt.linkedworkbooks.linkedworkbook1.xslx". Value is the password. You must not include path or protocol information in the filename 
* hadoopoffice.read.filter.metadata: filters documents according to metadata. For example, hadoopoffice.read.filter.metadata.author will filter by author and the filter defined as value. Filtering is done by the parser and it is recommended that it supports regular expression for filtering, but this is up to the parser!
* hadoopoffice.read.lowfootprint: uses low memory/cpu footprint for reading documents. Note: In this mode certain features are not availanble, such as reading formulas. Default: false
 * 
 * 
 */
public HadoopOfficeReadConfiguration(Configuration conf) {
    this.mimeType=conf.get(HadoopOfficeReadConfiguration.CONF_MIMETYPE,HadoopOfficeReadConfiguration.DEFAULT_MIMETYPE);
    this.sheets=conf.get(HadoopOfficeReadConfiguration.CONF_SHEETS,HadoopOfficeReadConfiguration.DEFAULT_SHEETS);
    this.localeStrBCP47=conf.get(HadoopOfficeReadConfiguration.CONF_LOCALE, HadoopOfficeReadConfiguration.DEFAULT_LOCALE);
    if (!("".equals(localeStrBCP47))) { // create locale
	this.locale=new Locale.Builder().setLanguageTag(this.localeStrBCP47).build();
     }
     this.readLinkedWorkbooks=conf.getBoolean(HadoopOfficeReadConfiguration.CONF_LINKEDWB,HadoopOfficeReadConfiguration.DEFAULT_LINKEDWB);
     this.ignoreMissingLinkedWorkbooks=conf.getBoolean(HadoopOfficeReadConfiguration.CONF_IGNOREMISSINGWB,HadoopOfficeReadConfiguration.DEFAULT_IGNOREMISSINGLINKEDWB);
     this.password=conf.get(HadoopOfficeReadConfiguration.CONF_DECRYPT); // null if no password is set
      this.metadataFilter=HadoopUtil.parsePropertiesFromBase(conf,HadoopOfficeReadConfiguration.CONF_FILTERMETADATA);
     this.linkedWBCredentialMap=HadoopUtil.parsePropertiesFromBase(conf,HadoopOfficeReadConfiguration.CONF_DECRYPTLINKEDWBBASE);
     this.lowFootprint=conf.getBoolean(HadoopOfficeReadConfiguration.CONF_LOWFOOTPRINT,HadoopOfficeReadConfiguration.DEFAULT_LOWFOOTPRINT);
}

/*
 * Returns the configuration for filtering sheets
 */
public String getSheets() {
	return this.sheets;
}

/*
 * Set sheets
 * 
 * @param sheets comma-separated list of sheets to take into account for parsing, null if all should be taken into account
 * 
 */
public void setSheets(String sheets) {
	this.sheets=sheets;
}

/*
 * Returns the configured mimetype
 * 
 */
public String getMimeType() {
	return this.mimeType;
}

/*
 * Sets the configured mimetype
 * 
 * @param mimetype MimeType
 * 
 * 
 */

public void setMimeType(String mimeType) {
	this.mimeType=mimeType;
}

/*
 * Locale used for interpreting values
 * 
 * @return locale or null if the default locale should be used
 * 
 */
public Locale getLocale() {
	return this.locale;
}

/*
 *  Set locale used for interpreting values
 *  
 *  @param locale locale or null if default locale should be used
 * 
 */
public void setLocale(Locale locale) {
	this.locale=locale;
}

/*
 * should linked workbooks be read
 * 
 * @return true, if yes, false if not
 */
public boolean getReadLinkedWorkbooks() {
	return this.readLinkedWorkbooks;
}

/**
 * Set if linked workbooks should be read 
 * 
 * @param readLinkedWorkbooks true if yes, false if not
 */

public void setReadLinkedWorkbooks(boolean readLinkedWorkbooks) {
	this.readLinkedWorkbooks=readLinkedWorkbooks;
}

/*
 * Should missed linked workbooks be ignored or not 
 * 
 * @return true, if yes, false if not
 * 
 */
public boolean getIgnoreMissingLinkedWorkbooks() {
	return this.ignoreMissingLinkedWorkbooks;
}

/**
 * Set if missed linked workbooks should be ignored
 * 
 * @param ignoreMissingLinkedWorkbooks true, if yes, false, if not
 * 
 */

public void setIgnoreMissingLinkedWorkbooks(boolean ignoreMissingLinkedWorkbooks) {
	this.ignoreMissingLinkedWorkbooks=ignoreMissingLinkedWorkbooks;
}

/*
 * Password for file, if any 
 * 
 * @return password, or null if no password
 * 
 */

public String getPassword() {
	return this.password;
}


/*
 * Set the password
 * 
 * @param password password
 * 
 */
public void setPassword(String password) {
	this.password=password;
}

/**
 * Meta data filter for filtering documents not part of the filter 
 * 
 * @return key/value map with filter values
 */
public Map<String,String> getMetaDataFilter() {
	return this.metadataFilter;
}

/*
 *  Set meta data filer for filtering documents not part of the filter
 * 
 * @param metadataFilter key/value map for filtering metadata
 * 
 */

public void setMetaDataFilter(Map<String,String> metadataFilter) {
	this.metadataFilter=metadataFilter;
}

/*
 * Get credential map for linked workbooks
 * 
 * @return credential map for linked workbooks
 * 
 */

public Map<String,String> getLinkedWBCredentialMap() {
	return this.linkedWBCredentialMap;
}

/*
 * Set the credential map for linked workbooks
 * 
 * 
 * @param linkedWBCredentialMap new credential map for linked workboooks 
 * 
 */

public void setLinkedWBCredentialMap(Map<String,String> linkedWBCredentialMap) {
	this.linkedWBCredentialMap=linkedWBCredentialMap;
	
}


/*
 *  returns filename of the document to which the configuration belongs
 *  
 *  @return filename
 */
public String getFileName() {
	return this.fileName;
}


/*
 * Sets the filename of the document to which this configuration belongs
 * 
 * 
 */
public void setFileName(String fileName) {
	this.fileName=fileName;
}

/*
 * Should files be read in low footprint mode or not
 * 
 * @return true, if yes, false if not
 * 
 */
public boolean getLowFootprint() {
	return this.lowFootprint;
}

/**
 * Set if files should be read in low footprint mode or not
 * 
 * @param lowFootprint true, if yes, false, if not
 * 
 */

public void setLowFootprint(boolean lowFootprint) {
	this.lowFootprint=lowFootprint;
}



}
