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
package org.zuinnote.flink.office.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.cert.Certificate;
import java.security.InvalidAlgorithmParameterException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

/**
 * @author jornfranke
 *
 */
public class FlinkKeyStoreManager {
	private static final Log LOG = LogFactory.getLog(FlinkKeyStoreManager.class.getName());
	private KeyStore keystore;
	private FlinkFileReader ffr;
	
	
	public FlinkKeyStoreManager() {
		this.ffr= new FlinkFileReader();
	}

	/****
	 * Opens a keystore on any Hadoop compatible filesystem
	 * 
	 * @param path path to key store, if null then a new keystore is created
	 * @param keyStoreType
	 * @param keyStorePassword
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 * @throws CertificateException
	 * @throws KeyStoreException
	 */
	public void openKeyStore(Path path, String keyStoreType, String keyStorePassword) throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException {
		this.keystore = KeyStore.getInstance(keyStoreType);
		if (path!=null) {
			InputStream keyStoreInputStream = ffr.openFile(path);
			this.keystore.load(keyStoreInputStream, keyStorePassword.toCharArray());
		} else {
			this.keystore.load(null, keyStorePassword.toCharArray());
		}
	}
	
	/**
	 * Reads a private key from  keystore
	 * 
	 * @param alias
	 * @param password
	 * @return
	 * @throws UnrecoverableKeyException
	 * @throws KeyStoreException
	 * @throws NoSuchAlgorithmException
	 */
	public Key getPrivateKey(String alias, String password) throws UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException {
		return this.keystore.getKey(alias, password.toCharArray());
	}
	
	/**
	 * Reads the certificate for a private key from keystore
	 * 
	 * @param alias
	 * @return
	 * @throws KeyStoreException
	 */
	
	
	public Certificate getCertificate(String alias) throws KeyStoreException {
		return this.keystore.getCertificate(alias);
	}
	
	/***
	 * Retrieves a password from the currently opened keystore
	 * 
	 * @param alias
	 * @param passwordPassword
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws UnrecoverableEntryException
	 * @throws KeyStoreException
	 * @throws InvalidKeySpecException
	 */
	public String getPassword(String alias, String passwordPassword) throws NoSuchAlgorithmException, UnrecoverableEntryException, KeyStoreException, InvalidKeySpecException {
		SecretKey sk =  (SecretKey) this.keystore.getKey(alias,passwordPassword.toCharArray());
		return new String(sk.getEncoded());
	}
	
	/**
	 * Sets the password in the currently openend keystore. Do not forget to store it afterwards
	 * 
	 * @param alias
	 * @param password to store 
	 * @param passwordPassword password for encrypting password. You can use the same as the keystore password
	 * @throws NoSuchAlgorithmException 
	 * @throws InvalidKeySpecException 
	 * @throws KeyStoreException 
	 */
	
	public void setPassword(String alias, String password, String passwordPassword) throws NoSuchAlgorithmException, InvalidKeySpecException, KeyStoreException {
		SecretKeyFactory skf = SecretKeyFactory.getInstance("PBE");
        SecretKey pSecret = skf.generateSecret(new PBEKeySpec(password.toCharArray()));
        KeyStore.PasswordProtection kspp = new KeyStore.PasswordProtection(passwordPassword.toCharArray());
        this.keystore.setEntry(alias, new KeyStore.SecretKeyEntry(pSecret), kspp);
	}
	
	/***
	 * Stores the keystore onto HDFS. Overwrites existing one.
	 * 
	 * 
	 * @param path
	 * @param keyStorePassword
	 * @throws KeyStoreException
	 * @throws NoSuchAlgorithmException
	 * @throws CertificateException
	 * @throws IOException
	 */
	public void store(Path path, String keyStorePassword) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
		OutputStream os = FileSystem.get(path.toUri()).create(path, true);
		this.keystore.store(os,keyStorePassword.toCharArray());
		if (os!=null) {
			os.close();
		}
	}
	
	/**
	 * Reads from keystore (truststore) the most trusted CA. You can use this for verification of a certification chain
	 * 
	 * @return
	 * @throws InvalidAlgorithmParameterException 
	 * @throws KeyStoreException 
	 */
	
	public List<X509Certificate> getMostTrustedCertificates() throws KeyStoreException, InvalidAlgorithmParameterException {
		PKIXParameters parameters = new PKIXParameters(this.keystore);
		Iterator<TrustAnchor> iterator = parameters.getTrustAnchors().iterator();
		ArrayList<X509Certificate> result = new ArrayList<>();
		while (iterator.hasNext()) {
			result.add(iterator.next().getTrustedCert());
		}
		return result;
	}

}
