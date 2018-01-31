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
package org.zuinnote.hadoop.office.format.common.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.InvalidAlgorithmParameterException;
import java.security.Key;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;


import java.security.cert.X509Certificate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.zuinnote.hadoop.office.format.common.HadoopKeyStoreManager;

/**
 * @author jornfranke
 *
 */
public class CertificateChainVerificationUtilTest {
	private static Configuration defaultConf = new Configuration();
	private static FileSystem localFs = null; 
	private static final String attempt = "attempt_201612311111_0001_m_000000_0";
	private static final String taskAttempt = "task_201612311111_0001_m_000000";
	private static final TaskAttemptID taskID = TaskAttemptID.forName(attempt);
	private static final String tmpPrefix = "hadoopofficetest";
	private static final String outputbaseAppendix = "-m-00000";
	private static java.nio.file.Path tmpPath;

	   @BeforeAll
	    public static void oneTimeSetUp() throws IOException {
	      // one-time initialization code   
	      defaultConf.set("fs.defaultFS", "file:///");
	      localFs = FileSystem.getLocal(defaultConf);
	      // create temp directory
	      tmpPath = Files.createTempDirectory(tmpPrefix);

	      // create shutdown hook to remove temp files after shutdown, may need to rethink to avoid many threads are created
		Runtime.getRuntime().addShutdownHook(new Thread(
	    	new Runnable() {
	      	@Override
	      	public void run() {
	        	try {
	          		Files.walkFileTree(tmpPath, new SimpleFileVisitor<java.nio.file.Path>() {
		
	            		@Override
	            		public FileVisitResult visitFile(java.nio.file.Path file,BasicFileAttributes attrs)
	                		throws IOException {
	              			Files.delete(file);
	             			return FileVisitResult.CONTINUE;
	        			}

	        		@Override
	        		public FileVisitResult postVisitDirectory(java.nio.file.Path dir, IOException e) throws IOException {
	          			if (e == null) {
	            				Files.delete(dir);
	            				return FileVisitResult.CONTINUE;
	          			}
	          			throw e;
	        	}
	        	});
	      	} catch (IOException e) {
	        throw new RuntimeException("Error temporary files in following path could not be deleted "+tmpPath, e);
	      }
	    }}));
	    }

	    @AfterAll
	    public static void oneTimeTearDown() {
	        // one-time cleanup code
	      }

	    @BeforeEach
	    public void setUp() {
	    }

	    @AfterEach
	    public void tearDown() {  
	    }
	    
	    @Disabled("We need to update the test certificate with certificate revocation lists (CRL)")
	 @Test
	    public void verifyCertificationChainPositive() throws IOException, UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException, CertificateException, InvalidAlgorithmParameterException, NoSuchProviderException {
	    		///// Load Certificate
		 	Configuration conf = new Configuration(defaultConf);
	    		ClassLoader classLoader = getClass().getClassLoader();
	    		String fileName="testsigningCA.pfx";
	    		String fileNameKeyStore=classLoader.getResource(fileName).getFile();	
			Path file = new Path(fileNameKeyStore);
	     	HadoopKeyStoreManager hksm = new HadoopKeyStoreManager(conf);
	       	hksm.openKeyStore(file, "PKCS12", "changeit");
	    		// load private key
	       	Key privateKey = hksm.getPrivateKey("testalias", "changeit");
	       	assertNotNull(privateKey,"Private key could be loaded");
	       	// load certificate
	       	Certificate certificate = hksm.getCertificate("testalias");
	       	assertNotNull(certificate,"Certificate for private key could be loaded");
	       	///// Load Chain
	       	classLoader = getClass().getClassLoader();
			String fileNameTS="signingtruststore.jks";
			String fileNameTrustStore=classLoader.getResource(fileNameTS).getFile();	
			Path fileTS = new Path(fileNameTrustStore);
			HadoopKeyStoreManager hksmTS = new HadoopKeyStoreManager(conf);
			hksmTS.openKeyStore(fileTS, "JKS", "changeit");
			// get chain from truststore
			// verify, should be false because no CRLs found / test case needs some enhancemen
			assertFalse(CertificateChainVerificationUtil.verifyCertificateChain((X509Certificate)certificate, hksmTS.getAllX509Certificates()), "Certification chain can be verified successfully");
	    }
}
