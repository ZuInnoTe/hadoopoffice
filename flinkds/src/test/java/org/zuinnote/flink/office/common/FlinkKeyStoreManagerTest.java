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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.InvalidAlgorithmParameterException;
import java.security.Key;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
import java.util.Set;

import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author jornfranke
 *
 */
public class FlinkKeyStoreManagerTest {

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
    
    @Test
    public void checkKeystoreAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="keystore.jceks";
		String fileNameKeyStore=classLoader.getResource(fileName).getFile();	
		assertNotNull(fileNameKeyStore,"Test Data File \""+fileName+"\" is not null in resource path");
		File file = new File(fileNameKeyStore);
		assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
		assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
    }
    

    @Test
    public void checkCertificateAvailable() {
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="testsigning.pfx";
		String fileNameKeyStore=classLoader.getResource(fileName).getFile();	
		assertNotNull(fileNameKeyStore,"Test Data File \""+fileName+"\" is not null in resource path");
		File file = new File(fileNameKeyStore);
		assertTrue( file.exists(),"Test Data File \""+fileName+"\" exists");
		assertFalse( file.isDirectory(),"Test Data File \""+fileName+"\" is not a directory");
    }
    
    @Test
    public void loadExistingKeyStore() throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException, UnrecoverableEntryException, InvalidKeySpecException {
	   
	    	ClassLoader classLoader = getClass().getClassLoader();
	    	String fileName="keystore.jceks";
		String fileNameKeyStore=classLoader.getResource(fileName).getFile();	
	    	Path file = new Path(fileNameKeyStore);
    		FlinkKeyStoreManager fksm = new FlinkKeyStoreManager();
    		fksm.openKeyStore(file, "JCEKS", "changeit");
    		String expectedPassword="test";
    		String password=fksm.getPassword("test.xlsx", "changeit");
    		assertEquals(expectedPassword,password,"Password is correctly read from keystore");
    }
    
   

    
    @Test
    public void createKeyStoreforPasswords() throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException, InvalidKeySpecException, UnrecoverableEntryException {	
       	String tmpDir=tmpPath.toString();	
       	Path outputFile= new Path(tmpDir,"keystore2.jceks");
       	FlinkKeyStoreManager fksm = new FlinkKeyStoreManager();
       	// create new key store
       	fksm.openKeyStore(null, "JCEKS", "changeit");
       	fksm.setPassword("test.xlsx", "test2", "changeit");
       	fksm.store(outputFile, "changeit");
       	// open existing keystore
       	fksm.openKeyStore(outputFile, "JCEKS", "changeit");
       	String expectedPassword="test2";
       	String password=fksm.getPassword("test.xlsx", "changeit");
  		assertEquals(expectedPassword,password,"Password is correctly read from new keystore");
    }
 
    
    @Test
    public void getPrivateKeyAndCertificate() throws IOException, UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
    		ClassLoader classLoader = getClass().getClassLoader();
    		String fileName="testsigning.pfx";
    		String fileNameKeyStore=classLoader.getResource(fileName).getFile();	
		Path file = new Path(fileNameKeyStore);
		FlinkKeyStoreManager fksm = new FlinkKeyStoreManager();
		fksm.openKeyStore(file, "PKCS12", "changeit");
    		// load private key
       	Key privateKey = fksm.getPrivateKey("testalias", "changeit");
       	assertNotNull(privateKey,"Private key could be loaded");
       	// load certificate
       	Certificate certificate = fksm.getCertificate("testalias");
       	assertNotNull(certificate,"Certificate for private key could be loaded");
       	
    }
    
    @Test
    public void getAllX509Certificates() throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException, InvalidAlgorithmParameterException {
    	
		ClassLoader classLoader = getClass().getClassLoader();
		String fileName="cacerts";
		String fileNameKeyStore=classLoader.getResource(fileName).getFile();	
		Path file = new Path(fileNameKeyStore);
		FlinkKeyStoreManager fksm = new FlinkKeyStoreManager();
		fksm.openKeyStore(file, "JKS", "changeit");
		// get most trusted certificates
		Set<X509Certificate> mostTrustedCAList=fksm.getAllX509Certificates();
		assertEquals(104,mostTrustedCAList.size(),"Most trusted CA list has length 104");
    }
}
