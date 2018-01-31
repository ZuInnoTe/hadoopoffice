/**
* Copyright 2018 ZuInnoTe (Jörn Franke) <zuinnote@gmail.com>
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

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CertPathBuilder;
import java.security.cert.CertPathBuilderException;
import java.security.cert.CertStore;
import java.security.cert.CertificateException;
import java.security.cert.CollectionCertStoreParameters;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.PKIXCertPathBuilderResult;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Verify the certificate chain of a given certificate. Requires BouncyCastle Library
 *
 */
public class CertificateChainVerificationUtil {
	
	private static final Log LOG = LogFactory.getLog(CertificateChainVerificationUtil.class.getName());
	public static boolean verifyCertificateChain(X509Certificate theCertificate,Set<X509Certificate> chainCertificates) throws CertificateException, NoSuchAlgorithmException, NoSuchProviderException, InvalidAlgorithmParameterException {
		
	
		// check if we can establish a trust chain
		if (isSelfSigned(theCertificate)) {
			LOG.error("Certificate is self-signed - no trust chain can be established with provided truststore");
			return false;
		}
		if (chainCertificates.size()<2) {
			LOG.error("One needs at least three certificates (including certificate used for signing to establish a trust chain. Please check that you included them");
			return false;
		}
		HashSet<X509Certificate> rootCertificates = new HashSet<>();
		HashSet<X509Certificate> subCertificates = new HashSet<>();
		subCertificates.add(theCertificate);
		for (X509Certificate currentCertificate: chainCertificates) {
			if (CertificateChainVerificationUtil.isSelfSigned(currentCertificate)) {
				LOG.debug("Root: "+currentCertificate.getSubjectDN().getName());
				rootCertificates.add(currentCertificate);
			} else {
				LOG.debug("Sub: "+currentCertificate.getSubjectDN().getName());
				subCertificates.add(currentCertificate);
			}
		}
		// Configure verification
		X509CertSelector selector = new X509CertSelector();
		selector.setCertificate(theCertificate);
		
		CertPathBuilder builder = CertPathBuilder.getInstance("PKIX", "BC");
		HashSet<TrustAnchor> trustAnchors = new HashSet<>();
		for (X509Certificate currentCertificate: rootCertificates) {
			trustAnchors.add(new TrustAnchor(currentCertificate,null));
		}
		
		PKIXBuilderParameters builderParams = new PKIXBuilderParameters(trustAnchors,selector);
		
		CertStore subCertStore = CertStore.getInstance("Collection", new CollectionCertStoreParameters(subCertificates), "BC");
		builderParams.addCertStore(subCertStore);
		
		try {
			PKIXCertPathBuilderResult result = (PKIXCertPathBuilderResult) builder.build(builderParams);
			return true;
		} catch (CertPathBuilderException e) {
			LOG.error("Exception: ",e);
			LOG.error("Cannot verify certification chain for "+theCertificate.getSubjectX500Principal());
		}
		return false;
	}
	
	private static boolean isSelfSigned(X509Certificate certificate) throws CertificateException, NoSuchAlgorithmException, NoSuchProviderException {
		try {
			PublicKey pubKey = certificate.getPublicKey();
			certificate.verify(pubKey);
			return true;
		} catch(SignatureException | InvalidKeyException e) {
			return false;
		} 
		
	}
    
	

}
