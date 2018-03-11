-- you can find the Apache POI libraries here: https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.poi%22
add jar '/tmp/poi-ooxml-3.17.jar';
add jar '/tmp/poi-3.17.jar';
-- you can find the HadoopOffice libraries here: https://search.maven.org/#search%7Cga%7C1%7Chadoopoffice
add jar '/tmp/hadoopoffice-fileformat-1.1.0.jar';
add jar '/tmp/hadoopoffice-hiveserde-1.1.0.jar';
-- optional: if you need support for digital signatures then add the following libraries
--- bouncycastle: https://search.maven.org/#search%7Cga%7C1%7Corg.bouncycastle
add jar '/tmp/bcprov-ext-jdk15on-1.58.jar';
add jar '/tmp/bcpkix-jdk15on-1.58.jar';
--- xmlsec: https://search.maven.org/#search%7Cga%7C1%7Ca%3A%22xmlsec%22
add jar '/tmp/xmlsec-2.1.0.jar';

