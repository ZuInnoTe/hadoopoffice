-- optional: if you need support for digital signatures then add the following libraries
--- bouncycastle: https://search.maven.org/#search%7Cga%7C1%7Corg.bouncycastle
add jar /tmp/bcprov-ext-jdk15-on-1.70.jar;
add jar /tmp/bcpkix-jdk15-on-1.70.jar;
--- xmlsec: https://search.maven.org/#search%7Cga%7C1%7Ca%3A%22xmlsec%22
add jar /tmp/xmlsec-3.0.0.jar;
-- you can find the HadoopOffice libraries here: https://search.maven.org/#search%7Cga%7C1%7Chadoopoffice
-- note this .jar is a fat jar containing all POI dependencies, because one cannot add them to Hive via ADD JAR individually (due to the way how POI uses the classloader)
add jar /tmp/hadoopoffice-hiveserde-1.7.0.jar;
