import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "example-ho-flinkds-scala-excel-dao",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
  .enablePlugins(JacocoItPlugin)



assemblyShadeRules in assembly := Seq(
   ShadeRule.rename("org.apache.commons.compress.**" -> "hadoopoffice.shade.org.apache.commons.compress.@1").inAll
)


assemblyMergeStrategy in assembly :=  {
    case PathList("META-INF/*.RSA", "META-INF/*.SF","META-INF/*.DSA") => MergeStrategy.discard
    case x => MergeStrategy.first
}

crossScalaVersions := Seq("2.11.12")


resolvers += Resolver.mavenLocal

assemblyJarName in assembly := "example-ho-flinkds-scala-excel-dao.jar"

fork  := true




libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.6.1" % "compile"

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-flinkds" % "1.6.1" % "compile"

// woodstox core (needed for Flink to find the XMLParser)

libraryDependencies += "com.fasterxml.woodstox" % "woodstox-core" % "5.0.3" % "compile"


libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.14.0" % "provided"

// needed for writable serializer
libraryDependencies += "org.apache.flink" %% "flink-hadoop-compatibility" % "1.14.0" % "compile"

libraryDependencies += "org.apache.flink" % "flink-shaded-hadoop2" % "2.4.1-1.8.3" % "provided"

// following three libraries are only needed for digital signatures
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.69" % "compile"
libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.69" % "compile"
libraryDependencies += "org.apache.santuario" % "xmlsec" % "2.2.3" % "compile"


libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.14.0" % "it"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.17.0" % "test"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.17.0" % "it"
// for integration testing we can only use 2.7.x, because higher versions of Hadoop have a bug in minidfs-cluster. Nevertheless, the library itself works also with higher Hadoop versions 
// see https://issues.apache.org/jira/browse/HDFS-5328
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs-client" % "2.8.0" % "it" classifier "" classifier "tests"