import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))

.settings(
organization := "com.github.zuinnote",
name := "hadoopoffice-flinkts",
version := "1.2.3"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
  .enablePlugins(JacocoItPlugin)


resolvers += Resolver.mavenLocal

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

fork  := true

crossScalaVersions := Seq("2.11.12","2.12.7")

scalacOptions += "-target:jvm-1.8"

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.2.3" % "compile"

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-flinkds" % "1.2.3" % "compile"
// following three libraries are only needed for digital signatures
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.60" % "provided"
libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.60" % "provided"
libraryDependencies += "org.apache.santuario" % "xmlsec" % "2.1.2" % "provided"

libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.7.0" % "provided" 

libraryDependencies += "org.apache.flink" %% "flink-table" % "1.7.0" % "provided" 
// needed for table environment 
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.7.0" % "provided" 

libraryDependencies += "org.apache.flink" % "flink-shaded-hadoop2" % "1.7.0" % "provided"  

// needed for writable serializer 
libraryDependencies += "org.apache.flink" %% "flink-hadoop-compatibility" % "1.7.0" % "provided" 

libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.7.0" % "it" 

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0" % "it" classifier "" classifier "tests"
