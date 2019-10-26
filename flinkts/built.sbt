import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))

.settings(
organization := "com.github.zuinnote",
name := "hadoopoffice-flinkts",
version := "1.3.4"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
  .enablePlugins(JacocoItPlugin)


resolvers += Resolver.mavenLocal

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

fork  := true

crossScalaVersions := Seq("2.11.12","2.12.10")

scalacOptions += "-target:jvm-1.8"

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.3.4" % "compile"

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-flinkds" % "1.3.4" % "compile"
// following three libraries are only needed for digital signatures
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.62" % "provided"
libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.62" % "provided"
libraryDependencies += "org.apache.santuario" % "xmlsec" % "2.1.2" % "provided"

libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.9.0" % "provided" 

libraryDependencies += "org.apache.flink" % "flink-table" % "1.9.0" % "provided" 

libraryDependencies += "org.apache.flink" % "flink-table-common" % "1.9.0" % "provided" 

libraryDependencies += "org.apache.flink" %% "flink-table-planner" % "1.9.0" % "provided" 
libraryDependencies += "org.apache.flink" %% "flink-table-api-java-bridge" % "1.9.0" % "provided" 
libraryDependencies += "org.apache.flink" %% "flink-table-api-scala-bridge" % "1.9.0" % "provided" 
// needed for table environment 
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.9.0" % "provided" 

// needed for writable serializer 
libraryDependencies += "org.apache.flink" %% "flink-hadoop-compatibility" % "1.9.0" % "provided" 

libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.9.0" % "it" 

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.5" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.5" % "it" classifier "" classifier "tests"
