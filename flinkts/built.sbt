import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))

.settings(
organization := "com.github.zuinnote",
name := "hadoopoffice-flinkts",
version := "1.1.0"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
  .enablePlugins(JacocoItPlugin)


resolvers += Resolver.mavenLocal

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

fork  := true

crossScalaVersions := Seq("2.10.7","2.11.10")

scalacOptions += "-target:jvm-1.7"

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.1.0" % "compile"

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-flinkds" % "1.1.0" % "compile"

libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.3.2" % "provided" 

libraryDependencies += "org.apache.flink" %% "flink-table" % "1.3.2" % "provided" 
// needed for table environment 
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.3.2" % "provided" 

libraryDependencies += "org.apache.flink" % "flink-shaded-hadoop2" % "1.3.2" % "provided"  

// needed for writable serializer 
libraryDependencies += "org.apache.flink" %% "flink-hadoop-compatibility" % "1.3.2" % "compile" 

libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.3.2" % "it" 

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.2.0" % "it" classifier "" classifier "tests"
