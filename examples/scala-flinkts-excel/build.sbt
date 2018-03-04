import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "example-ho-flink-ts-scala-excel",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
  .enablePlugins(JacocoItPlugin)


crossScalaVersions := Seq("2.10.5", "2.11.7")

scalacOptions += "-target:jvm-1.7"

resolvers += Resolver.mavenLocal

assemblyJarName in assembly := "example-ho-flink-ts-scala-excel.jar"

fork  := true


libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.1.0" % "compile"

libraryDependencies += "com.github.zuinnote" %% "hadoopoffice-flinkts" % "1.1.0" % "compile"

libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.3.2" % "provided" 

libraryDependencies += "org.apache.flink" %% "flink-table" % "1.3.2" % "provided" 

// following is needed for flink-table
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.3.2" % "provided" 

libraryDependencies += "org.apache.flink" % "flink-shaded-hadoop2" % "1.3.2" % "provided"  

// needed for writable serializer 
libraryDependencies += "org.apache.flink" %% "flink-hadoop-compatibility" % "1.3.2" % "compile" 

libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.3.2" % "it" 

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.2.0" % "it" classifier "" classifier "tests"