import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "example-ho-flinkds-scala-excel-simple",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
  .enablePlugins(JacocoItPlugin)


crossScalaVersions := Seq("2.10.5", "2.11.7")

scalacOptions += "-target:jvm-1.7"

resolvers += Resolver.mavenLocal

assemblyJarName in assembly := "example-ho-flinkds-scala-excel-simple.jar"

fork  := true

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}


libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.1.1" % "compile"

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-flinkds" % "1.1.1" % "compile"

// following three libraries are only needed for digital signatures
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.58" % "provided"
libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.58" % "provided"
libraryDependencies += "org.apache.santuario" % "xmlsec" % "2.1.0" % "provided"

libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.3.2" % "provided" 

libraryDependencies += "org.apache.flink" % "flink-shaded-hadoop2" % "1.3.2" % "provided"  

// needed for writable serializer 
libraryDependencies += "org.apache.flink" %% "flink-hadoop-compatibility" % "1.3.2" % "compile" 

libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.3.2" % "it" 

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.2.0" % "it" classifier "" classifier "tests"