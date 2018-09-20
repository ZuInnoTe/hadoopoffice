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

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

crossScalaVersions := Seq("2.11.12")

scalacOptions += "-target:jvm-1.8"

resolvers += Resolver.mavenLocal

assemblyJarName in assembly := "example-ho-flinkds-scala-excel-dao.jar"

fork  := true




libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.2.0" % "compile"

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-flinkds" % "1.2.0" % "compile"

libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.5.0" % "provided" 

libraryDependencies += "org.apache.flink" % "flink-shaded-hadoop2" % "1.5.0" % "provided"  

// following three libraries are only needed for digital signatures
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.60" % "compile"
libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.60" % "compile"
libraryDependencies += "org.apache.santuario" % "xmlsec" % "2.1.2" % "compile"

// needed for writable serializer 
libraryDependencies += "org.apache.flink" %% "flink-hadoop-compatibility" % "1.5.0" % "compile" 

libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.5.0" % "it" 

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.2.0" % "it" classifier "" classifier "tests"