import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "example-ho-flink-scala-excelin",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
  .enablePlugins(JacocoItPlugin)


crossScalaVersions := Seq("2.10.5", "2.11.7")

scalacOptions += "-target:jvm-1.7"

resolvers += Resolver.mavenLocal

assemblyJarName in assembly := "example-ho-flink-scala-excelin.jar"

fork  := true




libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.1.0" % "compile"

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-flinkds" % "1.1.0" % "compile"

libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.3.2" % "provided" 

libraryDependencies += "org.apache.flink" % "flink-shaded-hadoop2" % "1.3.2" % "provided"  

// following three libraries are only needed for digital signatures
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.58" % "compile"
libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.58" % "compile"
libraryDependencies += "org.apache.santuario" % "xmlsec" % "2.1.0" % "compile"

// needed for writable serializer 
libraryDependencies += "org.apache.flink" %% "flink-hadoop-compatibility" % "1.3.2" % "compile" 

libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.3.2" % "it" 

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.2.0" % "it" classifier "" classifier "tests"