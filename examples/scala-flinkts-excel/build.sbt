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


crossScalaVersions := Seq("2.11.12")

scalacOptions += "-target:jvm-1.8"


resolvers += Resolver.mavenLocal

assemblyJarName in assembly := "example-ho-flink-ts-scala-excel.jar"

fork  := true



assemblyShadeRules in assembly := Seq(
   ShadeRule.rename("org.apache.commons.compress.**" -> "hadoopoffice.shade.org.apache.commons.compress.@1").inAll
)

assemblyMergeStrategy in assembly :=  {
    case PathList("META-INF/*.RSA", "META-INF/*.SF","META-INF/*.DSA") => MergeStrategy.discard
    case x => MergeStrategy.first
}

// hadoopoffice
libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.6.1" % "compile"

libraryDependencies += "com.github.zuinnote" %% "hadoopoffice-flinkts" % "1.6.1" % "compile"

// woodstox core (needed for Flink to find the XMLParser)

libraryDependencies += "com.fasterxml.woodstox" % "woodstox-core" % "5.0.3" % "compile"

// following three libraries are only needed for digital signatures
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.69" % "compile"
libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.69" % "compile"
libraryDependencies += "org.apache.santuario" % "xmlsec" % "2.2.3" % "compile"

//flink
libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.10.3" % "provided"

libraryDependencies += "org.apache.flink" % "flink-table" % "1.10.3" % "provided"

libraryDependencies += "org.apache.flink" % "flink-table-common" % "1.10.3" % "provided"

libraryDependencies += "org.apache.flink" %% "flink-table-planner" % "1.10.3" % "provided"
libraryDependencies += "org.apache.flink" %% "flink-table-api-java-bridge" % "1.10.3" % "provided"
libraryDependencies += "org.apache.flink" %% "flink-table-api-scala-bridge" % "1.10.3" % "provided"
// needed for table environment
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.10.3" % "provided"

// needed for writable serializer
libraryDependencies += "org.apache.flink" %% "flink-hadoop-compatibility" % "1.10.3" % "provided"

libraryDependencies += "org.apache.flink" % "flink-shaded-hadoop2" % "2.4.1-1.8.3" % "provided"

libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.10.3" % "it"


libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"


// for integration testing we can only use 2.7.x, because higher versions of Hadoop have a bug in minidfs-cluster. Nevertheless, the library itself works also with higher Hadoop versions 
// see https://issues.apache.org/jira/browse/HDFS-5328
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs-client" % "2.8.0" % "it" classifier "" classifier "tests"


libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.7.0" % "it" classifier "" classifier "tests"