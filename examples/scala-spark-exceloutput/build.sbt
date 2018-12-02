import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "example-ho-spark-scala-excelout",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
  .enablePlugins(JacocoItPlugin)



crossScalaVersions := Seq("2.11.12")

scalacOptions += "-target:jvm-1.8"

resolvers += Resolver.mavenLocal

assemblyJarName in assembly := "example-ho-spark-scala-excelout.jar"

fork  := true



assemblyShadeRules in assembly := Seq(
   ShadeRule.rename("org.apache.commons.compress.**" -> "hadoopoffice.shade.org.apache.commons.compress.@1").inAll
)


libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.2.4" % "compile"

// following three libraries are only needed for digital signatures
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.60" % "compile"
libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.60" % "compile"
libraryDependencies += "org.apache.santuario" % "xmlsec" % "2.1.2" % "compile"


// Spark dependencies
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"


libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-minicluster" % "2.7.0" % "it"

