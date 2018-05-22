import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "example-ho-spark-scala-ds-excelout",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
  .enablePlugins(JacocoItPlugin)


crossScalaVersions := Seq("2.10.5", "2.11.7")

scalacOptions += "-target:jvm-1.7"

resolvers += Resolver.mavenLocal

assemblyJarName in assembly := "example-ho-spark-scala-ds-excelout.jar"


fork  := true

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

libraryDependencies += "com.github.zuinnote" %% "spark-hadoopoffice-ds" % "1.1.1" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"


libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-minicluster" % "2.7.0" % "it"
