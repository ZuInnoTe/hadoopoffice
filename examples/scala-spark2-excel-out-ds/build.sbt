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


crossScalaVersions := Seq("2.11.12")

scalacOptions += "-target:jvm-1.8"

resolvers += Resolver.mavenLocal

assemblyJarName in assembly := "example-ho-spark-scala-ds-excelout.jar"


fork  := true


assemblyShadeRules in assembly := Seq(
   ShadeRule.rename("org.apache.commons.compress.**" -> "hadoopoffice.shade.org.apache.commons.compress.@1").inAll
)

assemblyMergeStrategy in assembly :=  {
    case PathList("META-INF/*.RSA", "META-INF/*.SF","META-INF/*.DSA") => MergeStrategy.discard
    case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
    case x => MergeStrategy.first

}



libraryDependencies += "com.github.zuinnote" %% "spark-hadoopoffice-ds" % "1.6.0" % "compile"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8" % "provided"   
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided" 

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"


libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-minicluster" % "2.7.0" % "it"
