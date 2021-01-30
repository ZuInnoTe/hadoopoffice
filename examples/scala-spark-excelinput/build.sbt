import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "example-ho-spark-scala-excelin",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
  .enablePlugins(JacocoItPlugin)


crossScalaVersions := Seq("2.11.12")

scalacOptions += "-target:jvm-1.8"

resolvers += Resolver.mavenLocal

assemblyJarName in assembly := "example-ho-spark-scala-excelin.jar"

fork  := true




assemblyShadeRules in assembly := Seq(
   ShadeRule.rename("org.apache.commons.compress.**" -> "hadoopoffice.shade.org.apache.commons.compress.@1").inAll
)


assemblyMergeStrategy in assembly :=  {
    case PathList("META-INF/*.RSA", "META-INF/*.SF","META-INF/*.DSA") => MergeStrategy.discard
    case x => MergeStrategy.first
}

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.4.0" % "compile"

// following three libraries are only needed for digital signatures
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.67" % "compile"
libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.67" % "compile"
libraryDependencies += "org.apache.santuario" % "xmlsec" % "2.2.1" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.2" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"


libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.5" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.5" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-minicluster" % "2.7.5" % "it"
