import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))

.settings(
organization := "com.github.zuinnote",
name := "hadoopoffice-flinkts",
version := "1.4.0"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
  .enablePlugins(JacocoItPlugin)


resolvers += Resolver.mavenLocal

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

fork  := true

crossScalaVersions := Seq("2.11.12","2.12.13")

scalacOptions += "-target:jvm-1.8"


artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some(""))
}

addArtifact(artifact in (Compile, assembly), assembly)

assemblyShadeRules in assembly := Seq(
   ShadeRule.rename("org.apache.commons.compress.**" -> "hadoopoffice.shade.org.apache.commons.compress.@1").inAll
)
assemblyJarName in assembly := {
     val newName = s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"
     newName
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly :=  {
    case PathList("META-INF/*.RSA", "META-INF/*.SF","META-INF/*.DSA") => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
     oldStrategy(x)

}

libraryDependencies += "com.github.zuinnote" % "hadoopoffice-fileformat" % "1.4.0" % "compile" exclude("org.apache.xmlgraphics","batik-all")


libraryDependencies += "com.github.zuinnote" % "hadoopoffice-flinkds" % "1.4.0" % "compile" exclude("org.apache.xmlgraphics","batik-all")

// following three libraries are only needed for digital signatures
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.68" % "provided"
libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.68" % "provided"
libraryDependencies += "org.apache.santuario" % "xmlsec" % "2.2.1" % "provided"

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

libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.10.3" % "it"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.2" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.3.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs-client" % "3.3.0" % "it" classifier "" classifier "tests"
