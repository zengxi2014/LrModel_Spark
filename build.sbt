import AssemblyKeys._ 

assemblySettings

name := "lab-spark"

organization := "com.ucweb"

version := "2.0.0"

scalaVersion := "2.10.4"

exportJars := true

resolvers ++= Seq(
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Cloudera  Repository" at "https://repository.cloudera.com/cloudera/cloudera-repos"
)

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.3.0-cdh5.4.0" % "provided" ,
  "org.apache.spark" % "spark-sql_2.10" % "1.3.0-cdh5.4.0" % "provided",
  "org.apache.spark" % "spark-catalyst_2.10" % "1.3.0-cdh5.4.0" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.3.0-cdh5.4.0" % "provided",
  "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test",
  "com.typesafe" % "config" % "1.2.1",  
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
  "mysql" % "mysql-connector-java" % "5.1.18",
  "com.google.code.gson" % "gson"% "2.2.4"
)

//javacOptions += "-g:none"

javacOptions ++= Seq(
  "-source", "1.7",
  "-target", "1.7",
  "-encoding", "UTF-8"
)

javaOptions += "-XX:MaxPermSize=2048"

compileOrder := CompileOrder.JavaThenScala

//unmanagedClasspath in Runtime <+= (baseDirectory) map { bd => Attributed.blank(bd / "conf") }

unmanagedClasspath in Test <+= (baseDirectory) map { bd => Attributed.blank(bd / "conf") }

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = true)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {    
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case PathList(ps @ _*) => MergeStrategy.first
    case x => old(x)
  }
}

mainClass in assembly := Some("com.uc.bigdata.lab.exercise.WorkCount")
