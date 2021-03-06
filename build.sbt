name := "test-work-project"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.1.1"

libraryDependencies ++= Seq(
  "com.typesafe"      % "config"            % "1.3.1",
  "org.apache.spark"  % "spark-core_2.11"   % "2.2.0" ,
  "org.apache.spark"  % "spark-sql_2.11"    % "2.2.0"
)
