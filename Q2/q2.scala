// Databricks notebook source
// MAGIC %md
// MAGIC #### Q2 - Skeleton Scala Notebook
// MAGIC This template Scala Notebook is provided to provide a basic setup for reading in / writing out the graph file and help you get started with Scala.  Clicking 'Run All' above will execute all commands in the notebook and output a file 'toygraph.csv'.  See assignment instructions on how to to retrieve this file. You may modify the notebook below the 'Cmd2' block as necessary.
// MAGIC 
// MAGIC #### Precedence of Instruction
// MAGIC The examples provided herein are intended to be more didactic in nature to get you up to speed w/ Scala.  However, should the HW assignment instructions diverge from the content in this notebook, by incident of revision or otherwise, the HW assignment instructions shall always take precedence.  Do not rely solely on the instructions within this notebook as the final authority of the requisite deliverables prior to submitting this assignment.  Usage of this notebook implicitly guarantees that you understand the risks of using this template code. 

// COMMAND ----------

/*
DO NOT MODIFY THIS BLOCK
This assignment can be completely accomplished with the following includes and case class.
Do not modify the %language prefixes, only use Scala code within this notebook.  The auto-grader will check for instances of <%some-other-lang>, e.g., %python
*/
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
case class edges(Source: String, Target: String, Weight: Int)
import spark.implicits._

// COMMAND ----------

/* 
Create an RDD of graph objects from our toygraph.csv file, convert it to a Dataframe
Replace the 'toygraph.csv' below with the name of Q2 graph file.
*/

val df = spark.read.textFile("/FileStore/tables/bitcoinalpha.csv") 
  .map(_.split(","))
  .map(columns => edges(columns(0), columns(1), columns(2).toInt)).toDF()

// COMMAND ----------

// Insert blocks as needed to further process your graph, the division and number of code blocks is at your discretion.


// COMMAND ----------

// e.g. eliminate duplicate rows
//https://stackoverflow.com/questions/30248221/removing-duplicates-from-rows-based-on-specific-columns-in-an-rdd-spark-datafram
val df_no_duplicates = df.dropDuplicates()
//display(df_no_duplicates)

// COMMAND ----------

// e.g. filter nodes by edge weight >= supplied threshold in assignment instructions
// http://allaboutscala.com/big-data/spark/#filter-column-dataframe
val df_edge_by_weight = df_no_duplicates.filter("Weight >= 5")
//display(df_edge_by_weight)


// COMMAND ----------

// find node with highest in-degree, if two or more nodes have the same in-degree, report the one with the lowest node id
val df_in_degree = df_edge_by_weight.groupBy("Target").count().withColumnRenamed("Count","in-degree").orderBy(desc("Count"),asc("Target"))
display(df_in_degree.limit(1))

// find node with highest out-degree, if two or more nodes have the same out-degree, report the one with the lowest node id
val df_out_degree = df_edge_by_weight.groupBy("Source").count().withColumnRenamed("Count","out-degree").orderBy(desc("Count"),asc("Source"))
display(df_out_degree.limit(1))

// find node with highest total degree, if two or more nodes have the same total degree, report the one with the lowest node id
val resultDF = df_in_degree.join(df_out_degree, df_in_degree("Target") === df_out_degree("Source")).na.fill(0)
val result_total = resultDF.withColumn("total-degree",resultDF("in-degree") + resultDF("out-degree")).orderBy(desc("total-degree")).withColumnRenamed("Source","node").drop(("Target")).orderBy(desc("total-degree"))

display(result_total.limit(1))

// COMMAND ----------

/*
Create a dataframe to store your results
Schema: 3 columns, named: 'v', 'd', 'c' where:
'v' : vertex id
'd' : degree calculation (an integer value.  one row with highest in-degree, a row w/ highest out-degree, a row w/ highest total degree )
'c' : category of degree, containing one of three string values:
                                                'i' : in-degree
                                                'o' : out-degree                                                
                                                't' : total-degree
- Your output should contain exactly three rows.  
- Your output should contain exactly the column order specified.
- The order of rows does not matter.
                                                
A correct output would be:

v    d    c
1    3    i
2    3    o
2    6    t

whereas:
- Node 1 has highest in-degree with a value of 3
- Node 2 has highest out-degree with a value of 3
- Node 2 has highest total degree with a value of 6
*/


val report_total_degree = result_total.orderBy($"total-degree".desc, $"node").limit(1).withColumn("c", lit("t")).withColumnRenamed("total-degree", "d").withColumnRenamed("node", "v")

val report_in_degree = result_total.orderBy($"in-degree".desc, $"node").limit(1).withColumn("c", lit("i")).withColumnRenamed("in-degree", "d").withColumnRenamed("node", "v")

val report_out_degree = result_total.orderBy($"out-degree".desc, $"node").limit(1).withColumn("c", lit("o")).withColumnRenamed("out-degree", "d").withColumnRenamed("node", "v")

val report_result_total = report_in_degree.select("v", "d", "c").union(report_out_degree.select("v", "d", "c")).union(report_total_degree.select("v", "d", "c"))

display(report_result_total)


// COMMAND ----------



// COMMAND ----------


