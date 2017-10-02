/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import java.io.File
import java.nio.file.Paths

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.benchmark.Utils._

class TPCDSQuerySuite extends SparkFunSuite with BeforeAndAfterEach {

  private val regenerateGoldenFiles = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  private val baseResourcePath = {
    // If regenerateGoldenFiles is true, we must be running this in SBT and we use hard-coded
    // relative path. Otherwise, we use classloader's getResource to find the location.
    //
    // if (regenerateGoldenFiles) {
    //   Paths.get("src", "test", "resources", "tpcds").toFile
    // } else {
    //   val res = getClass.getClassLoader.getResource("tpcds")
    //   new File(res.getFile)
    // }

    // TODO: Can't resolve this path by classloader's getResource
    Paths.get("src", "test", "resources", "tpcds").toFile
  }

  private val queryFilePath = new File(baseResourcePath, "queries")
  private val goldenFilePath = new File(baseResourcePath, "results")

  private val envVarDataLocationForEnablingTests = "SPARK_TPCDS_DATA_LOCATION"
  private val dataLocation = System.getenv(envVarDataLocationForEnablingTests)
  private val shouldRunTests = dataLocation != null

  /** Run the test if environment variable is set or ignore the test */
  def testIfEnabled(testName: String)(testBody: => Unit) {
    if (shouldRunTests) {
      test(testName)(testBody)
    } else {
      ignore(s"$testName [enable by setting env var $envVarDataLocationForEnablingTests]")(testBody)
    }
  }

  val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("TPCDSQuerySuite")
      .set("spark.sql.parquet.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.driver.memory", "8g")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
      .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)
      .set("spark.sql.crossJoin.enabled", "true")

  var _spark: SparkSession = _

  val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
    "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
    "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
    "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
    "time_dim", "web_page")

  def setupTables(dataLocation: String): Unit = {
    tables.foreach { tableName =>
      _spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
      tableName -> _spark.table(tableName).count()
    }
  }

  protected override def beforeAll(): Unit = {
    SparkSession.sqlListener.set(null)
    if (_spark == null && shouldRunTests) {
      _spark = SparkSession.builder.config(conf).getOrCreate()
      setupTables(dataLocation)
    }
    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    if (_spark != null) {
      _spark.sessionState.catalog.reset()
      _spark.stop()
      _spark = null
    }
  }

  // List of all TPC-DS queries
  val tpcdsQueries = Seq(
    "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
    "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
    "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29", "q30",
    "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b", "q40",
    "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
    "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
    "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
    "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
    "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
    "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")

  tpcdsQueries.foreach { name =>
    testIfEnabled(name) {
      val queryString = fileToString(new File(queryFilePath, s"$name.sql"))
      val df = _spark.sql(queryString)
      val output = formatOutput(df, Int.MaxValue - 1, Int.MaxValue, true).trim
      val resultFile = new File(goldenFilePath, s"$name.sql.out")

      val header = s"-- Automatically generated by ${getClass.getSimpleName}"

      if (regenerateGoldenFiles) {
        val goldenOutput =
          s"""$header
             |$output
           """.stripMargin
        val parent = resultFile.getParentFile
        if (!parent.exists()) {
          assert(parent.mkdirs(), "Could not create directory: " + parent)
        }
        stringToFile(resultFile, goldenOutput)
      }

      // Read back the golden file
      val expectedOutput = fileToString(resultFile).replace(s"$header\n", "").trim
      assert(expectedOutput === output)
    }
  }
}
