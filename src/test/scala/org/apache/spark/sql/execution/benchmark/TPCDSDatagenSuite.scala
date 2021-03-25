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

import java.io.{File, FilenameFilter}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

class TPCDSDatagenSuite extends SparkFunSuite with SharedSparkSession {

  test("datagen") {
    val outputTempDir = Utils.createTempDir()
    val tpcdsTables = new Tables(spark.sqlContext, 1)
    tpcdsTables.genData(
      location = outputTempDir.getAbsolutePath,
      format = "parquet",
      overwrite = false,
      partitionTables = false,
      useDoubleForDecimal = false,
      useStringForChar = false,
      clusterByPartitionColumns = false,
      filterOutNullPartitionValues = false,
      tableFilter = Set.empty,
      numPartitions = 4)

    val tpcdsExpectedTables = Set(
      "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
      "customer_address", "customer_demographics", "date_dim", "household_demographics",
      "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
      "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
      "web_sales", "web_site")

    assert(outputTempDir.list.toSet === tpcdsExpectedTables)

    // Checks if output test data generated in each table
    val filenameFilter = new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.endsWith(".parquet")
    }
    tpcdsExpectedTables.foreach { table =>
      val f = new File(s"${outputTempDir.getAbsolutePath}/$table").listFiles(filenameFilter)
      assert(f.size === 1)
    }
  }

  // TODO: Adds tests to check the schemas of generated tables
}
