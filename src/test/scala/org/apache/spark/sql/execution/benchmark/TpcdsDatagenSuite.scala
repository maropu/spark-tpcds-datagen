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

import java.util.Properties

import org.scalatest.FunSuite

class TpcdsDatagenSuite extends FunSuite {

  test("conf") {
    val props = new Properties()
    // Set `java.home` to avoid some JVM exceptions
    props.setProperty("java.home", System.getenv("JAVA_HOME"))
    props.setProperty("spark.sql.dsdgen.scaleFactor", "3")
    props.setProperty("spark.sql.dsdgen.format", "csv")
    props.setProperty("spark.sql.dsdgen.overwrite", "true")
    props.setProperty("spark.sql.dsdgen.partitionTables", "true")
    props.setProperty("spark.sql.dsdgen.useDoubleForDecimal", "true")
    props.setProperty("spark.sql.dsdgen.clusterByPartitionColumns", "true")
    props.setProperty("spark.sql.dsdgen.filterOutNullPartitionValues", "true")
    props.setProperty("spark.sql.dsdgen.tableFilter", "testTable")
    props.setProperty("spark.sql.dsdgen.numPartitions", "12")
    System.setProperties(props)

    val conf = TpcdsConf()
    assert(conf.getInt("scaleFactor", 1) === 3)
    assert(conf.get("format", "parquet") === "csv")
    assert(conf.getBoolean("overwrite", false) === true)
    assert(conf.getBoolean("partitionTables", false) === true)
    assert(conf.getBoolean("useDoubleForDecimal", false) === true)
    assert(conf.getBoolean("clusterByPartitionColumns", false) === true)
    assert(conf.getBoolean("filterOutNullPartitionValues", false) === true)
    assert(conf.get("tableFilter", "") === "testTable")
    assert(conf.getInt("numPartitions", 100) === 12)
  }
}
