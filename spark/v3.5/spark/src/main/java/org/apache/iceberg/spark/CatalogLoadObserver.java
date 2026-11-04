/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark;

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;

/**
 * Observes successful {@link SparkCatalog#loadTable(Identifier)} calls.
 *
 * <p>Enabled with the catalog property {@code load-observer-impl}, set to the class name of an
 * implementation with a public no-arg constructor. Unset means no observer and no overhead.
 *
 * <p>Contract for implementations:
 *
 * <ul>
 *   <li>Observe only. Do not call {@code loadTable}, {@code refresh}, {@code invalidateTable} or
 *       any other catalog method from {@link #tableLoaded}; read only what is on the {@code table}
 *       argument. A second load from inside the hook can repopulate a caching catalog with a stale
 *       table (apache/iceberg#10493).
 *   <li>Do not block. The hook runs synchronously on the thread that called {@code loadTable},
 *       which is usually query analysis. Hand anything slow (I/O, network) to a bounded queue that
 *       drops when full.
 *   <li>Do not assume a thread. {@code loadTable} can be called from any thread, including Spark
 *       listener threads; the hook runs on whichever one called it.
 *   <li>Exceptions are caught and logged by the caller and never fail the load.
 * </ul>
 */
public interface CatalogLoadObserver {

  /** Catalog property naming the implementation class. */
  String IMPL_PROPERTY = "load-observer-impl";

  /** Prefix of catalog properties passed to {@link #initialize}; the prefix is stripped. */
  String PROPERTY_PREFIX = "load-observer.";

  CatalogLoadObserver NOOP = (catalogName, ident, table) -> {};

  /**
   * Called once when the catalog is initialized.
   *
   * @param catalogName Spark's catalog name
   * @param properties only the catalog properties under {@link #PROPERTY_PREFIX}, prefix stripped
   */
  default void initialize(String catalogName, Map<String, String> properties) {}

  /**
   * Called after a successful load, with the Spark table that is about to be returned.
   *
   * @param catalogName Spark's catalog name
   * @param ident the identifier as passed to {@code loadTable}, possibly a {@link PathIdentifier}
   * @param table the loaded table; for Iceberg tables an instance of {@code SparkTable} or {@code
   *     SparkChangelogTable}
   */
  void tableLoaded(String catalogName, Identifier ident, Table table);
}
