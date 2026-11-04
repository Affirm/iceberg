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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Loads a {@link CatalogLoadObserver} from catalog options and calls it without ever failing. */
final class CatalogLoadObservers {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogLoadObservers.class);

  // Log the first observer failure, then one in every 1000, so a broken observer cannot flood logs.
  private static final long FAILURE_LOG_EVERY = 1000;
  private static final AtomicLong FAILURES = new AtomicLong();

  private CatalogLoadObservers() {}

  /**
   * Returns the configured observer, or {@link CatalogLoadObserver#NOOP} if none is configured or
   * it cannot be created. A misconfigured observer is logged at WARN and does not fail catalog
   * initialization, because the observer is for detection only.
   */
  static CatalogLoadObserver load(String catalogName, CaseInsensitiveStringMap options) {
    String impl = options.get(CatalogLoadObserver.IMPL_PROPERTY);
    if (impl == null || impl.isEmpty()) {
      return CatalogLoadObserver.NOOP;
    }

    try {
      CatalogLoadObserver observer =
          DynConstructors.builder(CatalogLoadObserver.class)
              .loader(CatalogLoadObservers.class.getClassLoader())
              .impl(impl)
              .<CatalogLoadObserver>buildChecked()
              .newInstance();
      observer.initialize(catalogName, observerProperties(options));
      LOG.info("Catalog {}: using load observer {}", catalogName, impl);
      return observer;
    } catch (Exception | LinkageError e) {
      LOG.warn(
          "Catalog {}: cannot create load observer {}; loads are not observed",
          catalogName,
          impl,
          e);
      return CatalogLoadObserver.NOOP;
    }
  }

  /** Calls the observer; any exception or linkage error is swallowed and rate-limit logged. */
  static void notifySafely(
      CatalogLoadObserver observer, String catalogName, Identifier ident, Table table) {
    if (observer == CatalogLoadObserver.NOOP) {
      return;
    }

    try {
      observer.tableLoaded(catalogName, ident, table);
    } catch (RuntimeException | LinkageError e) {
      long count = FAILURES.incrementAndGet();
      if (count == 1 || count % FAILURE_LOG_EVERY == 0) {
        LOG.warn("Load observer failed ({} failures so far); the load is unaffected", count, e);
      }
    }
  }

  /** Only the {@code load-observer.*} options, prefix stripped, so credentials never reach it. */
  @VisibleForTesting
  static Map<String, String> observerProperties(CaseInsensitiveStringMap options) {
    Map<String, String> props = Maps.newHashMap();
    options
        .asCaseSensitiveMap()
        .forEach(
            (key, value) -> {
              if (key.startsWith(CatalogLoadObserver.PROPERTY_PREFIX)) {
                props.put(key.substring(CatalogLoadObserver.PROPERTY_PREFIX.length()), value);
              }
            });
    return props;
  }
}
