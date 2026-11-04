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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes one INFO line per observed load, starting with {@link #PREFIX} so it can be found and
 * parsed. Example:
 *
 * <pre>
 * iceberg-catalog-load catalog=my_catalog ident=db.events table=my_catalog.db.events
 *   app-id=app-123 thread=main app-name=nightly-report env.JOB=nightly prop.team=team-a
 * </pre>
 *
 * <p>{@code snapshot-id} or {@code ref} is only present when the identifier itself selects one (for
 * example {@code db.events.branch_audit}). For {@code VERSION AS OF} / {@code TIMESTAMP AS OF} the
 * observer sees the table before the time-travel selection is applied.
 *
 * <p>Options, all under {@code load-observer.}:
 *
 * <ul>
 *   <li>{@code env-keys}: comma-separated environment variable names to include, read once at
 *       initialize.
 *   <li>{@code table-properties}: comma-separated table property keys to include, read from the
 *       loaded table's in-memory metadata (no extra load, no refresh).
 *   <li>{@code dedup-window-ms}: log a given (catalog, identifier) at most once per window. Default
 *       0, which logs every load.
 * </ul>
 */
public class LoggingCatalogLoadObserver implements CatalogLoadObserver {
  public static final String PREFIX = "iceberg-catalog-load";
  public static final String ENV_KEYS = "env-keys";
  public static final String TABLE_PROPERTIES = "table-properties";
  public static final String DEDUP_WINDOW_MS = "dedup-window-ms";

  private static final Logger LOG = LoggerFactory.getLogger(LoggingCatalogLoadObserver.class);
  private static final Splitter COMMA = Splitter.on(',').trimResults().omitEmptyStrings();
  private static final long MAX_DEDUP_ENTRIES = 10_000;

  private String appId = null;
  private String staticFields = "";
  private List<String> tablePropertyKeys = ImmutableList.of();
  private Cache<String, Boolean> recent = null;

  @Override
  public void initialize(String catalogName, Map<String, String> properties) {
    this.appId = EnvironmentContext.get().get(CatalogProperties.APP_ID);

    StringBuilder fields = new StringBuilder();
    fields.append(" app-name=").append(appName());
    for (String key : COMMA.split(properties.getOrDefault(ENV_KEYS, ""))) {
      fields.append(" env.").append(key).append('=').append(System.getenv(key));
    }

    this.staticFields = fields.toString();
    this.tablePropertyKeys = COMMA.splitToList(properties.getOrDefault(TABLE_PROPERTIES, ""));

    long dedupMs = PropertyUtil.propertyAsLong(properties, DEDUP_WINDOW_MS, 0L);
    if (dedupMs > 0) {
      this.recent =
          Caffeine.newBuilder()
              .expireAfterWrite(dedupMs, TimeUnit.MILLISECONDS)
              .maximumSize(MAX_DEDUP_ENTRIES)
              .build();
    }
  }

  @Override
  public void tableLoaded(String catalogName, Identifier ident, Table table) {
    if (!LOG.isInfoEnabled()) {
      return;
    }

    if (recent != null
        && recent.asMap().putIfAbsent(catalogName + "/" + ident, Boolean.TRUE) != null) {
      return;
    }

    LOG.info(format(catalogName, ident, table));
  }

  String format(String catalogName, Identifier ident, Table table) {
    StringBuilder line = new StringBuilder(PREFIX);
    line.append(" catalog=").append(catalogName);
    line.append(" ident=").append(ident);
    line.append(" table=").append(table.name());

    // Read only from the object this load returned: snapshotId(), branch(), table() and
    // properties() are in-memory reads and never refresh or reload.
    if (table instanceof SparkTable) {
      SparkTable sparkTable = (SparkTable) table;
      if (sparkTable.snapshotId() != null) {
        line.append(" snapshot-id=").append(sparkTable.snapshotId());
      } else if (sparkTable.branch() != null) {
        line.append(" ref=").append(sparkTable.branch());
      }

      if (!tablePropertyKeys.isEmpty()) {
        Map<String, String> props = sparkTable.table().properties();
        for (String key : tablePropertyKeys) {
          line.append(" prop.").append(key).append('=').append(props.get(key));
        }
      }
    }

    line.append(" app-id=").append(appId);
    line.append(" thread=").append(Thread.currentThread().getName());
    line.append(staticFields);
    return line.toString();
  }

  private static String appName() {
    try {
      return SparkSession.active().sparkContext().appName();
    } catch (RuntimeException e) {
      return null;
    }
  }
}
