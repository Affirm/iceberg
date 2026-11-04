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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A load observer neither causes nor masks the caching-catalog stale read of apache/iceberg#10493.
 *
 * <p>With {@code cache-enabled=true}, a MERGE that runs longer than the cache expiration commits
 * through the table instance it resolved at analysis, while the cache entry for that instance has
 * expired. If anything loads the table during that window (for example a listener thread), the
 * cache is repopulated with a pre-commit instance and the next read after the MERGE is stale. The
 * observer runs after a load and never loads, so it must not change either outcome:
 *
 * <ul>
 *   <li>no background load: never stale, with or without the observer;
 *   <li>background load mid-MERGE (positive control): stale, with or without the observer.
 * </ul>
 */
public class TestCatalogLoadObserverStaleRead extends TestBase {
  private static final long CACHE_TTL_MS = 2_000;
  private static final long MERGE_SLEEP_MS = 3 * CACHE_TTL_MS;
  private static final int TRIALS = 2;

  private static final AtomicInteger CATALOG_COUNTER = new AtomicInteger();
  private static final AtomicInteger OBSERVED = new AtomicInteger();
  private static final List<String> OBSERVER_THREADS = Lists.newCopyOnWriteArrayList();
  private static volatile CountDownLatch mergeRunning = new CountDownLatch(1);
  // Spark may evaluate the MERGE source more than once; only the first evaluation sleeps
  private static final AtomicBoolean SLEPT = new AtomicBoolean(false);

  @TempDir private Path warehouse;

  public static class CountingObserver implements CatalogLoadObserver {
    @Override
    public void tableLoaded(String catalogName, Identifier ident, Table table) {
      OBSERVED.incrementAndGet();
      OBSERVER_THREADS.add(Thread.currentThread().getName());
    }
  }

  @BeforeEach
  public void registerSlowUdf() {
    spark
        .udf()
        .register(
            "slow_id",
            (UDF1<Long, Long>)
                id -> {
                  mergeRunning.countDown();
                  if (SLEPT.compareAndSet(false, true)) {
                    Thread.sleep(MERGE_SLEEP_MS);
                  }
                  return id;
                },
            DataTypes.LongType);
  }

  @Test
  public void observerDoesNotCauseStaleRead() throws Exception {
    assertThat(trials(false, false)).as("observer off").containsOnly("new");
    assertThat(trials(true, false)).as("observer on").containsOnly("new");
    assertThat(OBSERVED.get()).isPositive();
  }

  @Test
  public void observerDoesNotMaskStaleRead() throws Exception {
    // positive control: proves this harness can see the stale read at all
    assertThat(trials(false, true)).as("observer off, background load").containsOnly("old");
    OBSERVER_THREADS.clear();
    assertThat(trials(true, true)).as("observer on, background load").containsOnly("old");
    // the observer also ran on the background thread, and did not reload anything there
    assertThat(OBSERVER_THREADS).contains("background-loader");
  }

  /** Runs TRIALS fresh MERGEs and returns what a read right after each MERGE saw. */
  private List<String> trials(boolean observer, boolean backgroundLoad) throws Exception {
    List<String> readBacks = Lists.newArrayList();
    for (int trial = 0; trial < TRIALS; trial++) {
      readBacks.add(trial(observer, backgroundLoad));
    }
    return ImmutableList.copyOf(readBacks);
  }

  private String trial(boolean observer, boolean backgroundLoad) throws Exception {
    String catalogName = "stale_" + CATALOG_COUNTER.incrementAndGet();
    String prefix = "spark.sql.catalog." + catalogName;
    Map<String, String> options =
        ImmutableMap.of(
            "type",
            "hadoop",
            "warehouse",
            "file:" + warehouse.resolve(catalogName),
            "cache-enabled",
            "true",
            "cache.expiration-interval-ms",
            String.valueOf(CACHE_TTL_MS));
    spark.conf().set(prefix, SparkCatalog.class.getName());
    options.forEach((key, value) -> spark.conf().set(prefix + "." + key, value));
    if (observer) {
      spark
          .conf()
          .set(prefix + "." + CatalogLoadObserver.IMPL_PROPERTY, CountingObserver.class.getName());
    }

    String table = catalogName + ".db.tbl";
    sql("CREATE NAMESPACE IF NOT EXISTS %s.db", catalogName);
    sql("CREATE TABLE %s (id BIGINT, data STRING) USING iceberg", table);
    sql("INSERT INTO %s VALUES (1, 'old'), (2, 'old')", table);

    TableCatalog tableCatalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    Identifier ident = Identifier.of(new String[] {"db"}, "tbl");

    mergeRunning = new CountDownLatch(1);
    SLEPT.set(false);
    AtomicBoolean mergeDone = new AtomicBoolean(false);
    Thread loader =
        new Thread(
            () -> {
              try {
                // wait until the instance resolved by MERGE analysis has expired from the cache
                if (!mergeRunning.await(60, TimeUnit.SECONDS)) {
                  return;
                }
                Thread.sleep(CACHE_TTL_MS + 500);
                while (!mergeDone.get()) {
                  tableCatalog.loadTable(ident);
                  Thread.sleep(250);
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            },
            "background-loader");
    if (backgroundLoad) {
      loader.start();
    }

    sql(
        "MERGE INTO %s t USING (SELECT slow_id(1L) AS id, 'new' AS data) s ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET t.data = s.data",
        table);
    mergeDone.set(true);
    if (backgroundLoad) {
      loader.join();
    }

    return (String) sql("SELECT data FROM %s WHERE id = 1", table).get(0)[0];
  }
}
