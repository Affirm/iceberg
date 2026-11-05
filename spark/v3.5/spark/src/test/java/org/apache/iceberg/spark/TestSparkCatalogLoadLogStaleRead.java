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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The load log line neither causes nor masks the caching-catalog stale read of
 * apache/iceberg#10493.
 *
 * <p>With {@code cache-enabled=true}, a MERGE that runs longer than the cache expiration commits
 * through the table instance it resolved at analysis, while the cache entry for that instance has
 * expired. If anything loads the table during that window, the cache is repopulated with a
 * pre-commit instance and the next read after the MERGE is stale. Writing the line happens after a
 * load and never loads, so it must not change either outcome:
 *
 * <ul>
 *   <li>no background load: not stale, with or without the line;
 *   <li>background load mid-MERGE (positive control): stale, with or without the line.
 * </ul>
 */
public class TestSparkCatalogLoadLogStaleRead extends TestBase {
  private static final long CACHE_TTL_MS = 2_000;
  private static final long MERGE_SLEEP_MS = 3 * CACHE_TTL_MS;

  private static final AtomicInteger CATALOG_COUNTER = new AtomicInteger();
  private static volatile CountDownLatch mergeRunning = new CountDownLatch(1);
  // Spark may evaluate the MERGE source more than once; only the first evaluation sleeps
  private static final AtomicBoolean SLEPT = new AtomicBoolean(false);

  @TempDir private Path warehouse;

  private TestSparkCatalogLoadLog.LoadLogCapture capture = null;

  @BeforeEach
  public void before() {
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
    this.capture = TestSparkCatalogLoadLog.LoadLogCapture.start();
  }

  @AfterEach
  public void after() {
    capture.stop();
  }

  @Test
  public void loadLogDoesNotCauseStaleRead() throws Exception {
    assertThat(trial(false, false)).as("line off").isEqualTo("new");
    assertThat(trial(true, false)).as("line on").isEqualTo("new");
    assertThat(capture.lines()).isNotEmpty();
  }

  @Test
  public void loadLogDoesNotMaskStaleRead() throws Exception {
    // positive control: proves this harness can see the stale read at all
    assertThat(trial(false, true)).as("line off, background load").isEqualTo("old");
    assertThat(trial(true, true)).as("line on, background load").isEqualTo("old");
    // the line was also written on the background thread, which did not reload anything extra
    assertThat(capture.lines()).anyMatch(line -> line.endsWith(" thread=background-loader"));
  }

  /** Runs one MERGE in a fresh catalog and returns what a read right after it saw. */
  private String trial(boolean loadLogEnabled, boolean backgroundLoad) throws Exception {
    String catalogName = "stale_" + CATALOG_COUNTER.incrementAndGet();
    String prefix = "spark.sql.catalog." + catalogName;
    spark.conf().set(prefix, SparkCatalog.class.getName());
    spark.conf().set(prefix + ".type", "hadoop");
    spark.conf().set(prefix + ".warehouse", "file:" + warehouse.resolve(catalogName));
    spark.conf().set(prefix + ".cache-enabled", "true");
    spark.conf().set(prefix + ".cache.expiration-interval-ms", String.valueOf(CACHE_TTL_MS));
    spark.conf().set(prefix + "." + SparkCatalog.LOAD_LOG_ENABLED, String.valueOf(loadLogEnabled));

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

    if (!loadLogEnabled) {
      assertThat(capture.lines()).noneMatch(line -> line.contains(" catalog=" + catalogName + " "));
    }

    return (String) sql("SELECT data FROM %s WHERE id = 1", table).get(0)[0];
  }
}
