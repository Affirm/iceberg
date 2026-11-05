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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

/** With {@code load-log-enabled=true}, each successful loadTable writes exactly one line. */
@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkCatalogLoadLog extends CatalogTestBase {
  private static final AtomicInteger CATALOG_COUNTER = new AtomicInteger();
  private static final String UNSET_ENV = "ICEBERG_TEST_DEFINITELY_UNSET_VAR";
  private static final String CONF_KEY = "spark.test.load.log.conf";
  private static final String EMPTY_CONF_KEY = "spark.test.load.log.empty";

  private LoadLogCapture capture = null;

  /**
   * Records load log lines, and can make writing them throw. The Spark test classpath binds SLF4J
   * to slf4j-simple, which prints each event with {@code System.err.println(String)}.
   */
  static final class LoadLogCapture extends PrintStream {
    private final PrintStream original;
    private final List<String> lines = Lists.newCopyOnWriteArrayList();
    private volatile Throwable failure = null;

    private LoadLogCapture(PrintStream original) {
      super(original, true);
      this.original = original;
    }

    static LoadLogCapture start() {
      assertThat(LoggerFactory.getILoggerFactory().getClass().getName())
          .as("log capture expects slf4j-simple on the test classpath")
          .startsWith("org.slf4j.simple.");
      LoadLogCapture capture = new LoadLogCapture(System.err);
      System.setErr(capture);
      return capture;
    }

    void stop() {
      System.setErr(original);
    }

    /** Makes writing a load log line (or its failure warning) throw this. */
    void failWith(Throwable throwable) {
      this.failure = throwable;
    }

    List<String> lines() {
      return lines;
    }

    List<String> linesFor(String catalog, String ident) {
      return lines.stream()
          .filter(line -> line.contains(" catalog=" + catalog + " ident=" + ident + " "))
          .collect(Collectors.toList());
    }

    @Override
    public void println(String line) {
      if (line != null) {
        int start = line.indexOf(SparkCatalog.LOAD_LOG_PREFIX);
        if (start >= 0) {
          lines.add(line.substring(start));
        }

        if (start >= 0 || line.contains("Cannot write table load log line")) {
          Throwable toThrow = failure;
          if (toThrow instanceof RuntimeException) {
            throw (RuntimeException) toThrow;
          } else if (toThrow instanceof Error) {
            throw (Error) toThrow;
          }
        }
      }

      original.println(line);
    }
  }

  @BeforeEach
  @Override
  public void before() {
    // set before super.before(), which is the first use of the catalog in this Spark session
    spark
        .conf()
        .set("spark.sql.catalog." + catalogName + "." + SparkCatalog.LOAD_LOG_ENABLED, "true");
    super.before();

    sql("CREATE TABLE %s (id BIGINT, data STRING) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    this.capture = LoadLogCapture.start();
  }

  @AfterEach
  public void dropTable() {
    if (capture != null) {
      capture.stop();
    }

    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  private List<String> lines() {
    return capture.linesFor(catalogName, "default.table");
  }

  private TableCatalog tableCatalog() {
    return (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }

  @TestTemplate
  public void selectLogsOneLineWithTheExpectedFields() throws NoSuchTableException {
    assertThat(sql("SELECT * FROM %s", tableName)).containsExactly(row(1L, "a"));

    assertThat(lines()).hasSize(1);
    String line = lines().get(0);
    String loadedName =
        tableCatalog().loadTable(Identifier.of(new String[] {"default"}, "table")).name();
    // without the extra-field options the line is exactly these fields
    assertThat(line)
        .isEqualTo(
            SparkCatalog.LOAD_LOG_PREFIX
                + " catalog="
                + catalogName
                + " ident=default.table table="
                + loadedName
                + " app-id="
                + spark.sparkContext().applicationId()
                + " thread="
                + Thread.currentThread().getName());
    assertThat(line)
        .startsWith(
            SparkCatalog.LOAD_LOG_PREFIX
                + " catalog="
                + catalogName
                + " ident=default.table table=")
        .contains("default.table app-id=" + spark.sparkContext().applicationId() + " ")
        .endsWith(" thread=" + Thread.currentThread().getName())
        .doesNotContain("snapshot-id=", "ref=", "\n");
  }

  @TestTemplate
  public void cacheHitIsLoggedToo() {
    sql("SELECT * FROM %s", tableName);
    sql("SELECT * FROM %s", tableName);

    assertThat(lines()).hasSize(2);
  }

  @TestTemplate
  public void timeTravelIsLoggedOnceEachBeforeTheSelectionIsApplied() {
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();

    sql("SELECT * FROM %s VERSION AS OF %d", tableName, snapshotId);
    assertThat(lines()).hasSize(1);

    sql("SELECT * FROM %s TIMESTAMP AS OF current_timestamp()", tableName);
    assertThat(lines()).hasSize(2);

    assertThat(lines()).noneMatch(line -> line.contains("snapshot-id="));
  }

  @TestTemplate
  public void writeTargetIsLoggedOnce() {
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    assertThat(lines()).hasSize(1);
  }

  @TestTemplate
  public void snapshotOrBranchSelectedByTheIdentifierIsLogged() throws NoSuchTableException {
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    String[] namespace = {"default", "table"};

    tableCatalog().loadTable(Identifier.of(namespace, "snapshot_id_" + snapshotId));
    tableCatalog().loadTable(Identifier.of(namespace, "branch_main"));

    assertThat(capture.linesFor(catalogName, "default.table.snapshot_id_" + snapshotId))
        .singleElement()
        .asString()
        .contains(" snapshot-id=" + snapshotId + " ");
    assertThat(capture.linesFor(catalogName, "default.table.branch_main"))
        .singleElement()
        .asString()
        .contains(" ref=main ");
  }

  @TestTemplate
  public void missingTableIsNotLoggedAndStillThrows() {
    assertThatThrownBy(
            () -> tableCatalog().loadTable(Identifier.of(new String[] {"default"}, "missing")))
        .isInstanceOf(NoSuchTableException.class);

    assertThat(capture.lines()).noneMatch(line -> line.contains(" ident=default.missing "));
  }

  @TestTemplate
  public void offByDefaultAndDoesNotChangeResults() {
    // two catalogs over the same warehouse, one with the line enabled and one with defaults
    File sharedWarehouse = temp.resolve("shared-" + CATALOG_COUNTER.incrementAndGet()).toFile();
    String on = newHadoopCatalog("loadlog_on", sharedWarehouse, true);
    String off = newHadoopCatalog("loadlog_off", sharedWarehouse, false);
    sql("CREATE NAMESPACE IF NOT EXISTS %s.db", on);
    sql("CREATE TABLE %s.db.tbl (id BIGINT, data STRING) USING iceberg", on);
    sql("INSERT INTO %s.db.tbl VALUES (1, 'a'), (2, 'b')", on);

    List<Object[]> withLine = sql("SELECT * FROM %s.db.tbl ORDER BY id", on);
    List<Object[]> withoutLine = sql("SELECT * FROM %s.db.tbl ORDER BY id", off);

    assertThat(withLine).containsExactly(row(1L, "a"), row(2L, "b"));
    assertThat(withoutLine).containsExactly(withLine.toArray(new Object[0][]));
    assertThat(capture.linesFor(on, "db.tbl")).isNotEmpty();
    assertThat(capture.lines()).noneMatch(line -> line.contains(" catalog=" + off + " "));
  }

  @TestTemplate
  public void failureWritingTheLineDoesNotFailTheLoad() {
    capture.failWith(new IllegalStateException("injected logging failure"));
    assertThat(sql("SELECT * FROM %s", tableName)).containsExactly(row(1L, "a"));
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    capture.failWith(new NoClassDefFoundError("injected/MissingClass"));
    assertThat(sql("SELECT * FROM %s ORDER BY id", tableName))
        .containsExactly(row(1L, "a"), row(2L, "b"));

    // the line was attempted each time, and threw
    assertThat(lines()).hasSizeGreaterThanOrEqualTo(3);
  }

  @TestTemplate
  public void envKeysAddSetVariablesInOrderAndOmitUnsetOnes() {
    String path = System.getenv("PATH");
    String home = System.getenv("HOME");
    assertThat(path).as("PATH must be set for this test").isNotEmpty();
    assertThat(home).as("HOME must be set for this test").isNotEmpty();
    assertThat(System.getenv(UNSET_ENV)).isNull();

    // whitespace around names and empty entries are ignored
    String line = lineWithOptions(" PATH , ," + UNSET_ENV + ",, HOME ", null);

    assertThat(line)
        .endsWith(threadField() + " env.PATH=" + path + " env.HOME=" + home)
        .doesNotContain(UNSET_ENV, "=null", "conf.");
  }

  @TestTemplate
  public void confKeysAddSetKeysInOrderAndOmitUnsetOrEmptyOnes() {
    String appName = spark.conf().get("spark.app.name");
    String line =
        lineWithOptions(
            null,
            CONF_KEY + ", spark.test.load.log.unset,," + EMPTY_CONF_KEY + " , spark.app.name");

    assertThat(line)
        .endsWith(threadField() + " conf." + CONF_KEY + "=somevalue conf.spark.app.name=" + appName)
        .doesNotContain("spark.test.load.log.unset", EMPTY_CONF_KEY, "=null", "env.");
  }

  @TestTemplate
  public void envFieldsComeBeforeConfFieldsInTheGivenOrder() {
    String line = lineWithOptions("HOME,PATH", "spark.app.name," + CONF_KEY);

    assertThat(line)
        .endsWith(
            threadField()
                + " env.HOME="
                + System.getenv("HOME")
                + " env.PATH="
                + System.getenv("PATH")
                + " conf.spark.app.name="
                + spark.conf().get("spark.app.name")
                + " conf."
                + CONF_KEY
                + "=somevalue");
  }

  private static String threadField() {
    return " thread=" + Thread.currentThread().getName();
  }

  /**
   * Creates a new catalog with the line enabled and the given extra-field options, reads its table
   * once, and returns the line written for that read. The Spark configuration keys used by the
   * tests are set before the catalog is initialized, because they are read only at initialize.
   */
  private String lineWithOptions(String envKeys, String confKeys) {
    spark.conf().set(CONF_KEY, "somevalue");
    spark.conf().set(EMPTY_CONF_KEY, "");
    try {
      File catalogWarehouse = temp.resolve("fields-" + CATALOG_COUNTER.incrementAndGet()).toFile();
      String name = newHadoopCatalog("loadlog_fields", catalogWarehouse, true);
      String conf = "spark.sql.catalog." + name + ".";
      if (envKeys != null) {
        spark.conf().set(conf + SparkCatalog.LOAD_LOG_ENV_KEYS, envKeys);
      }

      if (confKeys != null) {
        spark.conf().set(conf + SparkCatalog.LOAD_LOG_CONF_KEYS, confKeys);
      }

      sql("CREATE NAMESPACE IF NOT EXISTS %s.db", name);
      sql("CREATE TABLE %s.db.tbl (id BIGINT) USING iceberg", name);
      int before = capture.linesFor(name, "db.tbl").size();
      sql("SELECT * FROM %s.db.tbl", name);

      List<String> lines = capture.linesFor(name, "db.tbl");
      assertThat(lines).hasSize(before + 1);
      String line = lines.get(lines.size() - 1);
      assertThat(line).startsWith(SparkCatalog.LOAD_LOG_PREFIX + " catalog=" + name + " ");
      assertThat(line.split(" thread=", -1)).hasSize(2);
      return line;
    } finally {
      spark.conf().unset(CONF_KEY);
      spark.conf().unset(EMPTY_CONF_KEY);
    }
  }

  private String newHadoopCatalog(String prefix, File catalogWarehouse, boolean loadLogEnabled) {
    String name = prefix + "_" + CATALOG_COUNTER.incrementAndGet();
    String conf = "spark.sql.catalog." + name;
    spark.conf().set(conf, SparkCatalog.class.getName());
    spark.conf().set(conf + ".type", "hadoop");
    spark.conf().set(conf + ".warehouse", "file:" + catalogWarehouse);
    if (loadLogEnabled) {
      spark.conf().set(conf + "." + SparkCatalog.LOAD_LOG_ENABLED, "true");
    }

    return name;
  }
}
