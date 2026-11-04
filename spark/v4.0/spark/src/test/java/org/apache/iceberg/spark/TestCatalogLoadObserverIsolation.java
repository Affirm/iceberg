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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A broken or misconfigured observer never affects loads, and only sees what it is given. */
public class TestCatalogLoadObserverIsolation extends TestBase {
  private static final AtomicInteger CATALOG_COUNTER = new AtomicInteger();
  private static final List<Table> SEEN = Lists.newCopyOnWriteArrayList();
  private static volatile Map<String, String> initProps = null;

  @TempDir private Path warehouse;

  public static class RecordingObserver implements CatalogLoadObserver {
    @Override
    public void initialize(String catalogName, Map<String, String> properties) {
      initProps = properties;
    }

    @Override
    public void tableLoaded(String catalogName, Identifier ident, Table table) {
      SEEN.add(table);
    }
  }

  public static class ThrowingObserver implements CatalogLoadObserver {
    @Override
    public void tableLoaded(String catalogName, Identifier ident, Table table) {
      throw new IllegalStateException("observer bug");
    }
  }

  public static class LinkageErrorObserver implements CatalogLoadObserver {
    @Override
    public void tableLoaded(String catalogName, Identifier ident, Table table) {
      throw new NoClassDefFoundError("missing/Dependency");
    }
  }

  public static class ConstructorFailsObserver implements CatalogLoadObserver {
    public ConstructorFailsObserver() {
      throw new IllegalStateException("cannot construct");
    }

    @Override
    public void tableLoaded(String catalogName, Identifier ident, Table table) {}
  }

  public static class InitializeFailsObserver implements CatalogLoadObserver {
    @Override
    public void initialize(String catalogName, Map<String, String> properties) {
      throw new IllegalStateException("cannot initialize");
    }

    @Override
    public void tableLoaded(String catalogName, Identifier ident, Table table) {
      SEEN.add(table);
    }
  }

  /** Configures a new hadoop catalog (new name = new instance) and returns its table name. */
  private String newCatalogWithTable(Map<String, String> extraOptions) {
    String name = "obs_" + CATALOG_COUNTER.incrementAndGet();
    String prefix = "spark.sql.catalog." + name;
    spark.conf().set(prefix, SparkCatalog.class.getName());
    spark.conf().set(prefix + ".type", "hadoop");
    spark.conf().set(prefix + ".warehouse", "file:" + warehouse.resolve(name));
    extraOptions.forEach((key, value) -> spark.conf().set(prefix + "." + key, value));

    String table = name + ".db.tbl";
    sql("CREATE NAMESPACE IF NOT EXISTS %s.db", name);
    sql("CREATE TABLE %s (id BIGINT, data STRING) USING iceberg", table);
    sql("INSERT INTO %s VALUES (1, 'a')", table);
    SEEN.clear();
    return table;
  }

  private String newCatalogWithObserver(String impl) {
    return newCatalogWithTable(
        ImmutableMap.of("cache-enabled", "false", CatalogLoadObserver.IMPL_PROPERTY, impl));
  }

  private void assertReadsWork(String table) {
    for (int i = 0; i < 3; i++) {
      assertThat(sql("SELECT * FROM %s", table)).containsExactly(row(1L, "a"));
    }
  }

  @Test
  public void throwingObserverDoesNotFailReadsOrWrites() {
    String table = newCatalogWithObserver(ThrowingObserver.class.getName());
    assertReadsWork(table);
    sql("INSERT INTO %s VALUES (2, 'b')", table);
    assertThat(sql("SELECT count(*) FROM %s", table)).containsExactly(row(2L));
  }

  @Test
  public void linkageErrorInObserverDoesNotFailReads() {
    assertReadsWork(newCatalogWithObserver(LinkageErrorObserver.class.getName()));
  }

  @Test
  public void failingConstructorFallsBackToNoop() {
    assertReadsWork(newCatalogWithObserver(ConstructorFailsObserver.class.getName()));
    assertThat(
            CatalogLoadObservers.load(
                "c",
                options(
                    CatalogLoadObserver.IMPL_PROPERTY, ConstructorFailsObserver.class.getName())))
        .isSameAs(CatalogLoadObserver.NOOP);
  }

  @Test
  public void failingInitializeFallsBackToNoop() {
    assertReadsWork(newCatalogWithObserver(InitializeFailsObserver.class.getName()));
    assertThat(SEEN).isEmpty();
  }

  @Test
  public void unknownClassFallsBackToNoop() {
    assertReadsWork(newCatalogWithObserver("com.example.DoesNotExist"));
    assertThat(
            CatalogLoadObservers.load(
                "c", options(CatalogLoadObserver.IMPL_PROPERTY, "com.example.DoesNotExist")))
        .isSameAs(CatalogLoadObserver.NOOP);
  }

  @Test
  public void noopWhenUnset() {
    assertThat(CatalogLoadObservers.load("c", CaseInsensitiveStringMap.empty()))
        .isSameAs(CatalogLoadObserver.NOOP);
    assertThat(CatalogLoadObservers.load("c", options(CatalogLoadObserver.IMPL_PROPERTY, "")))
        .isSameAs(CatalogLoadObserver.NOOP);

    String table = newCatalogWithTable(ImmutableMap.of("cache-enabled", "false"));
    assertReadsWork(table);
    assertThat(SEEN).isEmpty();
  }

  @Test
  public void credentialsAndOtherCatalogPropertiesAreNotPassed() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                "uri",
                "http://localhost:8181",
                "credential",
                "client-id:client-secret",
                "token",
                "secret-token",
                "warehouse",
                "s3://bucket/path",
                CatalogLoadObserver.IMPL_PROPERTY,
                RecordingObserver.class.getName(),
                "load-observer.env-keys",
                "SOME_ENV"));

    CatalogLoadObserver observer = CatalogLoadObservers.load("c", options);

    assertThat(observer).isInstanceOf(RecordingObserver.class);
    assertThat(initProps).isEqualTo(ImmutableMap.of("env-keys", "SOME_ENV"));
  }

  @Test
  public void cachingCatalogHitIsObserved() throws Exception {
    String table =
        newCatalogWithTable(
            ImmutableMap.of(
                "cache-enabled",
                "true",
                "cache.expiration-interval-ms",
                "60000",
                CatalogLoadObserver.IMPL_PROPERTY,
                RecordingObserver.class.getName()));

    sql("SELECT * FROM %s", table);
    sql("SELECT * FROM %s", table);

    assertThat(SEEN).hasSize(2);
    assertThat(((SparkTable) SEEN.get(1)).table()).isSameAs(((SparkTable) SEEN.get(0)).table());
  }

  @Test
  public void loggingObserverFormatsOneLineFromTheLoadedTable() throws Exception {
    String table =
        newCatalogWithTable(
            ImmutableMap.of(
                "cache-enabled",
                "false",
                CatalogLoadObserver.IMPL_PROPERTY,
                LoggingCatalogLoadObserver.class.getName()));
    String catalogName = table.substring(0, table.indexOf('.'));
    sql("ALTER TABLE %s SET TBLPROPERTIES ('team'='team-a')", table);
    assertReadsWork(table);

    LoggingCatalogLoadObserver observer = new LoggingCatalogLoadObserver();
    observer.initialize(
        catalogName,
        ImmutableMap.of(
            LoggingCatalogLoadObserver.ENV_KEYS, "OBSERVER_TEST_UNSET_VARIABLE",
            LoggingCatalogLoadObserver.TABLE_PROPERTIES, "team,missing"));
    TableCatalog tableCatalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    Identifier ident = Identifier.of(new String[] {"db"}, "tbl");
    Table loaded = tableCatalog.loadTable(ident);

    String line = observer.format(catalogName, ident, loaded);

    assertThat(line)
        .startsWith(LoggingCatalogLoadObserver.PREFIX + " catalog=" + catalogName + " ident=db.tbl")
        .contains(" prop.team=team-a", " prop.missing=null")
        .contains(" app-id=" + spark.sparkContext().applicationId())
        .contains(" thread=" + Thread.currentThread().getName())
        .contains(" app-name=" + spark.sparkContext().appName())
        .contains(" env.OBSERVER_TEST_UNSET_VARIABLE=null")
        .doesNotContain("\n");

    Table branch = tableCatalog.loadTable(Identifier.of(new String[] {"db", "tbl"}, "branch_main"));
    assertThat(observer.format(catalogName, ident, branch)).contains(" ref=main");
  }

  private static CaseInsensitiveStringMap options(String key, String value) {
    return new CaseInsensitiveStringMap(ImmutableMap.of(key, value));
  }
}
