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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/** Every Spark entry point to a table reaches {@link SparkCatalog#loadTable(Identifier)} once. */
@ExtendWith(ParameterizedTestExtension.class)
public class TestCatalogLoadObserver extends CatalogTestBase {

  /** One recorded call. Static because Spark creates the observer reflectively. */
  static final class Seen {
    private final String catalog;
    private final Identifier ident;
    private final Table table;
    private final Thread thread;

    Seen(String catalog, Identifier ident, Table table) {
      this.catalog = catalog;
      this.ident = ident;
      this.table = table;
      this.thread = Thread.currentThread();
    }
  }

  private static final ConcurrentLinkedQueue<Seen> SEEN = new ConcurrentLinkedQueue<>();
  private static final Map<String, Map<String, String>> INIT_PROPS = Maps.newConcurrentMap();

  public static class RecordingObserver implements CatalogLoadObserver {
    @Override
    public void initialize(String catalogName, Map<String, String> properties) {
      INIT_PROPS.put(catalogName, properties);
    }

    @Override
    public void tableLoaded(String catalogName, Identifier ident, Table table) {
      SEEN.add(new Seen(catalogName, ident, table));
    }
  }

  @BeforeEach
  @Override
  public void before() {
    // set before super.before(), which is the first use of the catalog in this Spark session
    String prefix = "spark.sql.catalog." + catalogName + ".";
    spark.conf().set(prefix + CatalogLoadObserver.IMPL_PROPERTY, RecordingObserver.class.getName());
    spark.conf().set(prefix + CatalogLoadObserver.PROPERTY_PREFIX + "custom-key", "custom-value");
    super.before();

    sql("CREATE TABLE %s (id BIGINT, data STRING) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    SEEN.clear();
  }

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    SEEN.clear();
  }

  /** Observations of the test table in the catalog under test. */
  private List<Seen> seen() {
    return SEEN.stream()
        .filter(s -> s.catalog.equals(catalogName) && s.ident.name().equals("table"))
        .collect(Collectors.toList());
  }

  @TestTemplate
  public void sqlSelectIsObservedOnceOnTheCallingThread() {
    sql("SELECT * FROM %s", tableName);

    assertThat(seen()).hasSize(1);
    assertThat(seen().get(0).thread).isSameAs(Thread.currentThread());
    assertThat(seen().get(0).table).isInstanceOf(SparkTable.class);
  }

  @TestTemplate
  public void cacheHitIsObservedToo() {
    sql("SELECT * FROM %s", tableName);
    sql("SELECT * FROM %s", tableName);

    List<Seen> seen = seen();
    assertThat(seen).hasSize(2);
    if (cachingCatalogEnabled()) {
      // the second load was served by CachingCatalog: same Iceberg table instance
      assertThat(((SparkTable) seen.get(1).table).table())
          .isSameAs(((SparkTable) seen.get(0).table).table());
    }
  }

  @TestTemplate
  public void dataFrameEntryPointsAreObservedOnceEach() {
    spark.table(tableName).collectAsList();
    assertThat(seen()).hasSize(1);

    spark.read().table(tableName).collectAsList();
    assertThat(seen()).hasSize(2);

    spark.read().format("iceberg").load(tableName).collectAsList();
    assertThat(seen()).hasSize(3);
  }

  @TestTemplate
  public void timeTravelIsObservedOnceEach() {
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();

    sql("SELECT * FROM %s VERSION AS OF %d", tableName, snapshotId);
    assertThat(seen()).hasSize(1);

    sql("SELECT * FROM %s TIMESTAMP AS OF current_timestamp()", tableName);
    assertThat(seen()).hasSize(2);

    // the time-travel selection is applied after the observed load
    assertThat(((SparkTable) seen().get(0).table).snapshotId()).isNull();
  }

  @TestTemplate
  public void writeTargetIsObserved() {
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    assertThat(seen()).hasSize(1);
  }

  @TestTemplate
  public void observerGetsTheInstanceLoadTableReturns() throws NoSuchTableException {
    TableCatalog tableCatalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);

    Table returned = tableCatalog.loadTable(Identifier.of(new String[] {"default"}, "table"));

    assertThat(seen()).hasSize(1);
    assertThat(seen().get(0).table).isSameAs(returned);
  }

  @TestTemplate
  public void missingTableIsNotObservedAndStillThrows() {
    TableCatalog tableCatalog =
        (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);

    assertThatThrownBy(
            () -> tableCatalog.loadTable(Identifier.of(new String[] {"default"}, "missing")))
        .isInstanceOf(NoSuchTableException.class);
    assertThat(SEEN).noneMatch(s -> s.ident.name().equals("missing"));
  }

  @TestTemplate
  public void observerReceivesOnlyItsOwnPrefixedProperties() {
    assertThat(INIT_PROPS.get(catalogName))
        .isEqualTo(ImmutableMap.of("custom-key", "custom-value"));
  }
}
