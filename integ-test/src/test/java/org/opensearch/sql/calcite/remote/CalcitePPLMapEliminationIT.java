/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for Calcite PPL MAP elimination optimization. Tests verify that the
 * MapEliminationRule correctly optimizes logical plans by removing unnecessary _dynamic_columns MAP
 * fields when only static fields are used.
 */
public class CalcitePPLMapEliminationIT extends PPLIntegTestCase {

  private static final String TEST_INDEX_DYNAMIC_COLUMNS = "test_dynamic_columns";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);
    loadIndex(Index.JSON_TEST);

    createDynamicColumnsTestData();
  }

  private void createDynamicColumnsTestData() throws IOException {
    createDocumentWithIdAndJsonData(
        TEST_INDEX_DYNAMIC_COLUMNS, 1, "{\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}");
    createDocumentWithIdAndJsonData(
        TEST_INDEX_DYNAMIC_COLUMNS, 2, "{\"name\": \"Jane\", \"age\": 25, \"country\": \"USA\"}");
    createDocumentWithIdAndJsonData(
        TEST_INDEX_DYNAMIC_COLUMNS, 3, "{\"product\": \"laptop\", \"price\": 999.99}");
  }

  private void createDocumentWithIdAndJsonData(String index, int id, String jsonContent)
      throws IOException {
    createDocumentWithIdAndJsonField(index, id, "json_data", jsonContent);
  }

  @Test
  public void testDynamicFieldAccessWithSpathPreservesMapOperations() throws IOException {
    String explain =
        explainQueryToYaml(
            source(TEST_INDEX_DYNAMIC_COLUMNS, "spath input=json_data | fields id, name"));
    assertEquals(
        "calcite:\n"
            + //
            "  logical: |\n"
            + //
            "    LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])\n"
            + //
            "      LogicalProject(id=[$1], name=[MAP_GET(COALESCE(MAP_MERGE(null:(VARCHAR NOT NULL,"
            + " ANY NOT NULL) MAP, JSON_EXTRACT_ALL($0)), JSON_EXTRACT_ALL($0)), 'name')])\n"
            + //
            "        CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_columns]])\n"
            + //
            "  physical: |\n"
            + //
            "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[null:(VARCHAR NOT NULL, ANY NOT NULL)"
            + " MAP], expr#3=[JSON_EXTRACT_ALL($t1)], expr#4=[MAP_MERGE($t2, $t3)],"
            + " expr#5=[COALESCE($t4, $t3)], expr#6=['name'], expr#7=[MAP_GET($t5, $t6)], id=[$t0],"
            + " $f1=[$t7])\n"
            + //
            "      CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_columns]],"
          + " PushDownContext=[[PROJECT->[id, json_data], LIMIT->10000],"
          + " OpenSearchRequestBuilder(sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"_source\":{\"includes\":[\"id\",\"json_data\"],\"excludes\":[]}},"
          + " requestedTotalSize=10000, pageSize=null, startFrom=0)])\n",
        explain);
  }

  @Test
  public void testSpathWithoutDynamicFieldSelectionEliminatesMapOperations() throws IOException {
    String explain =
        explainQueryToYaml(source(TEST_INDEX_DYNAMIC_COLUMNS, "spath input=json_data | fields id"));
    assertEquals(
        "calcite:\n"
            + //
            "  logical: |\n"
            + //
            "    LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])\n"
            + //
            "      LogicalProject(id=[$1])\n"
            + //
            "        CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_columns]])\n"
            + //
            "  physical: |\n"
            + //
            "    CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_columns]],"
          + " PushDownContext=[[PROJECT->[id], LIMIT->10000],"
          + " OpenSearchRequestBuilder(sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"_source\":{\"includes\":[\"id\"],\"excludes\":[]}},"
          + " requestedTotalSize=10000, pageSize=null, startFrom=0)])\n",
        explain);
  }

  @Test
  public void testTwoSpathCommand() throws IOException {
    String explain =
        explainQueryToYaml(
            source(
                TEST_INDEX_DYNAMIC_COLUMNS,
                "spath input=json_data | spath input=json_data | fields id"));
    assertEquals(
        "calcite:\n"
            + //
            "  logical: |\n"
            + //
            "    LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])\n"
            + //
            "      LogicalProject(id=[$1])\n"
            + //
            "        CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_columns]])\n"
            + //
            "  physical: |\n"
            + //
            "    CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_columns]],"
          + " PushDownContext=[[PROJECT->[id], LIMIT->10000],"
          + " OpenSearchRequestBuilder(sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"_source\":{\"includes\":[\"id\"],\"excludes\":[]}},"
          + " requestedTotalSize=10000, pageSize=null, startFrom=0)])\n",
        explain);
  }

  @Test
  public void testTwoSpathCommandWithSelectedField() throws IOException {
    String explain =
        explainQueryToYaml(
            source(
                TEST_INDEX_DYNAMIC_COLUMNS,
                "spath input=json_data | spath input=json_data | fields id, name"));
    assertEquals(
        "calcite:\n"
            + //
            "  logical: |\n"
            + //
            "    LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])\n"
            + //
            "      LogicalProject(id=[$1],"
            + " name=[MAP_GET(COALESCE(MAP_MERGE(COALESCE(MAP_MERGE(null:(VARCHAR NOT NULL, ANY NOT"
            + " NULL) MAP, JSON_EXTRACT_ALL($0)), JSON_EXTRACT_ALL($0)), JSON_EXTRACT_ALL($0)),"
            + " JSON_EXTRACT_ALL($0)), 'name')])\n"
            + //
            "        CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_columns]])\n"
            + //
            "  physical: |\n"
            + //
            "    EnumerableCalc(expr#0..1=[{inputs}], expr#2=[null:(VARCHAR NOT NULL, ANY NOT NULL)"
            + " MAP], expr#3=[JSON_EXTRACT_ALL($t1)], expr#4=[MAP_MERGE($t2, $t3)],"
            + " expr#5=[COALESCE($t4, $t3)], expr#6=[MAP_MERGE($t5, $t3)], expr#7=[COALESCE($t6,"
            + " $t3)], expr#8=['name'], expr#9=[MAP_GET($t7, $t8)], id=[$t0], $f1=[$t9])\n"
            + //
            "      CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_columns]],"
          + " PushDownContext=[[PROJECT->[id, json_data], LIMIT->10000],"
          + " OpenSearchRequestBuilder(sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"_source\":{\"includes\":[\"id\",\"json_data\"],\"excludes\":[]}},"
          + " requestedTotalSize=10000, pageSize=null, startFrom=0)])\n",
        explain);
  }

  @Test
  public void testMapAccessNotSelected() throws IOException {
    String explain =
        explainQueryToYaml(
            source(
                TEST_INDEX_DYNAMIC_COLUMNS,
                "spath input=json_data | eval nameage = name + age | fields id"));
    assertEquals(
        "calcite:\n"
            + //
            "  logical: |\n"
            + //
            "    LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])\n"
            + //
            "      LogicalProject(id=[$1])\n"
            + //
            "        CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_columns]])\n"
            + //
            "  physical: |\n"
            + //
            "    CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_columns]],"
          + " PushDownContext=[[PROJECT->[id], LIMIT->10000],"
          + " OpenSearchRequestBuilder(sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"_source\":{\"includes\":[\"id\"],\"excludes\":[]}},"
          + " requestedTotalSize=10000, pageSize=null, startFrom=0)])\n",
        explain);
  }
}
