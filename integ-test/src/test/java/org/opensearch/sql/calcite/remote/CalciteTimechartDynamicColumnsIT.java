/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.legacy.TestUtils.*;
import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteTimechartDynamicColumnsIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();

    // Create test data for dynamic columns testing
    createEventsIndex();
  }

  @Test
  public void testTimechartWithDynamicColumnsBasic() throws IOException {
    var response = executeQuery("source=events | timechart span=1h count() by host");

    verifySchema(
        response,
        schema("@timestamp", null, "timestamp"),
        schema("web-01_count", null, "long"),
        schema("web-02_count", null, "long"),
        schema("db-01_count", null, "long"));

    // Verify that we get dynamic columns with proper naming
    var dataRows = response.getJSONArray("datarows");
    assert dataRows.length() > 0 : "Should have data rows";
  }

  @Test
  public void testTimechartWithDynamicColumnsLimit() throws IOException {
    var response = executeQuery("source=events | timechart span=1h limit=2 count() by host");

    verifySchema(
        response,
        schema("@timestamp", null, "timestamp"),
        schema("web-01_count", null, "long"),
        schema("web-02_count", null, "long"),
        schema("OTHER_count", null, "long"));

    // Verify that we get exactly 4 columns (timestamp + 2 hosts + OTHER)
    var schema = response.getJSONArray("schema");
    assert schema.length() == 4 : "Should have exactly 4 columns with limit=2";
  }

  @Test
  public void testTimechartWithDynamicColumnsNoOther() throws IOException {
    var response =
        executeQuery("source=events | timechart span=1h limit=2 useother=false count() by host");

    verifySchema(
        response,
        schema("@timestamp", null, "timestamp"),
        schema("web-01_count", null, "long"),
        schema("web-02_count", null, "long"));

    // Verify that we don't get OTHER column when useother=false
    var schema = response.getJSONArray("schema");
    assert schema.length() == 3 : "Should have exactly 3 columns with limit=2 and useother=false";
  }

  @Test
  public void testTimechartWithDynamicColumnsAvgFunction() throws IOException {
    var response = executeQuery("source=events | timechart span=1h avg(cpu_usage) by host");

    verifySchema(
        response,
        schema("@timestamp", null, "timestamp"),
        schema("web-01_avg(cpu_usage)", null, "double"),
        schema("web-02_avg(cpu_usage)", null, "double"),
        schema("db-01_avg(cpu_usage)", null, "double"));

    // Verify that function names are properly included in column names
    var schema = response.getJSONArray("schema");
    for (int i = 1; i < schema.length(); i++) {
      var columnName = schema.getJSONObject(i).getString("name");
      assert columnName.contains("avg(cpu_usage)") : "Column name should contain function name";
    }
  }

  @Test
  public void testTimechartWithDynamicColumnsNullHandling() throws IOException {
    // Create events_null index for null testing
    createEventsNullIndex();

    var response = executeQuery("source=events_null | timechart span=1d count() by host");

    verifySchema(
        response,
        schema("@timestamp", null, "timestamp"),
        schema("web-01_count", null, "long"),
        schema("web-02_count", null, "long"),
        schema("db-01_count", null, "long"),
        schema("null_count", null, "long"));

    // Verify that null values are handled properly
    var schema = response.getJSONArray("schema");
    boolean hasNullColumn = false;
    for (int i = 1; i < schema.length(); i++) {
      var columnName = schema.getJSONObject(i).getString("name");
      if (columnName.equals("null_count")) {
        hasNullColumn = true;
        break;
      }
    }
    assert hasNullColumn : "Should have null_count column for null values";
  }

  @Test
  public void testTimechartWithDynamicColumnsComplexFieldNames() throws IOException {
    var response = executeQuery("source=events | timechart span=1h count() by region");

    // Verify that complex field names are handled properly in column names
    var schema = response.getJSONArray("schema");
    boolean hasComplexNames = false;
    for (int i = 1; i < schema.length(); i++) {
      var columnName = schema.getJSONObject(i).getString("name");
      if (columnName.contains("us-east")
          || columnName.contains("us-west")
          || columnName.contains("eu-west")) {
        hasComplexNames = true;
        break;
      }
    }
    assert hasComplexNames : "Should handle complex field names in column names";
  }

  @Test
  public void testTimechartWithDynamicColumnsZeroLimit() throws IOException {
    var response = executeQuery("source=events | timechart span=1h limit=0 count() by host");

    // With limit=0, should fall back to standard timechart behavior
    verifySchema(
        response,
        schema("@timestamp", null, "timestamp"),
        schema("host", null, "string"),
        schema("count", null, "bigint"));

    // Verify that we get the standard 3-column format
    var schema = response.getJSONArray("schema");
    assert schema.length() == 3 : "Should have exactly 3 columns with limit=0";
  }

  @Test
  public void testTimechartWithoutByFieldStillWorks() throws IOException {
    var response = executeQuery("source=events | timechart span=1h count()");

    verifySchema(
        response, schema("@timestamp", null, "timestamp"), schema("count", null, "bigint"));

    // Verify that timechart without by field still works normally
    var schema = response.getJSONArray("schema");
    assert schema.length() == 2 : "Should have exactly 2 columns without by field";
  }

  @Test
  public void testTimechartDynamicColumnsWithSumFunction() throws IOException {
    var response =
        executeQuery("source=events | timechart span=1h limit=2 sum(response_time) by host");

    verifySchema(
        response,
        schema("@timestamp", null, "timestamp"),
        schema("web-01_sum(response_time)", null, "bigint"),
        schema("web-02_sum(response_time)", null, "bigint"),
        schema("OTHER_sum(response_time)", null, "bigint"));

    // Verify that sum function works with dynamic columns
    var schema = response.getJSONArray("schema");
    assert schema.length() == 4 : "Should have exactly 4 columns with sum function";
  }

  @Test
  public void testTimechartDynamicColumnsDataIntegrity() throws IOException {
    var response = executeQuery("source=events | timechart span=1h count() by region");

    // Verify that data is properly aggregated
    var dataRows = response.getJSONArray("datarows");
    assert dataRows.length() > 0 : "Should have data rows";

    // Check that each row has the expected number of columns (timestamp + 3 regions)
    if (dataRows.length() > 0) {
      var firstRow = dataRows.getJSONArray(0);
      assert firstRow.length() == 4 : "Each data row should have 4 columns (timestamp + 3 regions)";
    }
  }

  private void createEventsIndex() throws IOException {
    loadIndex(Index.EVENTS);
  }

  private void createEventsNullIndex() throws IOException {
    String eventsMapping =
        "{\"mappings\":{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"host\":{\"type\":\"text\"},\"cpu_usage\":{\"type\":\"double\"},\"region\":{\"type\":\"keyword\"}}}}";
    if (!isIndexExist(client(), "events_null")) {
      createIndexByRestClient(client(), "events_null", eventsMapping);
      loadDataByRestClient(client(), "events_null", "src/test/resources/events_null.json");
    }
  }
}
