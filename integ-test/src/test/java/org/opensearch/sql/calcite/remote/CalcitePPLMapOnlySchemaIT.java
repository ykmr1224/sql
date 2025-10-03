/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLMapOnlySchemaIT extends PPLIntegTestCase {

  private static final String TEST_INDEX_MAP_ONLY = "test_map_only_schema";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    createMapOnlyTestData();
  }

  private void createMapOnlyTestData() throws IOException {
    // Create test documents with varying field structures to test MAP-only schema
    createDocumentWithIdAndJsonData(
        TEST_INDEX_MAP_ONLY, 1, "{\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}");
    createDocumentWithIdAndJsonData(
        TEST_INDEX_MAP_ONLY, 2, "{\"name\": \"Jane\", \"age\": 25, \"country\": \"USA\"}");
    createDocumentWithIdAndJsonData(
        TEST_INDEX_MAP_ONLY,
        3,
        "{\"product\": \"laptop\", \"price\": 999.99, \"brand\": \"Dell\"}");
    createDocumentWithIdAndJsonData(
        TEST_INDEX_MAP_ONLY,
        4,
        "{\"name\": \"Bob\", \"age\": 35, \"city\": \"Seattle\", \"country\": \"USA\"}");
  }

  private void createDocumentWithIdAndJsonData(String index, int id, String jsonContent)
      throws IOException {
    Request request = new Request("PUT", String.format("/%s/_doc/%d?refresh=true", index, id));
    String escapedJson = jsonContent.replace("\"", "\\\"");
    request.setJsonEntity(String.format("{\"id\": %d, \"json_data\": \"%s\"}", id, escapedJson));
    client().performRequest(request);
  }

  @Test
  public void testMapOnlySchemaBasicFieldAccess() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_MAP_ONLY,
                "spath input=json_data | fields id, name, age, city, country"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "string"),
        schema("city", "string"),
        schema("country", "string"));

    verifyDataRows(
        result,
        rows(1L, "John", "30", "New York", null),
        rows(2L, "Jane", "25", null, "USA"),
        rows(3L, null, null, null, null),
        rows(4L, "Bob", "35", "Seattle", "USA"));
  }

  @Test
  public void testMapOnlySchemaWithFiltering() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_MAP_ONLY,
                "spath input=json_data | where name = 'John' | fields id, name, age, city"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "string"),
        schema("city", "string"));

    verifyDataRows(result, rows(1L, "John", "30", "New York"));
  }

  @Test
  public void testMapOnlySchemaWithUnknownFields() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_MAP_ONLY,
                "spath input=json_data | fields id, product, price, brand, unknown_field"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("product", "string"),
        schema("price", "string"),
        schema("brand", "string"),
        schema("unknown_field", "undefined"));

    verifyDataRows(
        result,
        rows(1L, null, null, null, null),
        rows(2L, null, null, null, null),
        rows(3L, "laptop", "999.99", "Dell", null),
        rows(4L, null, null, null, null));
  }

  @Test
  public void testMapOnlySchemaWithAggregation() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_MAP_ONLY,
                "spath input=json_data | stats count() as total_count by country"));

    verifySchema(result, schema("total_count", "bigint"), schema("country", "string"));

    verifyDataRows(result, rows(2L, null), rows(2L, "USA"));
  }

  @Test
  public void testMapOnlySchemaWithEvalCommand() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_MAP_ONLY,
                "spath input=json_data | eval age_plus_ten = CAST(age AS INTEGER) + 10 | where"
                    + " isnotnull(age) | fields id, name, age, age_plus_ten"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "string"),
        schema("age_plus_ten", "int"));

    verifyDataRows(
        result, rows(1L, "John", "30", 40), rows(2L, "Jane", "25", 35), rows(4L, "Bob", "35", 45));
  }

  @Test
  public void testMapOnlySchemaWithSorting() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_MAP_ONLY,
                "spath input=json_data | where isnotnull(age) | eval age_numeric = CAST(age AS"
                    + " INTEGER) | sort age_numeric desc | fields id, name, age"));

    verifySchema(result, schema("id", "bigint"), schema("name", "string"), schema("age", "string"));

    verifyDataRows(result, rows(4L, "Bob", "35"), rows(1L, "John", "30"), rows(2L, "Jane", "25"));
  }

  @Test
  public void testMapOnlySchemaWithGroupBy() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_MAP_ONLY,
                "spath input=json_data | where isnotnull(country) | stats count() as"
                    + " count_by_country by country"));

    verifySchema(result, schema("count_by_country", "bigint"), schema("country", "string"));

    verifyDataRows(result, rows(2L, "USA"));
  }

  @Test
  public void testMapOnlySchemaWithComplexFiltering() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_MAP_ONLY,
                "spath input=json_data | where isnotnull(name) and isnotnull(age) | eval"
                    + " age_numeric = CAST(age AS INTEGER) | where age_numeric > 25 | fields id,"
                    + " name, age, city, country"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "string"),
        schema("city", "string"),
        schema("country", "string"));

    verifyDataRows(
        result, rows(1L, "John", "30", "New York", null), rows(4L, "Bob", "35", "Seattle", "USA"));
  }

  @Test
  public void testMapOnlySchemaWithFieldsWildcard() throws IOException {
    JSONObject result =
        executeQuery(
            source(TEST_INDEX_MAP_ONLY, "spath input=json_data | where id = 1 | fields *"));

    // Should include all available fields from the document
    verifySchema(
        result,
        schema("id", "string"), // to be fixed
        schema("json_data", "string"),
        schema("name", "string"),
        schema("age", "string"),
        schema("city", "string"));

    verifyDataRows(
        result,
        rows(
            "30",
            "New York",
            1L,
            "{\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}",
            "John"));
  }

  @Test
  public void testMapOnlySchemaWithMultipleSpathCalls() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_MAP_ONLY,
                "spath input=json_data | spath input=json_data path=name output=extracted_name |"
                    + " fields id, name, extracted_name"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("extracted_name", "string"));

    verifyDataRows(
        result,
        rows(1L, "John", "John"),
        rows(2L, "Jane", "Jane"),
        rows(3L, null, null),
        rows(4L, "Bob", "Bob"));
  }

  @Test
  public void testMapOnlySchemaTypePreservation() throws IOException {
    // Test that known field types are preserved in MAP-only schema
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_MAP_ONLY,
                "spath input=json_data | where isnotnull(price) | eval price_numeric = CAST(price"
                    + " AS DOUBLE) | fields id, product, price, price_numeric"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("product", "string"),
        schema("price", "string"),
        schema("price_numeric", "double"));

    verifyDataRows(result, rows(3L, "laptop", "999.99", 999.99));
  }

  @Test
  public void testMapOnlySchemaWithCoalesceFunction() throws IOException {
    // Test that coalesce function works correctly with MAP-only schema
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_MAP_ONLY,
                "spath input=json_data | eval location = coalesce(city, country, 'Unknown') |"
                    + " fields id, name, location"));

    verifySchema(
        result, schema("id", "bigint"), schema("name", "string"), schema("location", "string"));

    verifyDataRows(
        result,
        rows(1L, "John", "New York"),
        rows(2L, "Jane", "USA"),
        rows(3L, null, "Unknown"),
        rows(4L, "Bob", "Seattle"));
  }
}
