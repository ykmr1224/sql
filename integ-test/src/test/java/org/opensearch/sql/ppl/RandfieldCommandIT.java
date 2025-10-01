/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

/**
 * Integration test for randfield command demonstrating DynamicRowType column architecture. This
 * test shows how the randfield command adds dynamic fields using DynamicRowType.
 */
public class RandfieldCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    // Enable Calcite to ensure our DynamicRowType column architecture is used
    enableCalcite();
  }

  @Test
  public void testRandfieldCommand() throws IOException {
    // Test basic randfield command
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname, age | head 3 | randfield", TEST_INDEX_ACCOUNT));

    // Verify that we still have the original fields plus the schema shows DynamicRowType usage
    // Note: The actual random field will be generated at runtime, so we can't predict exact output
    // But we can verify the query executes successfully and returns data

    // The result should have the __dynamic_fields__ column containing all fields
    // This demonstrates the DynamicRowType column architecture
    System.out.println("Randfield command result: " + result.toString(2));

    // Basic verification that query executed successfully
    assert result.has("datarows");
    assert result.getJSONArray("datarows").length() > 0;
  }

  @Test
  public void testMultipleRandfieldCommands() throws IOException {
    // Test multiple randfield commands to show merging behavior
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields firstname | head 2 | randfield | randfield",
                TEST_INDEX_ACCOUNT));

    // This should demonstrate the DynamicRowType column merging functionality
    System.out.println("Multiple randfield commands result: " + result.toString(2));

    // Basic verification
    assert result.has("datarows");
    assert result.getJSONArray("datarows").length() > 0;
  }

  @Test
  public void testFieldHeadRnadfield() throws IOException {
    executeQuery(source(TEST_INDEX_ACCOUNT, "fields firstname, age | head 2 | randfield"));
  }

  @Test
  public void testFieldHeadRnadfieldEval() throws IOException {
    executeQuery(
        source(
            TEST_INDEX_ACCOUNT,
            "fields firstname, age | head 2 | randfield | eval age_plus_10 = age + 10"));
  }

  @Test
  public void testFieldHeadRandfieldEvalRandfield() throws IOException {
    executeQuery(
        source(
            TEST_INDEX_ACCOUNT,
            "fields firstname, age | head 2 | randfield | eval age_plus_10 = age + 10 |"
                + " randfield"));
  }

  @Test
  public void testFieldHeadRandfieldRandfield() throws IOException {
    executeQuery(
        source(TEST_INDEX_ACCOUNT, "fields firstname, age | head 2 | randfield | randfield"));
  }

  @Test
  public void testRandfieldWithOtherCommands() throws IOException {
    // Test randfield combined with other PPL commands
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where age > 30 | fields firstname, age | randfield | head 2",
                TEST_INDEX_ACCOUNT));

    // This shows how DynamicRowType column works with other PPL operations
    System.out.println("Randfield with other commands result: " + result.toString(2));

    // Basic verification
    assert result.has("datarows");
    assert result.getJSONArray("datarows").length() > 0;
  }
}
