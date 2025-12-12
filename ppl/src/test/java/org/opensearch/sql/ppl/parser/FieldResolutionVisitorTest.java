/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertEquals;

import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.sql.ast.analysis.FieldResolutionVisitor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;

/**
 * Unit tests for FieldResolutionVisitor using PPL parser.
 *
 * <p>This test validates that the field resolution visitor correctly identifies required fields
 * from PPL queries by parsing actual PPL syntax.
 */
public class FieldResolutionVisitorTest {

  private final FieldResolutionVisitor visitor = new FieldResolutionVisitor();
  private final PPLSyntaxParser parser = new PPLSyntaxParser();
  private Settings settings;

  @Before
  public void setUp() {
    settings = Mockito.mock(Settings.class);
  }

  private UnresolvedPlan parse(String query) {
    AstBuilder astBuilder = new AstBuilder(query, settings);
    return astBuilder.visit(parser.parse(query));
  }

  @Test
  public void testSimpleRelation() {
    UnresolvedPlan plan = parse("source=logs");
    Set<String> fields = visitor.analyze(plan);
    assertEquals("Simple relation should require all fields", Set.of("*"), fields);
  }

  @Test
  public void testFilterOnly() {
    UnresolvedPlan plan = parse("source=logs | where status > 200");
    Set<String> fields = visitor.analyze(plan);
    // Filter adds status field but keeps "*" since no projection limits fields
    assertEquals(Set.of("*", "status"), fields);
  }

  @Test
  public void testMultipleFilters() {
    UnresolvedPlan plan = parse("source=logs | where status > 200 AND region = 'us-west'");
    Set<String> fields = visitor.analyze(plan);
    // Filter adds fields but keeps "*" since no projection limits fields
    assertEquals(Set.of("*", "status", "region"), fields);
  }

  @Test
  public void testProjectOnly() {
    UnresolvedPlan plan = parse("source=logs | fields status, region");
    Set<String> fields = visitor.analyze(plan);
    assertEquals(Set.of("status", "region"), fields);
  }

  @Test
  public void testFilterThenProject() {
    UnresolvedPlan plan = parse("source=logs | where status > 200 | fields region");
    Set<String> fields = visitor.analyze(plan);
    // Project wants 'region', but filter needs 'status' to evaluate the condition
    assertEquals("Expected both region and status fields", Set.of("region", "status"), fields);
  }

  @Test
  public void testAggregationWithGroupBy() {
    UnresolvedPlan plan = parse("source=logs | stats count() by region");
    Set<String> fields = visitor.analyze(plan);
    assertEquals(Set.of("region"), fields);
  }

  @Test
  public void testAggregationWithFieldAndGroupBy() {
    UnresolvedPlan plan = parse("source=logs | stats avg(response_time) by region");
    Set<String> fields = visitor.analyze(plan);
    assertEquals(Set.of("region", "response_time"), fields);
  }

  @Test
  public void testComplexQuery() {
    UnresolvedPlan plan = parse("source=logs | where status > 200 | stats count() by region");
    Set<String> fields = visitor.analyze(plan);
    assertEquals(Set.of("region", "status"), fields);
  }

  @Test
  public void testSortCommand() {
    UnresolvedPlan plan = parse("source=logs | sort status");
    Set<String> fields = visitor.analyze(plan);
    // Sort adds status field but keeps "*" since no projection limits fields
    assertEquals(Set.of("*", "status"), fields);
  }

  @Test
  public void testEvalCommand() {
    UnresolvedPlan plan = parse("source=logs | eval new_field = old_field + 1");
    Set<String> fields = visitor.analyze(plan);
    // Eval adds old_field (input) but keeps "*" since no projection limits fields
    assertEquals(Set.of("*", "old_field"), fields);
  }

  @Test
  public void testEvalThenFilter() {
    UnresolvedPlan plan = parse("source=logs | eval doubled = value * 2 | where doubled > 100");
    Set<String> fields = visitor.analyze(plan);
    // Filter requires 'doubled', but eval computes it from 'value'
    // Eval removes 'doubled' from requirements and adds 'value' as input
    assertEquals(Set.of("*", "value"), fields);
  }

  @Test
  public void testNestedFields() {
    UnresolvedPlan plan = parse("source=logs | where `user.name` = 'john'");
    Set<String> fields = visitor.analyze(plan);
    // Nested field should be stored as single string, keeps "*"
    assertEquals(Set.of("*", "user.name"), fields);
  }

  @Test
  public void testFunctionInFilter() {
    UnresolvedPlan plan = parse("source=logs | where length(message) > 100");
    Set<String> fields = visitor.analyze(plan);
    // Filter adds message field but keeps "*"
    assertEquals(Set.of("*", "message"), fields);
  }

  @Test
  public void testMultipleAggregations() {
    UnresolvedPlan plan =
        parse("source=logs | stats count(), avg(response_time), max(bytes) by region, status");
    Set<String> fields = visitor.analyze(plan);
    assertEquals(Set.of("region", "status", "response_time", "bytes"), fields);
  }

  @Test
  public void testComplexNestedQuery() {
    UnresolvedPlan plan =
        parse(
            "source=logs | where status > 200 AND region = 'us-west' "
                + "| eval response_ms = response_time * 1000 "
                + "| stats avg(response_ms), max(bytes) by region, status "
                + "| sort region");
    Set<String> fields = visitor.analyze(plan);
    // Required: status, region (from where and group by), response_time (input to eval), bytes
    // (from stats)
    // Note: response_ms is computed by eval, so it's removed from requirements
    assertEquals(Set.of("status", "region", "response_time", "bytes"), fields);
  }

  @Test
  public void testWildcardPatternMerging() {
    UnresolvedPlan plan = parse("source=logs | fields `prefix*`, `prefix_sub*`");
    Set<String> fields = visitor.analyze(plan);
    // Multiple wildcard patterns should be merged with &
    assertEquals(Set.of("prefix* & prefix_sub*"), fields);
  }

  @Test
  public void testSingleWildcardPattern() {
    UnresolvedPlan plan = parse("source=logs | fields `prefix*`");
    Set<String> fields = visitor.analyze(plan);
    // Single wildcard pattern should remain as-is
    assertEquals(Set.of("prefix*"), fields);
  }

  @Test
  public void testWildcardWithRegularFields() {
    UnresolvedPlan plan = parse("source=logs | fields status, `prefix*`, region");
    Set<String> fields = visitor.analyze(plan);
    // Wildcard pattern should be kept alongside regular fields
    assertEquals(Set.of("status", "prefix*", "region"), fields);
  }
}
