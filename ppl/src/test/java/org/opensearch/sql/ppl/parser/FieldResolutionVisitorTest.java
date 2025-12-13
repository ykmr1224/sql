/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.sql.ast.analysis.FieldResolutionResult;
import org.opensearch.sql.ast.analysis.FieldResolutionVisitor;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;

/**
 * Unit tests for FieldResolutionVisitor using PPL parser.
 *
 * <p>This test validates that the field resolution visitor correctly identifies required fields
 * from PPL queries by parsing actual PPL syntax. Tests use the new multi-relation API.
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
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of(), result.getRegularFields());
    assertEquals("*", result.getWildcardPattern());
  }

  @Test
  public void testFilterOnly() {
    UnresolvedPlan plan = parse("source=logs | where status > 200");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("status"), result.getRegularFields());
    assertEquals("*", result.getWildcardPattern());
  }

  @Test
  public void testMultipleFilters() {
    UnresolvedPlan plan = parse("source=logs | where status > 200 AND region = 'us-west'");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("status", "region"), result.getRegularFields());
    assertEquals("*", result.getWildcardPattern());
  }

  @Test
  public void testProjectOnly() {
    UnresolvedPlan plan = parse("source=logs | fields status, region");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("status", "region"), result.getRegularFields());
    assertEquals(null, result.getWildcardPattern());
  }

  @Test
  public void testFilterThenProject() {
    UnresolvedPlan plan = parse("source=logs | where status > 200 | fields region");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("region", "status"), result.getRegularFields());
    assertEquals(null, result.getWildcardPattern());
  }

  @Test
  public void testAggregationWithGroupBy() {
    UnresolvedPlan plan = parse("source=logs | stats count() by region");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("region"), result.getRegularFields());
    assertEquals(null, result.getWildcardPattern());
  }

  @Test
  public void testAggregationWithFieldAndGroupBy() {
    UnresolvedPlan plan = parse("source=logs | stats avg(response_time) by region");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("region", "response_time"), result.getRegularFields());
    assertEquals(null, result.getWildcardPattern());
  }

  @Test
  public void testComplexQuery() {
    UnresolvedPlan plan = parse("source=logs | where status > 200 | stats count() by region");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("region", "status"), result.getRegularFields());
    assertEquals(null, result.getWildcardPattern());
  }

  @Test
  public void testSortCommand() {
    UnresolvedPlan plan = parse("source=logs | sort status");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("status"), result.getRegularFields());
    assertEquals("*", result.getWildcardPattern());
  }

  @Test
  public void testEvalCommand() {
    UnresolvedPlan plan = parse("source=logs | eval new_field = old_field + 1");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("old_field"), result.getRegularFields());
    assertEquals("*", result.getWildcardPattern());
  }

  @Test
  public void testEvalThenFilter() {
    UnresolvedPlan plan = parse("source=logs | eval doubled = value * 2 | where doubled > 100");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("value"), result.getRegularFields());
    assertEquals("*", result.getWildcardPattern());
  }

  @Test
  public void testNestedFields() {
    UnresolvedPlan plan = parse("source=logs | where `user.name` = 'john'");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("user.name"), result.getRegularFields());
    assertEquals("*", result.getWildcardPattern());
  }

  @Test
  public void testFunctionInFilter() {
    UnresolvedPlan plan = parse("source=logs | where length(message) > 100");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("message"), result.getRegularFields());
    assertEquals("*", result.getWildcardPattern());
  }

  @Test
  public void testMultipleAggregations() {
    UnresolvedPlan plan =
        parse("source=logs | stats count(), avg(response_time), max(bytes) by region, status");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("region", "status", "response_time", "bytes"), result.getRegularFields());
    assertEquals(null, result.getWildcardPattern());
  }

  @Test
  public void testComplexNestedQuery() {
    UnresolvedPlan plan =
        parse(
            "source=logs | where status > 200 AND region = 'us-west' "
                + "| eval response_ms = response_time * 1000 "
                + "| stats avg(response_ms), max(bytes) by region, status "
                + "| sort region");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("status", "region", "response_time", "bytes"), result.getRegularFields());
    assertEquals(null, result.getWildcardPattern());
  }

  @Test
  public void testWildcardPatternMerging() {
    UnresolvedPlan plan = parse("source=logs | fields `prefix*`, `prefix_sub*`");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of(), result.getRegularFields());
    assertEquals("prefix* | prefix_sub*", result.getWildcardPattern());
  }

  @Test
  public void testSingleWildcardPattern() {
    UnresolvedPlan plan = parse("source=logs | fields `prefix*`");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of(), result.getRegularFields());
    assertEquals("prefix*", result.getWildcardPattern());
  }

  @Test
  public void testWildcardWithRegularFields() {
    UnresolvedPlan plan = parse("source=logs | fields status, `prefix*`, region");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);

    assertEquals(Set.of("status", "region"), result.getRegularFields());
    assertEquals("prefix*", result.getWildcardPattern());
  }

  @Test
  public void testMultiRelationResult() {
    UnresolvedPlan plan = parse("source=logs | where status > 200");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);

    assertEquals(1, results.size());

    Relation relation = results.keySet().iterator().next();
    assertEquals("logs", relation.getTableQualifiedName().toString());

    FieldResolutionResult result = results.get(relation);
    assertEquals(Set.of("status"), result.getRegularFields());
    assertEquals("*", result.getWildcardPattern());
  }

  @Test
  public void testBackwardCompatibilityMethod() {
    UnresolvedPlan plan = parse("source=logs | where status > 200 | fields region");
    Set<String> fields = visitor.analyzeFields(plan);
    assertEquals(Set.of("region", "status"), fields);
  }
}
