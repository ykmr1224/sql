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

  private FieldResolutionResult getSingleRelationResult(String query) {
    UnresolvedPlan plan = parse(query);
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);
    Relation relation = results.keySet().iterator().next();
    return results.get(relation);
  }

  private void assertSingleRelationFields(
      String query, Set<String> expectedFields, String expectedWildcard) {
    FieldResolutionResult result = getSingleRelationResult(query);
    assertEquals(expectedFields, result.getRegularFields());
    assertEquals(expectedWildcard, result.getWildcardPattern());
  }

  private void assertJoinRelationFields(
      String query, Map<String, FieldResolutionResult> expectedResultsByTable) {
    UnresolvedPlan plan = parse(query);
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);

    assertEquals(expectedResultsByTable.size(), results.size());

    for (Map.Entry<Relation, FieldResolutionResult> entry : results.entrySet()) {
      String tableName = entry.getKey().getTableQualifiedName().toString();
      FieldResolutionResult expectedResult = expectedResultsByTable.get(tableName);

      if (expectedResult != null) {
        assertEquals(expectedResult.getRegularFields(), entry.getValue().getRegularFields());
        assertEquals(expectedResult.getWildcardPattern(), entry.getValue().getWildcardPattern());
      }
    }
  }

  @Test
  public void testSimpleRelation() {
    assertSingleRelationFields("source=logs", Set.of(), "*");
  }

  @Test
  public void testFilterOnly() {
    assertSingleRelationFields("source=logs | where status > 200", Set.of("status"), "*");
  }

  @Test
  public void testMultipleFilters() {
    assertSingleRelationFields(
        "source=logs | where status > 200 AND region = 'us-west'", Set.of("status", "region"), "*");
  }

  @Test
  public void testProjectOnly() {
    assertSingleRelationFields(
        "source=logs | fields status, region", Set.of("status", "region"), null);
  }

  @Test
  public void testFilterThenProject() {
    assertSingleRelationFields(
        "source=logs | where status > 200 | fields region", Set.of("region", "status"), null);
  }

  @Test
  public void testAggregationWithGroupBy() {
    assertSingleRelationFields("source=logs | stats count() by region", Set.of("region"), null);
  }

  @Test
  public void testAggregationWithFieldAndGroupBy() {
    assertSingleRelationFields(
        "source=logs | stats avg(response_time) by region",
        Set.of("region", "response_time"),
        null);
  }

  @Test
  public void testComplexQuery() {
    assertSingleRelationFields(
        "source=logs | where status > 200 | stats count() by region",
        Set.of("region", "status"),
        null);
  }

  @Test
  public void testSortCommand() {
    assertSingleRelationFields("source=logs | sort status", Set.of("status"), "*");
  }

  @Test
  public void testEvalCommand() {
    assertSingleRelationFields(
        "source=logs | eval new_field = old_field + 1", Set.of("old_field"), "*");
  }

  @Test
  public void testEvalThenFilter() {
    assertSingleRelationFields(
        "source=logs | eval doubled = value * 2 | where doubled > 100", Set.of("value"), "*");
  }

  @Test
  public void testNestedFields() {
    assertSingleRelationFields(
        "source=logs | where `user.name` = 'john'", Set.of("user.name"), "*");
  }

  @Test
  public void testFunctionInFilter() {
    assertSingleRelationFields("source=logs | where length(message) > 100", Set.of("message"), "*");
  }

  @Test
  public void testMultipleAggregations() {
    assertSingleRelationFields(
        "source=logs | stats count(), avg(response_time), max(bytes) by region, status",
        Set.of("region", "status", "response_time", "bytes"),
        null);
  }

  @Test
  public void testComplexNestedQuery() {
    assertSingleRelationFields(
        "source=logs | where status > 200 AND region = 'us-west' "
            + "| eval response_ms = response_time * 1000 "
            + "| stats avg(response_ms), max(bytes) by region, status "
            + "| sort region",
        Set.of("status", "region", "response_time", "bytes"),
        null);
  }

  @Test
  public void testWildcardPatternMerging() {
    assertSingleRelationFields(
        "source=logs | fields `prefix*`, `prefix_sub*`", Set.of(), "prefix* | prefix_sub*");
  }

  @Test
  public void testSingleWildcardPattern() {
    assertSingleRelationFields("source=logs | fields `prefix*`", Set.of(), "prefix*");
  }

  @Test
  public void testWildcardWithRegularFields() {
    assertSingleRelationFields(
        "source=logs | fields status, `prefix*`, region", Set.of("status", "region"), "prefix*");
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
  public void testSimpleJoin() {
    assertJoinRelationFields(
        "source=logs1 | join left=l right=r ON l.id = r.id logs2",
        Map.of(
            "logs1", new FieldResolutionResult(Set.of("id"), "*"),
            "logs2", new FieldResolutionResult(Set.of("id"), "*")));
  }

  @Test
  public void testJoinWithFilter() {
    assertJoinRelationFields(
        "source=logs1 | where status > 200 | join left=l right=r ON l.id = r.id logs2",
        Map.of(
            "logs1", new FieldResolutionResult(Set.of("status", "id"), "*"),
            "logs2", new FieldResolutionResult(Set.of("id"), "*")));
  }

  @Test
  public void testJoinWithProject() {
    assertJoinRelationFields(
        "source=logs1 | join left=l right=r ON l.id = r.id logs2 | fields l.name, r.value",
        Map.of(
            "logs1", new FieldResolutionResult(Set.of("name", "id")),
            "logs2", new FieldResolutionResult(Set.of("value", "id"))));
  }

  @Test
  public void testSelfJoin() {
    UnresolvedPlan plan =
        parse("source=logs | fields id | join left=l right=r ON l.id = r.parent_id logs");
    Map<Relation, FieldResolutionResult> results = visitor.analyze(plan);

    assertEquals(2, results.size());

    for (Map.Entry<Relation, FieldResolutionResult> entry : results.entrySet()) {
      Relation relation = entry.getKey();
      FieldResolutionResult result = entry.getValue();
      String tableName = relation.getTableQualifiedName().toString();

      assertEquals("logs", tableName);
      Set<String> fields = result.getRegularFields();
      assertEquals(1, fields.size());
      if (fields.contains("id")) {
        assertEquals(null, result.getWildcardPattern());
      } else {
        assert (fields.contains("parent_id"));
        assertEquals("*", result.getWildcardPattern());
      }
    }
  }

  @Test
  public void testJoinWithAggregation() {
    assertJoinRelationFields(
        "source=logs1 | join left=l right=r ON l.id = r.id logs2 | stats count() by l.region",
        Map.of(
            "logs1", new FieldResolutionResult(Set.of("region", "id")),
            "logs2", new FieldResolutionResult(Set.of("id"))));
  }

  @Test
  public void testJoinWithSubsearch() {
    assertJoinRelationFields(
        "source=idx1 | where b > 1 | join a [source=idx2 | where c > 2 ] | eval result = c * d",
        Map.of(
            "idx1", new FieldResolutionResult(Set.of("a", "b", "c", "d"), "*"),
            "idx2", new FieldResolutionResult(Set.of("a", "c", "d"), "*")));
  }
}
