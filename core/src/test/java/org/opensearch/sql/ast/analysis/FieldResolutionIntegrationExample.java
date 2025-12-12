/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.analysis;

import static org.opensearch.sql.ast.dsl.AstDSL.*;

import java.util.List;
import java.util.Set;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/**
 * Integration example demonstrating field resolution usage.
 *
 * <p>This class shows how to use FieldResolutionVisitor to analyze PPL queries and discover
 * required fields before query execution.
 */
public class FieldResolutionIntegrationExample {

  /**
   * Example: Analyze a simple filter query. Query: source=logs | where status > 200 Expected
   * fields: {status}
   */
  public static void exampleSimpleFilter() {
    UnresolvedPlan plan = filter(relation("logs"), compare(">", field("status"), intLiteral(200)));

    FieldResolutionVisitor visitor = new FieldResolutionVisitor();
    Set<String> requiredFields = visitor.analyze(plan);

    System.out.println("Query: source=logs | where status > 200");
    System.out.println("Required fields: " + requiredFields);
    // Output: Required fields: [status]
  }

  /**
   * Example: Analyze an aggregation query. Query: source=logs | where status > 200 | stats count()
   * by region Expected fields: {region, status}
   */
  public static void exampleAggregationWithFilter() {
    UnresolvedPlan plan =
        agg(
            filter(relation("logs"), compare(">", field("status"), intLiteral(200))),
            List.of(alias("count", aggregate("count", field("*")))),
            null,
            List.of(alias("region", field("region"))),
            List.of());

    FieldResolutionVisitor visitor = new FieldResolutionVisitor();
    Set<String> requiredFields = visitor.analyze(plan);

    System.out.println("Query: source=logs | where status > 200 | stats count() by region");
    System.out.println("Required fields: " + requiredFields);
    // Output: Required fields: [region, status]
  }

  /**
   * Example: Analyze a query with eval. Query: source=logs | eval doubled = value * 2 | where
   * doubled > 100 Expected fields: {doubled, value}
   *
   * <p>Note: 'doubled' is included because the filter references it. This is a known limitation -
   * eval doesn't remove computed fields from parent requirements.
   */
  public static void exampleEvalWithFilter() {
    UnresolvedPlan plan =
        filter(
            eval(
                relation("logs"),
                let(field("doubled"), function("*", field("value"), intLiteral(2)))),
            compare(">", field("doubled"), intLiteral(100)));

    FieldResolutionVisitor visitor = new FieldResolutionVisitor();
    Set<String> requiredFields = visitor.analyze(plan);

    System.out.println("Query: source=logs | eval doubled = value * 2 | where doubled > 100");
    System.out.println("Required fields: " + requiredFields);
    // Output: Required fields: [doubled, value]
  }

  /**
   * Example: Analyze a complex multi-aggregation query. Query: source=logs | stats count(),
   * avg(response_time), max(bytes) by region, status Expected fields: {region, status,
   * response_time, bytes}
   */
  public static void exampleComplexAggregation() {
    UnresolvedPlan plan =
        agg(
            relation("logs"),
            List.of(
                alias("count", aggregate("count", field("*"))),
                alias("avg_response", aggregate("avg", field("response_time"))),
                alias("max_bytes", aggregate("max", field("bytes")))),
            null,
            List.of(alias("region", field("region")), alias("status", field("status"))),
            List.of());

    FieldResolutionVisitor visitor = new FieldResolutionVisitor();
    Set<String> requiredFields = visitor.analyze(plan);

    System.out.println(
        "Query: source=logs | stats count(), avg(response_time), max(bytes) by region, status");
    System.out.println("Required fields: " + requiredFields);
    // Output: Required fields: [region, status, response_time, bytes]
  }

  /** Main method to run all examples. */
  public static void main(String[] args) {
    System.out.println("=== Field Resolution Examples ===\n");

    exampleSimpleFilter();
    System.out.println();

    exampleAggregationWithFilter();
    System.out.println();

    exampleEvalWithFilter();
    System.out.println();

    exampleComplexAggregation();
  }
}
