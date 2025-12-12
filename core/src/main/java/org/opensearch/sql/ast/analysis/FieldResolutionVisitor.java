/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.analysis;

import java.util.HashSet;
import java.util.Set;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/**
 * Visitor to analyze and collect required fields from PPL AST.
 *
 * <p>This visitor traverses the AST in a bottom-up manner, collecting field requirements from each
 * command and propagating them to child commands. The final result at the Relation (scan) node
 * represents all fields required from the data source.
 *
 * <p>Field resolution follows these principles:
 *
 * <ul>
 *   <li>Each command collects fields it directly uses
 *   <li>Commands pass required fields to their children
 *   <li>Wildcard patterns are merged using AND logic across commands
 *   <li>Computed fields (from eval) are excluded from requirements
 *   <li>Nested fields are stored as single strings (e.g., "user.name")
 * </ul>
 *
 * <p>Note: FieldResolutionResult class is available for future enhancement to properly model OR
 * (same command) vs AND (different commands) logic for wildcard patterns.
 *
 * <p>Example usage:
 *
 * <pre>
 * UnresolvedPlan ast = parser.parse("source=logs | where status > 200 | stats count() by region");
 * FieldResolutionVisitor visitor = new FieldResolutionVisitor();
 * Set&lt;String&gt; requiredFields = visitor.analyze(ast);
 * // Result: {region, status}
 * </pre>
 */
public class FieldResolutionVisitor
    extends AbstractNodeVisitor<Set<String>, FieldResolutionContext> {

  /**
   * Analyzes the AST and returns the set of required fields.
   *
   * @param plan The root AST node to analyze
   * @return Set of field names required by the query. Returns {"*"} if all fields are needed.
   */
  public Set<String> analyze(UnresolvedPlan plan) {
    // Start with "*" to indicate all fields are initially required
    FieldResolutionContext context = new FieldResolutionContext(Set.of("*"));
    return plan.accept(this, context);
  }

  /** Default visit method - visits children and aggregates their required fields. */
  @Override
  public Set<String> visitChildren(
      org.opensearch.sql.ast.Node node, FieldResolutionContext context) {
    Set<String> allFields = new HashSet<>();
    for (org.opensearch.sql.ast.Node child : node.getChild()) {
      Set<String> childFields = child.accept(this, context);
      if (childFields != null) {
        allFields.addAll(childFields);
      }
    }
    return allFields;
  }

  /**
   * Visit Relation node - this is the leaf node representing table scan. Returns all accumulated
   * required fields from parent commands. If "*" is in the set, it means all fields are required.
   * Merges wildcard patterns using AND logic.
   */
  @Override
  public Set<String> visitRelation(Relation node, FieldResolutionContext context) {
    // Relation is the leaf node - return all fields required by parent commands
    Set<String> requiredFields = new HashSet<>(context.getRequiredFields());

    // If the set is empty, default to "*" (all fields)
    if (requiredFields.isEmpty()) {
      requiredFields.add("*");
    }

    // Merge wildcard patterns
    return mergeWildcardPatterns(requiredFields);
  }

  /**
   * Visit Project/Fields command. Only passes through fields that are selected in the project list.
   * This replaces "*" with specific field names.
   */
  @Override
  public Set<String> visitProject(Project node, FieldResolutionContext context) {
    Set<String> projectFields = new HashSet<>();

    // Collect fields from project list
    for (UnresolvedExpression expr : node.getProjectList()) {
      projectFields.addAll(extractFieldsFromExpression(expr));
    }

    // Project acts as a barrier - only pass through fields in the project list
    // This replaces "*" with specific fields
    FieldResolutionContext childContext = new FieldResolutionContext(projectFields);

    // Visit children with the new context
    return visitChildren(node, childContext);
  }

  /**
   * Visit Filter/Where command. Collects fields from filter condition and passes all requirements
   * to children. If parent requires "*", adds filter fields to it.
   */
  @Override
  public Set<String> visitFilter(Filter node, FieldResolutionContext context) {
    // Collect fields from filter condition
    Set<String> filterFields = extractFieldsFromExpression(node.getCondition());

    // Merge with parent requirements
    Set<String> allRequiredFields = new HashSet<>(context.getRequiredFields());
    allRequiredFields.addAll(filterFields);

    // Create context with merged requirements
    FieldResolutionContext childContext = new FieldResolutionContext(allRequiredFields);

    // Visit children
    return visitChildren(node, childContext);
  }

  /**
   * Visit Aggregation/Stats command. Collects group-by fields and aggregation input fields. This
   * replaces "*" with specific fields needed for aggregation.
   */
  @Override
  public Set<String> visitAggregation(Aggregation node, FieldResolutionContext context) {
    Set<String> aggFields = new HashSet<>();

    // Collect fields from group-by expressions
    for (UnresolvedExpression groupExpr : node.getGroupExprList()) {
      aggFields.addAll(extractFieldsFromExpression(groupExpr));
    }

    // Collect fields from span (if present)
    if (node.getSpan() != null) {
      aggFields.addAll(extractFieldsFromExpression(node.getSpan()));
    }

    // Collect fields from aggregation functions
    for (UnresolvedExpression aggExpr : node.getAggExprList()) {
      aggFields.addAll(extractFieldsFromAggregation(aggExpr));
    }

    // Aggregation replaces "*" with specific fields
    FieldResolutionContext childContext = new FieldResolutionContext(aggFields);

    // Visit children
    return visitChildren(node, childContext);
  }

  /**
   * Visit Sort command. Collects fields from sort expressions and passes all requirements to
   * children.
   */
  @Override
  public Set<String> visitSort(Sort node, FieldResolutionContext context) {
    Set<String> sortFields = new HashSet<>();

    // Collect fields from sort list
    for (Field sortField : node.getSortList()) {
      sortFields.addAll(extractFieldsFromExpression(sortField));
    }

    // Merge with parent requirements
    Set<String> allRequiredFields = new HashSet<>(context.getRequiredFields());
    allRequiredFields.addAll(sortFields);

    // Create context with merged requirements
    FieldResolutionContext childContext = new FieldResolutionContext(allRequiredFields);

    // Visit children
    return visitChildren(node, childContext);
  }

  /**
   * Visit Eval command. Collects input fields from expressions but excludes computed field names.
   * Removes computed field names from parent requirements since eval provides them.
   */
  @Override
  public Set<String> visitEval(Eval node, FieldResolutionContext context) {
    Set<String> evalInputFields = new HashSet<>();
    Set<String> computedFields = new HashSet<>();

    // For each let expression, collect input fields and track computed field names
    for (Let letExpr : node.getExpressionList()) {
      // Extract fields from the expression being computed
      evalInputFields.addAll(extractFieldsFromExpression(letExpr.getExpression()));
      // Track the computed field name
      computedFields.add(letExpr.getVar().getField().toString());
    }

    // Start with parent requirements
    Set<String> allRequiredFields = new HashSet<>(context.getRequiredFields());

    // Remove computed fields from requirements since eval provides them
    allRequiredFields.removeAll(computedFields);

    // Add input fields needed by eval
    allRequiredFields.addAll(evalInputFields);

    // Create context with merged requirements
    FieldResolutionContext childContext = new FieldResolutionContext(allRequiredFields);

    // Visit children
    return visitChildren(node, childContext);
  }

  /**
   * Extracts field names from an expression. Handles Field, Alias, Function, and other expression
   * types.
   */
  private Set<String> extractFieldsFromExpression(UnresolvedExpression expr) {
    Set<String> fields = new HashSet<>();

    if (expr == null) {
      return fields;
    }

    if (expr instanceof Field field) {
      // Direct field reference - add the field name
      String fieldName = field.getField().toString();
      // Include "*" to indicate all fields are referenced
      fields.add(fieldName);
    } else if (expr instanceof Alias alias) {
      // Alias wraps another expression - extract from the delegated expression
      fields.addAll(extractFieldsFromExpression(alias.getDelegated()));
    } else if (expr instanceof Function function) {
      // Function may have field arguments
      for (UnresolvedExpression arg : function.getFuncArgs()) {
        fields.addAll(extractFieldsFromExpression(arg));
      }
    } else if (expr instanceof Span span) {
      // Span has a field and unit
      fields.addAll(extractFieldsFromExpression(span.getField()));
    } else if (expr instanceof Literal) {
      // Literals don't reference fields
      return fields;
    } else {
      // For other expression types, recursively visit children
      for (org.opensearch.sql.ast.Node child : expr.getChild()) {
        if (child instanceof UnresolvedExpression childExpr) {
          fields.addAll(extractFieldsFromExpression(childExpr));
        }
      }
    }

    return fields;
  }

  /**
   * Extracts field names from aggregation expressions. Handles AggregateFunction and Alias
   * wrapping.
   */
  private Set<String> extractFieldsFromAggregation(UnresolvedExpression expr) {
    Set<String> fields = new HashSet<>();

    if (expr instanceof Alias alias) {
      // Unwrap alias to get the actual aggregation function
      return extractFieldsFromAggregation(alias.getDelegated());
    } else if (expr instanceof AggregateFunction aggFunc) {
      // Extract field from aggregation function
      if (aggFunc.getField() != null) {
        fields.addAll(extractFieldsFromExpression(aggFunc.getField()));
      }
      // Also check additional arguments
      if (aggFunc.getArgList() != null) {
        for (UnresolvedExpression arg : aggFunc.getArgList()) {
          fields.addAll(extractFieldsFromExpression(arg));
        }
      }
    }

    return fields;
  }

  /**
   * Merges wildcard patterns in the field set using AND logic. For example, if the set contains
   * "prefix*" and "prefix_sub*", they are merged into a single entry "prefix* & prefix_sub*".
   *
   * @param fields Set of field names that may contain wildcard patterns
   * @return Set with wildcard patterns merged
   */
  private Set<String> mergeWildcardPatterns(Set<String> fields) {
    Set<String> wildcardPatterns = new HashSet<>();
    Set<String> regularFields = new HashSet<>();

    // Separate wildcard patterns from regular fields
    for (String field : fields) {
      if (field.contains("*")) {
        wildcardPatterns.add(field);
      } else {
        regularFields.add(field);
      }
    }

    // If we have multiple wildcard patterns, merge them
    if (wildcardPatterns.size() > 1) {
      String mergedPattern = FieldResolutionContext.mergeWildcardPatterns(wildcardPatterns);
      regularFields.add(mergedPattern);
    } else if (wildcardPatterns.size() == 1) {
      regularFields.add(wildcardPatterns.iterator().next());
    }

    return regularFields;
  }
}
