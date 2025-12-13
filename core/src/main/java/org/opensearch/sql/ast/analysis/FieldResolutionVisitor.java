/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.analysis;

import java.util.HashSet;
import java.util.Map;
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
 * Visitor to analyze and collect required fields from PPL AST using stack-based traversal.
 *
 * <p>Supports multiple relations including self-joins by using Relation node instances as keys.
 */
public class FieldResolutionVisitor extends AbstractNodeVisitor<Void, FieldResolutionContext> {

  public Map<Relation, FieldResolutionResult> analyze(UnresolvedPlan plan) {
    FieldResolutionContext context = new FieldResolutionContext();
    plan.accept(this, context);
    return context.getRelationResults();
  }

  /** Convenience method for single-relation queries. */
  public Set<String> analyzeFields(UnresolvedPlan plan) {
    Map<Relation, FieldResolutionResult> results = analyze(plan);
    if (results.isEmpty()) {
      return new HashSet<>();
    }
    Relation relation = results.keySet().iterator().next();
    FieldResolutionResult result = results.get(relation);
    return result != null ? result.getRegularFields() : new HashSet<>();
  }

  @Override
  public Void visitChildren(org.opensearch.sql.ast.Node node, FieldResolutionContext context) {
    for (org.opensearch.sql.ast.Node child : node.getChild()) {
      child.accept(this, context);
    }
    return null;
  }

  @Override
  public Void visitProject(Project node, FieldResolutionContext context) {
    Set<String> projectFields = new HashSet<>();
    for (UnresolvedExpression expr : node.getProjectList()) {
      projectFields.addAll(extractFieldsFromExpression(expr));
    }

    context.pushRequirements(new FieldResolutionResult(projectFields));
    visitChildren(node, context);
    context.popRequirements();
    return null;
  }

  @Override
  public Void visitFilter(Filter node, FieldResolutionContext context) {
    Set<String> filterFields = extractFieldsFromExpression(node.getCondition());
    FieldResolutionResult currentReq = context.getCurrentRequirements();
    Set<String> allRequiredFields = new HashSet<>(currentReq.getRegularFields());
    allRequiredFields.addAll(filterFields);

    context.pushRequirements(new FieldResolutionResult(allRequiredFields));
    visitChildren(node, context);
    context.popRequirements();
    return null;
  }

  @Override
  public Void visitAggregation(Aggregation node, FieldResolutionContext context) {
    Set<String> aggFields = new HashSet<>();
    for (UnresolvedExpression groupExpr : node.getGroupExprList()) {
      aggFields.addAll(extractFieldsFromExpression(groupExpr));
    }
    if (node.getSpan() != null) {
      aggFields.addAll(extractFieldsFromExpression(node.getSpan()));
    }
    for (UnresolvedExpression aggExpr : node.getAggExprList()) {
      aggFields.addAll(extractFieldsFromAggregation(aggExpr));
    }

    context.pushRequirements(new FieldResolutionResult(aggFields));
    visitChildren(node, context);
    context.popRequirements();
    return null;
  }

  @Override
  public Void visitSort(Sort node, FieldResolutionContext context) {
    Set<String> sortFields = new HashSet<>();
    for (Field sortField : node.getSortList()) {
      sortFields.addAll(extractFieldsFromExpression(sortField));
    }

    FieldResolutionResult currentReq = context.getCurrentRequirements();
    Set<String> allRequiredFields = new HashSet<>(currentReq.getRegularFields());
    allRequiredFields.addAll(sortFields);

    context.pushRequirements(new FieldResolutionResult(allRequiredFields));
    visitChildren(node, context);
    context.popRequirements();
    return null;
  }

  @Override
  public Void visitEval(Eval node, FieldResolutionContext context) {
    Set<String> evalInputFields = new HashSet<>();
    Set<String> computedFields = new HashSet<>();

    for (Let letExpr : node.getExpressionList()) {
      evalInputFields.addAll(extractFieldsFromExpression(letExpr.getExpression()));
      computedFields.add(letExpr.getVar().getField().toString());
    }

    FieldResolutionResult currentReq = context.getCurrentRequirements();
    Set<String> allRequiredFields = new HashSet<>(currentReq.getRegularFields());
    allRequiredFields.removeAll(computedFields);
    allRequiredFields.addAll(evalInputFields);

    context.pushRequirements(new FieldResolutionResult(allRequiredFields));
    visitChildren(node, context);
    context.popRequirements();
    return null;
  }

  private Set<String> extractFieldsFromExpression(UnresolvedExpression expr) {
    Set<String> fields = new HashSet<>();
    if (expr == null) {
      return fields;
    }

    if (expr instanceof Field field) {
      fields.add(field.getField().toString());
    } else if (expr instanceof Alias alias) {
      fields.addAll(extractFieldsFromExpression(alias.getDelegated()));
    } else if (expr instanceof Function function) {
      for (UnresolvedExpression arg : function.getFuncArgs()) {
        fields.addAll(extractFieldsFromExpression(arg));
      }
    } else if (expr instanceof Span span) {
      fields.addAll(extractFieldsFromExpression(span.getField()));
    } else if (expr instanceof Literal) {
      return fields;
    } else {
      for (org.opensearch.sql.ast.Node child : expr.getChild()) {
        if (child instanceof UnresolvedExpression childExpr) {
          fields.addAll(extractFieldsFromExpression(childExpr));
        }
      }
    }
    return fields;
  }

  @Override
  public Void visitRelation(Relation node, FieldResolutionContext context) {
    FieldResolutionResult currentReq = context.getCurrentRequirements();
    Set<String> allFields = new HashSet<>(currentReq.getRegularFields());

    if (allFields.isEmpty()) {
      allFields.add("*");
    }

    Set<String> regularFields = new HashSet<>();
    Set<String> wildcardPatterns = new HashSet<>();

    for (String field : allFields) {
      if (field.contains("*")) {
        wildcardPatterns.add(field);
      } else {
        regularFields.add(field);
      }
    }

    String wildcardPattern =
        wildcardPatterns.isEmpty()
            ? null
            : FieldResolutionContext.mergeWildcardPatterns(wildcardPatterns);

    context.setResult(node, new FieldResolutionResult(regularFields, wildcardPattern));
    return null;
  }

  private Set<String> extractFieldsFromAggregation(UnresolvedExpression expr) {
    Set<String> fields = new HashSet<>();
    if (expr instanceof Alias alias) {
      return extractFieldsFromAggregation(alias.getDelegated());
    } else if (expr instanceof AggregateFunction aggFunc) {
      if (aggFunc.getField() != null) {
        fields.addAll(extractFieldsFromExpression(aggFunc.getField()));
      }
      if (aggFunc.getArgList() != null) {
        for (UnresolvedExpression arg : aggFunc.getArgList()) {
          fields.addAll(extractFieldsFromExpression(arg));
        }
      }
    }
    return fields;
  }
}
