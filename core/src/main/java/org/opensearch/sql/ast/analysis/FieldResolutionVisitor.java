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
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.utils.WildcardUtils;

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
    boolean isSelectAll =
        node.getProjectList().stream().anyMatch(expr -> expr instanceof AllFields);

    if (isSelectAll) {
      visitChildren(node, context);
    } else {
      Set<String> projectFields = new HashSet<>();
      Set<String> wildcardPatterns = new HashSet<>();
      for (UnresolvedExpression expr : node.getProjectList()) {
        extractFieldsFromExpression(expr)
            .forEach(
                field -> {
                  if (WildcardUtils.containsWildcard(field)) {
                    wildcardPatterns.add(field);
                  } else {
                    projectFields.add(field);
                  }
                });
      }

      context.pushRequirements(new FieldResolutionResult(projectFields, wildcardPatterns));
      visitChildren(node, context);
      context.popRequirements();
    }
    return null;
  }

  @Override
  public Void visitFilter(Filter node, FieldResolutionContext context) {
    Set<String> filterFields = extractFieldsFromExpression(node.getCondition());

    context.pushRequirements(context.getCurrentRequirements().or(filterFields));
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

    context.pushRequirements(context.getCurrentRequirements().or(sortFields));
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

    context.pushRequirements(
        new FieldResolutionResult(allRequiredFields, currentReq.getWildcardPattern()));
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
  public Void visitJoin(Join node, FieldResolutionContext context) {
    Set<String> joinFields = new HashSet<>();

    if (node.getJoinCondition().isPresent()) {
      joinFields.addAll(extractFieldsFromExpression(node.getJoinCondition().get()));
    }

    if (node.getJoinFields().isPresent()) {
      for (Field field : node.getJoinFields().get()) {
        joinFields.addAll(extractFieldsFromExpression(field));
      }
    }

    org.opensearch.sql.ast.analysis.FieldResolutionResult currentReq =
        context.getCurrentRequirements();
    Set<String> baseRequiredFields = new HashSet<>(currentReq.getRegularFields());

    String leftAlias = node.getLeftAlias().orElse(null);
    String rightAlias = node.getRightAlias().orElse(null);

    Set<String> leftFields = filterFieldsByPrefix(baseRequiredFields, leftAlias);
    leftFields.addAll(filterFieldsByPrefix(joinFields, leftAlias));

    Set<String> rightFields = filterFieldsByPrefix(baseRequiredFields, rightAlias);
    rightFields.addAll(filterFieldsByPrefix(joinFields, rightAlias));

    if (node.getLeft() != null) {
      context.pushRequirements(
          new FieldResolutionResult(leftFields, currentReq.getWildcardPattern()));
      node.getLeft().accept(this, context);
      context.popRequirements();
    }

    if (node.getRight() != null) {
      context.pushRequirements(
          new FieldResolutionResult(rightFields, currentReq.getWildcardPattern()));
      node.getRight().accept(this, context);
      context.popRequirements();
    }

    return null;
  }

  private Set<String> filterFieldsByPrefix(Set<String> fields, String alias) {
    if (alias == null) {
      return fields;
    }

    Set<String> filtered = new HashSet<>();
    String prefix = alias + ".";
    for (String field : fields) {
      if (!isPrefixed(field)) {
        filtered.add(field);
      } else if (field.startsWith(prefix)) {
        // Strip the prefix to get the actual field name
        String fieldName = field.substring(prefix.length());
        filtered.add(fieldName);
      }
    }
    return filtered;
  }

  private boolean isPrefixed(String field) {
    return field.contains(".");
  }

  @Override
  public Void visitSubqueryAlias(SubqueryAlias node, FieldResolutionContext context) {
    visitChildren(node, context);
    return null;
  }

  @Override
  public Void visitRelation(Relation node, FieldResolutionContext context) {
    org.opensearch.sql.ast.analysis.FieldResolutionResult currentReq =
        context.getCurrentRequirements();

    context.setResult(
        node,
        new FieldResolutionResult(currentReq.getRegularFields(), currentReq.getWildcardPattern()));
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
