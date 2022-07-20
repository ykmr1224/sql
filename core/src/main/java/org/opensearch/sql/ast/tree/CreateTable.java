/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * AST node for create table statement.
 */
@ToString
@Getter
@EqualsAndHashCode(callSuper = false)
public class CreateTable extends UnresolvedPlan {

  private final UnresolvedExpression tableName;

  private UnresolvedPlan child;

  public CreateTable(UnresolvedExpression viewName) {
    this.tableName = viewName;
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<? extends Node> getChild() {
    return ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitCreateTable(this, context);
  }
}