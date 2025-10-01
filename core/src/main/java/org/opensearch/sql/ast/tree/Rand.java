/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/**
 * AST node for the rand command. The rand command adds a field with random name and random value
 * (random type too). This demonstrates DynamicRowType column architecture with a simple example.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Rand extends UnresolvedPlan {

  private UnresolvedPlan child;

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitRand(this, context);
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }
}
