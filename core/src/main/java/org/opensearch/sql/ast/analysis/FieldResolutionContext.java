/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.analysis;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.opensearch.sql.ast.tree.Relation;

/**
 * Context for field resolution using stack-based traversal. Uses Relation node instances as keys to
 * support self-joins.
 */
public class FieldResolutionContext {

  @Getter private final Map<Relation, FieldResolutionResult> relationResults;
  private final Deque<FieldResolutionResult> requirementsStack;

  public FieldResolutionContext() {
    this.relationResults = new IdentityHashMap<>();
    this.requirementsStack = new ArrayDeque<>();
    this.requirementsStack.push(new FieldResolutionResult(Set.of(), "*"));
  }

  public void pushRequirements(FieldResolutionResult result) {
    requirementsStack.push(result);
  }

  public FieldResolutionResult popRequirements() {
    return requirementsStack.pop();
  }

  public FieldResolutionResult getCurrentRequirements() {
    if (requirementsStack.isEmpty()) {
      throw new RuntimeException("empty stack");
    } else {
      return requirementsStack.peek();
    }
  }

  public void setResult(Relation relation, FieldResolutionResult result) {
    relationResults.put(relation, result);
  }

  public Set<Relation> getRelations() {
    return Collections.unmodifiableSet(relationResults.keySet());
  }

  public static String mergeWildcardPatterns(Set<String> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return null;
    }
    if (patterns.size() == 1) {
      return patterns.iterator().next();
    }
    return String.join(" | ", patterns.stream().sorted().toList());
  }

  @Override
  public String toString() {
    return "FieldResolutionContext{relationResults=" + relationResults + "}";
  }
}
