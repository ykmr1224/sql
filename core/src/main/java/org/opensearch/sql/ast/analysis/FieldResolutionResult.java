/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.analysis;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Field resolution result separating regular fields from wildcard patterns. */
@Getter
@EqualsAndHashCode
@ToString
public class FieldResolutionResult {

  private final Set<String> regularFields;
  private final String wildcardPattern;

  public FieldResolutionResult(Set<String> regularFields) {
    this.regularFields = new HashSet<>(regularFields);
    this.wildcardPattern = null;
  }

  public FieldResolutionResult(Set<String> regularFields, String wildcardPattern) {
    this.regularFields = new HashSet<>(regularFields);
    this.wildcardPattern = wildcardPattern;
  }

  public FieldResolutionResult(Set<String> regularFields, Set<String> wildcardPatterns) {
    this.regularFields = new HashSet<>(regularFields);
    this.wildcardPattern = mergeWithOr(wildcardPatterns);
  }

  public Set<String> getRegularFieldsUnmodifiable() {
    return Collections.unmodifiableSet(regularFields);
  }

  public boolean hasWildcards() {
    return wildcardPattern != null && !wildcardPattern.isEmpty();
  }

  public boolean hasRegularFields() {
    return !regularFields.isEmpty();
  }

  public static String mergeWithAnd(String pattern1, String pattern2) {
    if (pattern1 == null || pattern1.isEmpty()) {
      return pattern2;
    }
    if (pattern2 == null || pattern2.isEmpty()) {
      return pattern1;
    }
    return "(" + pattern1 + ") & (" + pattern2 + ")";
  }

  public static String mergeWithOr(Set<String> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return null;
    }
    if (patterns.size() == 1) {
      return patterns.iterator().next();
    }
    return patterns.stream().sorted().reduce((a, b) -> a + " | " + b).orElse(null);
  }

  public FieldResolutionResult combineWithAnd(FieldResolutionResult other) {
    Set<String> combinedFields = new HashSet<>(this.regularFields);
    combinedFields.addAll(other.regularFields);
    String combinedPattern = mergeWithAnd(this.wildcardPattern, other.wildcardPattern);
    return new FieldResolutionResult(combinedFields, combinedPattern);
  }

  public FieldResolutionResult or(Set<String> fields) {
    Set<String> combinedFields = new HashSet<>(this.regularFields);
    combinedFields.addAll(fields);
    return new FieldResolutionResult(combinedFields, this.wildcardPattern);
  }
}
