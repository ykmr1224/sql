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

/**
 * Result of field resolution analysis.
 *
 * <p>Separates regular field names from wildcard patterns to properly handle OR and AND logic:
 *
 * <ul>
 *   <li>Multiple wildcards in same command use OR: fields prefix*, suffix* → (prefix* | suffix*)
 *   <li>Wildcards across commands use AND: fields prefix* | fields *suffix → (prefix* & *suffix)
 * </ul>
 */
@Getter
@EqualsAndHashCode
@ToString
public class FieldResolutionResult {

  /** Set of regular field names (non-wildcard). */
  private final Set<String> regularFields;

  /**
   * Wildcard pattern expression. Can be: - Single pattern: "prefix*" - OR of patterns: "prefix* |
   * suffix*" (from same command) - AND of patterns: "prefix* & *suffix" (from different commands) -
   * null if no wildcards
   */
  private final String wildcardPattern;

  /** Creates a result with only regular fields. */
  public FieldResolutionResult(Set<String> regularFields) {
    this.regularFields = new HashSet<>(regularFields);
    this.wildcardPattern = null;
  }

  /** Creates a result with regular fields and a wildcard pattern. */
  public FieldResolutionResult(Set<String> regularFields, String wildcardPattern) {
    this.regularFields = new HashSet<>(regularFields);
    this.wildcardPattern = wildcardPattern;
  }

  /** Returns an unmodifiable view of regular fields. */
  public Set<String> getRegularFieldsUnmodifiable() {
    return Collections.unmodifiableSet(regularFields);
  }

  /** Checks if this result contains any wildcards. */
  public boolean hasWildcards() {
    return wildcardPattern != null && !wildcardPattern.isEmpty();
  }

  /** Checks if this result contains any regular fields. */
  public boolean hasRegularFields() {
    return !regularFields.isEmpty();
  }

  /**
   * Merges two wildcard patterns using AND logic. Used when combining requirements from different
   * commands.
   *
   * @param pattern1 First wildcard pattern (can be null)
   * @param pattern2 Second wildcard pattern (can be null)
   * @return Merged pattern, or null if both are null
   */
  public static String mergeWithAnd(String pattern1, String pattern2) {
    if (pattern1 == null || pattern1.isEmpty()) {
      return pattern2;
    }
    if (pattern2 == null || pattern2.isEmpty()) {
      return pattern1;
    }
    return "(" + pattern1 + ") & (" + pattern2 + ")";
  }

  /**
   * Merges multiple wildcard patterns using OR logic. Used when combining patterns from the same
   * command.
   *
   * @param patterns Set of wildcard patterns
   * @return Merged pattern, or null if empty
   */
  public static String mergeWithOr(Set<String> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return null;
    }
    if (patterns.size() == 1) {
      return patterns.iterator().next();
    }
    // Sort for consistent output
    return patterns.stream().sorted().reduce((a, b) -> a + " | " + b).orElse(null);
  }

  /**
   * Combines this result with another using AND logic for wildcards. Regular fields are merged into
   * a single set.
   *
   * @param other Another field resolution result
   * @return Combined result
   */
  public FieldResolutionResult combineWithAnd(FieldResolutionResult other) {
    Set<String> combinedFields = new HashSet<>(this.regularFields);
    combinedFields.addAll(other.regularFields);

    String combinedPattern = mergeWithAnd(this.wildcardPattern, other.wildcardPattern);

    return new FieldResolutionResult(combinedFields, combinedPattern);
  }
}
