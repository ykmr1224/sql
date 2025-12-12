/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.analysis;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;

/**
 * Context for field resolution analysis. Tracks required fields as the visitor traverses the AST.
 */
public class FieldResolutionContext {

  /**
   * Set of field names required by parent commands. This represents fields that must be provided by
   * child commands.
   */
  @Getter private final Set<String> requiredFields;

  /** Creates a new context with no required fields. */
  public FieldResolutionContext() {
    this.requiredFields = new HashSet<>();
  }

  /**
   * Creates a new context with specified required fields.
   *
   * @param requiredFields Initial set of required fields
   */
  public FieldResolutionContext(Set<String> requiredFields) {
    this.requiredFields = new HashSet<>(requiredFields);
  }

  /**
   * Adds a field to the required fields set.
   *
   * @param fieldName Name of the field to add
   */
  public void addRequiredField(String fieldName) {
    requiredFields.add(fieldName);
  }

  /**
   * Adds multiple fields to the required fields set.
   *
   * @param fieldNames Set of field names to add
   */
  public void addRequiredFields(Set<String> fieldNames) {
    requiredFields.addAll(fieldNames);
  }

  /**
   * Creates a copy of this context with the same required fields.
   *
   * @return New context with copied required fields
   */
  public FieldResolutionContext copy() {
    return new FieldResolutionContext(this.requiredFields);
  }

  /**
   * Returns an unmodifiable view of the required fields.
   *
   * @return Unmodifiable set of required field names
   */
  public Set<String> getRequiredFieldsUnmodifiable() {
    return Collections.unmodifiableSet(requiredFields);
  }

  /**
   * Merges wildcard patterns using AND logic. For example, "prefix*" & "prefix_sub*" becomes
   * "prefix* & prefix_sub*"
   *
   * @param patterns Set of wildcard patterns to merge
   * @return Merged pattern string, or null if no patterns
   */
  public static String mergeWildcardPatterns(Set<String> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return null;
    }
    if (patterns.size() == 1) {
      return patterns.iterator().next();
    }
    // Sort for consistent output
    return String.join(" & ", patterns.stream().sorted().toList());
  }

  @Override
  public String toString() {
    return "FieldResolutionContext{requiredFields=" + requiredFields + "}";
  }
}
