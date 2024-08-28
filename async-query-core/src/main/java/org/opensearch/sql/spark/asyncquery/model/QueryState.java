/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.opensearch.sql.spark.execution.statement.StatementState;

@Getter
public enum QueryState {
  WAITING("waiting"),
  RUNNING("running"),
  SUCCESS("success"),
  FAILED("failed"),
  TIMEOUT("timeout"),
  CANCELLED("cancelled");

  private final String state;

  QueryState(String state) {
    this.state = state;
  }

  private static final Map<String, StatementState> STATES =
      Arrays.stream(StatementState.values())
          .collect(Collectors.toMap(t -> t.name().toLowerCase(), t -> t));

  public static QueryState fromString(String key) {
    for (QueryState ss : QueryState.values()) {
      if (ss.getState().toLowerCase(Locale.ROOT).equals(key)) {
        return ss;
      }
    }
    throw new IllegalArgumentException("Invalid statement state: " + key);
  }
}
