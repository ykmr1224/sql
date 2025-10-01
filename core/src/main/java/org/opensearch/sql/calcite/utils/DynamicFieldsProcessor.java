/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

/**
 * Utility class for processing dynamic columns in query results. Handles expansion of MAP fields
 * containing dynamic columns into individual columns.
 */
@UtilityClass
public class DynamicFieldsProcessor {

  public static final String DYNAMIC_FIELDS = "__dynamic_fields__";

  /**
   * Convert dynamic columns in the QueryResponse into individual columns.
   *
   * @param response Original QueryResponse with MAP fields
   * @return New QueryResponse with expanded individual columns
   */
  public static QueryResponse expandDynamicFields(QueryResponse response) {
    if (!hasDynamicFields(response)) {
      return response;
    }

    Set<String> dynamicFieldNames = collectDynamicFieldNames(response.getResults());
    Schema expandedSchema = createExpandedSchema(response.getSchema(), dynamicFieldNames);
    List<ExprValue> expandedResults =
        expandResultRowsWithDynamicFields(response.getResults(), dynamicFieldNames);

    return new QueryResponse(expandedSchema, expandedResults, response.getCursor());
  }

  /**
   * Checks if the response contains dynamic columns (MAP fields with DYNAMIC_COLUMNS_FIELD name).
   */
  private static boolean hasDynamicFields(QueryResponse response) {
    return response.getSchema().getColumns().stream()
        .anyMatch(column -> DYNAMIC_FIELDS.equals(column.getName()));
  }

  private static Set<String> collectDynamicFieldNames(List<ExprValue> results) {
    Set<String> columnNames = new TreeSet<>(); // Use TreeSet for consistent ordering

    for (ExprValue row : results) {
      if (row instanceof ExprTupleValue) {
        ExprValue dynamicColumnsValue = row.tupleValue().get(DYNAMIC_FIELDS);
        Map<String, ExprValue> mapValue = extractMapValue(dynamicColumnsValue);
        columnNames.addAll(mapValue.keySet());
      }
    }

    return columnNames;
  }

  /** It will deduplicate columns if dynamic column name conflicts with original schema */
  private static Schema createExpandedSchema(
      Schema originalSchema, Set<String> dynamicColumnNames) {
    Set<String> originalColumnNames = collectColumnNames(originalSchema);
    List<Column> expandedColumns = getColumnsExcludingDynamic(originalSchema);

    for (String columnName : dynamicColumnNames) {
      if (!originalColumnNames.contains(columnName)) {
        expandedColumns.add(new Column(columnName, null, ExprCoreType.STRING));
      }
    }

    return new Schema(expandedColumns);
  }

  private static Set<String> collectColumnNames(Schema schema) {
    return schema.getColumns().stream().map(Column::getName).collect(Collectors.toSet());
  }

  private static List<Column> getColumnsExcludingDynamic(Schema originalSchema) {
    return originalSchema.getColumns().stream()
        .filter(c -> !DYNAMIC_FIELDS.equals(c.getName()))
        .collect(Collectors.toList());
  }

  private static List<ExprValue> expandResultRowsWithDynamicFields(
      List<ExprValue> originalResults, Set<String> dynamicColumnNames) {
    List<ExprValue> expandedResults = new ArrayList<>();

    for (ExprValue row : originalResults) {
      if (row instanceof ExprTupleValue) {
        Map<String, ExprValue> originalRow = row.tupleValue();
        Map<String, ExprValue> expandedRow = copyExceptDynamic(originalRow);

        Map<String, ExprValue> dynamicColumns = extractMapValue(originalRow.get(DYNAMIC_FIELDS));

        // This will overwrite existing columns if there's a name conflict
        for (String columnName : dynamicColumnNames) {
          ExprValue columnValue = dynamicColumns.get(columnName);
          if (columnValue != null) {
            expandedRow.put(columnName, columnValue);
          } else {
            expandedRow.put(columnName, ExprValueUtils.nullValue());
          }
        }

        expandedResults.add(ExprTupleValue.fromExprValueMap(expandedRow));
      } else {
        // Non-tuple rows are passed through unchanged
        expandedResults.add(row);
      }
    }

    return expandedResults;
  }

  private static Map<String, ExprValue> copyExceptDynamic(Map<String, ExprValue> originalRow) {
    Map<String, ExprValue> copiedRow = new LinkedHashMap<>();
    for (Map.Entry<String, ExprValue> entry : originalRow.entrySet()) {
      if (!DYNAMIC_FIELDS.equals(entry.getKey())) {
        copiedRow.put(entry.getKey(), entry.getValue());
      }
    }
    return copiedRow;
  }

  private static Map<String, ExprValue> extractMapValue(ExprValue mapValue) {
    if (mapValue == null || mapValue.isNull() || mapValue.isMissing()) {
      return Map.of();
    }

    if (mapValue instanceof ExprTupleValue) {
      return mapValue.tupleValue();
    }

    throw new IllegalStateException(
        "Expected MAP type for dynamic columns field, got: "
            + mapValue.type()
            + " value: "
            + mapValue.toString());
  }
}
