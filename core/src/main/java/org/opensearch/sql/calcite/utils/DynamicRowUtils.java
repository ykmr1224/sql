/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.type.DynamicRowType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/**
 * Utility class for handling DynamicRowType column operations. Provides functions for creating,
 * merging, and detecting DynamicRowType columns.
 */
public class DynamicRowUtils {

  /** Name of the special column that contains all dynamic fields */
  public static final String DYNAMIC_FIELDS_COLUMN = "__dynamic_fields__";

  /**
   * Check if the current RelNode has a DynamicRowType column.
   *
   * @param context CalcitePlanContext
   * @return true if __dynamic_fields__ column exists with DynamicRowType
   */
  public static boolean hasDynamicRowColumn(CalcitePlanContext context) {
    List<RelDataTypeField> fields = context.relBuilder.peek().getRowType().getFieldList();
    return fields.stream().anyMatch(field -> DYNAMIC_FIELDS_COLUMN.equals(field.getName()));
  }

  /**
   * Get the DynamicRowType column from the current RelNode.
   *
   * @param context CalcitePlanContext
   * @return RexNode representing the __dynamic_fields__ column
   * @throws IllegalStateException if no DynamicRowType column exists
   */
  public static Optional<RexNode> getDynamicRowColumn(CalcitePlanContext context) {
    if (hasDynamicRowColumn(context)) {
      return Optional.of(context.relBuilder.field(DYNAMIC_FIELDS_COLUMN));
    }
    return Optional.empty();
  }

  /**
   * Create a rand function that generates MAP<String, Object> with random field. This function
   * generates a field with random name and random value (random type too).
   *
   * @param context CalcitePlanContext
   * @return RexNode representing the rand function call
   */
  public static RexNode createRandFunction(CalcitePlanContext context) {
    // Use the RAND_FIELD function to generate MAP<String, Object> with random field
    return PPLFuncImpTable.INSTANCE.resolve(context.rexBuilder, BuiltinFunctionName.RAND_FIELD);
  }

  /**
   * Create initial DynamicRowType column containing all existing fields plus new cols fields. This
   * is used for the first cols operation to convert from regular schema to DynamicRowType.
   *
   * @param existingFields Current fields in the RelNode
   * @param colsMap MAP<String, Integer> from cols function
   * @param context CalcitePlanContext
   * @return RexNode representing the new DynamicRowType column
   */
  public static RexNode createInitialDynamicRow(
      List<RexNode> existingFields, RexNode colsMap, CalcitePlanContext context) {

    // Two-step approach:
    // 1. Create dynamic row from existing fields with their original names
    // 2. Merge it with the new cols MAP

    // Get the original field names from the current RelNode
    List<String> originalFieldNames = context.relBuilder.peek().getRowType().getFieldNames();

    // Step 1: Create dynamic row from existing fields using original names
    RexNode existingFieldsMap =
        createDynamicRowWithFieldNames(existingFields, originalFieldNames, context);

    // Step 2: Merge with the new cols MAP
    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder, BuiltinFunctionName.MERGE_DYNAMIC_ROW, existingFieldsMap, colsMap);
  }

  public static RexNode createDynamicRowColumn(CalcitePlanContext context) {
    List<String> originalFieldNames = context.relBuilder.peek().getRowType().getFieldNames();
    System.out.println(
        "#### createDynamicRowColumn ##### originalFieldNames: "
            + originalFieldNames.stream().collect(Collectors.joining(", ")));
    List<RexNode> existingFields = context.relBuilder.fields();
    return DynamicRowUtils.createDynamicRowWithFieldNames(
        existingFields, originalFieldNames, context);
  }

  /**
   * Create a dynamic row MAP from existing fields using their original field names.
   *
   * @param existingFields Current field values
   * @param fieldNames Original field names
   * @param context CalcitePlanContext
   * @return RexNode representing the MAP with original field names
   */
  public static RexNode createDynamicRowWithFieldNames(
      List<RexNode> existingFields, List<String> fieldNames, CalcitePlanContext context) {

    if (existingFields.size() != fieldNames.size()) {
      throw new IllegalArgumentException("Field values and names must have same length");
    }

    if (existingFields.isEmpty()) {
      return PPLFuncImpTable.INSTANCE.resolve(
          context.rexBuilder, BuiltinFunctionName.CREATE_DYNAMIC_ROW);
    }

    // Create MAP entries using original field names
    List<RexNode> mapEntries = new ArrayList<>();
    for (int i = 0; i < existingFields.size(); i++) {
      mapEntries.add(context.rexBuilder.makeLiteral(fieldNames.get(i)));
      // Cast the field value to VARCHAR to ensure consistent types in the MAP
      RexNode castedValue =
          context.rexBuilder.makeCast(
              context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.ANY),
              existingFields.get(i),
              true,
              true);
      mapEntries.add(castedValue);
    }

    // Create the MAP containing all existing fields with their original names
    return context.rexBuilder.makeCall(
        SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, mapEntries.toArray(new RexNode[0]));
  }

  /**
   * Merge existing DynamicRowType column with new cols MAP result. This is used for subsequent cols
   * operations to add new dynamic fields.
   *
   * @param existingDynamicRow Current DynamicRowType column
   * @param newColsMap New MAP<String, Integer> from cols function
   * @param context CalcitePlanContext
   * @return RexNode representing the merged DynamicRowType column
   */
  public static RexNode mergeDynamicRow(
      RexNode existingDynamicRow, RexNode newColsMap, CalcitePlanContext context) {

    // Use a special function to merge DynamicRowType with new cols MAP
    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder, BuiltinFunctionName.MERGE_DYNAMIC_ROW, existingDynamicRow, newColsMap);
  }

  /**
   * Create a DynamicRowType schema that can hold the specified fields. This is used during planning
   * to create the proper type information.
   *
   * @param fieldNames List of field names to include
   * @param fieldTypes List of field types (must match fieldNames length)
   * @param typeFactory OpenSearchTypeFactory
   * @return DynamicRowType containing the specified fields
   */
  public static DynamicRowType createDynamicRowTypeSchema(
      List<String> fieldNames, List<RelDataType> fieldTypes, OpenSearchTypeFactory typeFactory) {

    if (fieldNames.size() != fieldTypes.size()) {
      throw new IllegalArgumentException("Field names and types must have same length");
    }

    DynamicRowType.Builder builder = DynamicRowType.builder(typeFactory);

    for (int i = 0; i < fieldNames.size(); i++) {
      builder.addKnownField(fieldNames.get(i), fieldTypes.get(i));
    }

    return builder.build();
  }

  /**
   * Create a DynamicRowType schema for cols operation output. This creates a schema that includes
   * original fields plus potential dynamic fields.
   *
   * @param originalFields Original field information
   * @param colsFields Fields being processed by cols (for type planning)
   * @param typeFactory OpenSearchTypeFactory
   * @return DynamicRowType schema for planning purposes
   */
  public static DynamicRowType createColsOutputSchema(
      List<RelDataTypeField> originalFields,
      List<Field> colsFields,
      OpenSearchTypeFactory typeFactory) {

    DynamicRowType.Builder builder = DynamicRowType.builder(typeFactory);

    // Add all original fields
    for (RelDataTypeField field : originalFields) {
      builder.addKnownField(field.getName(), field.getType());
    }

    // Add potential dynamic fields (we don't know the exact names at planning time,
    // but we know they will be INTEGER type from cols operations)
    // For planning purposes, we can add some example dynamic fields
    for (Field colsField : colsFields) {
      String fieldName = colsField.getField().toString();
      // Add example dynamic fields that might be generated
      builder.addKnownField(
          fieldName + "_dynamic_example", typeFactory.createSqlType(SqlTypeName.INTEGER));
    }

    return builder.build();
  }
}
