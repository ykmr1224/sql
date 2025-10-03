/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.type.MapOnlyRelDataType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/**
 * Utility class for processing MAP-only schema field access. This processor handles field
 * resolution using only MAP storage with ITEM function access, eliminating the need for
 * post-processing expansion.
 */
@UtilityClass
public class MapOnlyColumnProcessor {

  /**
   * Checks if a field should be resolved using MAP-only access. This happens when: 1. The current
   * row type is MapOnlyRelDataType 2. We're not in a coalesce function (which has special null
   * handling)
   *
   * @param fieldName The field name to resolve
   * @param context CalcitePlanContext containing the RelBuilder and other context
   * @return true if the field should be resolved using MAP-only access
   */
  public static boolean shouldUseMapOnlyAccess(String fieldName, CalcitePlanContext context) {
    // Don't resolve dynamic fields in coalesce function (it has special null handling)
    if (context.isInCoalesceFunction()) {
      return false;
    }

    // Check if current row type is MapOnlyRelDataType OR if _MAP field exists
    List<String> currentFields = context.relBuilder.peek().getRowType().getFieldNames();
    return context.relBuilder.peek().getRowType() instanceof MapOnlyRelDataType
        || currentFields.contains(MapOnlyRelDataType.MAP_FIELD_NAME);
  }

  /**
   * Resolves a field using MAP-only access with ITEM function. Converts: fieldName -> ITEM(_MAP,
   * 'fieldName') For known fields, applies type casting: CAST(ITEM(_MAP, 'fieldName'), KNOWN_TYPE)
   *
   * @param fieldName The field name to resolve
   * @param context CalcitePlanContext containing the RelBuilder and other context
   * @return RexNode representing the field access
   */
  public static RexNode resolveMapOnlyField(String fieldName, CalcitePlanContext context) {
    // Create MAP field reference
    RexNode mapField = context.relBuilder.field(MapOnlyRelDataType.MAP_FIELD_NAME);

    // Create ITEM access: ITEM(_MAP, 'fieldName')
    RexNode keyLiteral = context.rexBuilder.makeLiteral(fieldName);
    RexNode itemAccess =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.INTERNAL_ITEM, mapField, keyLiteral);

    // Apply type casting for known fields if we have MapOnlyRelDataType
    var rowType = context.relBuilder.peek().getRowType();
    if (rowType instanceof MapOnlyRelDataType mapOnlyType && mapOnlyType.hasKnownField(fieldName)) {
      var knownType = mapOnlyType.getKnownFieldType(fieldName);
      itemAccess = context.rexBuilder.makeCast(knownType, itemAccess);
    }

    // CONDITIONAL ALIASING: Apply aliasing when in fields command context
    if (context.isInFieldsCommand()) {
      return context.relBuilder.alias(itemAccess, fieldName);
    } else {
      return itemAccess;
    }
  }

  /**
   * Creates a MapOnlyRelDataType from a list of current field names and their types. This is used
   * when transitioning from a regular schema to MAP-only schema.
   *
   * @param currentFields List of current field names
   * @param context CalcitePlanContext containing type factory and other context
   * @return MapOnlyRelDataType with the current fields as known fields
   */
  public static MapOnlyRelDataType createMapOnlyTypeFromCurrentFields(
      List<String> currentFields, CalcitePlanContext context) {
    var currentRowType = context.relBuilder.peek().getRowType();
    var knownFields = new java.util.HashMap<String, org.apache.calcite.rel.type.RelDataType>();

    // Collect known field types from current schema
    for (String fieldName : currentFields) {
      var field = currentRowType.getField(fieldName, false, false);
      if (field != null) {
        knownFields.put(fieldName, field.getType());
      } else {
        // Unknown field, use ANY type
        knownFields.put(
            fieldName, context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.ANY));
      }
    }

    return new MapOnlyRelDataType(context.rexBuilder.getTypeFactory(), knownFields);
  }

  /**
   * Ensures that the MAP field exists in the schema for MAP-only access. Adds a MAP field if it
   * doesn't exist and transitions to MAP-only schema.
   *
   * @param context CalcitePlanContext containing the RelBuilder and other context
   */
  public static void ensureMapOnlySchema(CalcitePlanContext context) {
    var currentRowType = context.relBuilder.peek().getRowType();

    // If already MAP-only, nothing to do
    if (currentRowType instanceof MapOnlyRelDataType) {
      return;
    }

    // Transition to MAP-only schema
    var currentFields = currentRowType.getFieldNames();
    var mapOnlyType = createMapOnlyTypeFromCurrentFields(currentFields, context);

    // Create MAP field with all current fields
    RexNode mapField = createMapFromCurrentFields(currentFields, context);

    // Project only the MAP field with the new type
    context.relBuilder.project(List.of(mapField), List.of(MapOnlyRelDataType.MAP_FIELD_NAME));

    // Update the row type to MapOnlyRelDataType
    // Note: This is a conceptual step - in practice, Calcite will infer the type
    // The MapOnlyRelDataType information is maintained separately for optimization
  }

  /**
   * Creates a MAP expression containing all current fields. This is used when transitioning from
   * regular schema to MAP-only schema.
   *
   * @param currentFields List of current field names
   * @param context CalcitePlanContext containing the RelBuilder and other context
   * @return RexNode representing a MAP with all current fields
   */
  private static RexNode createMapFromCurrentFields(
      List<String> currentFields, CalcitePlanContext context) {
    // Create MAP constructor with field name -> field value pairs
    var mapArgs = new java.util.ArrayList<RexNode>();

    for (String fieldName : currentFields) {
      // Add key (field name)
      mapArgs.add(context.rexBuilder.makeLiteral(fieldName));
      // Add value (field reference)
      mapArgs.add(context.relBuilder.field(fieldName));
    }

    // Use MAP constructor (this would need to be implemented or use existing MAP functions)
    // For now, create a placeholder - this would need proper MAP construction logic
    return context.rexBuilder.makeNullLiteral(
        context
            .rexBuilder
            .getTypeFactory()
            .createMapType(
                context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.ANY)));
  }

  /**
   * Checks if the current schema is already MAP-only.
   *
   * @param context CalcitePlanContext containing the RelBuilder and other context
   * @return true if the current schema is MAP-only
   */
  public static boolean isMapOnlySchema(CalcitePlanContext context) {
    return context.relBuilder.peek().getRowType() instanceof MapOnlyRelDataType;
  }

  /**
   * Gets the MapOnlyRelDataType from the current context if available.
   *
   * @param context CalcitePlanContext containing the RelBuilder and other context
   * @return MapOnlyRelDataType if current schema is MAP-only, null otherwise
   */
  public static MapOnlyRelDataType getMapOnlyType(CalcitePlanContext context) {
    var rowType = context.relBuilder.peek().getRowType();
    return rowType instanceof MapOnlyRelDataType ? (MapOnlyRelDataType) rowType : null;
  }
}
