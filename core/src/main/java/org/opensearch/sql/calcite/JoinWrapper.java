/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.calcite.plan.DynamicFieldsConstants.DYNAMIC_FIELDS_MAP;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.opensearch.sql.ast.tree.Lookup;

/** Utility class for join operations with dynamic fields support. */
public class JoinWrapper {
  // Modes for when field name conflicts between left and right inputs
  public enum ConflictResolution {
    RIGHT_OVERWRITE, // non-null right value overwrites left value
    LEFT_OVERWRITE, // non-null left value overwrites right value
    RIGHT_OR_LEFT // right column is used when it exist, otherwise left column is used
  }

  @Getter
  @AllArgsConstructor
  public enum Input {
    LEFT(0),
    RIGHT(1);

    int ordinal;
  }

  /**
   * Performs a left join with dynamic fields support and field merging.
   *
   * @param overwrite if true, right side values overwrite left side values for duplicate fields
   * @param joinFields list of field names to use as join keys
   * @param context CalcitePlanContext
   */
  public static void leftJoin(
      ConflictResolution resolution, List<String> joinFields, CalcitePlanContext context) {
    leftJoin(resolution, getJoinCondition(joinFields, context), context);
  }

  public static void leftJoin(
      ConflictResolution resolution, RexNode joinCondition, CalcitePlanContext context) {
    List<String> leftFields = getLeftFields(context);
    List<String> rightFields = getRightFields(context);

    context.relBuilder.join(JoinRelType.LEFT, joinCondition);

    projectFieldsAfterJoin(leftFields, rightFields, resolution, context);
  }

  private static List<String> getRightFields(CalcitePlanContext context) {
    return getRightNode(context).getRowType().getFieldNames();
  }

  private static List<String> getLeftFields(CalcitePlanContext context) {
    return getLeftNode(context).getRowType().getFieldNames();
  }

  private static RelNode getRightNode(CalcitePlanContext context) {
    return context.relBuilder.peek();
  }

  private static RelNode getLeftNode(CalcitePlanContext context) {
    return context.relBuilder.peek(1);
  }

  private static void projectFieldsAfterJoin(
      List<String> leftFields,
      List<String> rightFields,
      ConflictResolution resolution,
      CalcitePlanContext context) {
    int leftFieldCount = leftFields.size();
    List<RexNode> project = new ArrayList<>();
    Set<String> processedFields = new HashSet<>();
    Optional<RexNode> leftMap = Optional.empty();
    Optional<RexNode> rightMap = Optional.empty();

    // collect fields from left
    for (int i = 0; i < leftFields.size(); i++) {
      String leftFieldName = leftFields.get(i);
      if (DYNAMIC_FIELDS_MAP.equals(leftFieldName)) {
        leftMap = Optional.of(makeInputRef(i, context));
        continue;
      }
      int rightIndex = rightFields.indexOf(leftFieldName);
      if (rightIndex >= 0) {
        RexNode fromLeft = makeInputRef(i, context);
        RexNode fromRight = makeInputRef(leftFieldCount + rightIndex, context);
        RexNode coalesce =
            switch (resolution) {
              case ConflictResolution.RIGHT_OVERWRITE ->
                  context.rexBuilder.coalesce(fromRight, fromLeft);
              case ConflictResolution.LEFT_OVERWRITE ->
                  context.rexBuilder.coalesce(fromLeft, fromRight);
              default -> throw new IllegalStateException("Unsupported conflict resolution");
            };
        project.add(context.relBuilder.alias(coalesce, leftFieldName));
      } else {
        project.add(context.relBuilder.alias(makeInputRef(i, context), leftFieldName));
      }
      processedFields.add(leftFieldName);
    }

    // collect fields from right
    for (int i = 0; i < rightFields.size(); i++) {
      String rightFieldName = rightFields.get(i);
      if (processedFields.contains(rightFieldName)) {
        continue;
      } else if (DYNAMIC_FIELDS_MAP.equals(rightFieldName)) {
        rightMap = Optional.of(makeInputRef(leftFieldCount + i, context));
        continue;
      } else {
        project.add(
            context.relBuilder.alias(makeInputRef(leftFieldCount + i, context), rightFieldName));
      }
    }

    getDynamicFieldsMap(resolution, leftMap, rightMap, context)
        .map(m -> project.add(context.relBuilder.alias(m, DYNAMIC_FIELDS_MAP)));

    context.relBuilder.project(project);
  }

  private static Optional<RexNode> getDynamicFieldsMap(
      ConflictResolution resolution,
      Optional<RexNode> leftMap,
      Optional<RexNode> rightMap,
      CalcitePlanContext context) {
    if (leftMap.isPresent() && rightMap.isPresent()) {
      RexNode concat =
          switch (resolution) {
            case ConflictResolution.RIGHT_OVERWRITE ->
                context.relBuilder.call(
                    SqlLibraryOperators.MAP_CONCAT, leftMap.get(), rightMap.get());
            case ConflictResolution.LEFT_OVERWRITE ->
                context.relBuilder.call(
                    SqlLibraryOperators.MAP_CONCAT, rightMap.get(), leftMap.get());
            default -> throw new IllegalStateException("Unsupported conflict resolution");
          };
      return Optional.of(concat);
    } else if (leftMap.isPresent()) {
      return leftMap;
    } else if (rightMap.isPresent()) {
      return rightMap;
    }
    return Optional.empty();
  }

  private static RexNode makeInputRef(int i, CalcitePlanContext context) {
    return context.rexBuilder.makeInputRef(context.relBuilder.peek(), i);
  }

  static RexNode getJoinCondition(List<String> joinFields, CalcitePlanContext context) {
    return joinFields.stream()
        .map(
            field -> {
              RexNode lookupKey = analyzeFieldsForLookUp(field, Input.RIGHT, context);
              RexNode sourceKey = analyzeFieldsForLookUp(field, Input.LEFT, context);
              return context.rexBuilder.equals(sourceKey, lookupKey);
            })
        .reduce(context.rexBuilder::and)
        .orElse(context.relBuilder.literal(true));
  }

  static RexNode analyzeFieldsForLookUp(String fieldName, Input input, CalcitePlanContext context) {
    return context.relBuilder.field(2, input.getOrdinal(), fieldName);
  }

  public static void adjustJoinInputsForDynamicFields(CalcitePlanContext context) {
    adjustJoinInputsForDynamicFields(Optional.empty(), Optional.empty(), context);
  }

  /** Adjust fields to align the static/dynamic fields for join. */
  public static void adjustJoinInputsForDynamicFields(
      Optional<String> leftAlias, Optional<String> rightAlias, CalcitePlanContext context) {
    if (DynamicFieldsHelper.hasDynamicFields(context.relBuilder.peek())
        || DynamicFieldsHelper.hasDynamicFields(context.relBuilder.peek(1))) {
      // build once to modify the inputs already in the stack.
      RelNode right = context.relBuilder.build();
      RelNode left = context.relBuilder.build();
      org.opensearch.sql.common.utils.DebugUtils.debug(left.explain(), "left.explain()");
      org.opensearch.sql.common.utils.DebugUtils.debug(right.explain(), "right.explain()");
      List<RelNode> inputs =
          DynamicFieldsHelper.adjustInputsForDynamicFields(List.of(right, left), context);
      right = inputs.get(0);
      left = inputs.get(1);
      org.opensearch.sql.common.utils.DebugUtils.debug(left.explain(), "left.explain()");
      org.opensearch.sql.common.utils.DebugUtils.debug(right.explain(), "right.explain()");
      context.relBuilder.push(left);
      // `as(alias)` is needed since `build()` won't preserve alias
      leftAlias.map(alias -> context.relBuilder.as(alias));
      context.relBuilder.push(right);
      rightAlias.map(alias -> context.relBuilder.as(alias));
    }
  }

  /** Add projection for lookup operation based on field mappings. */
  public static void addProjectionForLookup(Lookup node, CalcitePlanContext context) {
    LinkedHashMap<String, String> fieldMapping = getLookupFieldMapping(node, context);

    List<RexNode> projectList =
        fieldMapping.entrySet().stream()
            .map(
                entry -> {
                  RexNode reference = context.relBuilder.field(entry.getKey());
                  return context.relBuilder.alias(reference, entry.getValue());
                })
            .toList();

    context.relBuilder.project(projectList);
  }

  /** Get field mapping for lookup operation. */
  private static LinkedHashMap<String, String> getLookupFieldMapping(
      Lookup node, CalcitePlanContext context) {
    Map<String, String> keyAliasMap = node.getMappingAliasMap();
    Map<String, String> outputAliasMap = node.getOutputAliasMap();
    List<String> keys = new ArrayList<>(keyAliasMap.keySet());
    LinkedHashMap<String, String> fieldMappings = new LinkedHashMap<>();
    for (String key : keys) {
      fieldMappings.put(key, keyAliasMap.get(key));
    }
    if (outputAliasMap != null && !outputAliasMap.isEmpty()) {
      List<String> outputFieldList = new ArrayList<>(outputAliasMap.keySet());
      Collections.sort(outputFieldList); // sort for consistent plan
      for (String outputField : outputFieldList) {
        fieldMappings.put(outputField, outputAliasMap.get(outputField));
      }
    } else {
      List<String> fieldNames = context.relBuilder.peek().getRowType().getFieldNames();
      for (String fieldName : fieldNames) {
        if (!fieldMappings.containsKey(fieldName)) {
          fieldMappings.put(fieldName, fieldName);
        }
      }
    }
    return fieldMappings;
  }
}
