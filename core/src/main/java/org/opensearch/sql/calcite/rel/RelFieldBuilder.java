/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.rel;

import static org.opensearch.sql.calcite.plan.DynamicFieldsConstants.DYNAMIC_FIELDS_MAP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.MAP_REMOVE;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/**
 * Wrapper for RelBuilder to handle field operations considering dynamic fields. It provides
 * explicit methods to access static fields or dynamic fields, and also helper methods to handle
 * dynamic fields operations.
 */
@RequiredArgsConstructor
public class RelFieldBuilder {
  private final RelBuilder relBuilder;
  private final RexBuilder rexBuilder;

  public List<String> getStaticFieldNames() {
    return getStaticFieldNames(0);
  }

  public List<String> getStaticFieldNames(int n) {
    return getAllFieldNames(n).stream()
        .filter(name -> !DYNAMIC_FIELDS_MAP.equals(name))
        .collect(Collectors.toList());
  }

  public List<String> getAllFieldNames() {
    return getAllFieldNames(0);
  }

  public List<String> getAllFieldNames(int inputCount, int inputOrdinal) {
    return getAllFieldNames(getStackPosition(inputCount, inputOrdinal));
  }

  public int getStackPosition(int inputCount, int inputOrdinal) {
    return inputCount - 1 - inputOrdinal;
  }

  public List<String> getAllFieldNames(int n) {
    return relBuilder.peek(n).getRowType().getFieldNames();
  }

  public RexInputRef staticField(String fieldName) {
    return relBuilder.field(fieldName);
  }

  public RexInputRef staticField(int inputCount, int inputOrdinal, String fieldName) {
    return relBuilder.field(inputCount, inputOrdinal, fieldName);
  }

  public RexInputRef staticField(int inputCount, int inputOrdinal, int fieldOrdinal) {
    return relBuilder.field(inputCount, inputOrdinal, fieldOrdinal);
  }

  public RexInputRef staticField(int fieldIndex) {
    return relBuilder.field(fieldIndex);
  }

  public List<RexNode> staticFields() {
    return excludeDynamicField(relBuilder.fields());
  }

  public List<RexNode> staticFields(Iterable<String> fieldNames) {
    return relBuilder.fields(fieldNames);
  }

  public List<RelDataTypeField> staticFieldList() {
    return relBuilder.peek().getRowType().getFieldList().stream()
        .filter(field -> !DYNAMIC_FIELDS_MAP.equals(field.getName()))
        .toList();
  }

  public ImmutableList<RexNode> staticFields(List<? extends Number> ordinals) {
    return relBuilder.fields(ordinals);
  }

  private List<RexNode> excludeDynamicField(List<RexNode> fields) {
    int dynamicFieldsMapIndex = getDynamicFieldsMapIndex();
    if (dynamicFieldsMapIndex >= 0) {
      return IntStream.range(0, fields.size())
          .filter(i -> i != dynamicFieldsMapIndex)
          .mapToObj(i -> fields.get(i))
          .collect(Collectors.toList());
    } else {
      return fields;
    }
  }

  private int getDynamicFieldsMapIndex() {
    return relBuilder.peek().getRowType().getFieldNames().indexOf(DYNAMIC_FIELDS_MAP);
  }

  public boolean isDynamicFieldsExist() {
    return isDynamicFieldsExist(1, 0);
  }

  public boolean isDynamicFieldsExist(int inputCount, int inputOrdinal) {
    return getAllFieldNames(inputCount, inputOrdinal).contains(DYNAMIC_FIELDS_MAP);
  }

  public List<RexNode> metaFieldsRef() {
    List<String> originalFields = relBuilder.peek().getRowType().getFieldNames();
    return originalFields.stream()
        .filter(OpenSearchConstants.METADATAFIELD_TYPE_MAP::containsKey)
        .map(metaField -> (RexNode) relBuilder.field(metaField))
        .toList();
  }

  public RexNode correlField(RexNode correlNode, int index) {
    return relBuilder.field(correlNode, index);
  }

  public RexNode dynamicField(String fieldName) {
    return dynamicField(1, 0, fieldName);
  }

  public RexNode getDynamicFieldsMap() {
    return relBuilder.field(DYNAMIC_FIELDS_MAP);
  }

  public RexNode dynamicField(int inputCount, int inputOrdinal, String fieldName) {
    return createItemAccess(
        relBuilder.field(inputCount, inputOrdinal, DYNAMIC_FIELDS_MAP), fieldName);
  }

  public void removeFieldsFromDynamicFields(List<String> fieldNames) {
    if (isDynamicFieldsExist()) {
      List<RexNode> list = staticFields();
      list.add(getDynamicFieldsWithout(fieldNames));
      relBuilder.project(list);
    }
  }

  public RexNode getDynamicFieldsWithout(List<String> fieldNames) {
    RexNode map = getDynamicFieldsMap();
    if (!fieldNames.isEmpty()) {
      map = mapRemoveCall(map, fieldNames);
    }
    return relBuilder.alias(map, DYNAMIC_FIELDS_MAP);
  }

  private RexNode mapRemoveCall(RexNode mapField, List<String> removedNames) {
    RexNode array = createStringArray(removedNames);
    return PPLFuncImpTable.INSTANCE.resolve(rexBuilder, MAP_REMOVE, mapField, array);
  }

  protected RexNode createStringArray(List<String> values) {
    RelDataType stringType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
    RelDataType arrayType = rexBuilder.getTypeFactory().createArrayType(stringType, -1);

    List<RexNode> elements = new java.util.ArrayList<>();
    for (String value : values) {
      elements.add(rexBuilder.makeLiteral(value));
    }

    return rexBuilder.makeCall(arrayType, SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, elements);
  }

  private RexNode createItemAccess(RexNode field, String itemName) {
    return PPLFuncImpTable.INSTANCE.resolve(
        rexBuilder, BuiltinFunctionName.INTERNAL_ITEM, field, rexBuilder.makeLiteral(itemName));
  }
}
