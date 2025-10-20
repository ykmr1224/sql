/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.rel;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

@RequiredArgsConstructor
public class RelFieldBuilder {
  public static final String _MAP = "_MAP";

  private final RelBuilder relBuilder;
  private final RexBuilder rexBuilder;

  public List<String> getStaticFieldNames() {
    return getStaticFieldNames(0);
  }

  public List<String> getStaticFieldNames(int n) {
    return getAllFieldNames(n).stream()
        .filter(name -> !_MAP.equals(name))
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
    return relBuilder.fields().stream()
        .filter(node -> !_MAP.equals(node))
        .collect(Collectors.toList());
  }

  public List<RexNode> staticFields(Iterable<String> fieldNames) {
    return relBuilder.fields(fieldNames);
  }

  public ImmutableList<RexNode> staticFields(List<? extends Number> ordinals) {
    return relBuilder.fields(ordinals);
  }

  public boolean isDynamicFieldsExist() {
    return isDynamicFieldsExist(1, 0);
  }

  public boolean isDynamicFieldsExist(int inputCount, int inputOrdinal) {
    return getAllFieldNames(inputCount, inputOrdinal).contains(_MAP);
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

  public RexNode dynamicField(int inputCount, int inputOrdinal, String fieldName) {
    return createItemAccess(relBuilder.field(inputCount, inputOrdinal, _MAP), fieldName);
  }

  private RexNode createItemAccess(RexNode field, String itemName) {
    return PPLFuncImpTable.INSTANCE.resolve(
        rexBuilder, BuiltinFunctionName.INTERNAL_ITEM, field, rexBuilder.makeLiteral(itemName));
  }
}
