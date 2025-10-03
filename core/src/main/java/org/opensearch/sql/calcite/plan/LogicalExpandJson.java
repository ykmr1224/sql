/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

public class LogicalExpandJson extends SingleRel {
  public static final String MAP_FIELD_NAME = "_MAP";

  final int jsonField;
  RelDataType rowType;

  public LogicalExpandJson(
      RelOptCluster cluster, RelTraitSet traits, RelNode input, int jsonField) {
    this(cluster, traits, input, jsonField, getDynamicType(cluster));
  }

  private static RelDataType getDynamicType(RelOptCluster cluster) {
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    return typeFactory.createStructType(List.of(getMapType(typeFactory)), List.of(MAP_FIELD_NAME));
  }

  private static RelDataType getMapType(RelDataTypeFactory typeFactory) {
    RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
    return typeFactory.createMapType(varcharType, anyType);
  }

  public LogicalExpandJson(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      int jsonField,
      RelDataType rowType) {
    super(cluster, traits, input);
    this.jsonField = jsonField;
    this.rowType = rowType;
  }

  @Override
  public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
    return new LogicalExpandJson(getCluster(), traits, sole(inputs), jsonField);
  }

  @Override
  protected RelDataType deriveRowType() {
    return this.rowType;
  }

  // Helper to “freeze” with a static row type once keys are known
  public LogicalExpandJson withStaticRowType(RelDataType rowType) {
    return new LogicalExpandJsonFixed(getCluster(), getTraitSet(), getInput(), jsonField, rowType);
  }

  static final class LogicalExpandJsonFixed extends LogicalExpandJson {
    LogicalExpandJsonFixed(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        int jsonField,
        RelDataType rowType) {
      super(cluster, traits, input, jsonField, rowType);
    }

    // ctor sets rowType
    @Override
    protected RelDataType deriveRowType() {
      return rowType;
    }
  }

  public static RelNode expandJson(RelBuilder b, int jsonFieldIndex) {
    final RelNode input = b.build();
    final RelNode node =
        new LogicalExpandJson(input.getCluster(), input.getTraitSet(), input, jsonFieldIndex);
    // push the new node back onto the builder stack
    return b.push(node).peek();
  }
}
