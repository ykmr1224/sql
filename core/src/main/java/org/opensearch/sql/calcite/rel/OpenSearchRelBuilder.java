/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.rel;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/** It prohibits direct use of field(s) methods to avoid inconsistency with dynamic fields */
public class OpenSearchRelBuilder extends RelBuilder {
  public OpenSearchRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

  @Override
  public AggCall avg(boolean distinct, String alias, RexNode operand) {
    return aggregateCall(
        SqlParserPos.ZERO,
        PPLBuiltinOperators.AVG_NULLABLE,
        distinct,
        false,
        false,
        null,
        null,
        ImmutableList.of(),
        alias,
        ImmutableList.of(),
        ImmutableList.of(operand));
  }

  private CalciteUnsupportedException prohibitedException() {
    return new CalciteUnsupportedException(
        "Direct call to RelBuilder.field(s) methods are prohibited.");
  }

  @Override
  public RexInputRef field(String fieldName) {
    throw prohibitedException();
  }

  @Override
  public RexInputRef field(int inputCount, int inputOrdinal, String fieldName) {
    throw prohibitedException();
  }

  @Override
  public RexInputRef field(int fieldOrdinal) {
    throw prohibitedException();
  }

  @Override
  public RexInputRef field(int inputCount, int inputOrdinal, int fieldOrdinal) {
    throw prohibitedException();
  }

  @Override
  public RexNode field(String alias, String fieldName) {
    throw prohibitedException();
  }

  @Override
  public RexNode field(int inputCount, String alias, String fieldName) {
    throw prohibitedException();
  }

  @Override
  public RexNode field(RexNode e, String name) {
    throw prohibitedException();
  }

  @Override
  public RexNode field(RexNode e, int ordinal) {
    throw prohibitedException();
  }

  @Override
  public ImmutableList<RexNode> fields() {
    throw prohibitedException();
  }

  @Override
  public ImmutableList<RexNode> fields(int inputCount, int inputOrdinal) {
    // access with ordinals is safe
    return super.fields(inputCount, inputOrdinal);
  }

  @Override
  public ImmutableList<RexNode> fields(RelCollation collation) {
    throw prohibitedException();
  }

  @Override
  public ImmutableList<RexNode> fields(List<? extends Number> ordinals) {
    // access with ordinals is safe
    return super.fields(ordinals);
  }

  @Override
  public ImmutableList<RexNode> fields(ImmutableBitSet ordinals) {
    // access with ordinals is safe
    return super.fields(ordinals);
  }

  @Override
  public ImmutableList<RexNode> fields(Iterable<String> fieldNames) {
    throw prohibitedException();
  }

  @Override
  public ImmutableList<RexNode> fields(Mappings.TargetMapping mapping) {
    throw prohibitedException();
  }

  // Access raw fields for wrapper (package private)

  RexInputRef field_(String fieldName) {
    return super.field(fieldName);
  }

  RexInputRef field_(int inputCount, int inputOrdinal, String fieldName) {
    return super.field(inputCount, inputOrdinal, fieldName);
  }

  RexInputRef field_(int fieldOrdinal) {
    return super.field(fieldOrdinal);
  }

  RexInputRef field_(int inputCount, int inputOrdinal, int fieldOrdinal) {
    return super.field(inputCount, inputOrdinal, fieldOrdinal);
  }

  RexNode field_(String alias, String fieldName) {
    return super.field(alias, fieldName);
  }

  RexNode field_(int inputCount, String alias, String fieldName) {
    return super.field(inputCount, alias, fieldName);
  }

  RexNode field_(RexNode e, String name) {
    return super.field(e, name);
  }

  RexNode field_(RexNode e, int ordinal) {
    return super.field(e, ordinal);
  }

  ImmutableList<RexNode> fields_() {
    return super.fields();
  }

  ImmutableList<RexNode> fields_(int inputCount, int inputOrdinal) {
    return super.fields(inputCount, inputOrdinal);
  }

  ImmutableList<RexNode> fields_(RelCollation collation) {
    return super.fields(collation);
  }

  ImmutableList<RexNode> fields_(List<? extends Number> ordinals) {
    return super.fields(ordinals);
  }

  ImmutableList<RexNode> fields_(ImmutableBitSet ordinals) {
    return super.fields(ordinals);
  }

  ImmutableList<RexNode> fields_(Iterable<String> fieldNames) {
    return super.fields(fieldNames);
  }

  ImmutableList<RexNode> fields_(Mappings.TargetMapping mapping) {
    return super.fields(mapping);
  }
}
