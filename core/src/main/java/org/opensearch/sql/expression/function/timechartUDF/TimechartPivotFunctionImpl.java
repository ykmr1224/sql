/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.timechartUDF;

import static org.opensearch.sql.calcite.utils.PPLReturnTypes.MAP_STRING_ANY_FORCE_NULLABLE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * TimechartPivotFunctionImpl implements the timechart_pivot function for dynamic columns. This
 * function is used internally by the timechart command when BY field is present to create
 * pivot-style column generation.
 *
 * <p>The function takes timechart parameters and returns a MAP<STRING, ANY> containing the pivoted
 * column data that will be used by the dynamic column resolution system.
 *
 * <p>Usage: timechart_pivot(span_expr, by_field, agg_function, limit, use_other) Returns: MAP with
 * keys as "by_field_value_agg_function" and values as aggregated results
 */
public class TimechartPivotFunctionImpl extends ImplementorUDF {

  public TimechartPivotFunctionImpl() {
    super(new TimechartPivotImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return MAP_STRING_ANY_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    // timechart_pivot(span_expr, by_field, agg_function, limit, use_other)
    return UDFOperandMetadata.wrap(
        OperandTypes.family(
            SqlTypeFamily.ANY, // span expression
            SqlTypeFamily.ANY, // by field
            SqlTypeFamily.ANY, // aggregate function
            SqlTypeFamily.INTEGER, // limit
            SqlTypeFamily.BOOLEAN // use_other
            ));
  }

  public static class TimechartPivotImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(TimechartPivotFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  /**
   * Evaluate the timechart_pivot function.
   *
   * <p>This is a placeholder implementation that returns a MAP indicating that timechart pivot
   * functionality should be handled by the physical execution layer. The actual pivot logic is
   * complex and requires access to the full dataset, so it's implemented in the
   * CalciteRelNodeVisitor.visitTimechart method.
   *
   * @param args Function arguments: [span_expr, by_field, agg_function, limit, use_other]
   * @return MAP<String, Object> containing pivot metadata for physical execution
   */
  public static Object eval(Object... args) {
    if (args.length < 5) {
      return null;
    }

    // Extract parameters
    Object spanExpr = args[0];
    Object byField = args[1];
    Object aggFunction = args[2];
    Integer limit = (Integer) args[3];
    Boolean useOther = (Boolean) args[4];

    // Create metadata map for the physical execution layer
    // This tells the execution engine that this is a timechart pivot operation
    // and provides the necessary parameters
    Map<String, Object> pivotMetadata = new HashMap<>();
    pivotMetadata.put("_timechart_pivot", true);
    pivotMetadata.put("span_expr", spanExpr);
    pivotMetadata.put("by_field", byField);
    pivotMetadata.put("agg_function", aggFunction);
    pivotMetadata.put("limit", limit);
    pivotMetadata.put("use_other", useOther);

    return pivotMetadata;
  }
}
