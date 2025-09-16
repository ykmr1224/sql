/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** AST node represent Timechart operation. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
@lombok.Builder(toBuilder = true)
public class Timechart extends UnresolvedPlan {
  private UnresolvedPlan child;
  private UnresolvedExpression binExpression;
  private UnresolvedExpression aggregateFunction;
  private UnresolvedExpression byField;
  private Integer limit;
  private Boolean useOther;

  public Timechart(UnresolvedPlan child, UnresolvedExpression aggregateFunction) {
    this(child, null, aggregateFunction, null, null, true);
  }

  public Timechart span(UnresolvedExpression binExpression) {
    return toBuilder().binExpression(binExpression).build();
  }

  public Timechart by(UnresolvedExpression byField) {
    return toBuilder().byField(byField).build();
  }

  public Timechart limit(Integer limit) {
    return toBuilder().limit(limit).build();
  }

  public Timechart useOther(Boolean useOther) {
    return toBuilder().useOther(useOther).build();
  }

  @Override
  public Timechart attach(UnresolvedPlan child) {
    return toBuilder().child(child).build();
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitTimechart(this, context);
  }

  /**
   * Rewrite timechart as eval with dynamic columns when BY field is present. This enables
   * pivot-style column generation where BY field values become column names.
   *
   * <p>For example: timechart span=1h count() by host becomes columns like: @timestamp,
   * host1_count, host2_count, host3_count
   *
   * @return Eval node that generates dynamic columns using timechart_pivot function
   */
  public Eval rewriteAsDynamicColumns() {
    if (byField == null) {
      // No BY field means no dynamic columns needed - use standard timechart
      return null;
    }

    // Create timechart_pivot function call that will:
    // 1. Group by time buckets (binExpression) and BY field values
    // 2. Apply the aggregate function
    // 3. Pivot BY field values into MAP keys with aggregated values
    return AstDSL.eval(
        this.child,
        AstDSL.let(
            AstDSL.field("_dynamic_columns"),
            AstDSL.function(
                "timechart_pivot",
                binExpression, // time span expression (e.g., span=1h)
                byField, // field to pivot (e.g., host)
                aggregateFunction, // aggregation function (e.g., count())
                AstDSL.intLiteral(limit != null ? limit : 10), // limit for top N values
                AstDSL.booleanLiteral(
                    useOther != null ? useOther : true) // whether to use OTHER category
                )));
  }

  /**
   * Check if this timechart should use dynamic columns. Dynamic columns are used when there's a BY
   * field for pivoting.
   *
   * @return true if BY field is present, false otherwise
   */
  public boolean isDynamicColumns() {
    return byField != null;
  }
}
