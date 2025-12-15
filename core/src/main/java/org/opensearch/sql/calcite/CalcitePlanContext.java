/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.ast.analysis.FieldResolutionResult;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.expression.function.FunctionProperties;

public class CalcitePlanContext {

  public FrameworkConfig config;
  public final Connection connection;
  public final RelBuilder relBuilder;
  public final ExtendedRexBuilder rexBuilder;
  public final FunctionProperties functionProperties;
  public final QueryType queryType;
  public final SysLimit sysLimit;

  /** This thread local variable is only used to skip script encoding in script pushdown. */
  public static final ThreadLocal<Boolean> skipEncoding = ThreadLocal.withInitial(() -> false);

  /** Thread-local switch that tells whether the current query prefers legacy behavior. */
  private static final ThreadLocal<Boolean> legacyPreferredFlag =
      ThreadLocal.withInitial(() -> true);

  @Getter @Setter private boolean isResolvingJoinCondition = false;
  @Getter @Setter private boolean isResolvingSubquery = false;
  @Getter @Setter private boolean inCoalesceFunction = false;

  /**
   * The flag used to determine whether we do metadata field projection for user 1. If a project is
   * never visited, we will do metadata field projection for user 2. Else not because user may
   * intend to show the metadata field themselves. // TODO: use stack here if we want to do similar
   * projection for subquery.
   */
  @Getter @Setter private boolean isProjectVisited = false;

  private final Stack<RexCorrelVariable> correlVar = new Stack<>();
  private final Stack<List<RexNode>> windowPartitions = new Stack<>();

  @Getter public Map<String, RexLambdaRef> rexLambdaRefMap;

  @Getter @Setter Map<UnresolvedPlan, FieldResolutionResult> fieldResolution;

  private CalcitePlanContext(FrameworkConfig config, SysLimit sysLimit, QueryType queryType) {
    this.config = config;
    this.sysLimit = sysLimit;
    this.queryType = queryType;
    this.connection = CalciteToolsHelper.connect(config, TYPE_FACTORY);
    this.relBuilder = CalciteToolsHelper.create(config, TYPE_FACTORY, connection);
    this.rexBuilder = new ExtendedRexBuilder(relBuilder.getRexBuilder());
    this.functionProperties = new FunctionProperties(QueryType.PPL);
    this.rexLambdaRefMap = new HashMap<>();
  }

  public RexNode resolveJoinCondition(
      UnresolvedExpression expr,
      BiFunction<UnresolvedExpression, CalcitePlanContext, RexNode> transformFunction) {
    isResolvingJoinCondition = true;
    RexNode result = transformFunction.apply(expr, this);
    isResolvingJoinCondition = false;
    return result;
  }

  public Optional<RexCorrelVariable> popCorrelVar() {
    if (!correlVar.empty()) {
      return Optional.of(correlVar.pop());
    } else {
      return Optional.empty();
    }
  }

  public void pushCorrelVar(RexCorrelVariable v) {
    correlVar.push(v);
  }

  public Optional<RexCorrelVariable> peekCorrelVar() {
    if (!correlVar.empty()) {
      return Optional.of(correlVar.peek());
    } else {
      return Optional.empty();
    }
  }

  public CalcitePlanContext clone() {
    return new CalcitePlanContext(config, sysLimit, queryType);
  }

  public static CalcitePlanContext create(
      FrameworkConfig config, SysLimit sysLimit, QueryType queryType) {
    return new CalcitePlanContext(config, sysLimit, queryType);
  }

  /**
   * Executes {@code action} with the thread-local legacy flag set according to the supplied
   * settings.
   */
  public static void run(Runnable action, Settings settings) {
    Boolean preferred = settings.getSettingValue(Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED);
    legacyPreferredFlag.set(preferred);
    try {
      action.run();
    } finally {
      legacyPreferredFlag.remove();
    }
  }

  /**
   * @return {@code true} when the current planning prefer legacy behavior.
   */
  public static boolean isLegacyPreferred() {
    return legacyPreferredFlag.get();
  }

  public void putRexLambdaRefMap(Map<String, RexLambdaRef> candidateMap) {
    this.rexLambdaRefMap.putAll(candidateMap);
  }

  /**
   * Get schema override table for a relation based on field resolution result. All fields are typed
   * as ANY.
   *
   * @param relation The relation to get schema override for
   * @return Optional containing the override RelOptTable with all fields typed as ANY, or empty if
   *     no field resolution exists
   */
  public Optional<RelOptTable> getSchemaOverride(Relation relation) {
    if (fieldResolution == null || !fieldResolution.containsKey(relation)) {
      return Optional.empty();
    }

    FieldResolutionResult result = fieldResolution.get(relation);
    if (!result.hasRegularFields()) {
      return Optional.empty();
    }

    // Build schema with all fields typed as ANY
    var typeFactory = relBuilder.getTypeFactory();
    var builder = typeFactory.builder();

    for (String fieldName : result.getRegularFieldsUnmodifiable()) {
      builder.add(
          fieldName, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.ANY));
    }

    RelDataType rowType = builder.build();

    // Get the original table and create a new one with override schema
    List<String> qualifiedName = relation.getTableQualifiedName().getParts();
    RelOptTable originalTable = relBuilder.getRelOptSchema().getTableForMember(qualifiedName);

    if (originalTable == null) {
      return Optional.empty();
    }

    // Create a wrapper table with the override schema
    return Optional.of(new SchemaOverrideTable(originalTable, rowType));
  }

  /** A wrapper table that overrides the row type of an existing table. */
  private static class SchemaOverrideTable implements RelOptTable {
    private final RelOptTable delegate;
    private final RelDataType overrideRowType;

    SchemaOverrideTable(RelOptTable delegate, RelDataType overrideRowType) {
      this.delegate = delegate;
      this.overrideRowType = overrideRowType;
    }

    @Override
    public RelDataType getRowType() {
      return overrideRowType;
    }

    @Override
    public List<String> getQualifiedName() {
      return delegate.getQualifiedName();
    }

    @Override
    public double getRowCount() {
      return delegate.getRowCount();
    }

    @Override
    public org.apache.calcite.plan.RelOptSchema getRelOptSchema() {
      return delegate.getRelOptSchema();
    }

    @Override
    public RelNode toRel(org.apache.calcite.plan.RelOptTable.ToRelContext context) {
      return delegate.toRel(context);
    }

    @Override
    public List<org.apache.calcite.rel.RelCollation> getCollationList() {
      return delegate.getCollationList();
    }

    @Override
    public org.apache.calcite.rel.RelDistribution getDistribution() {
      return delegate.getDistribution();
    }

    @Override
    public boolean isKey(org.apache.calcite.util.ImmutableBitSet columns) {
      return delegate.isKey(columns);
    }

    @Override
    public List<org.apache.calcite.rel.RelReferentialConstraint> getReferentialConstraints() {
      return delegate.getReferentialConstraints();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public List getColumnStrategies() {
      return delegate.getColumnStrategies();
    }

    @Override
    public RelOptTable extend(List<org.apache.calcite.rel.type.RelDataTypeField> extendedFields) {
      return delegate.extend(extendedFields);
    }

    @Override
    public org.apache.calcite.linq4j.tree.Expression getExpression(Class clazz) {
      return delegate.getExpression(clazz);
    }

    @Override
    public List<org.apache.calcite.util.ImmutableBitSet> getKeys() {
      return delegate.getKeys();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      return delegate.unwrap(clazz);
    }
  }
}
