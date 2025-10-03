/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Walks a plan, finds JSON paths or ITEM calls above ExpandJson, fixes ExpandJson's row type, and
 * rewrites expressions to input refs.
 */
public final class ExpandJsonMaterializer extends RelShuttleImpl {
  private final RelBuilderFactory relBuilderFactory;

  public ExpandJsonMaterializer(RelBuilderFactory f) {
    this.relBuilderFactory = f;
  }

  @Override
  public RelNode visit(RelNode other) {
    // relBuilderFactory.

    return visitChildren(other);
  }

  // @Override public RelNode visit(LogicalProject project) {
  //   RelNode input = project.getInput().accept(this);

  //   // If input is our ExpandJson, collect keys from the Project expressions
  //   if (input instanceof LogicalExpandJson) {
  //     LogicalExpandJson ej = (LogicalExpandJson) input;

  //     // 1) Discover keys mentioned in project expressions (JSON_VALUE, ITEM, etc.)
  //     final LinkedHashMap<String, RelDataType> keys = new LinkedHashMap<>();
  //     final RelDataTypeFactory tf = project.getCluster().getTypeFactory();

  //     for (RexNode e : project.getProjects()) {
  //       discoverKeys(e, tf, keys); // fill 'keys' deterministically
  //     }

  //     // 2) Build a fixed row type for ExpandJson
  //     final RelDataType fixed =
  //         new RelDataTypeFactory.Builder(tf).addAll(keys.entrySet().stream()
  //           .map(en -> new RelDataTypeFieldImpl(en.getKey(), keys.size(), en.getValue()))
  //           .toList()).build();

  //     // 3) Replace dynamic ExpandJson with fixed one
  //     final LogicalExpandJsonFixed fixedEj =
  //         new LogicalExpandJsonFixed(ej.getCluster(), ej.getTraitSet(), ej.getInput(),
  //                                    ej.jsonField, fixed);

  //     // 4) Rewrite expressions: JSON_VALUE/ITEM -> input refs to fixedEj
  //     final Map<String,Integer> indexByName = indexFields(fixed);
  //     final RexShuttle rewriter = new RexShuttle() {
  //       @Override public RexNode visitCall(RexCall call) {
  //         if (isJsonValueOn(call, ej) || isItemOn(call, ej)) {
  //           String key = extractKey(call);
  //           Integer idx = indexByName.get(key);
  //           if (idx != null) return new RexInputRef(idx,
  // fixed.getFieldList().get(idx).getType());
  //         }
  //         return super.visitCall(call);
  //       }
  //     };
  //     final List<RexNode> newProjects = project.getProjects().stream()
  //         .map(e -> e.accept(rewriter)).toList();

  //     return project.copy(project.getTraitSet(), fixedEj, newProjects, project.getRowType());
  //   }

  //   return project.copy(project.getTraitSet(), input, project.getProjects(),
  // project.getRowType());
  // }

  // -- helper methods discoverKeys / isJsonValueOn / isItemOn / extractKey / indexFields omitted
  // for brevity --
}
