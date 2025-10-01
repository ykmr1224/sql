/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Test for Rand AST node demonstrating simple DynamicRowType usage. */
class RandTest {

  @Test
  void testRandNodeCreation() {
    Rand randNode = new Rand();

    assertNotNull(randNode);
    assertEquals("Rand", randNode.getClass().getSimpleName());
  }

  @Test
  void testRandNodeHasNoChildren() {
    Rand randNode = new Rand();

    List<UnresolvedPlan> children = randNode.getChild();
    assertNotNull(children);
    assertTrue(children.isEmpty());
  }

  @Test
  void testRandNodeAttach() {
    Rand randNode = new Rand();
    UnresolvedPlan childNode = new Rand(); // Use another Rand as dummy child

    UnresolvedPlan attached = randNode.attach(childNode);
    assertNotNull(attached);
    assertTrue(attached instanceof Rand);
  }

  @Test
  void testRandNodeToString() {
    Rand randNode = new Rand();

    String toString = randNode.toString();
    assertNotNull(toString);
    assertTrue(toString.contains("Rand"));
  }
}
