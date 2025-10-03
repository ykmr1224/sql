/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.ExtendedRexBuilder;
import org.opensearch.sql.calcite.type.MapOnlyRelDataType;

@ExtendWith(MockitoExtension.class)
class MapOnlyColumnProcessorTest {

  @Mock private CalcitePlanContext context;
  @Mock private RelBuilder relBuilder;
  @Mock private ExtendedRexBuilder rexBuilder;

  private MapOnlyRelDataType mapOnlyType;

  @BeforeEach
  void setUp() {
    when(context.relBuilder).thenReturn(relBuilder);
    when(context.rexBuilder).thenReturn(rexBuilder);
    when(rexBuilder.getTypeFactory()).thenReturn(OpenSearchTypeFactory.TYPE_FACTORY);

    // Create a MapOnlyRelDataType with some known fields
    Map<String, RelDataType> knownFields =
        Map.of(
            "name", OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
            "age", OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
            "active", OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN));

    mapOnlyType = new MapOnlyRelDataType(OpenSearchTypeFactory.TYPE_FACTORY, knownFields);
  }

  @Test
  void testShouldUseMapOnlyAccess_WithMapOnlyType() {
    when(relBuilder.peek()).thenReturn(mock(org.apache.calcite.rel.RelNode.class));
    when(relBuilder.peek().getRowType()).thenReturn(mapOnlyType);
    when(context.isInCoalesceFunction()).thenReturn(false);

    assertTrue(MapOnlyColumnProcessor.shouldUseMapOnlyAccess("testField", context));
  }

  @Test
  void testShouldUseMapOnlyAccess_WithCoalesceFunction() {
    when(relBuilder.peek()).thenReturn(mock(org.apache.calcite.rel.RelNode.class));
    when(relBuilder.peek().getRowType()).thenReturn(mapOnlyType);
    when(context.isInCoalesceFunction()).thenReturn(true);

    assertFalse(MapOnlyColumnProcessor.shouldUseMapOnlyAccess("testField", context));
  }

  @Test
  void testShouldUseMapOnlyAccess_WithRegularType() {
    RelDataType regularType = OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    when(relBuilder.peek()).thenReturn(mock(org.apache.calcite.rel.RelNode.class));
    when(relBuilder.peek().getRowType()).thenReturn(regularType);
    when(context.isInCoalesceFunction()).thenReturn(false);

    assertFalse(MapOnlyColumnProcessor.shouldUseMapOnlyAccess("testField", context));
  }

  @Test
  void testIsMapOnlySchema() {
    when(relBuilder.peek()).thenReturn(mock(org.apache.calcite.rel.RelNode.class));
    when(relBuilder.peek().getRowType()).thenReturn(mapOnlyType);

    assertTrue(MapOnlyColumnProcessor.isMapOnlySchema(context));
  }

  @Test
  void testGetMapOnlyType() {
    when(relBuilder.peek()).thenReturn(mock(org.apache.calcite.rel.RelNode.class));
    when(relBuilder.peek().getRowType()).thenReturn(mapOnlyType);

    MapOnlyRelDataType result = MapOnlyColumnProcessor.getMapOnlyType(context);
    assertNotNull(result);
    assertEquals(mapOnlyType, result);
  }

  @Test
  void testCreateMapOnlyTypeFromCurrentFields() {
    // Mock a regular RelDataType with some fields
    RelDataType regularType = mock(RelDataType.class);
    when(regularType.getField("name", false, false))
        .thenReturn(
            new org.apache.calcite.rel.type.RelDataTypeFieldImpl(
                "name", 0, OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR)));
    when(regularType.getField("age", false, false))
        .thenReturn(
            new org.apache.calcite.rel.type.RelDataTypeFieldImpl(
                "age", 1, OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));

    when(relBuilder.peek()).thenReturn(mock(org.apache.calcite.rel.RelNode.class));
    when(relBuilder.peek().getRowType()).thenReturn(regularType);

    List<String> currentFields = List.of("name", "age", "unknown_field");

    MapOnlyRelDataType result =
        MapOnlyColumnProcessor.createMapOnlyTypeFromCurrentFields(currentFields, context);

    assertNotNull(result);
    assertTrue(result.hasKnownField("name"));
    assertTrue(result.hasKnownField("age"));
    assertTrue(result.hasKnownField("unknown_field"));

    assertEquals(SqlTypeName.VARCHAR, result.getKnownFieldType("name").getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, result.getKnownFieldType("age").getSqlTypeName());
    assertEquals(SqlTypeName.ANY, result.getKnownFieldType("unknown_field").getSqlTypeName());
  }
}
