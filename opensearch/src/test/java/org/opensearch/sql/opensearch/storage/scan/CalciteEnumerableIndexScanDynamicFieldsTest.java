/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

class CalciteEnumerableIndexScanDynamicFieldsTest {

  @Test
  void testOpenSearchIndexWithDynamicFieldsEnabled() {
    OpenSearchClient mockClient = mock(OpenSearchClient.class);
    Settings mockSettings = mock(Settings.class);

    // Test with dynamic fields enabled
    OpenSearchIndex indexWithDynamicFields =
        new OpenSearchIndex(mockClient, mockSettings, "test_index", true);
    assertTrue(indexWithDynamicFields.isDynamicFieldsEnabled());

    // Test with dynamic fields disabled (default)
    OpenSearchIndex indexWithoutDynamicFields =
        new OpenSearchIndex(mockClient, mockSettings, "test_index", false);
    assertFalse(indexWithoutDynamicFields.isDynamicFieldsEnabled());

    // Test default constructor (should disable dynamic fields)
    OpenSearchIndex defaultIndex = new OpenSearchIndex(mockClient, mockSettings, "test_index");
    assertFalse(defaultIndex.isDynamicFieldsEnabled());
  }

  @Test
  void testReservedFieldTypesWithDynamicFields() {
    OpenSearchClient mockClient = mock(OpenSearchClient.class);
    Settings mockSettings = mock(Settings.class);

    // Test with dynamic fields enabled
    OpenSearchIndex indexWithDynamicFields =
        new OpenSearchIndex(mockClient, mockSettings, "test_index", true);
    Map<String, ExprType> reservedFieldsWithDynamic =
        indexWithDynamicFields.getReservedFieldTypes();

    // Should contain the _MAP field
    assertTrue(reservedFieldsWithDynamic.containsKey(OpenSearchIndex.DYNAMIC_FIELDS_MAP));
    assertEquals(
        ExprCoreType.STRUCT, reservedFieldsWithDynamic.get(OpenSearchIndex.DYNAMIC_FIELDS_MAP));

    // Should also contain all standard metadata fields
    assertTrue(reservedFieldsWithDynamic.containsKey(OpenSearchIndex.METADATA_FIELD_ID));
    assertTrue(reservedFieldsWithDynamic.containsKey(OpenSearchIndex.METADATA_FIELD_INDEX));

    // Test with dynamic fields disabled
    OpenSearchIndex indexWithoutDynamicFields =
        new OpenSearchIndex(mockClient, mockSettings, "test_index", false);
    Map<String, ExprType> reservedFieldsWithoutDynamic =
        indexWithoutDynamicFields.getReservedFieldTypes();

    // Should NOT contain the _MAP field
    assertFalse(reservedFieldsWithoutDynamic.containsKey(OpenSearchIndex.DYNAMIC_FIELDS_MAP));

    // Should still contain standard metadata fields
    assertTrue(reservedFieldsWithoutDynamic.containsKey(OpenSearchIndex.METADATA_FIELD_ID));
    assertTrue(reservedFieldsWithoutDynamic.containsKey(OpenSearchIndex.METADATA_FIELD_INDEX));
  }

  @Test
  void testDynamicFieldsCollection() {
    // Create a mock ExprValue with both schema and dynamic fields
    Map<String, ExprValue> tupleData = new HashMap<>();
    tupleData.put("name", ExprValueUtils.stringValue("John")); // Schema field
    tupleData.put("age", ExprValueUtils.integerValue(30)); // Schema field
    tupleData.put("city", ExprValueUtils.stringValue("NYC")); // Dynamic field
    tupleData.put("country", ExprValueUtils.stringValue("USA")); // Dynamic field

    ExprTupleValue mockTuple = ExprTupleValue.fromExprValueMap(tupleData);

    // Mock OpenSearchIndex with dynamic fields enabled
    OpenSearchClient mockClient = mock(OpenSearchClient.class);
    Settings mockSettings = mock(Settings.class);
    OpenSearchIndex mockIndex = new OpenSearchIndex(mockClient, mockSettings, "test_index", true);

    // Mock schema fields (name and age are in schema)
    Map<String, ExprType> schemaFields = new HashMap<>();
    schemaFields.put("name", ExprCoreType.STRING);
    schemaFields.put("age", ExprCoreType.INTEGER);

    // Mock ResourceMonitor to return healthy
    org.opensearch.sql.monitor.ResourceMonitor mockMonitor =
        mock(org.opensearch.sql.monitor.ResourceMonitor.class);
    when(mockMonitor.isHealthy()).thenReturn(true);

    // Create enumerator with mock data
    OpenSearchIndexEnumerator enumerator =
        new OpenSearchIndexEnumerator(
            mockClient,
            List.of("name", "age", OpenSearchIndex.DYNAMIC_FIELDS_MAP),
            100,
            1000,
            null,
            mockMonitor);

    // Test the basic setup
    assertNotNull(enumerator);
  }
}
