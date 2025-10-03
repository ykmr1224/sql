/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Custom RelDataType that maintains known field types during planning while using single MAP field
 * at runtime. This approach simplifies runtime complexity by storing all fields in a MAP while
 * preserving type information for optimization.
 */
public class MapOnlyRelDataType extends RelRecordType {

  /** Field name used for storing all fields as MAP */
  public static final String MAP_FIELD_NAME = "_MAP";

  /** Map of known field names to their types for optimization */
  @Getter private final Map<String, RelDataType> knownFieldTypes;

  /** The MAP field type used at runtime */
  @Getter private final RelDataType mapFieldType;

  /**
   * Creates a MapOnlyRelDataType with known field types.
   *
   * @param typeFactory The RelDataTypeFactory to create types
   * @param knownFields Map of known field names to their types
   */
  public MapOnlyRelDataType(RelDataTypeFactory typeFactory, Map<String, RelDataType> knownFields) {
    // Physical schema: single MAP field
    super(
        List.of(
            new RelDataTypeFieldImpl(
                MAP_FIELD_NAME,
                0,
                typeFactory.createMapType(
                    typeFactory.createSqlType(SqlTypeName.VARCHAR),
                    typeFactory.createSqlType(SqlTypeName.ANY)))));

    // Logical schema: track known field types for planning
    this.knownFieldTypes = new HashMap<>(knownFields);
    this.mapFieldType = getFieldList().get(0).getType();
  }

  /**
   * Gets the known type for a field name.
   *
   * @param fieldName The field name to look up
   * @return The RelDataType for the field, or null if not known
   */
  public RelDataType getKnownFieldType(String fieldName) {
    return knownFieldTypes.get(fieldName);
  }

  /**
   * Checks if a field name has a known type.
   *
   * @param fieldName The field name to check
   * @return true if the field has a known type, false otherwise
   */
  public boolean hasKnownField(String fieldName) {
    return knownFieldTypes.containsKey(fieldName);
  }

  /**
   * Adds a new known field type.
   *
   * @param fieldName The field name
   * @param fieldType The field type
   */
  public void addKnownField(String fieldName, RelDataType fieldType) {
    knownFieldTypes.put(fieldName, fieldType);
  }

  /**
   * Creates a new MapOnlyRelDataType with additional known fields.
   *
   * @param typeFactory The RelDataTypeFactory to create types
   * @param additionalFields Additional fields to add to known fields
   * @return New MapOnlyRelDataType with combined known fields
   */
  public MapOnlyRelDataType withAdditionalFields(
      RelDataTypeFactory typeFactory, Map<String, RelDataType> additionalFields) {
    Map<String, RelDataType> combinedFields = new HashMap<>(knownFieldTypes);
    combinedFields.putAll(additionalFields);
    return new MapOnlyRelDataType(typeFactory, combinedFields);
  }

  /**
   * Gets all known field names.
   *
   * @return Set of known field names
   */
  public java.util.Set<String> getKnownFieldNames() {
    return knownFieldTypes.keySet();
  }

  @Override
  public String toString() {
    return String.format(
        "MapOnlyRelDataType(mapField=%s, knownFields=%s)", mapFieldType, knownFieldTypes.keySet());
  }
}
