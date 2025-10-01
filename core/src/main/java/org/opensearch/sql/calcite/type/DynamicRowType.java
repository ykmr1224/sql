/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

/**
 * DynamicRowType represents a flexible schema with known fields and their types, while allowing
 * unknown fields. During planning, it maintains rich type information for optimization. During
 * execution, it converts to MAP<VARCHAR, ANY> for simplicity.
 */
public class DynamicRowType extends AbstractExprRelDataType<MapSqlType> {

  private final LinkedHashMap<String, RelDataType> knownFields;
  private final boolean allowsUnknownFields = true; // Always true as specified

  public DynamicRowType(
      OpenSearchTypeFactory typeFactory, LinkedHashMap<String, RelDataType> knownFields) {
    super(ExprUDT.EXPR_DYNAMIC_ROW, createMapType(typeFactory));
    this.knownFields =
        new LinkedHashMap<>(knownFields != null ? knownFields : new LinkedHashMap<>());
  }

  private static MapSqlType createMapType(OpenSearchTypeFactory typeFactory) {
    RelDataType keyType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType valueType = typeFactory.createSqlType(SqlTypeName.ANY);
    return (MapSqlType) typeFactory.createMapType(keyType, valueType, false);
  }

  /**
   * Get the type of a known field by name.
   *
   * @param fieldName the field name
   * @return the RelDataType of the field, or null if not a known field
   */
  public RelDataType getKnownFieldType(String fieldName) {
    return knownFields.get(fieldName);
  }

  /**
   * Get all known field names in their defined order.
   *
   * @return ordered list of known field names
   */
  public List<String> getKnownFieldNames() {
    return new ArrayList<>(knownFields.keySet());
  }

  /**
   * Get all known fields as a map.
   *
   * @return copy of known fields map
   */
  public LinkedHashMap<String, RelDataType> getKnownFields() {
    return new LinkedHashMap<>(knownFields);
  }

  /**
   * Check if unknown fields are allowed (always true for DynamicRowType).
   *
   * @return true
   */
  public boolean allowsUnknownFields() {
    return allowsUnknownFields;
  }

  /**
   * Check if a field is a known field.
   *
   * @param fieldName the field name to check
   * @return true if the field is known
   */
  public boolean isKnownField(String fieldName) {
    return knownFields.containsKey(fieldName);
  }

  @Override
  public Type getJavaType() {
    return Map.class;
  }

  @Override
  public DynamicRowType createWithNullability(OpenSearchTypeFactory typeFactory, boolean nullable) {
    if (nullable) {
      throw new IllegalArgumentException(
          "DynamicRowType cannot be nullable as it represents a row type");
    }
    return this; // Already non-nullable
  }

  @Override
  public DynamicRowType createWithCharsetAndCollation(
      OpenSearchTypeFactory typeFactory, Charset charset, SqlCollation collation) {
    MapSqlType newMapType =
        (MapSqlType) typeFactory.createTypeWithCharsetAndCollation(relType, charset, collation);
    return new DynamicRowType(typeFactory, knownFields) {
      @Override
      public MapSqlType getRelType() {
        return newMapType;
      }
    };
  }

  public MapSqlType getRelType() {
    return relType;
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("DYNAMIC_ROW");
    if (withDetail && knownFields != null && !knownFields.isEmpty()) {
      sb.append("(");
      boolean first = true;
      for (Map.Entry<String, RelDataType> entry : knownFields.entrySet()) {
        if (!first) {
          sb.append(", ");
        }
        sb.append(entry.getKey()).append(": ").append(entry.getValue());
        first = false;
      }
      sb.append(")");
    }
  }

  @Override
  public List<RelDataTypeField> getFieldList() {
    // For planning purposes, return known fields as a structured list
    List<RelDataTypeField> fields = new ArrayList<>();
    int index = 0;
    for (Map.Entry<String, RelDataType> entry : knownFields.entrySet()) {
      fields.add(new RelDataTypeFieldImpl(entry.getKey(), index++, entry.getValue()));
    }
    return fields;
  }

  @Override
  public int getFieldCount() {
    return knownFields.size();
  }

  @Override
  public List<String> getFieldNames() {
    return new ArrayList<>(knownFields.keySet());
  }

  @Override
  public RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord) {
    RelDataType fieldType = knownFields.get(fieldName);
    if (fieldType != null) {
      int index = new ArrayList<>(knownFields.keySet()).indexOf(fieldName);
      return new RelDataTypeFieldImpl(fieldName, index, fieldType);
    }
    return null;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof DynamicRowType)) return false;
    if (!super.equals(obj)) return false;

    DynamicRowType that = (DynamicRowType) obj;
    return Objects.equals(knownFields, that.knownFields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), knownFields);
  }

  /** Builder for creating DynamicRowType instances. */
  public static class Builder {
    private final LinkedHashMap<String, RelDataType> knownFields = new LinkedHashMap<>();
    private final OpenSearchTypeFactory typeFactory;

    public Builder(OpenSearchTypeFactory typeFactory) {
      this.typeFactory = typeFactory;
    }

    public Builder addKnownField(String fieldName, RelDataType fieldType) {
      knownFields.put(fieldName, fieldType);
      return this;
    }

    public Builder addKnownField(String fieldName, SqlTypeName sqlTypeName) {
      knownFields.put(fieldName, typeFactory.createSqlType(sqlTypeName));
      return this;
    }

    public Builder addKnownField(String fieldName, ExprUDT exprUDT) {
      knownFields.put(fieldName, typeFactory.createUDT(exprUDT));
      return this;
    }

    public DynamicRowType build() {
      return new DynamicRowType(typeFactory, knownFields);
    }
  }

  /**
   * Create a new builder for DynamicRowType.
   *
   * @param typeFactory the type factory to use
   * @return a new builder instance
   */
  public static Builder builder(OpenSearchTypeFactory typeFactory) {
    return new Builder(typeFactory);
  }
}
