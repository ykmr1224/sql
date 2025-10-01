/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;

/**
 * Simple implementation of RAND_FIELD function for demonstrating DynamicRowType column
 * architecture. This function generates a MAP with a random field name and random value.
 */
public class RandFieldFunction extends FunctionExpression {

  private static final Random random = new Random();
  private static final String[] RANDOM_FIELD_NAMES = {
    "random_field_1", "random_field_2", "random_field_3", "dynamic_field", "test_field"
  };
  private static final Object[] RANDOM_VALUES = {42, "random_string", true, 3.14, 100L};

  public RandFieldFunction(FunctionName functionName) {
    super(functionName, java.util.List.of());
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    // Generate a random field name and value
    String randomFieldName = RANDOM_FIELD_NAMES[random.nextInt(RANDOM_FIELD_NAMES.length)];
    Object randomValue = RANDOM_VALUES[random.nextInt(RANDOM_VALUES.length)];

    // Create a MAP with the random field
    Map<String, ExprValue> randomFieldMap = new HashMap<>();
    randomFieldMap.put(randomFieldName, ExprValueUtils.fromObjectValue(randomValue));

    // Return as ExprTupleValue (MAP)
    return ExprTupleValue.fromExprValueMap(randomFieldMap);
  }

  @Override
  public ExprType type() {
    // Return MAP type (will be converted to DynamicRowType during planning)
    return ExprCoreType.STRUCT;
  }

  @Override
  public String toString() {
    return "rand_field()";
  }
}
