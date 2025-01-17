/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class FunctionTypeTest {
  @Test
  public void test() {
    assertEquals(FunctionType.AGGREGATE, FunctionType.fromFunctionName("any"));
    assertEquals(FunctionType.AGGREGATE, FunctionType.fromFunctionName("variance"));
    assertEquals(FunctionType.WINDOW, FunctionType.fromFunctionName("cume_dist"));
    assertEquals(FunctionType.WINDOW, FunctionType.fromFunctionName("row_number"));
    assertEquals(FunctionType.ARRAY, FunctionType.fromFunctionName("array"));
    assertEquals(FunctionType.ARRAY, FunctionType.fromFunctionName("sort_array"));
    assertEquals(FunctionType.MAP, FunctionType.fromFunctionName("element_at"));
    assertEquals(FunctionType.MAP, FunctionType.fromFunctionName("try_element_at"));
    assertEquals(FunctionType.DATE_TIMESTAMP, FunctionType.fromFunctionName("add_months"));
    assertEquals(FunctionType.DATE_TIMESTAMP, FunctionType.fromFunctionName("year"));
    assertEquals(FunctionType.JSON, FunctionType.fromFunctionName("from_json"));
    assertEquals(FunctionType.JSON, FunctionType.fromFunctionName("to_json"));
    assertEquals(FunctionType.MATH, FunctionType.fromFunctionName("abs"));
    assertEquals(FunctionType.MATH, FunctionType.fromFunctionName("width_bucket"));
    assertEquals(FunctionType.STRING, FunctionType.fromFunctionName("ascii"));
    assertEquals(FunctionType.STRING, FunctionType.fromFunctionName("upper"));
    assertEquals(FunctionType.CONDITIONAL, FunctionType.fromFunctionName("coalesce"));
    assertEquals(FunctionType.CONDITIONAL, FunctionType.fromFunctionName("nvl2"));
    assertEquals(FunctionType.BITWISE, FunctionType.fromFunctionName("bit_count"));
    assertEquals(FunctionType.BITWISE, FunctionType.fromFunctionName("shiftrightunsigned"));
    assertEquals(FunctionType.CONVERSION, FunctionType.fromFunctionName("bigint"));
    assertEquals(FunctionType.CONVERSION, FunctionType.fromFunctionName("tinyint"));
    assertEquals(FunctionType.PREDICATE, FunctionType.fromFunctionName("isnan"));
    assertEquals(FunctionType.PREDICATE, FunctionType.fromFunctionName("rlike"));
    assertEquals(FunctionType.CSV, FunctionType.fromFunctionName("from_csv"));
    assertEquals(FunctionType.CSV, FunctionType.fromFunctionName("to_csv"));
    assertEquals(FunctionType.MISC, FunctionType.fromFunctionName("aes_decrypt"));
    assertEquals(FunctionType.MISC, FunctionType.fromFunctionName("version"));
    assertEquals(FunctionType.GENERATOR, FunctionType.fromFunctionName("explode"));
    assertEquals(FunctionType.GENERATOR, FunctionType.fromFunctionName("stack"));
    assertEquals(FunctionType.UNCATEGORIZED, FunctionType.fromFunctionName("aggregate"));
    assertEquals(FunctionType.UNCATEGORIZED, FunctionType.fromFunctionName("forall"));
    assertEquals(FunctionType.UDF, FunctionType.fromFunctionName("unknown"));
  }
//
//  @Test
//  public void test2() {
//    String[] list = new String[] {
//        "abs",
//        "acos",
//        "acosh",
//        "add_months",
//        "aes_decrypt",
//        "aes_encrypt",
////        "aggregate", should fix
////        "and",
//        "any",
//        "any_value",
//        "approx_count_distinct",
//        "approx_percentile",
//        "array",
//        "array_agg",
//        "array_append",
//        "array_compact",
//        "array_contains",
//        "array_distinct",
//        "array_except",
//        "array_insert",
//        "array_intersect",
//        "array_join",
//        "array_max",
//        "array_min",
//        "array_position",
//        "array_prepend",
//        "array_remove",
//        "array_repeat",
////        "array_size", should fix
////        "array_sort", should fix
//        "array_union",
//        "arrays_overlap",
//        "arrays_zip",
//        "ascii",
//        "asin",
//        "asinh",
//        "assert_true",
//        "atan",
//        "atan2",
//        "atanh",
//        "avg",
//        "base64",
////        "between", not function form
//        "bigint",
//        "bin",
//        "binary",
//        "bit_and",
//        "bit_count",
//        "bit_get",
//        "bit_length",
//        "bit_or",
//        "bit_xor",
//        "bitmap_bit_position",
//        "bitmap_bucket_number",
//        "bitmap_construct_agg",
//        "bitmap_count",
//        "bitmap_or_agg",
//        "bool_and",
//        "bool_or",
//        "boolean",
//        "bround",
//        "btrim",
////        "cardinality", should fix
////        "case", not function form
//        "cast",
//        "cbrt",
//        "ceil",
//        "ceiling",
//        "char",
//        "char_length",
//        "character_length",
//        "chr",
//        "coalesce",
//        "collect_list",
//        "collect_set",
//        "concat",
//        "concat_ws",
//        "contains",
//        "conv",
//        "convert_timezone",
//        "corr",
//        "cos",
//        "cosh",
//        "cot",
//        "count",
//        "count_if",
//        "count_min_sketch",
//        "covar_pop",
//        "covar_samp",
////        "crc32", should fix
//        "csc",
//        "cume_dist",
//        "curdate",
//        "current_catalog",
//        "current_database",
//        "current_date",
//        "current_schema",
//        "current_timestamp",
//        "current_timezone",
//        "current_user",
//        "date",
//        "date_add",
//        "date_diff",
//        "date_format",
//        "date_from_unix_date",
//        "date_part",
//        "date_sub",
//        "date_trunc",
//        "dateadd",
//        "datediff",
//        "datepart",
//        "day",
//        "dayofmonth",
//        "dayofweek",
//        "dayofyear",
//        "decimal",
//        "decode",
//        "degrees",
//        "dense_rank",
////        "div", not function form
//        "double",
//        "e",
//        "element_at",
//        "elt",
//        "encode",
//        "endswith",
//        "equal_null",
//        "every",
////        "exists", should fix
//        "exp",
//        "explode",
//        "explode_outer",
//        "expm1",
//        "extract",
//        "factorial",
////        "filter", should fix
//        "find_in_set",
//        "first",
//        "first_value",
//        "flatten",
//        "float",
//        "floor",
////        "forall", should fix
//        "format_number",
//        "format_string",
//        "from_csv",
//        "from_json",
//        "from_unixtime",
//        "from_utc_timestamp",
//        "get",
//        "get_json_object",
//        "getbit",
//        "greatest",
//        "grouping",
//        "grouping_id",
////        "hash", should fix
//        "hex",
//        "histogram_numeric",
//        "hll_sketch_agg",
//        "hll_sketch_estimate",
//        "hll_union",
//        "hll_union_agg",
//        "hour",
//        "hypot",
//        "if",
//        "ifnull",
////        "ilike", should fix
////        "in", should fix
//        "initcap",
//        "inline",
//        "inline_outer",
//        "input_file_block_length",
//        "input_file_block_start",
//        "input_file_name",
//        "instr",
//        "int",
//        "isnan",
//        "isnotnull",
//        "isnull",
//        "java_method",
//        "json_array_length",
//        "json_object_keys",
//        "json_tuple",
//        "kurtosis",
//        "lag",
//        "last",
//        "last_day",
//        "last_value",
//        "lcase",
//        "lead",
//        "least",
//        "left",
//        "len",
//        "length",
//        "levenshtein",
////        "like", should fix
//        "ln",
//        "localtimestamp",
//        "locate",
//        "log",
//        "log10",
//        "log1p",
//        "log2",
//        "lower",
//        "lpad",
//        "ltrim",
//        "luhn_check",
//        "make_date",
//        "make_dt_interval",
//        "make_interval",
//        "make_timestamp",
//        "make_timestamp_ltz",
//        "make_timestamp_ntz",
//        "make_ym_interval",
//        "map",
//        "map_concat",
//        "map_contains_key",
//        "map_entries",
////        "map_filter", should fix
//        "map_from_arrays",
//        "map_from_entries",
//        "map_keys",
//        "map_values",
////        "map_zip_with", should fix
//        "mask",
//        "max",
//        "max_by",
////        "md5", should fix
//        "mean",
//        "median",
//        "min",
//        "min_by",
//        "minute",
////        "mod", should fix
//        "mode",
//        "monotonically_increasing_id",
//        "month",
//        "months_between",
////        "named_struct", should fix
//        "nanvl",
//        "negative",
//        "next_day",
////        "not", not function form
//        "now",
//        "nth_value",
//        "ntile",
//        "nullif",
//        "nvl",
//        "nvl2",
//        "octet_length",
////        "or",
//        "overlay",
////        "parse_url", should fix
//        "percent_rank",
//        "percentile",
//        "percentile_approx",
//        "pi",
//        "pmod",
//        "posexplode",
//        "posexplode_outer",
//        "position",
//        "positive",
//        "pow",
//        "power",
//        "printf",
//        "quarter",
//        "radians",
////        "raise_error", should fix
//        "rand",
//        "randn",
//        "random",
//        "rank",
////        "reduce", should fix
//        "reflect",
//        "regexp",
//        "regexp_count",
//        "regexp_extract",
//        "regexp_extract_all",
//        "regexp_instr",
//        "regexp_like",
//        "regexp_replace",
//        "regexp_substr",
//        "regr_avgx",
//        "regr_avgy",
//        "regr_count",
//        "regr_intercept",
//        "regr_r2",
//        "regr_slope",
//        "regr_sxx",
//        "regr_sxy",
//        "regr_syy",
//        "repeat",
//        "replace",
////        "reverse", should fix
//        "right",
//        "rint",
//        "rlike",
//        "round",
//        "row_number",
//        "rpad",
//        "rtrim",
//        "schema_of_csv",
//        "schema_of_json",
//        "sec",
//        "second",
//        "sentences",
//        "sequence",
//        "session_window",
////        "sha", should fix
////        "sha1",should fix
////        "sha2",should fix
//        "shiftleft",
//        "shiftright",
//        "shiftrightunsigned",
//        "shuffle",
//        "sign",
//        "signum",
//        "sin",
//        "sinh",
////        "size", should fix
//        "skewness",
//        "slice",
//        "smallint",
//        "some",
//        "sort_array",
//        "soundex",
//        "space",
//        "spark_partition_id",
//        "split",
//        "split_part",
//        "sqrt",
//        "stack",
//        "startswith",
//        "std",
//        "stddev",
//        "stddev_pop",
//        "stddev_samp",
//        "str_to_map",
//        "string",
////        "struct", should fix
//        "substr",
//        "substring",
//        "substring_index",
//        "sum",
//        "tan",
//        "tanh",
//        "timestamp",
//        "timestamp_micros",
//        "timestamp_millis",
//        "timestamp_seconds",
//        "tinyint",
//        "to_binary",
//        "to_char",
//        "to_csv",
//        "to_date",
//        "to_json",
//        "to_number",
//        "to_timestamp",
//        "to_timestamp_ltz",
//        "to_timestamp_ntz",
//        "to_unix_timestamp",
//        "to_utc_timestamp",
//        "to_varchar",
////        "transform", should fix
////        "transform_keys", should fix
////        "transform_values", should fix
//        "translate",
//        "trim",
//        "trunc",
//        "try_add",
//        "try_aes_decrypt",
//        "try_avg",
//        "try_divide",
//        "try_element_at",
//        "try_multiply",
//        "try_subtract",
//        "try_sum",
//        "try_to_binary",
//        "try_to_number",
//        "try_to_timestamp",
//        "typeof",
//        "ucase",
//        "unbase64",
//        "unhex",
//        "unix_date",
//        "unix_micros",
//        "unix_millis",
//        "unix_seconds",
//        "unix_timestamp",
//        "upper",
////        "url_decode", should fix
////        "url_encode", should fix
//        "user",
//        "uuid",
//        "var_pop",
//        "var_samp",
//        "variance",
//        "version",
//        "weekday",
//        "weekofyear",
////        "when", not function form
//        "width_bucket",
//        "window",
//        "window_time",
////        "xpath", should fix
////        "xpath_boolean", should fix
////        "xpath_double", should fix
////        "xpath_float", should fix
////        "xpath_int", should fix
////        "xpath_long", should fix
////        "xpath_number", should fix
////        "xpath_short", should fix
////        "xpath_string", should fix
////        "xxhash64", should fix
//        "year",
////        "zip_with" should fix
//    };
//    for (String fn: list) {
////      if (FunctionType.fromFunctionName(fn) == FunctionType.UDF) {
////        System.out.println(fn + " should not be UDF");
////      }
//      assertNotEquals(FunctionType.UDF, FunctionType.fromFunctionName(fn), fn + " should not be UDF");
//    }
//  }
}
