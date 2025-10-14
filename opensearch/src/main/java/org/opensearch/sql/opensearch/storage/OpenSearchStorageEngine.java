/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.utils.SystemIndexUtils.isSystemIndex;

import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver;
import org.opensearch.sql.calcite.OpenSearchSchema;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.system.OpenSearchSystemIndex;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/** OpenSearch storage engine implementation. */
@RequiredArgsConstructor
public class OpenSearchStorageEngine implements StorageEngine {

  /** OpenSearch client connection. */
  @Getter private final OpenSearchClient client;

  @Getter private final Settings settings;

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String name) {
    if (isSystemIndex(name)) {
      return new OpenSearchSystemIndex(client, settings, name);
    } else if (name.endsWith(DataSourceSchemaIdentifierNameResolver.DYNAMIC_FIELDS_SUFFIX)) {
      return new OpenSearchIndex(client, settings, name, true);
    } else {
      return new OpenSearchIndex(client, settings, name);
    }
  }

  @Override
  public Table getTable(
      DataSourceSchemaName dataSourceSchemaName, String name, Map<String, Object> options) {
    Object enableDynamicFields = options.get(OpenSearchSchema.ENABLE_DYNAMIC_FIELDS);
    return new OpenSearchIndex(client, settings, name, enableDynamicFields == Boolean.TRUE);
  }
}
