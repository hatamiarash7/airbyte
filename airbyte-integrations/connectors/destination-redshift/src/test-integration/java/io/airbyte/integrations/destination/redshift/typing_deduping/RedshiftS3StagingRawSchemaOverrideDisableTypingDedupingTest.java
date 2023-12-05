/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.redshift.typing_deduping;

public class RedshiftS3StagingRawSchemaOverrideDisableTypingDedupingTest extends AbstractRedshiftTypingDedupingTest {

  @Override
  protected String getConfigPath() {
    return "secrets/1s1t_config_staging_raw_schema_override.json";
  }

  @Override
  protected String getRawSchema() {
    return "overridden_raw_dataset";
  }

  @Override
  protected boolean disableFinalTableComparison() {
    return true;
  }

}
