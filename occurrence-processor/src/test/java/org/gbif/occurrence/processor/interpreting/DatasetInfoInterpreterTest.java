package org.gbif.occurrence.processor.interpreting;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.net.URI;
import java.util.UUID;

import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;
import org.gbif.occurrence.processor.guice.ApiClientConfiguration;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("requires live webservice")
public class DatasetInfoInterpreterTest {
  static final ApiClientConfiguration cfg = new ApiClientConfiguration();;
  static final DatasetInfoInterpreter interpreter;
  static {
    cfg.url = URI.create("http://api.gbif-uat.org/v1/");
    interpreter = new DatasetInfoInterpreter(cfg.newApiClient());
  }

  private static final String BGBM_KEY = "57254bd0-8256-11d8-b7ed-b8a03c50a862";
  private static final String BOGART_DATASET_KEY = "85697f04-f762-11e1-a439-00145eb45e9a";

  @Test
  public void testOrgLookup() {
    Organization org = interpreter.getDatasetData(UUID.fromString(BOGART_DATASET_KEY)).getOrganization();
    assertEquals(BGBM_KEY, org.getKey());
  }

  @Test
  public void testCountryLookup() {
    Country result = interpreter.getDatasetData(UUID.fromString(BGBM_KEY)).getOrganization().getCountry();
    assertEquals(Country.GERMANY, result);
  }

  @Test
  public void testBadLookup() {
    assertNull(interpreter.getDatasetData(UUID.randomUUID()));
  }
}
