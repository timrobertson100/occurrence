package org.gbif.metrics.ws.client;

import org.gbif.api.service.occurrence.OccurrenceDatasetIndexService;
import org.gbif.api.vocabulary.Country;
import java.util.SortedMap;
import java.util.UUID;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * A web service client to support the accession of occurrence dataset indexes.
 */
public interface OccurrenceDatasetIndexWsClient extends OccurrenceDatasetIndexService {

  String DATASETS_PATH = "occurrence/counts/datasets";

  String NUBKEY_PARAM = "nubKey";
  String COUNTRY_PARAM = "country";

  @Override
  default SortedMap<UUID, Long> occurrenceDatasetsForCountry(@RequestParam(COUNTRY_PARAM) Country country) {
    return occurrenceDatasetsForCountry(OccurrenceCountryIndexWsClient.COUNTRY_TO_ISO2.apply(country));
  }

  @RequestMapping(
    method = RequestMethod.GET,
    value = DATASETS_PATH,
    produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  SortedMap<UUID, Long> occurrenceDatasetsForCountry(@RequestParam(COUNTRY_PARAM) String country);

  @RequestMapping(
    method = RequestMethod.GET,
    value = DATASETS_PATH,
    produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  @Override
  SortedMap<UUID, Long> occurrenceDatasetsForNubKey(@RequestParam(NUBKEY_PARAM) int nubKey);

}
