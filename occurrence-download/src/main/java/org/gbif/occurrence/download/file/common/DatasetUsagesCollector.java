package org.gbif.occurrence.download.file.common;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.gbif.api.vocabulary.License;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Collects dataset records/information used in a occurrence download.
 */
public class DatasetUsagesCollector {

  private Map<UUID, Long> datasetUsages = Maps.newHashMap();

  // we simply keep the String used to identify the licenses to avoid the conversion to License each
  // time
  private Set<String> datasetLicensesString = Sets.newHashSet();
  private Set<License> datasetLicenses = Sets.newHashSet();

  /**
   * Increments in 1 the number of records coming from the dataset (if any) parameter.
   */
  public void incrementDatasetUsage(String datasetKey) {
    if (datasetKey != null) {
      datasetUsages.compute(UUID.fromString(datasetKey), (key, count) -> (count == null) ? 1L : count + 1);
    }
  }

  /**
   * Increments in 1 the number of records coming from the dataset (if any) parameter. Record the
   * license.
   *
   * @param datasetKey
   * @param license
   */
  public void collectDatasetUsage(String datasetKey, String license) {
    incrementDatasetUsage(datasetKey);

    if (license != null && !datasetLicensesString.contains(license)) {
      Optional<License> l = License.fromString(license);
      if (l.isPresent()) {
        datasetLicensesString.add(license);
        datasetLicenses.add(l.get());
      }
    }
  }

  /**
   * Sums all the dataset usages to current instance.
   */
  public void sumUsages(Map<UUID, Long> fromDatasetUsages) {
    datasetUsages = Stream.concat(datasetUsages.entrySet().stream(), fromDatasetUsages.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
  }

  public void mergeLicenses(Set<License> licenses) {
    // we don't really need to update datasetLicensesString
    datasetLicenses.addAll(licenses);
  }

  /**
   * Dataset usages: number of records used per dataset in download.
   */
  public Map<UUID, Long> getDatasetUsages() {
    return datasetUsages;
  }

  /**
   * Dataset licenses: all distinct licenses used in the download.
   * 
   * @return
   */
  public Set<License> getDatasetLicenses() {
    return datasetLicenses;
  }
}
