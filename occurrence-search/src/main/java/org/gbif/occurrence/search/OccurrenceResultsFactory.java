package org.gbif.occurrence.search;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.common.search.builder.ResultsFactory;
import org.gbif.occurrence.search.solr.OccurrenceSolrField;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OccurrenceResultsFactory implements ResultsFactory<Occurrence> {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceResultsFactory.class);

  private final OccurrenceService occurrenceService;

  public OccurrenceResultsFactory(OccurrenceService occurrenceService){
    this.occurrenceService = occurrenceService;
  }

  @Override
  public List<Occurrence> fromSolrDocuments(SolrDocumentList documentList){
    // Populates the results
    List<Occurrence> occurrences = Lists.newArrayList();
    for (SolrDocument doc : documentList) {
      // Only field key is returned in the result
      Integer occKey = (Integer) doc.getFieldValue(OccurrenceSolrField.KEY.getFieldName());
      Occurrence occ = occurrenceService.get(occKey);
      if (occ == null || occ.getKey() == null) {
        LOG.warn("Occurrence {} not found in store, but present in solr", occKey);
      } else {
        occurrences.add(occ);
      }
    }
    return occurrences;
  }
}
