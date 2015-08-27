package org.gbif.occurrence.search.solr;

import org.gbif.api.model.occurrence.search.HeatmapResponse;

import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.NamedList;

/**
 * This class contains the response of a facet.heatmap search.
 * It had to be created because Solrj doesn't provide a response object for this type faceted search.
 */
public class HeatmapResponseBuilder {

  public static final HeatmapResponse EMPTY_RESPONSE  =  new HeatmapResponse(0,0,0L,0d,0d,0d,0d,null);

  /**
   * Private constructor.
   */
  private HeatmapResponseBuilder() {
    //empty block
  }

  /**
   * Creates an instance of a HeatMapResponse from Solr response.
   */
  public static HeatmapResponse build(QueryResponse response, String ptrField) {
    final NamedList heatmapSolrResponse = (NamedList)((NamedList)((NamedList)response.getResponse().get("facet_counts"))
                                          .get("facet_heatmaps")).get(ptrField);
    return new HeatmapResponse((Integer)heatmapSolrResponse.get("columns"),(Integer)heatmapSolrResponse.get("rows"),
    response.getResults().getNumFound(),(Double)heatmapSolrResponse.get("minX"),(Double)heatmapSolrResponse.get("maxX"),
    (Double)heatmapSolrResponse.get("minY"),(Double)heatmapSolrResponse.get("maxY"),
    (List<List<Integer>>)heatmapSolrResponse.get("counts_ints2D"));
  }

}
