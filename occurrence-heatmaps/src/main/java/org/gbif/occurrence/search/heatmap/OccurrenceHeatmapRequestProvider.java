package org.gbif.occurrence.search.heatmap;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.api.util.VocabularyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class OccurrenceHeatmapRequestProvider {

  public static final String POLYGON_PATTERN = "POLYGON((%s))";
  public static final String PARAM_QUERY_STRING = "q";
  public static final String GEOM_PARAM = "geom";
  public static final String ZOOM_PARAM = "z";
  private static final int DEFAULT_ZOOM_LEVEL = 3;
  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceHeatmapRequestProvider.class);

  /**
   * Making constructor private.
   */
  private OccurrenceHeatmapRequestProvider() {
    // empty block
  }

  /**
   * Translate the raw value into value that API understands.
   */
  private static String translateFilterValue(OccurrenceSearchParameter param, String value) {
    if (param == OccurrenceSearchParameter.GEOMETRY) {
      try { // checks if the parameters is in WKT format
        SearchTypeValidator.validate(OccurrenceSearchParameter.GEOMETRY, value);
        return value;
      } catch (IllegalArgumentException ex) { // if not is WKT, assumes to be a POLYGON
        return String.format(POLYGON_PATTERN, value);
      }
    }
    if (Enum.class.isAssignableFrom(param.type())) {
      return value.toUpperCase();
    }
    return value;
  }

  public static OccurrenceHeatmapRequest buildOccurrenceHeatmapRequest(HttpServletRequest request) {
    OccurrenceHeatmapRequest occurrenceHeatmapSearchRequest = new OccurrenceHeatmapRequest();

    String q = request.getParameter(PARAM_QUERY_STRING);

    if (!Strings.isNullOrEmpty(q)) {
      occurrenceHeatmapSearchRequest.setQ(q);
    }
    // find search parameter enum based filters
    setSearchParams(occurrenceHeatmapSearchRequest, request);
    return occurrenceHeatmapSearchRequest;
  }

  /**
   * Iterates over the params map and adds to the search request the recognized parameters (i.e.:
   * those that have a correspondent value in the P generic parameter). Empty (of all size) and null
   * parameters are discarded.
   */
  private static void setSearchParams(OccurrenceHeatmapRequest occurrenceHeatmapSearchRequest, HttpServletRequest request) {
    for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
      OccurrenceSearchParameter p = findSearchParam(entry.getKey());
      if (p != null) {
        for (String val : removeEmptyParameters(entry.getValue())) {
          String translatedVal = translateFilterValue(p, val); // this transformation is require
          SearchTypeValidator.validate(p, translatedVal);
          occurrenceHeatmapSearchRequest.addParameter(p, translatedVal);
        }
      }
    }

    occurrenceHeatmapSearchRequest.setZoom(getIntParam(request, ZOOM_PARAM, DEFAULT_ZOOM_LEVEL));
    if (request.getParameterMap().containsKey(GEOM_PARAM)) {
      occurrenceHeatmapSearchRequest.setGeometry(request.getParameterMap().get(GEOM_PARAM)[0]);
    }

    LOG.debug("Querying using Geometry", occurrenceHeatmapSearchRequest.getGeometry());

  }


  private static OccurrenceSearchParameter findSearchParam(String name) {
    try {
      return (OccurrenceSearchParameter) VocabularyUtils.lookupEnum(name, OccurrenceSearchParameter.class);
    } catch (IllegalArgumentException e) {
      // we have all params here, not only the enum ones, so this is ok to end up here a few times
    }
    return null;
  }


  /**
   * Removes all empty and null parameters from the list. Each value is trimmed(String.trim()) in
   * order to remove all sizes of empty parameters.
   */
  private static List<String> removeEmptyParameters(String[] parameters) {
    List<String> cleanParameters = Lists.newArrayListWithCapacity(parameters.length);
    for (String param : parameters) {
      String cleanParam = Strings.nullToEmpty(param).trim();
      if (!cleanParam.isEmpty()) {
        cleanParameters.add(cleanParam);
      }
    }
    return cleanParameters;
  }

  public static int getIntParam(HttpServletRequest request, String param, int defaultVal) {
    String[] vals = request.getParameterValues(param);
    if (vals != null && vals.length > 0) {
      try {
        return Integer.parseInt(vals[0]);
      } catch (NumberFormatException e) {
        return defaultVal;
      }
    }
    return defaultVal;
  }
}
