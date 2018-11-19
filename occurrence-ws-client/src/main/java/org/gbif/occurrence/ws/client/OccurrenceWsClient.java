package org.gbif.occurrence.ws.client;

import static org.gbif.ws.paths.OccurrencePaths.FRAGMENT_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.VERBATIM_PATH;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.ws.client.BaseWsGetClient;

import com.google.inject.Inject;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;

public class OccurrenceWsClient extends BaseWsGetClient<Occurrence, Integer> implements OccurrenceService {

  private static final GenericType<VerbatimOccurrence> GT_VERBATIM_OCCURRENCE = new GenericType<VerbatimOccurrence>() {};
  private static final GenericType<String> GT_FRAGMENT = new GenericType<String>() {};


  /**
   * @param resource to the occurrence webapp
   */
  @Inject
  protected OccurrenceWsClient(WebResource resource) {
    super(Occurrence.class, resource.path(OCCURRENCE_PATH), null);
  }

  @Override
  public String getFragment(int key) {
    return get(GT_FRAGMENT, String.valueOf(key), FRAGMENT_PATH);
  }

  /**
   * Gets the VerbatimOccurrence object.
   *
   * @return requested resource or {@code null} if it couldn't be found
   */
  @Override
  public VerbatimOccurrence getVerbatim(Integer key) {
    if (key == null) {
      throw new IllegalArgumentException("Key cannot be null");
    }
    return get(GT_VERBATIM_OCCURRENCE, String.valueOf(key), VERBATIM_PATH);
  }
}
