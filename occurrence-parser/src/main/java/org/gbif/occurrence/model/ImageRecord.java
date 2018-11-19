/*
 * Copyright 2011 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.gbif.occurrence.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.gbif.occurrence.constants.PrioritizedPropertyNameEnum;
import org.gbif.occurrence.parsing.xml.PrioritizedProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds a single image for a RawOccurrenceRecord.
 */
public class ImageRecord extends PropertyPrioritizer implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ImageRecord.class);

  private String rawImageType;
  private Integer imageType;
  private String url;
  private String pageUrl;
  private String description;
  private String rights;
  private String htmlForDisplay;

  /**
   * Once this object has been populated by a Digester, there may be several PrioritizedProperties
   * that need to be resolved, and thereby set the final value of the corresponding field on this
   * object.
   */
  @Override
  public void resolvePriorities() {
    for (Map.Entry<PrioritizedPropertyNameEnum, Set<PrioritizedProperty>> entry : prioritizedProps.entrySet()) {
      PrioritizedPropertyNameEnum name = entry.getKey();
      String result = findHighestPriority(prioritizedProps.get(name));
      switch (name) {
        case IMAGE_URL:
          url = result;
          break;
        case IMAGE_RIGHTS:
          rights = result;
          break;
        default:
          LOG.warn("Fell through priority resolution for [{}]", name);
      }
    }
  }

  public String getRawImageType() {
    return rawImageType;
  }

  public void setRawImageType(String rawImageType) {
    this.rawImageType = rawImageType;
  }

  public Integer getImageType() {
    return imageType;
  }

  public void setImageType(Integer imageType) {
    this.imageType = imageType;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getRights() {
    return rights;
  }

  public void setRights(String rights) {
    this.rights = rights;
  }

  public String getHtmlForDisplay() {
    return htmlForDisplay;
  }

  public void setHtmlForDisplay(String htmlForDisplay) {
    this.htmlForDisplay = htmlForDisplay;
  }

  public String getPageUrl() {
    return pageUrl;
  }

  public void setPageUrl(String pageUrl) {
    this.pageUrl = pageUrl;
  }

  public boolean isEmpty() {
    return StringUtils.isEmpty(rawImageType) && imageType == null && StringUtils.isEmpty(url) && StringUtils.isEmpty(pageUrl)
        && StringUtils.isEmpty(description) && StringUtils.isEmpty(rights) && StringUtils.isEmpty(htmlForDisplay);
  }

  public String debugDump() {
    return "ImageRecord [\nrawImageType=" + rawImageType + ",\nimageType=" + imageType + ",\nurl=" + url + ",\npageUrl=" + pageUrl
        + ",\ndescription=" + description + ",\nrights=" + rights + ",\nhtmlForDisplay=" + htmlForDisplay + "]";
  }
}
