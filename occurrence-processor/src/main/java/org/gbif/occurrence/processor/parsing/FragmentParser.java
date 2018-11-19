package org.gbif.occurrence.processor.parsing;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.model.ImageRecord;
import org.gbif.occurrence.model.RawOccurrenceRecord;
import org.gbif.occurrence.model.TypificationRecord;
import org.gbif.occurrence.parsing.xml.XmlFragmentParser;
import org.gbif.occurrence.persistence.api.Fragment;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.google.common.base.Strings;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * A thin wrapper around the XmlFragmentParser from the occurrence-parser project, and the local
 * JsonFragmentParser, in order to produce VerbatimOccurrence objects from Fragments.
 */
public class FragmentParser {

  private static final Timer XML_PARSING_TIMER =
      Metrics.newTimer(FragmentParser.class, "verb xml parse time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private static final Timer JSON_PARSING_TIMER =
      Metrics.newTimer(FragmentParser.class, "verb json parse time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  // should not be instantiated
  private FragmentParser() {}

  /**
   * Parse the given fragment into a VerbatimOccurrence object. TODO: there is a lot of hacking here
   * that should be refactored out with general rework of parsing project
   *
   * @param fragment containing parsing to be parsed
   *
   * @return a VerbatimOccurrence or null if the fragment could not be parsed
   */
  @Nullable
  public static VerbatimOccurrence parse(Fragment fragment) {
    VerbatimOccurrence verbatim = null;

    switch (fragment.getFragmentType()) {
      case XML:
        final TimerContext xmlContext = XML_PARSING_TIMER.time();
        try {
          RawOccurrenceRecord ror = XmlFragmentParser.parseRecord(fragment.getData(), fragment.getXmlSchema(), fragment.getUnitQualifier());
          verbatim = buildVerbatim(ror, fragment);
        } finally {
          xmlContext.stop();
        }
        break;
      case JSON:
        final TimerContext jsonContext = JSON_PARSING_TIMER.time();
        try {
          verbatim = JsonFragmentParser.parseRecord(fragment);
        } finally {
          jsonContext.stop();
        }
        break;
    }

    if (verbatim != null) {
      // gbifID is neither verbatim nor interpreted, but this seems least wrong
      verbatim.setVerbatimField(GbifTerm.gbifID, fragment.getKey().toString());
      verbatim.setProtocol(fragment.getProtocol());
      verbatim.setLastParsed(new Date());
    }

    return verbatim;
  }

  private static VerbatimOccurrence buildVerbatim(RawOccurrenceRecord ror, Fragment frag) {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setKey(frag.getKey());
    v.setDatasetKey(frag.getDatasetKey());
    v.setLastCrawled(frag.getHarvestedDate());
    // these we don't have verb terms for yet
    // set(v, DwcTerm.acceptedNameUsage, ror.getDayIdentified());
    // set(v, DwcTerm.acceptedNameUsage, ror.getMonthIdentified());
    // set(v, DwcTerm.acceptedNameUsage, ror.getYearIdentified());
    // set(v, DwcTerm.acceptedNameUsage, ror.getIdentifierRecords());
    // set(v, DwcTerm.acceptedNameUsage, ror.getLinkRecords());

    set(v, GbifTerm.elevationAccuracy, ror.getAltitudePrecision());
    set(v, DwcTerm.scientificNameAuthorship, ror.getAuthor());
    set(v, DwcTerm.basisOfRecord, ror.getBasisOfRecord());
    set(v, DwcTerm.catalogNumber, ror.getCatalogueNumber());
    set(v, DwcTerm.collectionCode, ror.getCollectionCode());
    set(v, DwcTerm.recordedBy, ror.getCollectorName());
    set(v, DwcTerm.continent, ror.getContinentOrOcean());
    set(v, DwcTerm.country, ror.getCountry());
    set(v, DwcTerm.county, ror.getCounty());
    set(v, DwcTerm.day, ror.getDay());
    set(v, DwcTerm.dateIdentified, ror.getDateIdentified());
    set(v, GbifTerm.depthAccuracy, ror.getDepthPrecision());
    set(v, DwcTerm.family, ror.getFamily());
    set(v, DwcTerm.genus, ror.getGenus());
    set(v, DwcTerm.geodeticDatum, ror.getGeodeticDatum());
    set(v, DwcTerm.occurrenceID, ror.getId());
    set(v, DwcTerm.identifiedBy, ror.getIdentifierName());
    set(v, DwcTerm.institutionCode, ror.getInstitutionCode());
    set(v, DwcTerm.kingdom, ror.getKingdom());
    set(v, DwcTerm.class_, ror.getKlass());
    set(v, DwcTerm.decimalLatitude, ror.getLatitude());
    set(v, DwcTerm.coordinateUncertaintyInMeters, ror.getLatLongPrecision());
    set(v, DwcTerm.locality, ror.getLocality());
    set(v, DwcTerm.decimalLongitude, ror.getLongitude());
    set(v, DwcTerm.maximumElevationInMeters, ror.getMaxAltitude());
    set(v, DwcTerm.maximumDepthInMeters, ror.getMaxDepth());
    set(v, DwcTerm.minimumElevationInMeters, ror.getMinAltitude());
    set(v, DwcTerm.minimumDepthInMeters, ror.getMinDepth());
    set(v, DwcTerm.month, ror.getMonth());
    set(v, DwcTerm.eventDate, ror.getOccurrenceDate());
    set(v, DwcTerm.order, ror.getOrder());
    set(v, DwcTerm.phylum, ror.getPhylum());
    set(v, DwcTerm.taxonRank, ror.getRank());
    set(v, DwcTerm.scientificName, ror.getScientificName());
    set(v, DwcTerm.specificEpithet, ror.getSpecies());
    set(v, DwcTerm.stateProvince, ror.getStateOrProvince());
    set(v, DwcTerm.infraspecificEpithet, ror.getSubspecies());
    set(v, DwcTerm.year, ror.getYear());
    if (ror.getTypificationRecords() != null && !ror.getTypificationRecords().isEmpty()) {
      // just use first one - any more makes no sense
      TypificationRecord record = ror.getTypificationRecords().get(0);
      set(v, DwcTerm.typeStatus, record.getTypeStatus());
      set(v, GbifTerm.typifiedName, record.getScientificName());
    }

    if (ror.getImageRecords() != null && !ror.getImageRecords().isEmpty()) {
      List<Map<Term, String>> verbMediaList = Lists.newArrayList();
      for (ImageRecord image : ror.getImageRecords()) {
        Map<Term, String> mediaTerms = Maps.newHashMap();
        // the only fields we map from abcd 2 so far
        mediaTerms.put(DcTerm.format, image.getRawImageType());
        mediaTerms.put(DcTerm.identifier, image.getUrl());
        mediaTerms.put(DcTerm.references, image.getPageUrl());
        mediaTerms.put(DcTerm.description, image.getDescription());
        mediaTerms.put(DcTerm.license, image.getRights());
        verbMediaList.add(mediaTerms);
      }
      v.getExtensions().put(Extension.MULTIMEDIA, verbMediaList);
    }

    return v;
  }

  private static void set(VerbatimOccurrence v, Term t, String val) {
    if (t != null && !Strings.isNullOrEmpty(val)) {
      v.setVerbatimField(t, val);
    }
  }
}
