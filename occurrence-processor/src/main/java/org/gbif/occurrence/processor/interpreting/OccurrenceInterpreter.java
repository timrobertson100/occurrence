package org.gbif.occurrence.processor.interpreting;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import javax.annotation.Nullable;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.LifeStage;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.common.parsers.BasisOfRecordParser;
import org.gbif.common.parsers.EstablishmentMeansParser;
import org.gbif.common.parsers.LifeStageParser;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.SexParser;
import org.gbif.common.parsers.TypeStatusParser;
import org.gbif.common.parsers.TypifiedNameParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.common.parsers.core.Parsable;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Interprets/Validates verbatim occurrence records. This class doesn't persist any information, it
 * only collects possible issues and generates a interpreted version of the verbatim record.
 */
@Singleton
public class OccurrenceInterpreter implements Serializable {

  @FunctionalInterface
  private interface Interpreter {
    void interpret(VerbatimOccurrence verbatim, Occurrence occurrence);
  }

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceInterpreter.class);

  private static final TypeStatusParser TYPE_PARSER = TypeStatusParser.getInstance();
  private static final Parsable<String> TYPE_NAME_PARSER = TypifiedNameParser.getInstance();
  private static final BasisOfRecordParser BOR_PARSER = BasisOfRecordParser.getInstance();
  private static final SexParser SEX_PARSER = SexParser.getInstance();
  private static final EstablishmentMeansParser EST_PARSER = EstablishmentMeansParser.getInstance();
  private static final LifeStageParser LST_PARSER = LifeStageParser.getInstance();

  private final DatasetInfoInterpreter datasetInfoInterpreter;

  // Holds the list of Interpreters that will be applied
  private final List<Interpreter> interpreters;

  @Inject
  public OccurrenceInterpreter(@Nullable DatasetInfoInterpreter datasetInfoInterpreter, TaxonomyInterpreter taxonomyInterpreter,
      LocationInterpreter locationInterpreter) {
    this.datasetInfoInterpreter = datasetInfoInterpreter;
    // the list of interpreters is initialized with all the interpretations methods used
    ImmutableList.Builder<Interpreter> bldr = new ImmutableList.Builder<Interpreter>().add(locationInterpreter::interpretLocation,
        taxonomyInterpreter::interpretTaxonomy, MultiMediaInterpreter::interpretMedia, OccurrenceInterpreter::interpretBor,
        OccurrenceInterpreter::interpretSex, OccurrenceInterpreter::interpretEstablishmentMeans, OccurrenceInterpreter::interpretLifeStage,
        OccurrenceInterpreter::interpretTypification, TemporalInterpreter::interpretTemporal, OccurrenceInterpreter::interpretReferences,
        OccurrenceInterpreter::interpretIndividualCount);

    if (datasetInfoInterpreter != null) {
      bldr.add(this::interpretDatasetInfo);
    }

    interpreters = bldr.build();
  }

  /**
   * Constructor of OccurrenceInterpreter that will not fill the information from the dataset.
   *
   * @param taxonomyInterpreter
   * @param locationInterpreter
   */
  public OccurrenceInterpreter(TaxonomyInterpreter taxonomyInterpreter, LocationInterpreter locationInterpreter) {
    this(null, taxonomyInterpreter, locationInterpreter);
  }

  /**
   * Interpret all the verbatim fields into our standard Occurrence fields. TODO: send messages/write
   * logs for interpretation errors
   *
   * @param verbatim the verbatim occurrence to interpret
   *
   * @return an OccurrenceInterpretationResult that contains an "updated" Occurrence with interpreted
   *         fields and an "original" occurrence iff this was an update to an existing record (will be
   *         null otherwise)
   */
  public OccurrenceInterpretationResult interpret(VerbatimOccurrence verbatim, Occurrence original) {
    Occurrence occ = new Occurrence(verbatim);
    interpreters.forEach(interpreter -> {
      try {
        interpreter.interpret(verbatim, occ);
      } catch (Exception e) {
        LOG.warn("Caught a runtime exception during interpretation", e);
        occ.addIssue(OccurrenceIssue.INTERPRETATION_ERROR);
      }
    });

    occ.setLastInterpreted(new Date());
    return new OccurrenceInterpretationResult(original, occ);
  }

  /**
   * This method was created to be follow the Interpreter functional interface contract. Note that
   * datasetInfoInterpreter is nullable but will not be added to the list if null
   */
  private void interpretDatasetInfo(VerbatimOccurrence verbatim, Occurrence occ) {
    datasetInfoInterpreter.interpretDatasetInfo(occ);
  }

  private static void interpretReferences(VerbatimOccurrence verbatim, Occurrence occ) {
    if (verbatim.hasVerbatimField(DcTerm.references)) {
      String val = verbatim.getVerbatimField(DcTerm.references);
      if (!Strings.isNullOrEmpty(val)) {
        occ.setReferences(UrlParser.parse(val));
        if (occ.getReferences() == null) {
          occ.getIssues().add(OccurrenceIssue.REFERENCES_URI_INVALID);
        }
      }
    }
  }

  private static void interpretTypification(VerbatimOccurrence verbatim, Occurrence occ) {
    if (verbatim.hasVerbatimField(DwcTerm.typeStatus)) {
      ParseResult<TypeStatus> parsed = TYPE_PARSER.parse(verbatim.getVerbatimField(DwcTerm.typeStatus));
      occ.setTypeStatus(parsed.getPayload());

      ParseResult<String> parsedName = TYPE_NAME_PARSER.parse(verbatim.getVerbatimField(DwcTerm.typeStatus));
      occ.setTypifiedName(parsedName.getPayload());
      // TODO: flag value invalid issue (new API enum value to be created)
    }
    if (verbatim.hasVerbatimField(GbifTerm.typifiedName)) {
      occ.setTypifiedName(verbatim.getVerbatimField(GbifTerm.typifiedName));
    }
  }

  private static void interpretBor(VerbatimOccurrence verbatim, Occurrence occ) {
    ParseResult<BasisOfRecord> parsed = BOR_PARSER.parse(verbatim.getVerbatimField(DwcTerm.basisOfRecord));
    if (parsed.isSuccessful()) {
      occ.setBasisOfRecord(parsed.getPayload());
    } else {
      LOG.debug("Unknown basisOfRecord [{}]", verbatim.getVerbatimField(DwcTerm.basisOfRecord));
      occ.addIssue(OccurrenceIssue.BASIS_OF_RECORD_INVALID);
      occ.setBasisOfRecord(BasisOfRecord.UNKNOWN);
    }
  }

  private static void interpretSex(VerbatimOccurrence verbatim, Occurrence occ) {
    ParseResult<Sex> parsed = SEX_PARSER.parse(verbatim.getVerbatimField(DwcTerm.sex));
    if (parsed.isSuccessful()) {
      occ.setSex(parsed.getPayload());
    } else {
      // TODO: flag value invalid issue (new API enum value to be created)
      LOG.debug("Unknown sex [{}]", verbatim.getVerbatimField(DwcTerm.sex));
    }
  }

  private static void interpretEstablishmentMeans(VerbatimOccurrence verbatim, Occurrence occ) {
    ParseResult<EstablishmentMeans> parsed = EST_PARSER.parse(verbatim.getVerbatimField(DwcTerm.establishmentMeans));
    if (parsed.isSuccessful()) {
      occ.setEstablishmentMeans(parsed.getPayload());
    } else {
      // TODO: flag value invalid issue (new API enum value to be created)
      LOG.debug("Unknown establishmentMeans [{}]", verbatim.getVerbatimField(DwcTerm.establishmentMeans));
    }
  }

  private static void interpretLifeStage(VerbatimOccurrence verbatim, Occurrence occ) {
    ParseResult<LifeStage> parsed = LST_PARSER.parse(verbatim.getVerbatimField(DwcTerm.lifeStage));
    if (parsed.isSuccessful()) {
      occ.setLifeStage(parsed.getPayload());
    } else {
      // TODO: flag value invalid issue (new API enum value to be created)
      LOG.debug("Unknown lifeStage [{}]", verbatim.getVerbatimField(DwcTerm.lifeStage));
    }
  }

  private static void interpretIndividualCount(VerbatimOccurrence verbatim, Occurrence occ) {
    if (verbatim.hasVerbatimField(DwcTerm.individualCount)) {
      occ.setIndividualCount(NumberParser.parseInteger(verbatim.getVerbatimField(DwcTerm.individualCount)));
      if (occ.getIndividualCount() == null && !verbatim.getVerbatimField(DwcTerm.individualCount).isEmpty()) {
        occ.getIssues().add(OccurrenceIssue.INDIVIDUAL_COUNT_INVALID);
        LOG.debug("Invalid individualCount {}", verbatim.getVerbatimField(DwcTerm.individualCount));
      }
    }
  }
}
