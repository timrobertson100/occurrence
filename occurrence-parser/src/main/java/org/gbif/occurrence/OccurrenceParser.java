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
package org.gbif.occurrence;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.digester.Digester;
import org.apache.commons.digester.NodeCreateRule;
import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.occurrence.constants.ExtractionSimpleXPaths;
import org.gbif.occurrence.model.RawOccurrenceRecord;
import org.gbif.occurrence.parsing.RawXmlOccurrence;
import org.gbif.occurrence.parsing.response_file.ParsedSearchResponse;
import org.gbif.occurrence.parsing.xml.XmlFragmentParser;
import org.gbif.occurrence.util.XmlSanitizingReader;
import org.gbif.utils.file.CharsetDetection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.sun.org.apache.xerces.internal.impl.io.MalformedByteSequenceException;

/**
 * Entry point into the parsing of raw occurrence records as retrieved from publishers. Will attempt
 * to determine both XML encodings and schema type. Parse happens in two steps - first extracts each
 * record element into a RawXmlOccurrence, and then parses each of those into RawOccurrenceRecords.
 */
public class OccurrenceParser {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceParser.class);
  private static final String ENCONDING_EQ = "encoding=";
  private static final Pattern ENCODING_PATTERN = Pattern.compile(ENCONDING_EQ);
  private static final Pattern REPLACE_QUOTES_PAT = Pattern.compile("[\"']");

  public static final String ADD_RECORD_AS_XML = "addRecordAsXml";
  public static final String SET_ABCD_1_HEADER = "setAbcd1Header";

  public List<RawOccurrenceRecord> parseResponseFileToRor(File inputFile) {
    List<RawXmlOccurrence> raws = parseResponseFileToRawXml(inputFile);
    return parseRawXmlToRor(raws);
  }

  /**
   * This parses a stream of uncompressed ABCD or DwC Occurrences into {@link RawXmlOccurrence}s. No
   * care is taken to handle wrong encodings or character sets in general. This might be changed later
   * on.
   *
   * @param is stream to parse
   *
   * @return list of parsed occurrences
   *
   * @throws ParsingException if there were any problems during parsing the stream
   */
  // TODO: Optionally handle compressed streams
  public List<RawXmlOccurrence> parseStream(InputStream is) throws ParsingException {
    checkNotNull(is, "is can't be null");
    try {
      ParsedSearchResponse responseBody = new ParsedSearchResponse();
      InputSource inputSource = new InputSource(is);

      Digester digester = new Digester();
      digester.setNamespaceAware(true);
      digester.setValidating(false);
      digester.push(responseBody);

      NodeCreateRule rawAbcd = new NodeCreateRule();
      digester.addRule(ExtractionSimpleXPaths.ABCD_RECORD_XPATH, rawAbcd);
      digester.addSetNext(ExtractionSimpleXPaths.ABCD_RECORD_XPATH, ADD_RECORD_AS_XML);

      NodeCreateRule rawAbcd1Header = new NodeCreateRule();
      digester.addRule(ExtractionSimpleXPaths.ABCD_HEADER_XPATH, rawAbcd1Header);
      digester.addSetNext(ExtractionSimpleXPaths.ABCD_HEADER_XPATH, SET_ABCD_1_HEADER);

      NodeCreateRule rawDwc1_0 = new NodeCreateRule();
      digester.addRule(ExtractionSimpleXPaths.DWC_1_0_RECORD_XPATH, rawDwc1_0);
      digester.addSetNext(ExtractionSimpleXPaths.DWC_1_0_RECORD_XPATH, ADD_RECORD_AS_XML);

      NodeCreateRule rawDwc1_4 = new NodeCreateRule();
      digester.addRule(ExtractionSimpleXPaths.DWC_1_4_RECORD_XPATH, rawDwc1_4);
      digester.addSetNext(ExtractionSimpleXPaths.DWC_1_4_RECORD_XPATH, ADD_RECORD_AS_XML);

      // NodeCreateRule rawDwcManis = new NodeCreateRule();
      // digester.addRule(ExtractionSimpleXPaths.DWC_MANIS_RECORD_XPATH, rawDwcManis);
      // digester.addSetNext(ExtractionSimpleXPaths.DWC_MANIS_RECORD_XPATH, "addRecordAsXml");

      NodeCreateRule rawDwc2009 = new NodeCreateRule();
      digester.addRule(ExtractionSimpleXPaths.DWC_2009_RECORD_XPATH, rawDwc2009);
      digester.addSetNext(ExtractionSimpleXPaths.DWC_2009_RECORD_XPATH, ADD_RECORD_AS_XML);

      digester.parse(inputSource);
      return responseBody.getRecords();
    } catch (ParserConfigurationException | TransformerException e) {
      throw new ServiceUnavailableException("Error setting up Commons Digester", e);
    } catch (SAXException | IOException e) {
      throw new ParsingException("Parsing failed", e);
    }
  }

  /**
   * Parses a single response gzipFile and returns a List of the contained RawXmlOccurrences.
   */
  public List<RawXmlOccurrence> parseResponseFileToRawXml(File gzipFile) {
    if (LOG.isDebugEnabled())
      LOG.debug(">> parseResponseFileToRawXml [{}]", gzipFile.getAbsolutePath());
    ParsedSearchResponse responseBody = null;
    try {
      responseBody = new ParsedSearchResponse();
      List<String> charsets = getCharsets(gzipFile);
      String goodCharset = null;
      boolean encodingError = false;
      for (String charsetName : charsets) {
        LOG.debug("Trying charset [{}]", charsetName);
        try (FileInputStream fis = new FileInputStream(gzipFile);
            GZIPInputStream inputStream = new GZIPInputStream(fis);
            BufferedReader inputReader = new BufferedReader(new XmlSanitizingReader(new InputStreamReader(inputStream, charsetName)));) {
          InputSource inputSource = new InputSource(inputReader);

          Digester digester = new Digester();
          digester.setNamespaceAware(true);
          digester.setValidating(false);
          digester.push(responseBody);

          NodeCreateRule rawAbcd = new NodeCreateRule();
          digester.addRule(ExtractionSimpleXPaths.ABCD_RECORD_XPATH, rawAbcd);
          digester.addSetNext(ExtractionSimpleXPaths.ABCD_RECORD_XPATH, ADD_RECORD_AS_XML);

          NodeCreateRule rawAbcd1Header = new NodeCreateRule();
          digester.addRule(ExtractionSimpleXPaths.ABCD_HEADER_XPATH, rawAbcd1Header);
          digester.addSetNext(ExtractionSimpleXPaths.ABCD_HEADER_XPATH, SET_ABCD_1_HEADER);

          NodeCreateRule rawDwc1_0 = new NodeCreateRule();
          digester.addRule(ExtractionSimpleXPaths.DWC_1_0_RECORD_XPATH, rawDwc1_0);
          digester.addSetNext(ExtractionSimpleXPaths.DWC_1_0_RECORD_XPATH, ADD_RECORD_AS_XML);

          NodeCreateRule rawDwc1_4 = new NodeCreateRule();
          digester.addRule(ExtractionSimpleXPaths.DWC_1_4_RECORD_XPATH, rawDwc1_4);
          digester.addSetNext(ExtractionSimpleXPaths.DWC_1_4_RECORD_XPATH, ADD_RECORD_AS_XML);

          // TODO: dwc_manis appears to work without a NodeCreateRule here - why?

          NodeCreateRule rawDwc2009 = new NodeCreateRule();
          digester.addRule(ExtractionSimpleXPaths.DWC_2009_RECORD_XPATH, rawDwc2009);
          digester.addSetNext(ExtractionSimpleXPaths.DWC_2009_RECORD_XPATH, ADD_RECORD_AS_XML);

          digester.parse(inputSource);

          LOG.debug("Success with charset [{}] - skipping any others", charsetName);
          goodCharset = charsetName;
          break;
        } catch (SAXException e) {
          String msg = "SAX exception when parsing parsing from response gzipFile [" + gzipFile.getAbsolutePath() + "] using encoding ["
              + charsetName + "] - trying another charset";
          LOG.debug(msg, e);
        } catch (MalformedByteSequenceException e) {
          LOG.debug("Malformed utf-8 byte when parsing with encoding [{}] - trying another charset", charsetName);
          encodingError = true;
        } catch (IOException ex) {
          LOG.warn("Error reading input files", ex);
        }
      }

      if (goodCharset == null) {
        if (encodingError) {
          LOG.warn("Could not parse gzipFile - all encoding attempts failed  with malformed utf8 - skipping gzipFile [{}]",
              gzipFile.getAbsolutePath());
        } else {
          LOG.warn("Could not parse gzipFile (malformed parsing) - skipping gzipFile [{}]", gzipFile.getAbsolutePath());
        }
      }

    } catch (IOException e) {
      LOG.warn("Could not find response gzipFile [{}] - skipping gzipFile", gzipFile.getAbsolutePath(), e);
    } catch (TransformerException e) {
      LOG.warn("Could not create parsing transformer for [{}] - skipping gzipFile", gzipFile.getAbsolutePath(), e);
    } catch (ParserConfigurationException e) {
      LOG.warn("Failed to pull raw parsing from response gzipFile [{}] - skipping gzipFile", gzipFile.getAbsolutePath(), e);
    }

    if (LOG.isDebugEnabled())
      LOG.debug("<< parseResponseFileToRawXml [{}]", gzipFile.getAbsolutePath());
    return (responseBody == null) ? null : responseBody.getRecords();
  }

  /**
   * Utility method to extract character encondings from a gzip file. Charsets are a nightmare and
   * users can't be trusted, so strategy is try these encodings in order until one of them (hopefully)
   * works (note the last two could be repeats of the first two): - utf-8 - latin1 (iso-8859-1) - the
   * declared encoding from the parsing itself - a guess at detecting the charset from the raw
   * gzipFile bytes
   */
  private static List<String> getCharsets(File gzipFile) throws IOException {
    List<String> charsets = new ArrayList<String>();
    charsets.add("UTF-8");
    charsets.add("ISO-8859-1");

    // read parsing declaration

    try (FileInputStream fis = new FileInputStream(gzipFile);
        GZIPInputStream inputStream = new GZIPInputStream(fis);
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
      boolean gotEncoding = false;
      String encoding;
      int lineCount = 0;
      while (bufferedReader.ready() && !gotEncoding && lineCount < 5) {
        String line = bufferedReader.readLine();
        lineCount++;
        if (line != null && line.contains(ENCONDING_EQ)) {
          encoding = ENCODING_PATTERN.split(line, 0)[1];
          // drop trailing ?>
          encoding = encoding.substring(0, encoding.length() - 2);
          // drop quotes
          encoding = REPLACE_QUOTES_PAT.matcher(encoding).replaceAll("").trim();
          LOG.debug("Found encoding [{}] in parsing declaration", encoding);
          try {
            Charset.forName(encoding);
            charsets.add(encoding);
          } catch (Exception e) {
            LOG.debug("Could not find supported charset matching detected encoding of [{}] - trying other guesses instead", encoding);
          }
          gotEncoding = true;
        }
      }
    }
    // attempt detection from bytes
    charsets.add(CharsetDetection.detectEncoding(gzipFile).name());
    return charsets;
  }

  public List<RawOccurrenceRecord> parseRawXmlToRor(List<RawXmlOccurrence> raws) {
    List<RawOccurrenceRecord> rors = new ArrayList<RawOccurrenceRecord>();
    for (RawXmlOccurrence raw : raws) {
      List<RawOccurrenceRecord> innerRors = XmlFragmentParser.parseRecord(raw);
      rors.addAll(innerRors);
    }
    return rors;
  }

}
