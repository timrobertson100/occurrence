package org.gbif.occurrence.ws.resources.provider;

import static junit.framework.TestCase.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

import javax.ws.rs.ext.MessageBodyWriter;

import org.apache.commons.io.IOUtils;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.ws.provider.OccurrenceVerbatimDwcXMLBodyWriter;
import org.gbif.utils.file.FileUtils;
import org.junit.Test;

import com.google.common.base.CharMatcher;

/**
 * Test for {@link OccurrenceVerbatimDwcXMLBodyWriter} behavior.
 *
 */
public class VerbatimOccurrenceDwcXMLBodyWriterTest {

  @Test
  public void testVerbatimOccurrenceXML() throws IOException {
    MessageBodyWriter<VerbatimOccurrence> occurrenceDwcXMLBodyWriter = new OccurrenceVerbatimDwcXMLBodyWriter();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    VerbatimOccurrence occ = new VerbatimOccurrence();

    occ.setVerbatimField(DwcTerm.verbatimLocality, "mad");
    Term customTerm = TermFactory.instance().findTerm("MyTerm");
    occ.setVerbatimField(customTerm, "MyTerm value");

    occurrenceDwcXMLBodyWriter.writeTo(occ, null, null, null, null, null, baos);

    String expectedContent = IOUtils.toString(new FileInputStream(FileUtils.getClasspathFile("dwc_xml/verbatim_occurrence.xml")));
    assertEquals(CharMatcher.WHITESPACE.removeFrom(expectedContent), CharMatcher.WHITESPACE.removeFrom(baos.toString()));
  }
}
