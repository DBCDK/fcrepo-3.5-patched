/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */

package org.fcrepo.server.storage.translation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.fcrepo.common.Models.CONTENT_MODEL_3_0;
import org.fcrepo.server.errors.ObjectIntegrityException;
import org.fcrepo.server.errors.StreamIOException;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DigitalObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;


/**
 * Unit tests for FOXML1_1DODeserializer.
 *
 * @author Chris Wilper
 */
public class TestFOXML1_1DODeserializer
        extends TestFOXMLDODeserializer {
    private static final Logger logger =
            LoggerFactory.getLogger(TestFOXML1_1DODeserializer.class);

    public TestFOXML1_1DODeserializer() {
        // superclass sets protected fields
        // m_deserializer and m_serializer as given below
        super(new FOXML1_1DODeserializer(), new FOXML1_1DOSerializer());
    }

    //---
    // Tests
    //---

    @Test
    public void testDeserializeSimpleCModelObject() {
        doSimpleTest(CONTENT_MODEL_3_0);
    }
    
    @Test
    public void testDoDeserializeNestedNamespaces() throws ObjectIntegrityException, StreamIOException, IOException, SAXException{
        String content = "<a:root xmlns:a=\"http://somenamespace/\">\n"
                + "          <a:child xmlns:a=\"http://anothernamespace/\">Member of 'http://anothernamespace/'</a:child>\n"
                + "          <a:child>Member of 'http://somenamespace/'</a:child>\n"
                + "       </a:root>";

        String foxml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<foxml:digitalObject VERSION=\"1.1\" PID=\"150029-ucviden:a4f86e0a-5345-437e-884c-87d13e2e7836\" xmlns:foxml=\"info:fedora/fedora-system:def/foxml#\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"info:fedora/fedora-system:def/foxml# http://www.fedora.info/definitions/1/0/foxml1-1.xsd\">\n"
                + "  <foxml:objectProperties>\n"
                + "    <foxml:property NAME=\"info:fedora/fedora-system:def/model#state\" VALUE=\"A\"/>\n"
                + "  </foxml:objectProperties>\n"
                + "  <foxml:datastream ID=\"commonData\" STATE=\"A\" CONTROL_GROUP=\"X\" VERSIONABLE=\"false\">\n"
                + "    <foxml:datastreamVersion ID=\"commonData.0\" MIMETYPE=\"text/xml\">\n"
                + "      <foxml:xmlContent>\n"
                + content
                + "      </foxml:xmlContent>\n"
                + "    </foxml:datastreamVersion>\n"
                + "  </foxml:datastream>\n"
                + "</foxml:digitalObject>";
        InputStream in = new ByteArrayInputStream(foxml.getBytes(StandardCharsets.UTF_8));
        DigitalObject obj = doDeserialize(in);
        Iterable<Datastream> datastreams = obj.datastreams("commonData");
        Datastream commonData = datastreams.iterator().next();
        String actual = IOUtils.toString(commonData.getContentStream());
        assertXMLEqual(content, actual);
    }
    
    // Supports legacy test runners
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(TestFOXML1_1DODeserializer.class);
    }

}
