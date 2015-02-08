/*
This file is part of opensearch.
Copyright © 2009, Dansk Bibliotekscenter a/s,
Tempovej 7-11, DK-2750 Ballerup, Denmark. CVR: 15149043

opensearch is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

opensearch is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with opensearch.  If not, see <http://www.gnu.org/licenses/>.
*/

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.fcrepo.oai;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import org.fcrepo.utilities.DateUtility;
import org.junit.AfterClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.BeforeClass;

public class OAIResponderTest
{
    protected final Date from = new Date(993355506000L); // 2001-06-24T04:05:06Z
    protected final Date until = new Date(1293196455000L); //2010-12-24T13:14:15Z

    protected final Date obj1Date = new Date(1025503628000L); // 2002-07-01T06:07:08Z
    protected final Date obj2Date = new Date(1262474665000L); // 2010-01-02T23:24:25Z
    protected final Set<String> emptySet = Collections.emptySet();

    static class MockOAIProvider implements OAIProvider {

        @Override
        public String getRepositoryName() throws RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String getBaseURL(String protocol, String port) throws RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String getProtocolVersion() throws RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Date getEarliestDatestamp() throws RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public DeletedRecordSupport getDeletedRecordSupport() throws RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public DateGranularitySupport getDateGranularitySupport() throws RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Set getAdminEmails() throws RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Set getSupportedCompressionEncodings() throws RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Set getDescriptions() throws RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Record getRecord(String identifier, String metadataPrefix)
                throws CannotDisseminateFormatException, IDDoesNotExistException, RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List getRecords(Date from, Date until, String metadataPrefix, String set)
                throws CannotDisseminateFormatException, NoRecordsMatchException, NoSetHierarchyException, RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List getRecords(String resumptionToken)
                throws CannotDisseminateFormatException, NoRecordsMatchException, NoSetHierarchyException, BadResumptionTokenException, RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List getHeaders(Date from, Date until, String metadataPrefix, String set)
                throws CannotDisseminateFormatException, NoRecordsMatchException, NoSetHierarchyException, RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List getHeaders(String resumptionToken)
                throws CannotDisseminateFormatException, NoRecordsMatchException, NoSetHierarchyException, BadResumptionTokenException, RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List getSets()
                throws NoSetHierarchyException, RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List getSets(String resumptionToken)
                throws BadResumptionTokenException, NoSetHierarchyException, RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Set getMetadataFormats(String id)
                throws IDDoesNotExistException, NoMetadataFormatsException, RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public long getMaxSets() throws RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public long getMaxRecords() throws RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public long getMaxHeaders() throws RepositoryException {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    public OAIResponderTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Test
    public void testGetUTCString()
    {
        assertEquals("2001-06-24T04:05:06Z", OAIResponder.getUTCString(from, true));
        assertEquals("2010-12-24T13:14:15Z", OAIResponder.getUTCString(until, true));
        assertEquals("2001-06-24", OAIResponder.getUTCString(from, false));
        assertEquals("2010-12-24", OAIResponder.getUTCString(until, false));
    }

    @Test
    public void testRespondToIdentify() throws Exception {

        OAIProvider provider = new MockOAIProvider() {
            @Override
            public DateGranularitySupport getDateGranularitySupport() throws RepositoryException {
                return DateGranularitySupport.SECONDS;
            }
        };

        OAIResponder responder = new OAIResponder(provider, null);

        StringWriter writer = new StringWriter();

        Map args = new HashMap();

        args.put("verb", "identify");

        // 1970-01-01T00:00:00Z
        Date earliestDatestamp = new Date(0);

        responder.respondToIdentify(args, "http://localhost:8080/fedora/oai", "repositoryName", "protocolVersion", earliestDatestamp, DeletedRecordSupport.TRANSIENT,
                emptySet, emptySet, emptySet, new PrintWriter(writer));

        String expectedXml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+
            "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"\n"+
            "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"+
            "         xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd\">\n"+
            // This might fail: the time may pass a second boundary between this and the date call inside the tested function
            // Could be fixed by adding the 'now' timestamp as parameter to the respondToIdentify method
            "  <responseDate>" + DateUtility.convertDateToString(new Date(), false) + "</responseDate>\n"+
            "  <request verb=\"identify\">http://localhost:8080/fedora/oai</request>\n"+
            "  <Identify>\n"+
            "    <repositoryName>repositoryName</repositoryName>\n"+
            "    <baseURL>http://localhost:8080/fedora/oai</baseURL>\n"+
            "    <protocolVersion>protocolVersion</protocolVersion>\n"+
            "    <earliestDatestamp>1970-01-01T00:00:00Z</earliestDatestamp>\n"+
            "    <deletedRecord>transient</deletedRecord>\n"+
            "    <granularity>YYYY-MM-DDThh:mm:ssZ</granularity>\n"+
            "  </Identify>\n"+
            "</OAI-PMH>\n";

        assertEquals( expectedXml, writer.toString() );

    }

    @Test
    public void testProcessListIdentifiers() throws Exception {

        final String fromStr = "2001-06-24T04:05:06Z";
        final String untilStr = "2010-12-24T13:14:15Z";

        OAIProvider provider = new MockOAIProvider() {
            @Override
            public DateGranularitySupport getDateGranularitySupport() throws RepositoryException {
                return DateGranularitySupport.SECONDS;
            }
            @Override
            public List getHeaders(Date from, Date until, String metadataPrefix, String set)
                    throws CannotDisseminateFormatException, NoRecordsMatchException, NoSetHierarchyException, RepositoryException {

                // Verify arguments passed to provider
                // (Could just compare Date objects, but then error message would show local times)
                String actualFrom = DateUtility.convertDateToString(from, false);
                String actualUntil = DateUtility.convertDateToString(until, false);

                assertEquals(fromStr, actualFrom);
                assertEquals(untilStr, actualUntil);

                List ret = new ArrayList();
                ret.add(new SimpleHeader("oai:domain:obj:1", obj1Date, emptySet, true));
                ret.add(new SimpleHeader("oai:domain:obj:2", obj2Date, emptySet, false));

                return ret;
            }
        };

        OAIResponder responder = new OAIResponder(provider, null);
        Map args = new HashMap();

        args.put("verb", "ListIdentifiers");
        args.put("metadataPrefix", "oai_dc");
        args.put("from", fromStr);
        args.put("until", untilStr);

        List headers = responder.processListIdentifiers(args);
    }

    @Test
    public void testProcessListIdentifiersDateGranularity() throws Exception {
        OAIProvider provider = new MockOAIProvider() {
            @Override
            public List getHeaders(Date from, Date until, String metadataPrefix, String set)
                    throws CannotDisseminateFormatException, NoRecordsMatchException, NoSetHierarchyException, RepositoryException {

                // Verify arguments passed to provider
                // (Could just compare Date objects, but then error message would show local times)
                String actualFrom = DateUtility.convertDateToString(from);
                String actualUntil = DateUtility.convertDateToString(until);

                assertEquals("2001-06-24T00:00:00.000Z", actualFrom);
                assertEquals("2010-12-24T23:59:59.999Z", actualUntil);

                List ret = new ArrayList();
                ret.add(new SimpleHeader("oai:domain:obj:1", obj1Date, emptySet, true));
                ret.add(new SimpleHeader("oai:domain:obj:2", obj2Date, emptySet, false));

                return ret;
            }
        };

        OAIResponder responder = new OAIResponder(provider, null);
        Map args = new HashMap();

        args.put("verb", "ListIdentifiers");
        args.put("metadataPrefix", "oai_dc");
        args.put("from", "2001-06-24");
        args.put("until", "2010-12-24");

        List headers = responder.processListIdentifiers(args);
    }

    @Test
    public void testRespondToListIdentifiers() throws Exception {

        final OAIProvider provider = new MockOAIProvider() {
            @Override
            public DateGranularitySupport getDateGranularitySupport() throws RepositoryException {
                return DateGranularitySupport.SECONDS;
            }
        };
        OAIResponder responder = new OAIResponder(provider, null);

        StringWriter writer = new StringWriter();

        Map args = new HashMap();

        args.put("verb", "identify");

        List<Header> headers = new ArrayList<Header>();
        headers.add(new SimpleHeader("oai:domain:obj:1", obj1Date, emptySet, true));
        headers.add(new SimpleHeader("oai:domain:obj:2", obj2Date, emptySet, false));

        // Invoke tested method
        responder.respondToListIdentifiers(args, "http://localhost:8080/fedora/oai", headers, null, new PrintWriter(writer));

        // Verify result
        String expectedXml =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"\n" +
            "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
            "         xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd\">\n" +
            "  <responseDate>" + DateUtility.convertDateToString(new Date(), false) + "</responseDate>\n"+
            "  <request verb=\"identify\">http://localhost:8080/fedora/oai</request>\n" +
            "  <ListIdentifiers>\n" +
            "    <header>\n" +
            "      <identifier>oai:domain:obj:1</identifier>\n" +
            "      <datestamp>2002-07-01T06:07:08Z</datestamp>\n" +
            "    </header>\n" +
            "    <header status=\"deleted\">\n" +
            "      <identifier>oai:domain:obj:2</identifier>\n" +
            "      <datestamp>2010-01-02T23:24:25Z</datestamp>\n" +
            "    </header>\n" +
            "  </ListIdentifiers>\n" +
            "</OAI-PMH>\n";

        assertEquals( expectedXml, writer.toString() );
    }

    @Test
    public void testProcessListRecords() throws Exception {

        final String fromStr = "2001-06-24T04:05:06Z";
        final String untilStr = "2010-12-24T13:14:15Z";

        OAIProvider provider = new MockOAIProvider() {
            @Override
            public DateGranularitySupport getDateGranularitySupport() throws RepositoryException {
                return DateGranularitySupport.SECONDS;
            }
            @Override
            public List getRecords(Date from, Date until, String metadataPrefix, String set)
                    throws CannotDisseminateFormatException, NoRecordsMatchException, NoSetHierarchyException, RepositoryException {

                // Verify arguments passed to provider
                // (Could just compare Date objects, but then error message would show local times)
                String actualFrom = DateUtility.convertDateToString(from, false);
                String actualUntil = DateUtility.convertDateToString(until, false);

                // Verify the timestamps passed are correct
                assertEquals(fromStr, actualFrom);
                assertEquals(untilStr, actualUntil);

                List ret = new ArrayList();
                ret.add(new SimpleRecord(new SimpleHeader("oai:domain:obj:1", obj1Date, emptySet, true),
                        "", emptySet));

                ret.add(new SimpleRecord(new SimpleHeader("oai:domain:obj:2", obj2Date, emptySet, false),
                        "", emptySet));

                return ret;
            }
        };

        OAIResponder responder = new OAIResponder(provider, null);
        Map args = new HashMap();

        args.put("verb", "ListRecords");
        args.put("metadataPrefix", "oai_dc");
        args.put("from", fromStr);
        args.put("until", untilStr);

        List records = responder.processListRecords(args);
    }

}