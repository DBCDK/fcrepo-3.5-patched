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

package org.fcrepo.server.oai;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.fcrepo.oai.DateGranularitySupport;
import org.fcrepo.oai.DeletedRecordSupport;
import org.fcrepo.oai.Header;
import org.fcrepo.oai.Record;
import org.fcrepo.oai.ResumptionToken;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.search.Condition;
import org.fcrepo.server.search.FieldSearch;
import org.fcrepo.server.search.FieldSearchQuery;
import org.fcrepo.server.search.FieldSearchResult;
import org.fcrepo.server.search.ObjectFields;
import org.fcrepo.server.search.Operator;
import org.fcrepo.server.storage.DOReader;
import org.fcrepo.server.utilities.DCField;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author thp
 */
public class FedoraOAIProviderTest
{
    protected final Date from = new Date(993355506000L); // 2001-06-24T04:05:06Z
    protected final Date until = new Date(1293196455000L); //2010-12-24T13:14:15Z

    protected final Date obj1Date = new Date(1025503628000L); // 2002-07-01T06:07:08Z
    protected final Date obj2Date = new Date(1262474665000L); // 2010-01-02T23:24:25Z

    /**
     * To be overridden in test case. Implement methods with behavior required by
     */
    public static class MockFieldSearch implements FieldSearch {

        @Override
        public void update(DOReader reader) throws ServerException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean delete(String pid) throws ServerException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public FieldSearchResult findObjects(String[] resultFields, int maxResults, FieldSearchQuery query) throws ServerException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public FieldSearchResult resumeFindObjects(String sessionToken) throws ServerException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int findHighestID(String namespace) throws ServerException {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    public static class MockFieldSearchResult implements FieldSearchResult {

        private final List<ObjectFields> result;
        private final String token;

        public MockFieldSearchResult(List<ObjectFields> result, String token) {
            this.result = result;
            this.token = token;
        }

        @Override
        public List<ObjectFields> objectFieldsList() {
            return result;
        }

        @Override
        public String getToken() {
            return token;
        }

        @Override
        public long getCursor() {
            return 0;
        }

        @Override
        public long getCompleteListSize() {
            return result.size();
        }

        @Override
        public Date getExpirationDate() {
            return new Date(0);
        }
    };

    private static FedoraOAIProvider getInstance(MockFieldSearch fs) {
        FedoraOAIProvider provider = new FedoraOAIProvider("repositoryName", "repositoryDomain","localName",
                "/context/oai", Collections.emptySet(), Collections.emptySet(), "namespaceID",
                101, 102, 103, fs);
        return provider;
    }

    public FedoraOAIProviderTest()
    {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp()
    {
    }

    @After
    public void tearDown()
    {
    }

    @Test
    public void testGetRepositoryName() {
        FedoraOAIProvider provider = getInstance(null);

        assertEquals("repositoryName", provider.getRepositoryName());
    }

    @Test
    public void testGetBaseURL() {
        FedoraOAIProvider provider = getInstance(null);

        assertEquals("http://localName:8080/context/oai", provider.getBaseURL("http", "8080"));
    }

    @Test
    public void testGetProtocolVersion() {
        FedoraOAIProvider provider = getInstance(null);

        assertEquals("2.0", provider.getProtocolVersion());
    }

    @Test
    public void testGetEarliestDatestamp() {
        FedoraOAIProvider provider = getInstance(null);

        Date date = provider.getEarliestDatestamp();

        // Verify Value
        assertEquals(0, date.getTime());
    }

    @Test
    public void testGetDeletedRecordSupport() {
        FedoraOAIProvider provider = getInstance(null);

        //assertEquals(DeletedRecordSupport.NO, provider.getDeletedRecordSupport());
        assertSame(DeletedRecordSupport.TRANSIENT, provider.getDeletedRecordSupport());
    }

    @Test
    public void testGetDateGranularitySupport() {
        FedoraOAIProvider provider = getInstance(null);

        assertSame(DateGranularitySupport.SECONDS, provider.getDateGranularitySupport());
    }

    @Test
    @Ignore
    public void testGetAdminEmails() {
    }

    @Test
    @Ignore
    public void testGetSupportedCompressionEncodings() {
    }

    @Test
    @Ignore
    public void testGetDescriptions() {
    }

    @Test
    public void testGetRecord() throws Exception {

        MockFieldSearch fs = new MockFieldSearch() {

            public FieldSearchResult findObjects(String[] resultFields, int maxResults, FieldSearchQuery query) throws ServerException {

                List<Condition> conditions = query.getConditions();

                // Verify the search conditions match the expected
                assertEquals(new Condition("pid", Operator.EQUALS, "obj:1"), conditions.get(0));
                assertEquals(new Condition("dcmDate", Operator.GREATER_THAN, "2000-01-01"), conditions.get(1));

                // Create the mock result for the reponse

                List<ObjectFields> result = new ArrayList<ObjectFields>();

                ObjectFields obj1 = new ObjectFields();
                obj1.setPid("obj:1");

                obj1.setMDate(obj1Date);
                obj1.setState("A");

                obj1.titles().add(new DCField("title"));
                obj1.creators().add(new DCField("creator"));
                obj1.subjects().add(new DCField("subject"));
                obj1.descriptions().add(new DCField("description"));
                obj1.publishers().add(new DCField("publisher"));
                obj1.contributors().add(new DCField("contributor"));
                obj1.dates().add(new DCField("date"));
                obj1.types().add(new DCField("type"));
                obj1.formats().add(new DCField("format"));
                obj1.identifiers().add(new DCField("identifier"));
                obj1.sources().add(new DCField("source"));
                obj1.languages().add(new DCField("language"));
                obj1.relations().add(new DCField("relation"));
                obj1.coverages().add(new DCField("coverage"));
                obj1.rights().add(new DCField("right"));

                result.add(obj1);

                return new MockFieldSearchResult(result, "token");
            }
        };

        FedoraOAIProvider provider = getInstance(fs);

        Record record = provider.getRecord("oai:repositoryDomain:obj:1", "oai_dc");
        Header header = record.getHeader();

        assertEquals("oai:repositoryDomain:obj:1", header.getIdentifier());
        assertEquals(obj1Date, header.getDatestamp());
        assertTrue(header.isAvailable());

        String expectedXml =
            "<oai_dc:dc xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" " +
            "xmlns:dc=\"http://purl.org/dc/elements/1.1/\" " +
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
            "xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd\">" +
            "<dc:title>title</dc:title>" +
            "<dc:creator>creator</dc:creator>" +
            "<dc:subject>subject</dc:subject>" +
            "<dc:description>description</dc:description>" +
            "<dc:publisher>publisher</dc:publisher>" +
            "<dc:contributor>contributor</dc:contributor>" +
            "<dc:date>date</dc:date>" +
            "<dc:type>type</dc:type>" +
            "<dc:format>format</dc:format>" +
            "<dc:identifier>identifier</dc:identifier>" +
            "<dc:source>source</dc:source>" +
            "<dc:language>language</dc:language>" +
            "<dc:relation>relation</dc:relation>" +
            "<dc:coverage>coverage</dc:coverage>" +
            "<dc:rights>right</dc:rights>" +
            "</oai_dc:dc>";

        assertEquals(expectedXml, record.getMetadata());
    }

    @Test
    public void testGetRecords_4args() throws Exception {

        MockFieldSearch fs = new MockFieldSearch() {

            public FieldSearchResult findObjects(String[] resultFields, int maxResults, FieldSearchQuery query) throws ServerException {

                List<Condition> conditions = query.getConditions();

                // Verify the search conditions match the expected
                assertEquals(new Condition("dcmDate", Operator.GREATER_THAN, "2000-01-01"), conditions.get(0));

                assertEquals(new Condition("mDate", Operator.GREATER_OR_EQUAL, "2001-06-24T04:05:06Z"), conditions.get(1));
                assertEquals(new Condition("mDate", Operator.LESS_OR_EQUAL, "2010-12-24T13:14:15Z"), conditions.get(2));

                // Create the mock result for the reponse

                List<ObjectFields> result = new ArrayList<ObjectFields>();

                ObjectFields obj1 = new ObjectFields();
                obj1.setPid("obj:1");

                obj1.setMDate(obj1Date);
                obj1.setState("A");

                obj1.titles().add(new DCField("title"));
                obj1.creators().add(new DCField("creator"));
                obj1.subjects().add(new DCField("subject"));
                obj1.descriptions().add(new DCField("description"));
                obj1.publishers().add(new DCField("publisher"));
                obj1.contributors().add(new DCField("contributor"));
                obj1.dates().add(new DCField("date"));
                obj1.types().add(new DCField("type"));
                obj1.formats().add(new DCField("format"));
                obj1.identifiers().add(new DCField("identifier"));
                obj1.sources().add(new DCField("source"));
                obj1.languages().add(new DCField("language"));
                obj1.relations().add(new DCField("relation"));
                obj1.coverages().add(new DCField("coverage"));
                obj1.rights().add(new DCField("right"));

                result.add(obj1);

                return new MockFieldSearchResult(result, "token");
            }
        };

        FedoraOAIProvider provider = getInstance(fs);

        // Invoke tested method
        List records = provider.getRecords(from, until, "oai_dc", null);

        // Verify results
        assertEquals(2, records.size());

        Record record = (Record) records.get(0);
        Header header = record.getHeader();

        assertEquals("oai:repositoryDomain:obj:1", header.getIdentifier());
        assertEquals(obj1Date, header.getDatestamp());
        assertTrue(header.isAvailable());

        String expectedXml =
            "<oai_dc:dc xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" " +
            "xmlns:dc=\"http://purl.org/dc/elements/1.1/\" " +
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
            "xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd\">" +
            "<dc:title>title</dc:title>" +
            "<dc:creator>creator</dc:creator>" +
            "<dc:subject>subject</dc:subject>" +
            "<dc:description>description</dc:description>" +
            "<dc:publisher>publisher</dc:publisher>" +
            "<dc:contributor>contributor</dc:contributor>" +
            "<dc:date>date</dc:date>" +
            "<dc:type>type</dc:type>" +
            "<dc:format>format</dc:format>" +
            "<dc:identifier>identifier</dc:identifier>" +
            "<dc:source>source</dc:source>" +
            "<dc:language>language</dc:language>" +
            "<dc:relation>relation</dc:relation>" +
            "<dc:coverage>coverage</dc:coverage>" +
            "<dc:rights>right</dc:rights>" +
            "</oai_dc:dc>";

        assertEquals(expectedXml, record.getMetadata());

        ResumptionToken token = (ResumptionToken) records.get(1);
        assertEquals("token", token.getValue());
    }

    @Test
    public void testGetRecords_String() throws Exception {

        MockFieldSearch fs = new MockFieldSearch() {

            public FieldSearchResult resumeFindObjects(String sessionToken) throws ServerException {

                // Verify the parameters match the expected
                assertEquals("token", sessionToken);

                // Create the mock result for the reponse

                List<ObjectFields> result = new ArrayList<ObjectFields>();

                ObjectFields obj1 = new ObjectFields();
                obj1.setPid("obj:1");

                obj1.setMDate(obj1Date);
                obj1.setState("A");

                obj1.titles().add(new DCField("title"));
                obj1.creators().add(new DCField("creator"));
                obj1.subjects().add(new DCField("subject"));
                obj1.descriptions().add(new DCField("description"));
                obj1.publishers().add(new DCField("publisher"));
                obj1.contributors().add(new DCField("contributor"));
                obj1.dates().add(new DCField("date"));
                obj1.types().add(new DCField("type"));
                obj1.formats().add(new DCField("format"));
                obj1.identifiers().add(new DCField("identifier"));
                obj1.sources().add(new DCField("source"));
                obj1.languages().add(new DCField("language"));
                obj1.relations().add(new DCField("relation"));
                obj1.coverages().add(new DCField("coverage"));
                obj1.rights().add(new DCField("right"));

                result.add(obj1);

                return new MockFieldSearchResult(result, null);
            }
        };

        FedoraOAIProvider provider = getInstance(fs);

        List records = provider.getRecords("token");

        assertEquals(1, records.size());

        Record record = (Record) records.get(0);
        Header header = record.getHeader();

        assertEquals("oai:repositoryDomain:obj:1", header.getIdentifier());
        assertEquals(obj1Date, header.getDatestamp());
        assertTrue(header.isAvailable());

        String expectedXml =
            "<oai_dc:dc xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" " +
            "xmlns:dc=\"http://purl.org/dc/elements/1.1/\" " +
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
            "xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd\">" +
            "<dc:title>title</dc:title>" +
            "<dc:creator>creator</dc:creator>" +
            "<dc:subject>subject</dc:subject>" +
            "<dc:description>description</dc:description>" +
            "<dc:publisher>publisher</dc:publisher>" +
            "<dc:contributor>contributor</dc:contributor>" +
            "<dc:date>date</dc:date>" +
            "<dc:type>type</dc:type>" +
            "<dc:format>format</dc:format>" +
            "<dc:identifier>identifier</dc:identifier>" +
            "<dc:source>source</dc:source>" +
            "<dc:language>language</dc:language>" +
            "<dc:relation>relation</dc:relation>" +
            "<dc:coverage>coverage</dc:coverage>" +
            "<dc:rights>right</dc:rights>" +
            "</oai_dc:dc>";

        assertEquals(expectedXml, record.getMetadata());
    }

    @Test
    public void testGetHeaders_4args() throws Exception {

        MockFieldSearch fs = new MockFieldSearch() {

            public FieldSearchResult findObjects(String[] resultFields, int maxResults, FieldSearchQuery query) throws ServerException {

                List<Condition> conditions = query.getConditions();

                // Verify the search conditions match the expected
                assertEquals(new Condition("dcmDate", Operator.GREATER_THAN, "2000-01-01"), conditions.get(0));

                assertEquals(new Condition("mDate", Operator.GREATER_OR_EQUAL, "2001-06-24T04:05:06Z"), conditions.get(1));
                assertEquals(new Condition("mDate", Operator.LESS_OR_EQUAL, "2010-12-24T13:14:15Z"), conditions.get(2));

                // Create the mock result for the reponse

                List<ObjectFields> result = new ArrayList<ObjectFields>();

                ObjectFields obj1 = new ObjectFields();
                obj1.setPid("obj:1");
                obj1.setMDate(obj1Date);
                obj1.setState("A");
                result.add(obj1);

                ObjectFields obj2 = new ObjectFields();
                obj2.setPid("obj:2");
                obj2.setMDate(obj2Date);
                obj2.setState("D");
                result.add(obj2);

                return new MockFieldSearchResult(result, "token");
            }
        };

        FedoraOAIProvider provider = getInstance(fs);

        List headers = provider.getHeaders(from, until, "oai_dc", null);

        // Verify the result matches the data from the field search

        assertEquals(3, headers.size());

        Header header1 = (Header) headers.get(0);
        assertEquals("oai:repositoryDomain:obj:1", header1.getIdentifier());
        assertEquals(obj1Date, header1.getDatestamp());
        assertTrue(header1.isAvailable());

        Header header2 = (Header) headers.get(1);
        assertEquals("oai:repositoryDomain:obj:2", header2.getIdentifier());
        assertEquals(obj2Date, header2.getDatestamp());
        assertFalse(header2.isAvailable());

        ResumptionToken token = (ResumptionToken) headers.get(2);
        assertEquals("token", token.getValue());
    }

    @Test
    public void testGetHeaders_String() throws Exception {

        MockFieldSearch fs = new MockFieldSearch() {

            public FieldSearchResult resumeFindObjects(String sessionToken) throws ServerException {

                // Verify the parameters match the expected
                assertEquals("token", sessionToken);

                // Create the mock result for the reponse

                List<ObjectFields> result = new ArrayList<ObjectFields>();

                ObjectFields obj1 = new ObjectFields();
                obj1.setPid("obj:1");
                obj1.setMDate(obj1Date);
                obj1.setState("A");
                result.add(obj1);

                ObjectFields obj2 = new ObjectFields();
                obj2.setPid("obj:2");
                obj2.setMDate(obj2Date);
                obj2.setState("D");
                result.add(obj2);

                return new MockFieldSearchResult(result, null);
            }
        };

        FedoraOAIProvider provider = getInstance(fs);

        List headers = provider.getHeaders("token");

        // Verify the result matches the data from the field search

        assertEquals(2, headers.size());

        Header header1 = (Header) headers.get(0);
        assertEquals("oai:repositoryDomain:obj:1", header1.getIdentifier());
        assertEquals(obj1Date, header1.getDatestamp());
        assertTrue(header1.isAvailable());

        Header header2 = (Header) headers.get(1);
        assertEquals("oai:repositoryDomain:obj:2", header2.getIdentifier());
        assertEquals(obj2Date, header2.getDatestamp());
        assertFalse(header2.isAvailable());
    }

    @Test
    @Ignore
    public void testGetSets_0args() throws Exception {
    }

    @Test
    @Ignore
    public void testGetSets_String() throws Exception {
    }

    @Test
    @Ignore
    public void testGetMetadataFormats() throws Exception {
    }

    @Test
    public void testGetMaxSets() throws Exception {
        FedoraOAIProvider provider = getInstance(null);

        assertEquals(101, provider.getMaxSets());
    }

    @Test
    public void testGetMaxRecords() throws Exception {
        FedoraOAIProvider provider = getInstance(null);

        assertEquals(102, provider.getMaxRecords());
    }

    @Test
    public void testGetMaxHeaders() throws Exception {
        FedoraOAIProvider provider = getInstance(null);

        assertEquals(103, provider.getMaxHeaders());
    }

}