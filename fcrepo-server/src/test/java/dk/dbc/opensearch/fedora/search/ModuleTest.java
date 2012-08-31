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
package dk.dbc.opensearch.fedora.search;

import java.util.LinkedHashSet;
import org.fcrepo.server.storage.types.RelationshipTuple;
import java.text.SimpleDateFormat;
import java.io.File;
import org.fcrepo.server.Module;
import org.fcrepo.server.Parameterized;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.errors.QueryParseException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.search.Condition;
import org.fcrepo.server.search.FieldSearchQuery;
import org.fcrepo.server.search.FieldSearchResult;
import org.fcrepo.server.search.ObjectFields;
import org.fcrepo.server.search.Operator;
import org.fcrepo.server.storage.DOManager;
import org.fcrepo.server.storage.DOReader;
import org.fcrepo.server.storage.DefaultDOManager;
import org.fcrepo.server.storage.RepositoryReader;
import org.fcrepo.server.storage.lowlevel.ILowlevelStorage;
import org.fcrepo.server.storage.translation.DOTranslator;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DatastreamXMLMetadata;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import mockit.Deencapsulation;
import mockit.Expectations;

import mockit.Mocked;
import mockit.Mockit;
import mockit.NonStrictExpectations;
import org.fcrepo.server.utilities.DCField;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * This test targets the FieldSearch module as a whole. There may be tests that
 * overlap with unittests for their respective files, bu the aim of this test
 * suite is to run end-to-end tests of the module.
 *
 * The access to the search functionality of the module is provided through the
 * FieldSearchQuery class which can be instantiated using either a list of
 * Conditions or a String containing searchterms.
 *
 * @author stm
 */
public class ModuleTest
{
    private FieldSearchLucene fieldsearch;
    private static final long timeNow = System.currentTimeMillis();
    private static final int maxResults = 10;
    private static String indexLocation = "build/test-index";
    private static SimpleDateFormat zTimeFormatter;
    private static SimpleDateFormat utcFormatter;
    private static SimpleDateFormat simpleUtcFormatter;
    private static SimpleDateFormat simpleTimeFormatter;

    private static final Date yesterDate = new Date( timeNow - 86400000L );
    private static final Date toDate = new Date( timeNow );
    private static final Date tomorrowDate = new Date( timeNow + 86400000L );

    /**
     * Mock-outs of the entire Fedora Server complex. The mocks are primarily
     * and almost exclusively used in Expectations.
     */
    @Mocked RepositoryReader repoReader;
    @Mocked DOReader reader;
    @Mocked Server server;
    @Mocked Parameterized parm;
    @Mocked Module mod;
    @Mocked DOManager doma;
    @Mocked ILowlevelStorage storage;
    @Mocked DOTranslator translator;

    /**
     * Before each test is run, the servers initialization phase of the
     * FieldSearch module is mocked through Mockit.Expectations
     *
     */
    @Before
    public void setUp() throws ModuleInitializationException, ServerException
    {
        Mockit.setUpMocks();
        zTimeFormatter = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" );
        utcFormatter = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" );
        utcFormatter.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
        simpleUtcFormatter = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSS" );
        simpleUtcFormatter.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
        simpleTimeFormatter = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSS" );

        final Map<String, String> params = getParameters();

        final DOManager domareal = new DefaultDOManager( params, server, "DOManager" );
        new NonStrictExpectations()
        {
            {
                parm.getParameter( "writeLockTimeout" );
                returns( params.get( "writeLockTimeout") );
                parm.getParameter( "resultLifetime" );
                returns( params.get( "resultLifetime" ) );
                parm.getParameter( "luceneDirectory" );
                returns( params.get( "luceneDirectory" ) );
                parm.getParameter( "indexLocation" );
                returns( params.get( "indexLocation" ) );
                mod.getServer(); returns( server );
                server.getModule( "org.fcrepo.server.storage.DOManager" );
                returns( domareal );
            }
        };

        Deencapsulation.setField( domareal, storage );
        Deencapsulation.setField( domareal, translator );

        fieldsearch = new FieldSearchLucene( params, server, "org.fcrepo.server.search.FieldSearch" );
        fieldsearch.postInitModule();


    }

    /**
     * After each test has run, a call to the FieldSearch module shutdownModule
     * method is made and all files left by lucene is deleted. This should
     * ensure that each test starts up on a completely blank slate.
     */
    @After
    public void tearDown() throws Exception
    {
        fieldsearch.shutdownModule();

        //clean up directory
        for( File f : new File( indexLocation ).listFiles() )
        {
            f.delete();
        }
        server.shutdown( null );
        Mockit.tearDownMocks();
        System.gc();
    }

    /**
     * Tests the happy path of the FieldSearchLucene plugin. One object ingested,
     * and retrieved through a FieldSearchResultLucene instance
     */
    @Test
    public void testSearchOfOneDigitalObject() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "demo object2" }, "", "unit test" );
        //Emulate the call from the Server instance --> update()
        fieldsearch.update( reader );
        //Emulate search from Server instance --> FSR
        String[] fields = new String[]{ "pid", "title" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "pid", new Pair<Operator, String>( Operator.EQUALS, "demo:1" ) );
        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );
        assertEquals( "demo:1", fsr.objectFieldsList().get( 0 ).getPid() );
    }

    /**
     * Tests that an object can be retrieved through a non-unique field, the
     * title field
     */
    @Test
    public void testSearchTitleOfObject() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "test search title2" }, "", "unit test" );
        fieldsearch.update( reader );

        String[] returnfields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.EQUALS, "test search title2" ) );
        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );
        assertEquals( "demo:1", fsr.objectFieldsList().get( 0).getPid() );

    }

    /**
     * Shows that searches on title fields are case-insensitive. Both searches
     * and contents of the title field is case-insensitive
     */
    @Test
    public void testCaseSensitiveSearchTitleOfObjectWorksLikeCaseInsensitiveSearch() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "demo object2" }, "", "unit test" );
        fieldsearch.update( reader );

        ingestObject( "demo:2", new String[]{ "Demo object2" }, "", "second" );
        fieldsearch.update( reader );

        String[] returnfields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.EQUALS, "Demo Object2" ) );
        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( 2, fsr.objectFieldsList().size() );
    }

    /**
     * Unlike all other fields, searches on pids are case-sensitive. Because the
     * case of pids is significant in the fedora server, the FieldSearch module
     * must preserve cases in pids.
     *
     */
    @Test
    public void testPidSearchesAreCaseSensitive() throws Exception
    {
        ingestObject( "Demo:1", new String[]{ "demo object2" }, "", "unit test" );
        fieldsearch.update( reader );

        String[] returnfields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "pid", new Pair<Operator, String>( Operator.EQUALS, "demo:1" ) );
        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( "demo:1 will not return result for Demo:1", 0, fsr.objectFieldsList().size() );

        query = new HashMap<String, Pair<Operator, String>>();
        query.put( "pid", new Pair<Operator, String>( Operator.EQUALS, "Demo:1" ) );
        fsq = getFieldSearchQuery( query );

        fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( "Demo:1 returns result for Demo:1", 1, fsr.objectFieldsList().size() );
    }


    /**
     * When conducting EQUALS searches on (non-date) fields, whitespace is
     * significant; the amount of white-space in the query must match exactly
     * the whitespace of the field in the index.
     */
    @Test
    public void testWhiteSpaceSearchTitleOfObject() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "demo object2" }, "", "unit test" );
        fieldsearch.update( reader );

        String[] returnfields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.EQUALS, "demo object2" ) );
        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( "demo:1", fsr.objectFieldsList().get( 0).getPid() );

        query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.EQUALS, "demo  object2" ) );
        fsq = getFieldSearchQuery( query );

        fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( 0, fsr.objectFieldsList().size() );
    }

    @Test
    public void testWildcardedSearchIsNotCaseSensitive() throws Exception
    {

        ingestObject( "demo:1", new String[]{ "Cas Se" }, "timestamps", "unit test", toDate, toDate );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "Voi Me" }, "timestamps", "unit test", toDate, toDate );
        fieldsearch.update( reader );
        ingestObject( "demo:3", new String[]{ "Kas Se" }, "timestamps", "unit test", toDate, toDate );
        fieldsearch.update( reader );

        String[] returnfields = new String[]{ "pid" };
        FieldSearchQuery fsq = new FieldSearchQuery( "Ca*" );

        FieldSearchResult fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );

        fsq = new FieldSearchQuery( "ca*" );
        fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );

        fsq = new FieldSearchQuery( "*se" );
        fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( 2, fsr.objectFieldsList().size() );
    }

    @Test
    public void testCombinedExactSearchOnPidAndTitle() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "demo object2" }, "", "unit test" );
        fieldsearch.update( reader );

        String[] returnfields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "pid", new Pair<Operator, String>( Operator.CONTAINS, "demo:*" ) );
        query.put( "title", new Pair<Operator, String>( Operator.EQUALS, "demo object2" ) );
        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( "demo:1", fsr.objectFieldsList().get( 0).getPid() );
    }


    @Test
    public void testSearchOnMultipleTitles() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "demo object2", "second title" }, "", "unit test" );
        fieldsearch.update( reader );

        String[] returnfields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.EQUALS, "second title" ) );
        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( "demo:1", fsr.objectFieldsList().get( 0).getPid() );

        query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.EQUALS, "demo object2" ) );
        fsq = getFieldSearchQuery( query );

        fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( "demo:1", fsr.objectFieldsList().get( 0).getPid() );

    }


    @Test
    public void testSearchExpressionsWithSquareBrackets() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "[demo object2]" }, "", "unit test" );
        fieldsearch.update( reader );

        String[] returnfields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.EQUALS, "[demo object2]" ) );
        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( "demo:1", fsr.objectFieldsList().get( 0).getPid() );
    }


    @Test
    public void testSearchExpressionsWithIllegalChars() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "døds*en" }, "", "unit test" );
        fieldsearch.update( reader );

        String[] returnfields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.CONTAINS, "døds*en" ) );
        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( "demo:1", fsr.objectFieldsList().get( 0).getPid() );
    }

    @Test
    public void testSearchOnEmptyFieldsReturnsNothing() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "" }, "", "unit test" );
        fieldsearch.update( reader );

        String[] returnfields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.CONTAINS, "something" ) );
        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertTrue( 0 == fsr.objectFieldsList().size() );
    }

    @Test
    public void testMatchingObjectTitles() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "Jenseits von Gut und Böse" }, "", "unit test"  );
        fieldsearch.update( reader );

        ingestObject( "coll:1", new String[]{ "jenseits von gut und böse" }, "", "unit test"  );
        fieldsearch.update( reader );

        ingestObject( "demo:2", new String[]{ "zappa" }, "", "unit test" );
        fieldsearch.update( reader );


        //Emulate the call from the Server instance --> update()
        //Emulate search from Server instance --> FSR
        String[] fields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.EQUALS, "jenseits von gut und Böse" ) );

        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( "Resultset should contain two results", 2, fsr.objectFieldsList().size() );

    }

    @Test
    public void testMultipleIngestsWithSamePidReturnsOnlyOneHit() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "title of demo1"}, "", "author" );
        fieldsearch.update( reader );

        ingestObject( "demo:1", new String[]{ "title of demo1"}, "", "another author" );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.EQUALS, "title of demo1" ) );

        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        List<ObjectFields> objectFieldsList = fsr.objectFieldsList();

        assertEquals( "Resultset should match only one object", 1, objectFieldsList.size() );
    }

    @Test
    public void testDeleteRemovesDocumentFromIndex() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "Jenseits von Gut und Böse" }, "", "unit test"  );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();

        query.put( "pid", new Pair<Operator, String>( Operator.EQUALS, "demo:1" ) );
        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( "Resultset contains one result", 1, fsr.objectFieldsList().size() );

        fieldsearch.delete( "demo:1" );

        fields = new String[]{ "pid" };
        query = new HashMap<String, Pair<Operator, String>>();

        query.put( "pid", new Pair<Operator, String>( Operator.EQUALS, "demo:1" ) );
        fsq = getFieldSearchQuery( query );

        fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( "Object Entry is deleted from index", 0, fsr.objectFieldsList().size() );

    }


    @Test
    public void testContainsSearchOnEmptyFieldsReturnsIndex() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "Jenseits von Gut und Böse" }, "", "unit test"  );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();

        query.put( "source", new Pair<Operator, String>( Operator.CONTAINS, "" ) );
        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( "Resultset contains all results", 1, fsr.objectFieldsList().size() );
    }

    @Test
    public void testNoResultsShallBeReturnedForPartialEqualsSearches() throws Exception
    {

        ingestObject( "demo:1", new String[]{ "Jenseits von Gut und Böse" }, "", "unit test"  );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.EQUALS, "von" ) );

        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertTrue( 0 == fsr.objectFieldsList().size() );
    }

    @Test
    public void testFieldsWithOneWordCanBeSearchWithOneWord() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "Von" }, "", "unit test"  );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.EQUALS, "von" ) );

        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertTrue( 1 == fsr.objectFieldsList().size() );
        assertTrue( "demo:1".equals( fsr.objectFieldsList().get( 0 ).getPid() ) );
    }

    @Test
    public void testUnicodeCharsInIndexAreRecognizedInSearches() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "Jenseits von Gut und Böse" }, "", "unit test"  );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "title", new Pair<Operator, String>( Operator.EQUALS, "jenseits von gut und böse" ) );

        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertTrue( 1 == fsr.objectFieldsList().size() );
        assertTrue( "demo:1".equals( fsr.objectFieldsList().get( 0 ).getPid() ) );
    }


    /**
     * Demonstrates the functionality of searches on pids, where the namespace
     * is known and all identifiers within this namespace is requested
     */
    @Test
    public void testWildcardSearchOnPidIdentifiers() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "Jenseits von Gut und Böse" }, "", "Freddy Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "Götterdämmerung" }, "", "Freddy Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "work:1", new String[]{ "Ecce Homo" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "pid", new Pair<Operator, String>( Operator.CONTAINS, "demo:*" ) );

        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertTrue( 2 == fsr.objectFieldsList().size() );

        Set<String> mandatories = new HashSet<String>();
        mandatories.add( "demo:1" );
        mandatories.add( "demo:2" );

        for( ObjectFields field: fsr.objectFieldsList())
        {
            assertTrue( mandatories.contains( field.getPid() ) );
        }
    }

    /**
     * Demonstrates the functionality of searches on pids, where the identifier
     * is known and all namespaces containing this identifier is requested
     */
    @Test
    public void testWildcardConditionsSearchOnPidNamespaces() throws Exception
    {

        ingestObject( "demo:1", new String[]{ "Jenseits von Gut und Böse" }, "", "Freddy Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "Götterdämmerung" }, "", "Freddy Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "work:1", new String[]{ "Ecce Homo" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        query.put( "pid", new Pair<Operator, String>( Operator.CONTAINS, "*:1" ) );

        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        fsq = getFieldSearchQuery( query );

        fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertTrue( 2 == fsr.objectFieldsList().size() );

        Set<String> mandatories = new HashSet<String>();
        mandatories = new HashSet<String>();
        mandatories.add( "demo:1" );
        mandatories.add( "work:1" );
        for( ObjectFields field: fsr.objectFieldsList())
        {
            assertTrue( mandatories.contains( field.getPid() ) );
        }
    }

    @Test
    public void testWildcardTermsSearchOnPidNamespaces() throws Exception
    {

        ingestObject( "demo:1", new String[]{ "Jenseits von Gut und Böse" }, "", "Freddy Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "Götterdämmerung" }, "", "Freddy Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "work:1", new String[]{ "Ecce Homo" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };

        FieldSearchQuery fsq = new FieldSearchQuery( "*:1" );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertTrue( 2 == fsr.objectFieldsList().size() );

        Set<String> mandatories = new HashSet<String>();
        mandatories = new HashSet<String>();
        mandatories.add( "demo:1" );
        mandatories.add( "work:1" );
        for( ObjectFields field: fsr.objectFieldsList())
        {
            assertTrue( mandatories.contains( field.getPid() ) );
        }
    }

    /**
     * A wildcard search matching some part of the pid will return that pid
     */
    @Test
    public void testStarInPidSearchResultsInWildcardsearch() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "Jenseits von Gut und Böse" }, "", "Freddy Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "Götterdämmerung" }, "", "Freddy Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "work:1", new String[]{ "Ecce Homo" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();

        query.put( "pid", new Pair<Operator, String>( Operator.CONTAINS, "*o*" ) );

        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 3, fsr.objectFieldsList().size() );
        Set<String> mandatories = new HashSet<String>();
        mandatories.add( "demo:1" );
        mandatories.add( "demo:2" );
        mandatories.add( "work:1" );
        for( ObjectFields field: fsr.objectFieldsList())
        {
            assertTrue( mandatories.contains( field.getPid() ) );
        }
    }

    @Test
    public void testSingleQuestionmarkInWildcardInConditionsSearchIsInterpretedVerbatim() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "?" }, "", "Mark Question"  );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "*" }, "", "Asterix"  );
        fieldsearch.update( reader );
        ingestObject( "work:1", new String[]{ "Ecce Homo" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };

        FieldSearchQuery fsq = getFieldSearchQuery( FedoraFieldName.TITLE.toString(), Operator.EQUALS, "?" );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );
        assertEquals( "demo:1", fsr.objectFieldsList().get( 0 ).getPid() );
}

    @Test
    public void testSingleStarInWildcardInConditionsSearchIsInterpretedVerbatim() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "?" }, "", "Mark Question"  );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "*" }, "", "Asterix"  );
        fieldsearch.update( reader );
        ingestObject( "work:1", new String[]{ "Ecce Homo" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };

        FieldSearchQuery fsq = getFieldSearchQuery( FedoraFieldName.TITLE.toString(), Operator.EQUALS, "*" );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );
        assertEquals( "demo:2", fsr.objectFieldsList().get( 0 ).getPid() );
    }


    @Test
    public void testSeveralStarsInWildcardInConditionsSearchIsInterpretedVerbatim() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "?" }, "", "Mark Question"  );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "****" }, "", "Asterix"  );
        fieldsearch.update( reader );
        ingestObject( "work:1", new String[]{ "Ecce Homo" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };

        FieldSearchQuery fsq = getFieldSearchQuery( FedoraFieldName.TITLE.toString(), Operator.EQUALS, "****" );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );
        assertEquals( "demo:2", fsr.objectFieldsList().get( 0 ).getPid() );
    }


    @Test
    public void testStarNotInterpretedAsWildcardInConditionsSearch() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "Dødsstjernen" }, "", "Alfa"  );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "Døds*n" }, "", "Beta"  );
        fieldsearch.update( reader );
        ingestObject( "work:1", new String[]{ "Dødsidioten" }, "", "Gamma"  );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };

        FieldSearchQuery fsq = getFieldSearchQuery( FedoraFieldName.TITLE.toString(), Operator.EQUALS, "Døds*n" );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );
        assertEquals( "demo:2", fsr.objectFieldsList().get( 0 ).getPid() );
    }


    /**
     * Tests that searches for date ranges returns (correct) hits
     */
    @Test
    public void testSearchForDateHits() throws Exception
    {

        ingestObject( "demo:1", new String[]{ "testSearchForDateHits" }, "timestamps", "unit test", toDate, toDate );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };

        FieldSearchQuery fsq = getFieldSearchQuery( FedoraFieldName.CDATE.toString(), Operator.EQUALS, simpleUtcFormatter.format( toDate ) );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );

    }

    @Test
    public void testSearchForDateWithNonDateFormatDropsQuery() throws Exception
    {
        zTimeFormatter = new SimpleDateFormat( "yyyy-MM-dd-HH" );

        ingestObject( "demo:1", new String[]{ "testSearchForDateHits" }, "timestamps", "unit test", toDate, toDate );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };

        FieldSearchQuery fsq = getFieldSearchQuery( FedoraFieldName.CDATE.toString(), Operator.EQUALS, zTimeFormatter.format( toDate ) );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 0, fsr.objectFieldsList().size() );

    }


    @Test
    public void testSearchForDatesLessThan() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "testSearchForDatesLessThan" }, "timestamps", "unit test", toDate, toDate );
        fieldsearch.update( reader );

        FieldSearchQuery fsq = getFieldSearchQuery( FedoraFieldName.CDATE.toString(), Operator.LESS_THAN, simpleUtcFormatter.format( toDate ) );

        String[] fields = new String[]{ "pid" };

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 0, fsr.objectFieldsList().size() );

        fsq = getFieldSearchQuery( FedoraFieldName.CDATE.toString(), Operator.LESS_THAN, simpleUtcFormatter.format( tomorrowDate ) );

        fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );
    }

    @Test
    public void testSearchForDatesLessThanOrEquals() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "testSearchForDatesLessThanOrEquals" }, "timestamps", "unit test", toDate, toDate );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };

        FieldSearchQuery fsq = getFieldSearchQuery( FedoraFieldName.CDATE.toString(), Operator.LESS_OR_EQUAL, simpleUtcFormatter.format( yesterDate ) );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 0, fsr.objectFieldsList().size() );

        fsq = getFieldSearchQuery( FedoraFieldName.CDATE.toString(), Operator.LESS_OR_EQUAL, simpleUtcFormatter.format( toDate ) );

        fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );

        fsq = getFieldSearchQuery( FedoraFieldName.CDATE.toString(), Operator.LESS_OR_EQUAL, simpleUtcFormatter.format( tomorrowDate ) );

        fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );
    }

    @Test
    public void testSearchForDatesGreaterThan() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "testSearchForDatesGreaterThan" }, "timestamps", "unit test", toDate, toDate );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };

        FieldSearchQuery fsq = getFieldSearchQuery( FedoraFieldName.CDATE.toString(), Operator.GREATER_THAN, simpleUtcFormatter.format( yesterDate ) );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );

        fsq = getFieldSearchQuery( FedoraFieldName.CDATE.toString(), Operator.GREATER_THAN, simpleUtcFormatter.format( toDate ) );

        fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 0, fsr.objectFieldsList().size() );

    }

    @Test
    public void testSearchForDatesGreaterThanOrEquals() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "testSearchForDatesGreaterThanOrEquals" }, "timestamps", "unit test", toDate, toDate );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid" };

        FieldSearchQuery fsq = getFieldSearchQuery( FedoraFieldName.CDATE.toString(), Operator.GREATER_OR_EQUAL, simpleUtcFormatter.format( yesterDate ) );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );

        fsq = getFieldSearchQuery( FedoraFieldName.CDATE.toString(), Operator.GREATER_OR_EQUAL, simpleUtcFormatter.format( toDate ) );

        fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );

        fsq = getFieldSearchQuery( FedoraFieldName.CDATE.toString(), Operator.GREATER_OR_EQUAL, simpleUtcFormatter.format( tomorrowDate ) );

        fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 0, fsr.objectFieldsList().size() );

    }

    @Test
    public void testDatesArePreservedInSpiteOfInternalNormalization() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "yesterday" }, "timestamps", "unit test", yesterDate, yesterDate );
        fieldsearch.update( reader );

        ingestObject( "demo:2", new String[]{ "today" }, "timestamps", "unit test", toDate, toDate );
        fieldsearch.update( reader );

        ingestObject( "demo:3", new String[]{ "tomorrow" }, "timestamps", "unit test", tomorrowDate, tomorrowDate );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid", "mDate", "cDate", "dcmDate" };

        FieldSearchQuery fsq = getFieldSearchQuery( FedoraFieldName.PID.toString(), Operator.CONTAINS, "demo" );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        Set<Date> mandatories = new HashSet<Date>();
        mandatories.add( yesterDate );
        mandatories.add( toDate );
        mandatories.add( tomorrowDate );


        for( ObjectFields objectFields : fsr.objectFieldsList() )
        {
            assertTrue( mandatories.contains( objectFields.getCDate() ) );
            assertTrue( mandatories.contains( objectFields.getDCMDate() ) );
            assertTrue( mandatories.contains( objectFields.getMDate() ) );
        }
    }

    @Test
    public void testDatesGivenInLocalTimeAreSearchableWithUTCTimestamps() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "today" }, "timestamps", "unit test", toDate, toDate );
        fieldsearch.update( reader );

        String[] fields = new String[]{ "pid", "mDate", "cDate", "dcmDate", "date" };

        FieldSearchQuery fsq = getFieldSearchQuery( FedoraFieldName.CDATE.toString(), Operator.EQUALS, simpleUtcFormatter.format( toDate ) );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );

    }




    /**
     * Tests that if four objects exists, it is possible to construct a query that
     * effectively AND's the separate queries.
     *
     * This means that an appending of Condition objects to a List containing
     * one Condition already to the FieldSearchQuery object effectively works as
     * filters to the first Condition object given to the FieldSearchQuery
     */
    @Test
    public void testConditionsSearchIsInterpretedAsANDSearch() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "Jenseits von Gut und Böse" }, "", "Freddy Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "Götterdämmerung" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "work:1", new String[]{ "Ecce Homo" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "work:2", new String[]{ "Jenseits von Gut und Böse" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );

        List<Condition> conditions = new ArrayList<Condition>();
        conditions.add( new Condition( "title", Operator.EQUALS, "Jenseits von Gut und Böse" ) );
        FieldSearchQuery fsq = new FieldSearchQuery( conditions );

        String[] fields = new String[]{ "pid" };

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 2, fsr.objectFieldsList().size() );

        conditions = new ArrayList<Condition>();
        conditions.add( new Condition( "title", Operator.EQUALS, "Jenseits von Gut und Böse" ) );
        conditions.add( new Condition( "pid", Operator.EQUALS, "work:*" ) );

        fsq = new FieldSearchQuery( conditions );

        fields = new String[]{ "pid" };

        fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( "work:2", fsr.objectFieldsList().get( 0 ).getPid() );

        conditions = new ArrayList<Condition>();
        conditions.add( new Condition( "creator", Operator.EQUALS, "Friedrich Nietzsche" ) );
        conditions.add( new Condition( "title", Operator.EQUALS, "Jenseits von Gut und Böse" ) );

        fsq = new FieldSearchQuery( conditions );

        fields = new String[]{ "pid" };

        fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 1, fsr.objectFieldsList().size() );

    }

    @Test
    public void testTermQueryIsInterpretedAsORSearch() throws Exception
    {

        ingestObject( "demo:1", new String[]{ "Jenseits von Gut und Böse" }, "", "Freddy Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "Götterdämmerung" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "work:1", new String[]{ "Ecce Homo" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "work:2", new String[]{ "Jenseits von Gut und Böse" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );

        FieldSearchQuery fsq = new FieldSearchQuery( "I" );

        String[] fields = new String[]{ "pid" };

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 4, fsr.objectFieldsList().size() );

    }

    /**
     * Using an empty term in searches will return all documents in the index,
     * equivalent to a single "*" search
     */
    @Test
    public void testEmptyTermQueryReturnsAllDocumentsInIndex() throws Exception
    {

        ingestObject( "demo:1", new String[]{ "Jenseits von Gut und Böse" }, "", "Freddy Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "Götterdämmerung" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "work:1", new String[]{ "Ecce Homo" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );
        ingestObject( "work:2", new String[]{ "Jenseits von Gut und Böse" }, "", "Friedrich Nietzsche"  );
        fieldsearch.update( reader );

        FieldSearchQuery fsq = new FieldSearchQuery( "" );

        String[] fields = new String[]{ "pid" };

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 4, fsr.objectFieldsList().size() );

    }

    /**
     * Using an empty value for the search term in a Condition will return all
     * documents in the index, equivalent to a "*" search
     */
    @Test
    public void testEmptyConditionsSearchReturnsAllDocumentsInIndex() throws Exception
    {

        ingestObject( "demo:1", new String[]{ "demo object1" }, "", "unit test" );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "demo object2" }, "", "unit test" );
        fieldsearch.update( reader );
        ingestObject( "demo:3", new String[]{ "demo object3" }, "", "unit test" );
        fieldsearch.update( reader );

        String[] returnfields = new String[]{ "pid" };
        Map<String, Pair<Operator, String>> query = new HashMap<String, Pair<Operator, String>>();
        FieldSearchQuery fsq = getFieldSearchQuery( query );
        FieldSearchResult fsr = fieldsearch.findObjects( returnfields, maxResults, fsq );

        assertEquals( 3, fsr.objectFieldsList().size() );
    }



    @Test
    public void testRetrievalOfSetsOfResults() throws Exception
    {
        String pid = "";
        String[] title = new String[1];
        String author = "";
        String source = "testRetrievalOfSetsOfResults";
        for( int i = 0; i < 11; i++ )
        {
            pid = "demo:" + i;
            title[0] = "Object title " + i;
            ingestObject( pid, title, source, author );
            fieldsearch.update( reader );
        }

        FieldSearchQuery fsq = new FieldSearchQuery( "I" );

        String[] fields = new String[]{ "pid" };

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 11, fsr.getCompleteListSize() );
        assertEquals( 10, fsr.objectFieldsList().size() );

        String token = fsr.getToken();
        assertNotNull( "FieldSearchResult must have a token, as there are more results in it", token );

        fsr = fieldsearch.resumeFindObjects( token );
        assertNull( "When there are no more results in a FieldSearchResult, the token is nulled", fsr.getToken() );
        assertEquals( 1, fsr.objectFieldsList().size() );
    }

    @Test
    public void testCorrectNumberOfResultsReturnedFromStarTermsSearch() throws Exception
    {
        ingestObject( "demo:1", new String[]{ "demo object1" }, "", "unit test" );
        fieldsearch.update( reader );
        ingestObject( "demo:2", new String[]{ "demo object2" }, "", "unit test" );
        fieldsearch.update( reader );
        fieldsearch.delete( "demo:1" );
        ingestObject( "demo:1", new String[]{ "demo object1 revised" }, "", "unit test" );
        fieldsearch.update( reader );

        FieldSearchQuery fsq = new FieldSearchQuery( "*" );

        String[] fields = new String[]{ "pid" };

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );

        assertEquals( 2, fsr.objectFieldsList().size() );
    }

    /**
     * Tests the happy path of the FieldSearchLucene plugin. One object with a
     * single object relationship ingested and retrieved via a search on the
     * relationship object identifier through a FieldSearchResultLucene instance
     */
    @Ignore
    @Test
    public void testRetrievalOfObjectWithsingleRelationship() throws Exception
    {
        final String subject = "demo:1";
        final String object = "work:1";
        final String predicate = "http://oss.dbc.dk/rdf/dkbib#isMemberOfWork";

        Set relationships = new LinkedHashSet< RelationshipTuple >();
        relationships.add( new RelationshipTuple( subject, predicate, object,
            true, "") );

        ingestObjectWithRelationships( subject, new String[]{ "demo object1" },
            "", "unit test", relationships );

        //Emulate the call from the Server instance --> update()
        fieldsearch.update( reader );
        //Emulate search from Server instance --> FSR
        String[] fields = new String[]{ "pid", "relpredobj" };
        Map<String, Pair<Operator, String>> query =
            new HashMap<String, Pair<Operator, String>>();

        query.put( "relobj",
            new Pair<Operator, String>( Operator.EQUALS, object ) );

        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr =
            fieldsearch.findObjects( fields, maxResults, fsq );

        List< ObjectFields > objectFieldsList = fsr.objectFieldsList();

        String expectedRelPredObj = String.format( "%s|%s", predicate, object );

        assertEquals( 1, fsr.objectFieldsList().size() );
        assertEquals( subject, objectFieldsList.get( 0 ).getPid() );
        assertEquals( expectedRelPredObj,
            objectFieldsList.get( 0 ).relPredObjs().get( 0 ).getValue() );
    }

    /**
     * Tests the happy path of the FieldSearchLucene plugin. One object with
     * multiple object relationships ingested and retrieved via a search on one
     * of the relationship object identifiers through a FieldSearchResultLucene
     * instance
     */
    @Ignore
    @Test
    public void testRetrievalOfObjectWithMultipleRelationships()
        throws Exception
    {
        final String subject = "demo:1";
        final String object1 = "work:1";
        final String predicate1 = "http://oss.dbc.dk/rdf/dkbib#isMemberOfWork";
        final String object2 = "anm:1";
        final String predicate2 = "http://oss.dbc.dk/rdf/dbcbibaddi#hasReview";

        Set relationships = new LinkedHashSet< RelationshipTuple >();
        relationships.add( new RelationshipTuple( subject, predicate1, object1,
            true, "") );
        relationships.add( new RelationshipTuple( subject, predicate2, object2,
            true, "") );

        ingestObjectWithRelationships( subject, new String[]{ "demo object1" },
            "", "unit test", relationships );

        //Emulate the call from the Server instance --> update()
        fieldsearch.update( reader );
        //Emulate search from Server instance --> FSR
        String[] fields = new String[]{ "pid", "relpredobj" };
        Map<String, Pair<Operator, String>> query =
            new HashMap<String, Pair<Operator, String>>();

        query.put( "relobj",
            new Pair<Operator, String>( Operator.EQUALS, object1 ) );

        FieldSearchQuery fsq = getFieldSearchQuery( query );

        FieldSearchResult fsr =
            fieldsearch.findObjects( fields, maxResults, fsq );

        List< ObjectFields > objectFieldsList = fsr.objectFieldsList();

        assertEquals( 1, fsr.objectFieldsList().size() );
        assertEquals( subject, objectFieldsList.get( 0 ).getPid() );

        assertEquals( String.format( "%s|%s", predicate1, object1 ),
            objectFieldsList.get( 0 ).relPredObjs().get( 0 ).getValue() );

        assertEquals( String.format( "%s|%s", predicate2, object2 ),
            objectFieldsList.get( 0 ).relPredObjs().get( 1 ).getValue() );
    }



    ////////////////////////////////////////////////////////////////////////////
    //
    // Below follows helper methods for the tests
    //

    private Expectations setExpectationsForReader( final String pid,
                                                   final DatastreamXMLMetadata dsxml,
                                                   final InputStream metadata ) throws ServerException
    {
        return new Expectations()
        {{
                reader.getCreateDate();returns( toDate );
                reader.getLastModDate(); returns( toDate );
                reader.GetObjectPID();returns( pid );
                reader.GetObjectState();returns( "I" );
                reader.GetObjectLabel(); returns( "" );
                reader.getOwnerId(); returns( anyString );
                reader.GetDatastream( anyString, (Date) any ); returns( (Datastream) dsxml );
                reader.getRelationships(); returns( new LinkedHashSet< RelationshipTuple >() );

                dsxml.getContentStream();returns( metadata );
        }};
    }

    private Expectations setExpectationsWithRelationshipsForReader( final String pid,
                                                   final DatastreamXMLMetadata dsxml,
                                                   final InputStream metadata,
                                                   final Set< RelationshipTuple > rels ) throws ServerException
    {
        return new Expectations()
        {{
                reader.getCreateDate();returns( toDate );
                reader.getLastModDate(); returns( toDate );
                reader.GetObjectPID();returns( pid );
                reader.GetObjectState();returns( "I" );
                reader.GetObjectLabel(); returns( "" );
                reader.getOwnerId(); returns( anyString );
                reader.GetDatastream( anyString, (Date) any ); returns( (Datastream) dsxml );
                reader.getRelationships(); returns( rels );

                dsxml.getContentStream();returns( metadata );
        }};
    }


    private Expectations setExpectationsWithDatesForReader( final String pid,
                                                            final DatastreamXMLMetadata dsxml,
                                                            final InputStream metadata,
                                                            final Date createDate,
                                                            final Date modifyDate ) throws ServerException
    {
        return new Expectations()
        {{
                reader.getCreateDate();returns( createDate );
                reader.getLastModDate(); returns( modifyDate );
                reader.GetObjectPID();returns( pid );
                reader.GetObjectState();returns( "I" );
                reader.GetObjectLabel(); returns( "" );
                reader.getOwnerId(); returns( anyString );
                reader.GetDatastream( anyString, (Date) any ); returns( (Datastream) dsxml );
                reader.getRelationships(); returns( new LinkedHashSet< RelationshipTuple >() );

                dsxml.getContentStream();returns( metadata );
        }};
    }

    private Expectations setNonStrictExpectationsForReader( final String pid,
                                                            final DatastreamXMLMetadata dsxml,
                                                            final InputStream metadata ) throws ServerException
    {
        return new NonStrictExpectations()
        {{
                reader.getCreateDate();returns( toDate );
                reader.getLastModDate(); returns( toDate );
                reader.GetObjectPID();returns( pid );
                reader.GetObjectState();returns( "I" );
                reader.GetObjectLabel(); returns( "" );
                reader.getOwnerId(); returns( anyString );
                reader.GetDatastream( anyString, (Date) any ); returns( (Datastream) dsxml );
                reader.getRelationships(); returns( new LinkedHashSet< RelationshipTuple >() );

                dsxml.getContentStream();returns( metadata );
        }};
    }

    private Expectations ingestObject( String pid, String[] titles, String source, String creator ) throws ServerException
    {
        DatastreamXMLMetadata dsxml = constructMockObject( pid, titles, source, creator );
        InputStream metadata = constructMockMetadata( pid, titles, source, creator );
        return setExpectationsForReader( pid, dsxml, metadata );
    }

    private Expectations ingestObject( String pid, String[] titles, String source, String creator, Date createDate, Date lastModDate ) throws ServerException
    {
        DatastreamXMLMetadata dsxml = constructMockObject( pid, titles, source, creator );
        InputStream metadata = constructMockMetadata( pid, titles, source, creator );
        return setExpectationsWithDatesForReader( pid, dsxml, metadata, createDate, lastModDate );
    }

    private Expectations ingestObject( String pid ) throws ServerException
    {
        DatastreamXMLMetadata dsxml = constructMockObject( pid, new String[]{ pid}, "", "" );
        InputStream metadata = constructMockMetadata( pid, new String[]{pid}, "", "" );
        return setNonStrictExpectationsForReader( pid, dsxml, metadata );

    }

    private Expectations ingestObjectWithRelationships( String pid, String[] titles, String source, String creator, Set< RelationshipTuple > rels ) throws ServerException
    {
        DatastreamXMLMetadata dsxml = constructMockObject( pid, titles, source, creator );
        InputStream metadata = constructMockMetadata( pid, titles, source, creator );
        return setExpectationsWithRelationshipsForReader( pid, dsxml, metadata, rels );
    }

    private DatastreamXMLMetadata constructMockObject( String pid, String[] titles, String source, String creator )
    {
        DatastreamXMLMetadata xmlds = new DatastreamXMLMetadata();
        String dublinCoreData = constructDublinCoreWithMultipleTitles( pid, titles, source, creator );
        xmlds.xmlContent = dublinCoreData.getBytes();

        return xmlds;
    }

    private InputStream constructMockMetadata( String pid, String[] titles, String source, String creator )
    {
        String dublinCoreData = constructDublinCoreWithMultipleTitles( pid, titles, source, creator );
        InputStream md = new ByteArrayInputStream( dublinCoreData.getBytes() );

        return md;
    }

    private String constructDublinCoreWithMultipleTitles( String pid, String[] titles, String source, String creator )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "<oai_dc:dc xmlns:oai_dc=\"http://www.openarchives.org/OAI/2.0/oai_dc/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\">" );
        for( String title : titles )
        {
            sb.append( String.format( "<dc:title>%s</dc:title>", title ) );
        }
        sb.append( String.format( "<dc:source>%s</dc:source>", source ) );
        sb.append( String.format( "<dc:creator>%s</dc:creator>", creator ) );
        sb.append( String.format( "<dc:identifier>%s</dc:identifier>", pid ) );
        sb.append( "</oai_dc:dc>" );
        return sb.toString();

    }

    private Map<String, String> getParameters()
    {
        final Map<String, String> params = new HashMap<String, String>();
        params.put( "writeLockTimeout", "1000" );
        params.put( "resultLifetime", "10" );
        //TODO: a separate tests should verify that invalid values cannot be
        // given as parameters
        params.put( "luceneDirectory", "SimpleFSDirectory" );
        params.put( "indexLocation", indexLocation );

//        params.put( "luceneDirectory", "RAMDirectory" );
        return params;
    }


    private FieldSearchQuery getFieldSearchQuery( Map<String, Pair<Operator, String> > searchTerms ) throws QueryParseException
    {
        List<Condition> conditions = new ArrayList<Condition>();
        for( Map.Entry<String, Pair<Operator, String>> terms: searchTerms.entrySet() )
        {
            conditions.add( new Condition( terms.getKey(), terms.getValue().getFirst(), terms.getValue().getSecond() ) );
        }

        FieldSearchQuery fsq = new FieldSearchQuery( conditions );
        return fsq;
    }

    private FieldSearchQuery getFieldSearchQuery( String field, Operator op, String searchTerm ) throws QueryParseException
    {
        List<Condition> conditions = new ArrayList<Condition>();
        conditions.add( new Condition( field, op, searchTerm ) );

        FieldSearchQuery fsq = new FieldSearchQuery( conditions );
        return fsq;
    }

}
