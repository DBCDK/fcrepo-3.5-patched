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


import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.fcrepo.server.errors.InvalidOperatorException;
import org.fcrepo.server.errors.QueryParseException;
import org.fcrepo.server.search.Condition;
import org.fcrepo.server.search.FieldSearchQuery;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author stm
 */
public class LuceneFieldIndexTest {

    /**
     * Test data setup
     */
    private static final Pair<FedoraFieldName, String> pid = new Pair<FedoraFieldName, String>( FedoraFieldName.PID, "demo:1" );
    private static final Pair<FedoraFieldName, String> pid2 = new Pair<FedoraFieldName, String>( FedoraFieldName.PID, "Demo:1" );
    private static final Pair<FedoraFieldName, String> state = new Pair<FedoraFieldName, String >( FedoraFieldName.STATE, "a" );
    private static final Pair<FedoraFieldName, String> title = new Pair<FedoraFieldName, String >( FedoraFieldName.TITLE, "demo object title" );
    private static final Pair<FedoraFieldName, String> type = new Pair<FedoraFieldName, String >( FedoraFieldName.TYPE, "demo object type" );
    private static final Pair<FedoraFieldName, String> label = new Pair<FedoraFieldName, String >( FedoraFieldName.LABEL, "demo object label" );
    private static final Pair<FedoraFieldName, String> relObj = new Pair<FedoraFieldName, String>( FedoraFieldName.RELOBJ, "work:1" );
    private static final Pair<FedoraFieldName, String> relPredObj = new Pair<FedoraFieldName, String>( FedoraFieldName.RELPREDOBJ, "http://oss.dbc.dk/rdf/dkbib#isMemberOfWork|work:1" );

    private static final long now = System.currentTimeMillis();
    private static final DateFormat dateFormatter = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSS Z" );

    private final static int PID_COLLECTOR_MAX_IN_MEMORY = Integer.MAX_VALUE;
    private final static File PID_COLLECTOR_TMP_DIR = null;

    private final Pair<FedoraFieldName, String> date;

    private static FSDirectory fsdir;
    private static String indexLocation = "build/test-index";
    private static LuceneFieldIndex instance;

    public LuceneFieldIndexTest() throws ParseException
    {
        String dateNow = dateFormatter.format( new Date( now ) );
        date = new Pair<FedoraFieldName, String >( FedoraFieldName.DATE, dateNow );
    }

    @Before
    public void setup() throws Exception
    {
        fsdir = FSDirectory.open( new File( indexLocation ) );
        TieredMergePolicy tieredMergePolicy = new TieredMergePolicy();
        IndexWriterConfig conf = new IndexWriterConfig( Version.LUCENE_41, new WhitespaceAnalyzer( Version.LUCENE_41 ) ).
                setWriteLockTimeout( 1000L ).
                setMergePolicy( tieredMergePolicy );
        IndexWriter writer = new IndexWriter( fsdir, conf );

        instance = new LuceneFieldIndex( writer, tieredMergePolicy,
                PID_COLLECTOR_MAX_IN_MEMORY, PID_COLLECTOR_TMP_DIR, null );
    }

    @After
    public void teardown() throws Exception
    {
        instance.closeIndex();
        //clean up directory
        for( File f : new File( indexLocation ).listFiles() )
        {
            f.delete();
        }
    }

    @AfterClass
    public static void closeTestSuite() throws IOException
    {
    }

    /**
     * Test of indexFields method, of class LuceneFieldIndex.
     */
    @Test
    public void testIndexFields() throws Exception
    {
        instance.indexFields( constructIndexFields( pid ), 0 );

        //verify that the index contains the correct values
        FieldSearchQuery fsq = getFieldSearchQuery( "PID", "eq", pid.getSecond() );

        IPidList searchResult = instance.search( fsq );

        assertFalse( "Search result must not be empty", searchResult.size() == 0 );
        assertEquals( "Search result must contain exactly 1 hits", 1, searchResult.size() );
        String[] nextPidArray = searchResult.getNextPids(1).toArray(new String[0]);
        assertEquals( pid.getSecond(), nextPidArray[0] );
    }


    /**
     * Build a test that tries to add a non-existing field.
     */
    @Test
    public void testAddNonAllowedFieldAreIgnoredInIndexing() throws Exception
    {
        List<List<Pair<FedoraFieldName, String>>> fieldList = new ArrayList<List<Pair<FedoraFieldName, String>>>();

        List<Pair<FedoraFieldName, String>> wrapper = new ArrayList<Pair<FedoraFieldName, String>>();
        Pair<FedoraFieldName, String> nondate  = new Pair<FedoraFieldName, String >( FedoraFieldName.DATE, dateFormatter.format( new Date( now ) ) );
        wrapper.add( nondate );
        fieldList.add( wrapper );
        instance.indexFields( wrapper, 0 );

        FieldSearchQuery fsq = getFieldSearchQuery( "PID", "eq", pid.getSecond() );

        IPidList searchResult = instance.search( fsq );

        assertEquals( 0, searchResult.size() );
    }


    /**
     * Test that the order for removal of documents actually gets through to the IndexWriter
     */
    @Test
    public void testRemoveDocument() throws Exception
    {
        instance.indexFields( constructIndexFields( pid ), 0 );
        FieldSearchQuery fsq = getFieldSearchQuery( "PID", "eq", pid.getSecond() );

        IPidList searchResult = instance.search( fsq );

        assertEquals( 1, searchResult.size() );

        String uid = "demo:1";
        instance.removeDocument( uid );
        //it is possible to remove documents that does not exist
        instance.removeDocument( uid );

        searchResult = instance.search( fsq );

        assertEquals( 0, searchResult.size() );
    }


    /**
     * happy path test of the search method
     */

    @Test
    public void testSearch() throws Exception
    {
        instance.indexFields( constructIndexFields( pid ), 0 );

        //verify that the index contains the correct values
        FieldSearchQuery fsq = getFieldSearchQuery( "title", "eq", title.getSecond() );

        IPidList searchResult = instance.search( fsq );

        String[] nextPidArray = searchResult.getNextPids(1).toArray(new String[0]);
        assertEquals( 1, nextPidArray.length );
        assertEquals( "demo:1", nextPidArray[0] );
    }

    @Test
    public void findHighestId() throws Exception
    {
        instance.indexFields( constructIndexFields( new Pair<FedoraFieldName, String>( FedoraFieldName.PID, "demo:a" ) ), 0 );
        instance.indexFields( constructIndexFields( new Pair<FedoraFieldName, String>( FedoraFieldName.PID, "demo:2" ) ), 0 );
        instance.indexFields( constructIndexFields( new Pair<FedoraFieldName, String>( FedoraFieldName.PID, "demo:6" ) ), 0 );
        instance.indexFields( constructIndexFields( new Pair<FedoraFieldName, String>( FedoraFieldName.PID, "demo:4" ) ), 0 );
        instance.indexFields( constructIndexFields( new Pair<FedoraFieldName, String>( FedoraFieldName.PID, "demo:B" ) ), 0 );

        int id = instance.findHighestId("demo");
        assertEquals( 6, id );
    }


    @Test
    public void findHighestId_whenIndexHasNoNumericalIdentifier() throws Exception
    {
        instance.indexFields( constructIndexFields( new Pair<FedoraFieldName, String>( FedoraFieldName.PID, "demo:a" ) ), 0 );

        int id = instance.findHighestId("demo");
        assertEquals( 0, id );
    }

    @Test
    public void findHighestId_whenIndexHasNoMatchingNamespace() throws Exception
    {
        instance.indexFields( constructIndexFields( new Pair<FedoraFieldName, String>( FedoraFieldName.PID, "demo:3" ) ), 0 );

        int id = instance.findHighestId("other");
        assertEquals( 0, id );
    }

    @Test
    public void testAddIdenticalFieldsTwiceYieldOneSearchResult() throws Exception
    {
        instance.indexFields( constructIndexFields( pid ), 0 );
        instance.indexFields( constructIndexFields( pid ), 0 );

        FieldSearchQuery fsq = getFieldSearchQuery( "title", "eq", title.getSecond() );

        IPidList searchResult = instance.search( fsq );

        assertEquals( 1, searchResult.size() );
    }

    @Test
    public void testAddTwoFieldSets() throws Exception
    {
        instance.indexFields( constructIndexFields( pid ), 0 );
        instance.indexFields( constructIndexFields( pid2 ), 0 );

        FieldSearchQuery fsq = getFieldSearchQuery( "title", "eq", title.getSecond() );

        IPidList searchResult = instance.search( fsq );

        assertEquals( 2, searchResult.size() );
    }

    @Test
    public void testUpdatedIndex() throws Exception
    {
        instance.indexFields( constructIndexFields( pid ), 0 );

        FieldSearchQuery fsq = getFieldSearchQuery( "title", "eq", title.getSecond() );

        IPidList searchResult = instance.search( fsq );

        assertEquals( 1, searchResult.size() );

        // Add a new and verify that search results change

        instance.indexFields( constructIndexFields( pid2 ), 0 );

        fsq = getFieldSearchQuery( "title", "eq", title.getSecond() );

        searchResult = instance.search( fsq );

        assertEquals( 2, searchResult.size() );
    }

    @Test
    public void testRelPredFields() throws Exception
    {
        instance.indexFields( constructIndexFields( pid ), 0 );
        instance.indexFields( constructIndexFields( pid2 ), 0 );

        //verify that the index contains the correct values
        FieldSearchQuery fsq = getFieldSearchQuery( "RELOBJ", "eq", relObj.getSecond() );

        IPidList searchResult = instance.search( fsq );

        assertFalse( "Search result must not be empty", searchResult.size() == 0 );
        assertEquals( "Search result must contain exactly 2 hits", 2, searchResult.size() );

        /*
        Pair<FedoraFieldName, String> resultDate = new Pair<FedoraFieldName, String> (FedoraFieldName.DATE, String.valueOf( now ));

        for( List<Pair<FedoraFieldName, String>> resultBundles : searchResult )
        {
            assertTrue( resultBundles.contains( pid ) || resultBundles.contains( pid2 ) );
            assertTrue( resultBundles.contains( state ) );
            assertTrue( resultBundles.contains( title ) );
            assertTrue( resultBundles.contains( type ) );
            assertTrue( resultBundles.contains( label ) );
            assertTrue( resultBundles.contains( resultDate ) );
            assertTrue( resultBundles.contains( relObj ) );
            assertTrue( resultBundles.contains( relPredObj ) );
        }
        */
    }

    @Test
    public void testSearchDateEq() throws Exception
    {
        instance.indexFields( constructIndexFields( pid ), 0 );

        List<Pair<FedoraFieldName, String>> fields2 = constructIndexFields( pid2 );
        fields2.add( new Pair<FedoraFieldName, String >( FedoraFieldName.DATE, "1930" ));
        instance.indexFields( fields2, 0 );

        FieldSearchQuery fsq = getFieldSearchQuery( "date", "eq", "1930" );

        IPidList searchResult = instance.search( fsq );

        assertEquals( 1, searchResult.size() );
        String[] nextPidArray = searchResult.getNextPids(1).toArray(new String[0]);
        String pidFromResult = nextPidArray[0];
        assertFalse( pidFromResult.contains( pid.getSecond() ) );
        assertTrue( pidFromResult.contains( pid2.getSecond() ) );
    }

    @Test
    public void testSearchDateHas() throws Exception
    {
        instance.indexFields( constructIndexFields( pid ), 0 );

        List<Pair<FedoraFieldName, String>> fields2 = constructIndexFields( pid2 );
        fields2.add( new Pair<FedoraFieldName, String >( FedoraFieldName.DATE, "987654" ));
        instance.indexFields( fields2, 0 );

        FieldSearchQuery fsq = getFieldSearchQuery( "date", "has", "?8765?" );

        IPidList searchResult = instance.search( fsq );

        assertEquals( 1, searchResult.size() );
        String[] nextPidArray = searchResult.getNextPids(1).toArray(new String[0]);
        String pidFromResult = nextPidArray[0];
        assertFalse( pidFromResult.contains( pid.getSecond() ) );
        assertTrue( pidFromResult.contains( pid2.getSecond() ) );
    }


    /**
     * If data contains a value to be indexed which is cased in some way and the
     * search condition does not match the case exactly, the search will not hit
     */
    @Test
    public void testIndexingOfPidsAreNotNormalized() throws Exception
    {
        instance.indexFields( constructCaseSensitiveIndexFields(), 0 );

        FieldSearchQuery fsq = getFieldSearchQuery( "pid", "eq", pid.getSecond() );
        IPidList searchResult = instance.search( fsq );

        assertEquals( 0, searchResult.size() );

        fsq = getFieldSearchQuery( "pid", "eq", pid2.getSecond() );
        searchResult = instance.search( fsq );

        assertEquals( 1, searchResult.size() );


    }

    @Test
    public void testCaseSensibilityOfResultFields() throws Exception
    {
        instance.indexFields( constructIndexFields( pid ), 0 );

        FieldSearchQuery fsq = getFieldSearchQuery( "title", "eq", title.getSecond() );

        IPidList searchResult = instance.search( fsq );

        assertEquals( 1, searchResult.size() );
    }

    @Test
    public void testSearchForValueWithSingleQuoteConsition() throws Exception
    {
        List< Pair< FedoraFieldName, String >> fieldList = new ArrayList< Pair< FedoraFieldName, String >>();

        fieldList.add( pid );

        fieldList.add( new Pair<FedoraFieldName, String >( FedoraFieldName.TITLE, "Hello'World" ) );

        instance.indexFields( fieldList, 0 );

        FieldSearchQuery fsq = getFieldSearchQuery( "title='Hello\\'World'" );

        IPidList searchResult = instance.search( fsq );

        assertEquals( 1, searchResult.size() );

        assertEquals( pid.getSecond(), searchResult.getNextPids( 1 ).iterator().next() );
    }

    @Test
    public void testSearchForValueWithSingleQuoteTerm() throws Exception
    {
        List< Pair< FedoraFieldName, String >> fieldList = new ArrayList< Pair< FedoraFieldName, String >>();

        fieldList.add( pid );

        fieldList.add( new Pair<FedoraFieldName, String >( FedoraFieldName.TITLE, "Hello'World" ) );

        instance.indexFields( fieldList, 0 );

        FieldSearchQuery fsq = getFieldSearchTerm( "Hello'World" );

        IPidList searchResult = instance.search( fsq );

        assertEquals( 1, searchResult.size() );

        assertEquals( pid.getSecond(), searchResult.getNextPids( 1 ).iterator().next() );
    }

    /**
     * Below follows helper methods
     */

    private List< Pair< FedoraFieldName, String > > constructIndexFields(Pair<FedoraFieldName, String> pid)
    {
        List< Pair< FedoraFieldName, String >> fieldList = new ArrayList< Pair< FedoraFieldName, String >>();

        fieldList.add( pid );
        fieldList.add( state );
        fieldList.add( title );
        fieldList.add( label );
        fieldList.add( type );
        fieldList.add( date );
        fieldList.add( relObj );
        fieldList.add( relPredObj );

        return fieldList;
    }


    private List< Pair< FedoraFieldName, String > > constructCaseSensitiveIndexFields()
    {
        List< Pair< FedoraFieldName, String >> fieldList = new ArrayList< Pair< FedoraFieldName, String >>();

        fieldList.add( pid2 );
        fieldList.add( title );
        fieldList.add( date );

        return fieldList;
    }


    private FieldSearchQuery getFieldSearchQuery( String field, String operator, String value ) throws InvalidOperatorException, QueryParseException
    {
        List<Condition> conditions = new ArrayList<Condition>();
        Condition cond = new Condition( field, operator, value );

        conditions.add( cond );

        FieldSearchQuery query = new FieldSearchQuery( conditions );

        return query;
    }
    private FieldSearchQuery getFieldSearchQuery( String queryStr ) throws InvalidOperatorException, QueryParseException
    {
        List<Condition> conditions = Condition.getConditions( queryStr );

        FieldSearchQuery query = new FieldSearchQuery( conditions );

        return query;
    }
    private FieldSearchQuery getFieldSearchTerm( String terms ) throws InvalidOperatorException, QueryParseException
    {
        FieldSearchQuery query = new FieldSearchQuery( terms );

        return query;
    }
}
