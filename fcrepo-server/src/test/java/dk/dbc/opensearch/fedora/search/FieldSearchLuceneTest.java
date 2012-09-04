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

import dk.dbc.opensearch.fedora.storage.MockDigitalObject;
import dk.dbc.opensearch.fedora.storage.MockRepositoryReader;

import org.fcrepo.server.Module;
import org.fcrepo.server.Parameterized;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.InvalidStateException;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.errors.ModuleShutdownException;
import org.fcrepo.server.errors.QueryParseException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.search.Condition;
import org.fcrepo.server.search.FieldSearchQuery;
import org.fcrepo.server.search.FieldSearchResult;
import org.fcrepo.server.storage.DOManager;
import org.fcrepo.server.storage.DOReader;
import org.fcrepo.server.storage.DefaultDOManager;
import org.fcrepo.server.storage.RepositoryReader;
import org.fcrepo.server.storage.types.DatastreamXMLMetadata;
import org.fcrepo.server.storage.types.DigitalObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockClass;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import static mockit.Mockit.setUpMocks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 *
 * @author stm
 */

public class FieldSearchLuceneTest {

    private FieldSearchLucene fieldsearch;
    private static final long timeNow = System.currentTimeMillis();
    private static final String now = Long.toString( timeNow );
    private final int maxResults = 10;

    private MockRepositoryReader repo = new MockRepositoryReader();
    private DOReader reader;

    @Mocked Server server;
    @Mocked Parameterized parm;
    @Mocked Module mod;
    @Mocked DOManager doma;

    @Before
    public void setUp() throws ModuleInitializationException, ServerException {


        final Map<String, String> params = new HashMap<String,String>();
        params.put( "writeLockTimeout", "1000");
        params.put( "resultLifetime", "10" );
        params.put( "luceneDirectory", "RAMDirectory" );
        params.put( "defaultAnalyzer", "SimpleAnalyzer" );

        final DOManager domanager = new DefaultDOManager( params, server, "DOManager" );

        new NonStrictExpectations( )
        {{
                parm.getParameter( "writeLockTimeout" ); returns( "1000" );
                parm.getParameter( "luceneDirectory" ); returns( "RAMDirectory" );
                parm.getParameter( "defaultAnalyzer"); returns( "SimpleAnalyzer" );
                parm.getParameter( "resultLifetime" ); returns( "10" );
                mod.getServer(); returns( server );
                server.getModule( "org.fcrepo.server.storage.DOManager" ); returns( domanager );
        }};

        fieldsearch = new FieldSearchLucene( params, server, "");
        fieldsearch.postInitModule();
        DigitalObject digo = new MockDigitalObject();
        digo.setPid( "demo:1" );
        DatastreamXMLMetadata dc = new DatastreamXMLMetadata();
        dc.xmlContent = "<?xml version=\"1.0\"?><dc>test</dc>".getBytes();
        dc.DSVersionID = "DC.0";
        dc.DatastreamID = "DC";
        dc.DSCreateDT = new Date( System.currentTimeMillis());

        digo.addDatastreamVersion( dc, true);
        this.repo.putObject( digo );
        reader = this.repo.getReader( true, null, "demo:1" );

        setUpMocks( MockFieldSearchResult.class );
    }

    @After
    public void tearDown() throws ModuleShutdownException
    {
       fieldsearch.shutdownModule();
       fieldsearch = null;
    }


    /**
     * Test of update method, of class FieldSearchLucene.
     */
    @Test
    @SuppressWarnings( "unchecked" )
    public void testUpdate( ) throws Exception
    {
        new Expectations(){
            @Mocked LuceneFieldIndex luceneindexer;
            {
                luceneindexer.indexFields( (List< Pair< FedoraFieldName, String > >) withNotNull(), anyLong );times=1;
            }
        };
        fieldsearch.update( reader );
    }


    /**
     * Test of findObjects method, of class FieldSearchLucene.
     */
    @Test
    public void testFindObjects() throws Exception
    {
        final String[] fields = new String[]{ "pid" };
        List<Condition> conds = new ArrayList<Condition>();
        conds.add( new Condition( "title", "eq", "demo"));
        FieldSearchQuery fsq = new FieldSearchQuery( conds );
        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );
    }

    @Test public void testQueryWithColonIsEscaped() throws Exception
    {
        final String[] fields = new String[]{ "pid" };
        List<Condition> conds = new ArrayList<Condition>();
        conds.add( new Condition( "title", "eq", "demo:1"));
        FieldSearchQuery fsq = new FieldSearchQuery( conds );

        FieldSearchResult fsr = fieldsearch.findObjects( fields, maxResults, fsq );
    }

        /**
     * This test tests a condition, that should never happen in a 'normal' context;
     * one of the requested resultfields does not map to an enum, an incorrect
     * state that would normally be detected by the FieldSearchLucene class.
     *
     * This test reflects that the internal Collector cannot contruct an
     * enum given an incorrect string. The Collector will drop such fields,
     * yielding only a warning. The net result can be found in the search
     * results, where non existing fields will result in empty search results.
     */
    @Test
    public void testSearchWithUnknownResultFieldFails() throws Exception
    {

        String[] resultFields = new String[]{ "no_field_named_this" };
        List<Condition> conds = new ArrayList<Condition>();
        conds.add( new Condition( "title", "eq", "demo:1" ));
        FieldSearchQuery fsq = new FieldSearchQuery( conds );

        FieldSearchResult fsr = fieldsearch.findObjects( resultFields, maxResults, fsq );
    }


    @Test( expected=QueryParseException.class )
    public void testConditionsMayNotContainApostrophes() throws Exception
    {
        List<Condition> conds = new ArrayList<Condition>();
        conds.add( new Condition( "title", "eq", "de'mo"));
    }


    @MockClass( realClass=FieldSearchResultLucene.class)
    public static class MockFieldSearchResult {

        @Mock
        public void $init( LuceneFieldIndex indexSearcher,
                                       RepositoryReader repoReader,
                                       String[] resultFields,
                                       FieldSearchQuery query,
                                       int maxResults,
                                       int timeout) throws InvalidStateException
        {
        }

        @Mock
        public String getToken()
        {
            return "token";
        }

        @Mock
        void dispose()
        {
        }
    }
}
