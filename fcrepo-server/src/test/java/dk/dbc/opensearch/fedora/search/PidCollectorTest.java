/*
 * This file is part of opensearch.
 * Copyright (c) 2012, Dansk Bibliotekscenter a/s,
 * Tempovej 7-11, DK-2750 Ballerup, Denmark. CVR: 15149043
 *
 * opensearch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * opensearch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with opensearch. If not, see <http://www.gnu.org/licenses/>.
 */

package dk.dbc.opensearch.fedora.search;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * PidCollector unit tests
 * <p>
 * The test methods of this class uses the following naming convention:
 *
 *  unitOfWork_stateUnderTest_expectedBehavior
 */
public class PidCollectorTest
{
    private final static String PID_FIELD_NAME = "pid";
    private final static String PID_FIELD_VALUE_1 = "pid_1";
    private Directory index;

    @Before
    public void setUp() throws IOException
    {
        index = new RAMDirectory();
    }

    @After
    public void tearDown() throws IOException
    {
        index.close();
    }

    @Test
    public void constructor_whenCalled_returnsNewInstanceWithEmptyPidList() throws IOException
    {
        IndexReader reader = populateIndexAndGetIndexReader();
        PidCollector instance = new PidCollector();
        instance.setNextReader( reader, 0 );
        assertEquals( 0, instance.getResults().size() );
    }

    @Test
    public void acceptsDocsOutOfOrder_whenCalled_returnsTrue() throws IOException
    {
        PidCollector instance = new PidCollector();
        assertTrue( instance.acceptsDocsOutOfOrder() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void collect_docIdArgExceedsMaxDoc_throwsIllegalArgumentException() throws IOException
    {
        IndexReader reader = populateIndexAndGetIndexReader();
        PidCollector instance = new PidCollector();
        instance.setNextReader( reader, 0 );
        instance.collect( 42 );
    }

    @Test
    public void collect_docIdArgExistsInIndexWithPidField_pidIsAddedToPidList() throws IOException
    {
        IndexReader reader = populateIndexAndGetIndexReader( newIndexDocument( PID_FIELD_NAME, PID_FIELD_VALUE_1 ) );
        PidCollector instance = new PidCollector();
        instance.setNextReader( reader, 0 );

        int maxDoc = reader.maxDoc();
        for( int i = 0; i < maxDoc; i++ )
        {
            instance.collect( i );
        }

        IPidList pidList = instance.getResults();
        assertEquals( 1, pidList.size() );
        assertEquals( PID_FIELD_VALUE_1, pidList.getNextPid() );
    }

    @Test
    public void collect_docIdArgExistsInIndexWithEmptyPidField_nothingIsAddedToPidList() throws IOException
    {
        IndexReader reader = populateIndexAndGetIndexReader( newIndexDocument( PID_FIELD_NAME, "" ) );
        PidCollector instance = new PidCollector();
        instance.setNextReader( reader, 0 );

        int maxDoc = reader.maxDoc();
        for( int i = 0; i < maxDoc; i++ )
        {
            instance.collect( i );
        }

        IPidList pidList = instance.getResults();
        assertEquals( 0, pidList.size() );
    }

    @Test
    public void collect_docIdArgExistsInIndexWithoutPidField_nothingIsAddedToPidList() throws IOException
    {
        IndexReader reader = populateIndexAndGetIndexReader( newIndexDocument( "not_pid", PID_FIELD_VALUE_1 ) );
        PidCollector instance = new PidCollector();
        instance.setNextReader( reader, 0 );

        int maxDoc = reader.maxDoc();
        for( int i = 0; i < maxDoc; i++ )
        {
            instance.collect( i );
        }

        IPidList pidList = instance.getResults();
        assertEquals( 0, pidList.size() );
    }

    private IndexReader populateIndexAndGetIndexReader( Document... docs ) throws IOException
    {
        IndexWriterConfig config = new IndexWriterConfig( Version.LUCENE_35, new SimpleAnalyzer( Version.LUCENE_35 ) );
        IndexWriter indexWriter = new IndexWriter( index, config );
        for( Document doc : docs )
        {
            indexWriter.addDocument( doc );
        }
        indexWriter.commit();
        indexWriter.close();
        return IndexReader.open( index );
    }

    private Document newIndexDocument( String fieldName, String fieldValue )
    {
        Document doc = new Document();
        doc.add( new Field( fieldName, fieldValue, Field.Store.YES, Field.Index.ANALYZED ) );
        return doc;
    }
}
