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

package dk.dbc.opensearch.fedora.search;

import dk.dbc.opensearch.fedora.search.WriteAheadLog.DocumentData;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author thp
 */
public class WriteAheadLogTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private IndexWriter writer;

    public WriteAheadLogTest()
    {
    }


    @BeforeClass
    public static void setUpClass() throws Exception
    {
    }


    @AfterClass
    public static void tearDownClass() throws Exception
    {
    }

    @Before
    public void setUp() throws IOException
    {
        TieredMergePolicy tieredMergePolicy = new TieredMergePolicy();
        IndexWriterConfig conf = new IndexWriterConfig( Version.LUCENE_35, new SimpleAnalyzer( Version.LUCENE_35 ) ).
                setWriteLockTimeout( 1000L ).
                setMergePolicy( tieredMergePolicy );

        writer = new IndexWriter( new RAMDirectory(), conf );
    }

    @After
    public void tearDown()
    {
    }


    private Document makeLuceneDocument( String pid, Pair<String, String>... fields )
    {
        Document doc = new Document();
        doc.add( new StringField( "pid", pid, Field.Store.YES ) );
        for( Pair<String, String> pair : fields )
        {
            doc.add( new TextField( pair.getFirst(), pair.getSecond(), Field.Store.YES ) );
        }
        doc.add( new LongField( "longValue", 123456, Field.Store.YES ) );

        return doc;
    }


    @Test
    @Ignore
    public void testSearchInUncomittedWriterReturnsMultipleDocumentsIfNotAskedToApplyDeletes() throws Exception
    {
        // This is a piece of test code that shows that duplicate search results
        // can be returned if the reader is not configured to flush updates.
        // It is not part of normal test.
        boolean applyAllDeletes = false;

        String pid = "obj:1";
        Document doc = makeLuceneDocument( pid );
        Term pidTerm = new Term( "pid", pid );
        writer.updateDocument( pidTerm, doc );
        writer.commit();
        writer.updateDocument( pidTerm, doc );

        IndexReader reader = IndexReader.open( writer, applyAllDeletes );
        IndexSearcher searcher = new IndexSearcher( reader );

        TopDocs result = searcher.search( new TermQuery( pidTerm ), 100 );

        System.out.println( "Found " + result.scoreDocs.length );

        for ( ScoreDoc sdoc : result.scoreDocs )
        {
            Bits liveDocs = MultiFields.getLiveDocs( reader );
            boolean isDeleted =  ( liveDocs != null && !liveDocs.get( sdoc.doc ) );

            System.out.println( sdoc );
            System.out.println( isDeleted );
            Document document = reader.document( sdoc.doc );
            System.out.println( document );
        }

        assertEquals( 1, result.totalHits );
    }

    @Test
    public void testDocumentDataClassWithDocument() throws Exception
    {
        String pid = "obj:1";
        Document doc = makeLuceneDocument( pid );

        WriteAheadLog.DocumentData docData = new WriteAheadLog.DocumentData( pid, doc );

        assertEquals( pid, docData.pid );
        assertEquals( doc.toString(), docData.docOrNull.toString() );
    }

    @Test
    public void testDocumentDataClassWithNullDocument() throws Exception
    {
        String pid = "obj:1";

        WriteAheadLog.DocumentData docData = new WriteAheadLog.DocumentData( pid, null );

        assertEquals( pid, docData.pid );
        assertNull( docData.docOrNull );
    }


    @Test
    public void testReadWriteDocumentDataWithDocument() throws Exception
    {
        File objectFile = new File( folder.getRoot(), "writeaheadlog.log");
        RandomAccessFile fileAccess = new RandomAccessFile( objectFile, "rwd" );

        String pid = "obj:1";
        Document doc = makeLuceneDocument( pid );

        WriteAheadLog.writeDocumentData( fileAccess, pid, doc );
        fileAccess.seek( 0 );
        DocumentData docData1 = WriteAheadLog.readDocumentData( fileAccess );
        assertEquals( pid, docData1.pid );
        assertEquals( doc.toString(), docData1.docOrNull.toString() );
    }


    @Test ( expected = IOException.class )
    public void testUpdateDocumentOnUnitializedWriteAheadLogThrowsException() throws Exception
    {
        WriteAheadLog wal = new WriteAheadLog( writer, folder.getRoot(), 1000, true );

        String pid = "obj:1";
        Document doc = makeLuceneDocument( pid );

        wal.updateDocument( pid, doc );
    }


    @Test ( expected = IOException.class )
    public void testUpdateDocumentOnClosedWriteAheadLogThrowsException() throws Exception
    {
        WriteAheadLog wal = new WriteAheadLog( writer, folder.getRoot(), 1000, true );
        wal.initialize();
        wal.shutdown();

        String pid = "obj:1";
        Document doc = makeLuceneDocument( pid );

        wal.updateDocument( pid, doc );
    }


    @Test
    public void testReadWriteDocumentDataWithNullDocument() throws Exception
    {
        File objectFile = new File( folder.getRoot(), "writeaheadlog.log");
        RandomAccessFile fileAccess = new RandomAccessFile( objectFile, "rwd" );

        String pid = "obj:1";

        WriteAheadLog.writeDocumentData( fileAccess, pid, null );
        fileAccess.seek( 0 );
        DocumentData docData = WriteAheadLog.readDocumentData( fileAccess );
        assertEquals( pid, docData.pid );
        assertNull( docData.docOrNull );
    }


    @Test
    public void testUpdateTwoDocumentsAndDeleteOneOfThem() throws Exception
    {
        WriteAheadLog wal = new WriteAheadLog( writer, folder.getRoot(), 1000, true );
        wal.initialize();

        String pid1 = "obj:1";
        Document doc1 = makeLuceneDocument( pid1 );

        wal.updateDocument( pid1, doc1 );
        assertEquals( 1, writer.numDocs() );

        wal.deleteDocument( pid1 );
        assertEquals( 1, writer.numDocs() );

        String pid2 = "obj:2";
        Document doc2 = makeLuceneDocument( pid2);
        wal.updateDocument( pid2, doc2 );
        assertEquals( 2, writer.numDocs() );

        // Verify data written to log file:

        File objectFile = new File( folder.getRoot(), "writeaheadlog.log");

        assertTrue( objectFile + " exists", objectFile.exists() );

        RandomAccessFile fileAccess = new RandomAccessFile( objectFile, "r" );
        DocumentData docData1 = WriteAheadLog.readDocumentData( fileAccess );

        assertEquals( pid1, docData1.pid );
        assertEquals( doc1.toString(), docData1.docOrNull.toString() );

        DocumentData docData1d = WriteAheadLog.readDocumentData( fileAccess );
        assertEquals( pid1, docData1d.pid );
        assertNull( docData1d.docOrNull );

        DocumentData docData2 = WriteAheadLog.readDocumentData( fileAccess );
        assertEquals( pid2, docData2.pid );
        assertEquals( doc2.toString(), docData2.docOrNull.toString() );

        wal.shutdown();
        assertFalse( "Log files is deleted at shutdown", objectFile.exists() );

        assertEquals( 1, writer.numDocs() );
    }


    @Test
    public void testRecoverUncomittedLogFile() throws Exception
    {
        File objectFile = new File( folder.getRoot(), "writeaheadlog.log");
        RandomAccessFile fileAccess = new RandomAccessFile( objectFile, "rwd" );

        String pid1 = "obj:1";
        Document doc1 = makeLuceneDocument( pid1 );

        String pid2 = "obj:2";
        Document doc2 = makeLuceneDocument( pid2 );

        // Given a writer with one document

        writer.updateDocument( WriteAheadLog.getPidTerm( pid1 ), doc1 );
        writer.commit();

        // And a log file with that document deleted and a new document added

        WriteAheadLog.writeDocumentData( fileAccess, pid1, null );
        WriteAheadLog.writeDocumentData( fileAccess, pid2, doc2 );

        fileAccess.close();
        // Recover the log file
        int recovered = WriteAheadLog.recoverUncomittedFile( objectFile, writer );

        // Verify that
        assertEquals( 2, recovered );

        IndexReader reader = IndexReader.open( writer, false );
        IndexSearcher searcher = new IndexSearcher( reader );

        TopDocs result = searcher.search( new TermQuery( WriteAheadLog.getPidTerm( pid1 ) ), 100 );

        assertEquals( 0, result.scoreDocs.length );

        result = searcher.search( new TermQuery( WriteAheadLog.getPidTerm( pid2 ) ), 100 );

        assertEquals( 1, result.scoreDocs.length );
    }

    @Test
    public void testInitializeRecoversUncomittedFiles() throws Exception
    {
        File comitting = new File( folder.getRoot(), "writeaheadlog.committing");
        File writeaheadlog = new File( folder.getRoot(), "writeaheadlog.log");
        RandomAccessFile comittingRaf = new RandomAccessFile( comitting, "rwd" );
        RandomAccessFile writeaheadlogRaf = new RandomAccessFile( writeaheadlog, "rwd" );

        String pid1 = "obj:1";
        Document doc1 = makeLuceneDocument( pid1 );

        String pid2 = "obj:2";
        Document doc2a = makeLuceneDocument( pid2, new Pair<String,String> ( "field","value1" ) );
        Document doc2b = makeLuceneDocument( pid2, new Pair<String,String> ( "field","value2" )  );

        String pid3 = "obj:3";
        Document doc3 = makeLuceneDocument( pid3 );

        // Given a writer with one document

        writer.updateDocument( WriteAheadLog.getPidTerm( pid1 ), doc1 );
        writer.commit();

        // And a comitting file with that document deleted and a new document added

        WriteAheadLog.writeDocumentData( comittingRaf, pid1, null );
        WriteAheadLog.writeDocumentData( comittingRaf, pid2, doc2a );

        // And a log file with one new document and one updated document

        WriteAheadLog.writeDocumentData( writeaheadlogRaf, pid2, doc2b );
        WriteAheadLog.writeDocumentData( writeaheadlogRaf, pid3, doc3 );

        comittingRaf.close();
        writeaheadlogRaf.close();

        // Initialize the WAL to recover the lost files

        WriteAheadLog wal = new WriteAheadLog( writer, folder.getRoot(), 1000, true );
        int recovered = wal.initialize();
        assertEquals( 4, recovered );

        // Verify that

        IndexReader reader = DirectoryReader.open( writer, false );
        IndexSearcher searcher = new IndexSearcher( reader );

        TopDocs result = searcher.search( new TermQuery( WriteAheadLog.getPidTerm( pid1 ) ), 100 );
        assertEquals( 0, result.scoreDocs.length );
        System.out.println( "" );

        result = searcher.search( new TermQuery( WriteAheadLog.getPidTerm( pid2 ) ), 100 );
        assertEquals( 1, result.scoreDocs.length );
        Document doc2 = reader.document( result.scoreDocs[0].doc );

        Iterator<IndexableField> it1 = doc2b.iterator();
        Iterator<IndexableField> it2 = doc2.iterator();
        do
        {
            IndexableField expected = it1.next();
            IndexableField actual = it2.next();
            assertEquals( expected.fieldType().stored(), actual.fieldType().stored() );
            if (! (expected instanceof LongField) )
            {
                assertEquals( expected.fieldType().indexed(), actual.fieldType().indexed() );
                assertEquals( expected.fieldType().omitNorms(), actual.fieldType().omitNorms() );
                assertEquals( expected.fieldType().indexOptions(), actual.fieldType().indexOptions() );
            }
            assertEquals( expected.name(), actual.name() );
            assertEquals( expected.stringValue(), actual.stringValue() );
            assertEquals( expected.numericValue(), actual.numericValue() );
        }
        while (it1.hasNext() && it2.hasNext());

//        assertEquals( doc2b.toString(), doc2.toString() );

        result = searcher.search( new TermQuery( WriteAheadLog.getPidTerm( pid3 ) ), 100 );
        assertEquals( 1, result.scoreDocs.length );

    }


}