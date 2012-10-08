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


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.fcrepo.server.search.Condition;
import org.fcrepo.server.search.FieldSearchQuery;
import org.fcrepo.server.search.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class ensures that the underlying lucene index can be manipulated in
 * a thread-safe manner. It also facilitates decoupling of index writing
 * operations from index reading operations. Additionally, the class will manage
 * optimizations of the underlying indices.
 */
public final class LuceneFieldIndex
{
    /**
     * Possible targets for performance improvements/better behaviour in a
     * multithreaded environment:
     *
     * 1. Using a RAMFSDirectory for fast intermediate storage
     * 2. Using an IndexWriterPool for handling multiple requesting threads, and at favourable times merging the indices.
     */
    private static final Logger log = LoggerFactory.getLogger( LuceneFieldIndex.class );
    private IndexWriter writer;

    private IndexReader reader = null;
    private IndexSearcher searcher = null;

    private final int pidCollectorMaxInMemory;
    private final File pidCollectorTmpDir;

    private final long writeLockTimeout;
    private final Directory directory;
    /** Searches on dates cannot precede Sat Jan 01 2000 00:00:00 GMT+0100 (CET). */
    private static final long earliest_date_searchable = 946681200L;
    /** Searches on dates cannot succeed Wed Jan 01 2050 00:00:00 GMT+0100 (CET). */
    private static final long latest_date_searchable = 2524604400000L;

    /** FieldSearch index optimizations aka. private FedoraFieldNames. */
    private final static String PID_NAMESPACE = FedoraFieldName.PID.toString() + "_namespace";
    private final static String PID_IDENTIFIER = FedoraFieldName.PID.toString() + "_identifier";

    private final static String DATE_RAW = "dateraw";
    private final static String DATE_RAW_EQ = "dateraw_eq";

    /**
     * Lucene, up until 2.9.1, does not have a way to specify a search starting
     * from the beginning of a field and ending at the end of a field. The
     * Lucene understanding of `exact match` is therefore limited to matching
     * substrings, where substrings can be the complete field value, but not
     * necessarily so.
     *
     * The LuceneFieldIndex adds Beginning Of Line (BOL) and End Of Line (EOL)
     * token markers as suggested by Karl Wettin (here:
     * http://www.gossamer-threads.com/lists/lucene/java-dev/75327)
     * and Andrzej Bialecki (here:
     * http://www.lucidimagination.com/search/document/3aa1e64d1a70e40b/phrase_search)
     */
    private final static char FIELDSTART = '^';
    private final static char FIELDEND = '$';


    public static interface IndexMonitorMBean
    {
        int getNumDocs() throws IOException;
        int getMaxDoc();

        // These methods have been hidden from interface, since they only add to the confusion
        // They are information about the writers buffered documents, not search cache information
        //int getNumRamDocs();
        //long getRamSizeInBytes();

        void forceMerge() throws IOException, IllegalArgumentException;
    }

    public class IndexMonitor implements IndexMonitorMBean
    {
        public void forceMerge() throws IOException, IllegalArgumentException
        {
            if( LuceneFieldIndex.this.canPerformOptimize() )
            {
                log.info( "Performing forced merge of segments" );
                LuceneFieldIndex.this.writer.forceMerge( 1 );
                log.info( "Forced merge of segments completed" );
            }
            else
            {
                throw new IllegalArgumentException( "Not enough space to perform optimization" );
            }
        }

        public int getNumDocs() throws IOException
        {
            return writer.numDocs();
        }

        public int getNumRamDocs()
        {
            return writer.numRamDocs();
        }

        public int getMaxDoc()
        {
            return writer.maxDoc();
        }

        public long getRamSizeInBytes()
        {
            return writer.ramSizeInBytes();
        }
    }

    public static interface LuceneFieldIndexMonitorMBean
    {
        long getDocumentsIndexed();
        long getDocumentsDeleted();
        long getSearchesPerformed();
        long getSearchersReused();
        long getNewSearchersCreated();

        long getLastSearchTimeMS();
        long getAverageSearchTimeMS();
        long getLastIndexTimeMS();
        long getAverageIndexTimeMS();

        void resetCounters();
    }

    public class LuceneFieldIndexMonitor implements LuceneFieldIndexMonitorMBean
    {
        @Override
        public long getDocumentsIndexed()
        {
            return documentsIndexed.get();
        }

        @Override
        public long getDocumentsDeleted()
        {
            return documentsDeleted.get();
        }

        @Override
        public long getSearchesPerformed()
        {
            return searchesPerformed.get();
        }

        @Override
        public long getSearchersReused()
        {
            return searchersReused.get();
        }

        @Override
        public long getNewSearchersCreated()
        {
            return newSearchersCreated.get();
        }

        @Override
        public long getLastSearchTimeMS()
        {
            return lastSearchTimeMS;
        }

        @Override
        public long getAverageSearchTimeMS()
        {
            long count = searchesPerformed.get();

            return (count == 0 ) ? 0 : totalSearchTimeMS.get() / count;
        }

        @Override
        public long getLastIndexTimeMS()
        {
            return lastIndexTimeMS;
        }

        @Override
        public long getAverageIndexTimeMS()
        {
            long count = documentsIndexed.get();

            return (count == 0 ) ? 0 : totalIndexTimeMS.get() / count;
        }

        @Override
        public void resetCounters()
        {
            documentsIndexed.set( 0 );
            documentsDeleted.set( 0 );
            searchesPerformed.set( 0 );
            lastSearchTimeMS = 0;
            searchersReused.set( 0 );
            newSearchersCreated.set( 0 );

        }
    }

    public static interface TieredMergePolicyMonitorMBean
    {
        public void setMaxMergeAtOnce( int v );
        public int getMaxMergeAtOnce();
        public void setMaxMergeAtOnceExplicit( int v );
        public int getMaxMergeAtOnceExplicit();
        public void setMaxMergedSegmentMB( double v );
        public double getMaxMergedSegmentMB();
        public void setReclaimDeletesWeight( double v );
        public double getReclaimDeletesWeight();
        public void setFloorSegmentMB( double v );
        public double getFloorSegmentMB();
        public void setForceMergeDeletesPctAllowed( double v );
        public double getForceMergeDeletesPctAllowed();
        public void setSegmentsPerTier( double v );
        public double getSegmentsPerTier();
        public void setUseCompoundFile( boolean useCompoundFile );
        public boolean getUseCompoundFile();
        public void setNoCFSRatio( double noCFSRatio );
        public double getNoCFSRatio();
    }


    /**
     * Provide MBean interface to TieredMergePolicy
     */
    public static class TieredMergePolicyMonitor implements TieredMergePolicyMonitorMBean
    {
        private final TieredMergePolicy mergePolicy;
        TieredMergePolicyMonitor( TieredMergePolicy mergePolicy )
        {
            this.mergePolicy = mergePolicy;

        }
        @Override
        public void setMaxMergeAtOnce( int v )
        {
            mergePolicy.setMaxMergeAtOnce( v );
        }
        @Override
        public int getMaxMergeAtOnce()
        {
            return mergePolicy.getMaxMergeAtOnce();
        }
        @Override
        public void setMaxMergeAtOnceExplicit( int v )
        {
            mergePolicy.setMaxMergeAtOnceExplicit( v );
        }
        @Override
        public int getMaxMergeAtOnceExplicit()
        {
            return mergePolicy.getMaxMergeAtOnceExplicit();
        }
        @Override
        public void setMaxMergedSegmentMB( double v )
        {
            mergePolicy.setMaxMergedSegmentMB( v );
        }
        @Override
        public double getMaxMergedSegmentMB()
        {
            return mergePolicy.getMaxMergedSegmentMB();
        }
        @Override
        public void setReclaimDeletesWeight( double v )
        {
            mergePolicy.setReclaimDeletesWeight( v );
        }
        @Override
        public double getReclaimDeletesWeight()
        {
            return mergePolicy.getReclaimDeletesWeight();
        }
        @Override
        public void setFloorSegmentMB( double v )
        {
            mergePolicy.setFloorSegmentMB( v );
        }
        @Override
        public double getFloorSegmentMB()
        {
            return mergePolicy.getFloorSegmentMB();
        }
        @Override
        public void setForceMergeDeletesPctAllowed( double v )
        {
            mergePolicy.setForceMergeDeletesPctAllowed( v );
        }
        @Override
        public double getForceMergeDeletesPctAllowed()
        {
            return mergePolicy.getForceMergeDeletesPctAllowed();
        }
        @Override
        public void setSegmentsPerTier( double v )
        {
            mergePolicy.setSegmentsPerTier( v );
        }
        @Override
        public double getSegmentsPerTier()
        {
            return mergePolicy.getSegmentsPerTier();
        }
        @Override
        public void setUseCompoundFile( boolean useCompoundFile )
        {
            mergePolicy.setUseCompoundFile( useCompoundFile );
        }
        @Override
        public boolean getUseCompoundFile()
        {
            return mergePolicy.getUseCompoundFile();
        }
        @Override
        public void setNoCFSRatio( double noCFSRatio )
        {
            mergePolicy.setNoCFSRatio( noCFSRatio );
        }
        @Override
        public double getNoCFSRatio()
        {
            return mergePolicy.getNoCFSRatio();
        }
    }

    private final AtomicLong documentsIndexed = new AtomicLong();
    private final AtomicLong documentsDeleted = new AtomicLong();
    private final AtomicLong newSearchersCreated = new AtomicLong();
    private final AtomicLong searchersReused = new AtomicLong();
    private final AtomicLong searchesPerformed = new AtomicLong();
    private final AtomicLong totalSearchTimeMS = new AtomicLong();
    private volatile long lastSearchTimeMS = 0;
    private final AtomicLong totalIndexTimeMS = new AtomicLong();
    private volatile long lastIndexTimeMS = 0;

    LuceneFieldIndex( final long indexWriterLockTimeout, final Analyzer analyzer, final Directory luceneDirectory, int pidCollectorMaxInMemory, File pidCollectorTmpDir ) throws IOException
    {
        this.writeLockTimeout = indexWriterLockTimeout;
        this.directory = luceneDirectory;
        this.pidCollectorMaxInMemory = pidCollectorMaxInMemory;
        this.pidCollectorTmpDir = pidCollectorTmpDir;
        TieredMergePolicy mergePolicy = new TieredMergePolicy();

        openWriter( analyzer, mergePolicy );

        // Register the JMX monitoring bean
        try
        {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();

            server.registerMBean( new IndexMonitor(), new ObjectName( "FieldSearchLucene:name=Index" ) );
            server.registerMBean( new LuceneFieldIndexMonitor(), new ObjectName( "FieldSearchLucene:name=LuceneFieldIndex" ) );
            server.registerMBean( new TieredMergePolicyMonitor( mergePolicy ), new ObjectName( "FieldSearchLucene:name=TieredMergePolicy" ) );
        }
        catch( JMException ex )
        {
            log.error( "Unable to register monitor. JMX Monitoring will be unavailable", ex);
        }
    }

    void openWriter( final Analyzer analyzer, final MergePolicy mergePolicy ) throws IllegalStateException, IOException
    {
        log.debug( "openWriter called" );

        if( null == this.directory || null == analyzer )
        {
            String error = "Cannot open an IndexWriter without a Directory or an Analyzer";
            throw new IllegalStateException( error );
        }
        if( null != this.writer )
        {
            try
            {
                closeIndex();
            } catch( IOException ex )
            {
                String error = String.format( "Index cannot be closed. Unable to open a new IndexWriter, %s", ex.getMessage() );
                log.error( error, ex );
                throw new IllegalStateException( error, ex );
            }
        }
        // KULMULE
        // OLD:
        // this.writer = new IndexWriter( this.directory, analyzer, IndexWriter.MaxFieldLength.LIMITED );
        // this.writer.setWriteLockTimeout( this.writeLockTimeout );
        // NEW:
        // note: I have not dedicated time to substitute IndexWriter.MaxFieldLength.LIMITED,
        //       even though it is specified how here:
        //       http://lucene.apache.org/core/old_versioned_docs/versions/3_5_0/api/core/index.html
        IndexWriterConfig conf = new IndexWriterConfig( Version.LUCENE_35, analyzer ).setWriteLockTimeout( this.writeLockTimeout ).
                setMergePolicy( mergePolicy );
        this.writer = new IndexWriter( this.directory, conf );
        // DONE
        this.reader = IndexReader.open( this.writer, false );
        this.searcher = new IndexSearcher( reader );
        newSearchersCreated.incrementAndGet();
    }


    void indexFields( final List<Pair<FedoraFieldName, String>> fieldList, long extractTimeNs ) throws IOException
    {
        log.debug( "Indexing {} fields", fieldList.size() );
        long startTimeNs = System.nanoTime();

        final Document doc = new Document();
	    String pid = "";

        for( Pair<FedoraFieldName, String> field : fieldList )
        {
            // fieldName is any value from FedoraFieldName.FedoraFieldName
            String fieldValue = field.getSecond();

            FedoraFieldName fieldName = field.getFirst();

            if( "".equals( fieldValue ) || null == fieldValue )
            {
                log.warn( "value for field {} is empty; will not be added to index", fieldName );
            }
            else
            {
                switch( fieldName )
                {
                case PID:
                    doc.add( new Field( fieldName.toString(), fieldValue, Store.YES, Index.NOT_ANALYZED ) );
                    log.trace( "Added { {}: {} } to index document", fieldName.toString(), fieldValue );
                    pid = fieldValue;

                    doc.add( new Field( PID_IDENTIFIER, fieldValue.split( ":" )[1], Store.NO, Index.NOT_ANALYZED ) );
                    doc.add( new Field( PID_NAMESPACE, fieldValue.split( ":" )[0], Store.NO, Index.NOT_ANALYZED ) );

                    doc.add( new Field( fieldName.equalsFieldName(), FIELDSTART + fieldValue + FIELDEND, Store.NO, Index.NOT_ANALYZED ) );

                    break;
                case DATE:
                    // DATE field in DC data is also stored in raw form so EQ an HAS searches can search
                    // against the raw string even, if it is not parsable as a timestamp (bug 13799)
                    String fieldLower = fieldValue.toLowerCase();
                    doc.add( new Field( DATE_RAW, fieldLower, Store.NO, Index.ANALYZED ));
                    doc.add( new Field( DATE_RAW_EQ, FIELDSTART + fieldLower + FIELDEND, Store.NO, Index.ANALYZED ));
                    // Fall through to next case, so it is also parsed as timestamp if possible:
                case CDATE:
                case MDATE:
                case DCMDATE:
                    long timestamp = 0L;
                    try
                    {
                        // since DateFormat is not threadsafe, and its only ise is here, we create an instance every time:
                        SimpleDateFormat zTimeFormatter = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSS Z" );
                        Date parsedDate = zTimeFormatter.parse( fieldValue );
                        timestamp = parsedDate.getTime();
                    } catch( java.text.ParseException ex )
                    {
                        log.warn( "'{}' is unparsable as a date, and will not be indexed: {}", fieldValue, ex.getMessage() );
                        break;
                    }

                    if( timestamp > 0 )
                    {
                        NumericField date = new NumericField( fieldName.toString(), Store.YES, true );
                        date.setLongValue( timestamp );
                        doc.add( date );
                        log.trace( "Added { {}: {} } to index document", fieldName.toString(), timestamp );
                    }

                    break;
                default:
                    fieldValue = fieldValue.toLowerCase();
                    doc.add( new Field( fieldName.toString(), fieldValue, Store.YES, Index.ANALYZED ) );
                    doc.add( new Field( fieldName.equalsFieldName(), FIELDSTART + fieldValue + FIELDEND, Store.NO, Index.NOT_ANALYZED ) );
                    log.trace( "Added { {}: {} } to index document", fieldName.toString(), fieldValue );
                }
            }
        }

        if( doc.getFields().isEmpty() )
        {
            log.info( "Skipping indexing of empty Document" );
        }
        else
        {
            long count = documentsIndexed.incrementAndGet();
            if( null == this.writer )
            {
                throw new IllegalStateException( "IndexWriter could not be retrieved." );
            }
            log.trace( "Adding document {}", doc );
            // this.writer.addDocument( doc );
	    Term term = new Term( "pid", pid );
	    this.writer.updateDocument( term, doc );
            log.trace( "Committing {} docs", this.writer.numRamDocs() );
            log.trace( "Documents in index: {} docs", this.writer.numDocs() );
            this.writer.commit();
            log.trace( "Done Committing." );

            long indexTimeNs = System.nanoTime() - startTimeNs ;
            long indexTimeMs = (indexTimeNs + extractTimeNs ) / 1000000;
            lastIndexTimeMS = indexTimeMs;
            totalIndexTimeMS.addAndGet( indexTimeMs  );

            if ( count % 1000 == 0)
            {
                // Log as microseconds
                log.info( String.format( "HANDLE Timing: indexFields(). Extracing data: %d µs, Indexing document: %d µs, Total %d µs.",
                        extractTimeNs / 1000, indexTimeNs / 1000, ( extractTimeNs + indexTimeNs ) / 1000 ) );
            }
        }
    }


    void removeDocument( final String uid ) throws IOException
    {
        documentsDeleted.incrementAndGet();

        log.trace( "Entering removeDocument" );
        //Term term = new Term( "pid", FIELDSTART + uid + FIELDEND );
        Term term = new Term( "pid", uid );

        if( null == this.writer )
        {
            throw new IllegalStateException( "IndexWriter could not be retrieved on index " );
        }

        log.debug( "Removing document referenced by {}", uid );
        log.trace( "Documents in index before delete: {}", this.writer.numDocs() );
        log.trace( "Deleting doc with term {}", term );
        this.writer.deleteDocuments( term );
        log.trace( "Documents in index after delete: {}", this.writer.numDocs() );
        log.trace( "Commiting {} docs", this.writer.numRamDocs() );

        this.writer.commit();
    }


    /**
     * This method takes a {@link FieldSearchQuery} consisting of {@link Pair}s
     * of {@link FedoraFieldName}s and {@link String}s and executes the
     * corresponding search against the lucene index
     *
     * For a Query consisting of
     * <pre>
     * List< Pair< "CREATOR", "Friedrich Nietzche" > >
     * </pre>
     *
     * the search will retrieve all pids matching that query
     *
     * @param fsq a FieldSearchQuery object containing the query
     * @return all PIDs in result set as IPidList object
     */
    IPidList search( final FieldSearchQuery fsq ) throws IOException, ParseException
    {
        long time = System.currentTimeMillis();

        Query luceneQuery = constructQuery( fsq );

        IPidList results = null;

        if( luceneQuery instanceof AllFieldsQuery )
        {
            log.info( "AllFieldsQuery detected, returning all documents from index" );
            results = getAll();
        }
        else
        {
            // Local variables to hold current value in case another thread changes the class members
            IndexReader localReader = null;
            IndexSearcher localSearcher;
            try
            {
                synchronized (this)
                {
                    IndexReader newReader = IndexReader.openIfChanged( reader, this.writer, false );
                    if ( newReader != null )
                    {
                        log.debug( "Reader has changed. Creating new reader and searcher.");
                        // Release reference to old reader. reader will automatically close when reference count reaches 0
                        reader.decRef();
                        reader = newReader;
                        searcher = new IndexSearcher( reader );
                        newSearchersCreated.incrementAndGet();
                    }
                    else
                    {
                        log.debug( "Reader is unchanged. Reusing searcher.");
                        searchersReused.incrementAndGet();
                    }
                    localReader = reader;
                    // Grab reference to reader so it is not closed while search is running
                    localReader.incRef();
                    localSearcher = searcher;
                }
                // DONE
                log.trace( "number of deleted documents in reader: {}", localReader.numDeletedDocs() );
                log.trace( "number of documents in reader: {}", localReader.numDocs() );
                final PidCollector pidCollector = new PidCollector( pidCollectorMaxInMemory, pidCollectorTmpDir );
                log.debug( "Query: {}", luceneQuery.toString() );
                localSearcher.search( luceneQuery, pidCollector );
                results = pidCollector.getResults();
            }
            finally
            {
                if( null != localReader )
                {
                    // Release reference. reader will automatically close when no-one holds a reference to them
                    localReader.decRef();
                }
            }
        }

        time = System.currentTimeMillis() - time;
        lastSearchTimeMS = time;
        totalSearchTimeMS.addAndGet( time );
        searchesPerformed.incrementAndGet();

        log.trace( "Size of result set: {}, time {} ms", results.size(), time );

        /*
        For this to be enabled, we need a method to (re)set cursor position on a
        pid list!!!

        if( log.isTraceEnabled() )
        {
            log.trace( "Size of result set: {}, time {} ms", results.size(), time );
            int i = 1;
            String pid = results.getNextPid();
            while( pid != null )
            {
                log.trace( "result no {} has PID {}", i++, pid );
                pid = results.getNextPid();
            }
        }
        */

        return results;
    }

    /**
     * For queries that are beforehand known to retrieve all (active) documents
     * from the index, this method can bypass the performance penalty of an
     * actual search, and simply return all documents from an IndexReader.
     * @return all PIDs in index as IPidList object
     * @throws IOException if IndexWriter or IndexReader throws an exception
     */
    IPidList getAll() throws IOException
    {
        IPidList results = null;

        IndexReader localReader;

        synchronized (this)
        {
            IndexReader newReader = IndexReader.openIfChanged( reader, this.writer, false );
            if ( newReader != null )
            {
                log.debug( "Reader has changed. Creating new reader and searcher.");
                // Release reference to old reader. reader will automatically close when reference couunt reaches 0
                reader.decRef();
                reader = newReader;
                searcher = new IndexSearcher( reader );
                newSearchersCreated.incrementAndGet();
            }
            else
            {
                log.debug( "Reader is unchanged. Reusing searcher.");
                searchersReused.incrementAndGet();
            }
            localReader = reader;
            // Grab reference to reader so it is not closed while search is running
            localReader.incRef();
        }

        try
        {
            int numDocs = localReader.numDocs();
            int numDelDocs = localReader.numDeletedDocs();
            log.debug( "getAll, reader has {} documents, {} deleted documents", numDocs, numDelDocs );
            PidCollector pidCollector = new PidCollector( pidCollectorMaxInMemory, pidCollectorTmpDir );
            pidCollector.setNextReader( localReader, 0 );
            for( int i = 0; i < numDocs+numDelDocs ; i++ )
            {
                if( localReader.isDeleted( i ) )
                {
                    // Skip deleted documents
                    log.trace( "Skipping deleted document {}", i );
                    continue;
                }

                log.trace( "Getting doc id {}", i );
                pidCollector.collect( i );
            }

            results = pidCollector.getResults();
        }
        finally
        {
            localReader.decRef();
        }

        return results;
    }

    /**
     * Tries to close all operations on the index and unlock the directory if
     * it is still locked. This method is non-reentrant and should only be used
     * on server shutdown.
     *
     * @throws IOException if any of the shutdown operations fails
     */
    void closeIndex() throws IOException
    {
        if( null != this.searcher )
        {
            this.searcher.close();
        }
        if( null != this.reader )
        {
            this.reader.close();
        }

        if( null != this.writer )
        {
            try
            {
                this.writer.close();
            } catch( AlreadyClosedException ex )
            {
                log.info( "While trying to close the IndexWriter, an AlreadyClosedException was thrown: {}", ex.getMessage() );
            }
        }
    }

    // KULMULE
    // It seems like all use of this method has been removed through
    // deprecation of optimize(). See above.
    // If this method is enabled again, please notice that it contains
    // a deprecated method (getFile)
    // OLD:
    /**
     * Only FSDirectory and subclasses can return true within the
     * bounds of this method.
     */
    private boolean canPerformOptimize()
    {
        if( !(this.directory instanceof FSDirectory) )
        {
            return false;
        }

        File f = null;
        long avail = 0L;
        long needed = 0L;
        f = ((FSDirectory) this.directory).getFile();
        long dirSize = 0L;
        for( File a : f.listFiles() )
        {
            dirSize += a.length();
        }
        avail = f.getUsableSpace();
        needed = dirSize * 2;
        if( needed > avail )
        {
            log.info( "Need {} bytes for optimization, only have {}", needed, avail );
            return false;
        }
        log.debug( "Need {} bytes for optimization, have {}", needed, avail );
        return true;
    }
    // NEW:
    // DONE

    private Query constructQuery( final FieldSearchQuery fsq ) throws ParseException
    {
        BooleanQuery booleanQuery = new BooleanQuery();
        if( fsq.getType() == FieldSearchQuery.CONDITIONS_TYPE && fsq.getConditions().isEmpty() )
        {
            return new AllFieldsQuery( "*" );
        }
        if( fsq.getType() == FieldSearchQuery.CONDITIONS_TYPE && !fsq.getConditions().isEmpty() )
        {
            log.trace( "Building map from conditions" );
            for( Condition cond : fsq.getConditions() )
            {
                String searchField = cond.getProperty().toUpperCase();
                Operator operator = cond.getOperator();
                String value = cond.getValue();

                log.info( "Raw condition: {}{}{}", new Object[] { searchField, operator.getSymbol(), value } );

                if( ! ( searchField.equals( FedoraFieldName.CDATE.name() )
                        || searchField.equals( FedoraFieldName.DATE.name() )
                        || searchField.equals( FedoraFieldName.DCMDATE.name() )
                        || searchField.equals( FedoraFieldName.MDATE.name() )
                        || searchField.equals( FedoraFieldName.PID.name() ) ) )
                {
                    log.trace( "Lowercasing {} ({})", value, searchField );
                    value = value.toLowerCase();
                }
                if( ( operator.equals( Operator.CONTAINS ) ) && ( isSpecialCaseQuery( searchField, value ) ) )
                {
                    return new AllFieldsQuery( "*" );
                }

                String debugQuery = String.format( "Building query: '%s %s %s'", searchField.toLowerCase(), operator, value );
                log.debug( debugQuery );
                try
                {
                    booleanQuery.add( buildQueryFromClause( searchField.toLowerCase(), operator, value ), Occur.MUST );
                } catch( IllegalArgumentException ex )
                {
                    log.warn( "Could not add query {}: {}", debugQuery, ex.getMessage() );
                }
            }
        }
        else if( fsq.getType() == FieldSearchQuery.TERMS_TYPE )
        {
            log.trace( "Building map from terms" );
            String value = fsq.getTerms();

            // See the javadoc for #buildQueryFromClause 2) b) and d) (and 3))
            if( isSpecialCaseQuery( "dummy_value", value ) )
            {
                return new AllFieldsQuery( "*" );
            }

            for( FedoraFieldName fieldName : FedoraFieldName.values() )
            {
                try
                {
                    booleanQuery.add( buildQueryFromClause( fieldName.toString(), Operator.CONTAINS, value ), Occur.SHOULD );
                }
                catch( IllegalArgumentException ex )
                {
                    log.warn( "Could not add query {}{}{}: {}", new Object[] { fieldName.toString(), "~", value, ex.getMessage() } );
                }
            }
            booleanQuery.setMinimumNumberShouldMatch( 1 );
        }

        return booleanQuery;
    }

    /**
     * Rules for interpreting a FieldSearchQuery (fsq):
     *
     * 1) If the fsq contains a list of conditions, they must be AND'ed in the search tree
     * 2) If the fsq contains a single term, all fields must be searched for containing the term text using an OR'ed search tree
     *   a) if the term contains a * coupled with a word, the word is searched using a wildcard (case insensitive). E.g. Paul* will match "Paul", "pauli" and "Paulus", but not "apauli"
     *   b) if the term contains a single *, the search is conducted as 3)
     *   c) if the term contains a ? coupled with a word, the ? sign will act as a placeholder for a letter. E.g. ?aul will match "Paul" and "Saul", but not "aul"
     *   d) if the term contains a single ?, the search will match all fields that contains a single letter, returning only the fields in the resultFields.
     *      Paradoxically this kind of search will also (as with the * search above) match the entire base, as the field 'status' always only contains a single letter
     *   e) if the term contains a single ? and the operator is CONTAINS, the search will match all fields, as in 3)
     * 3) If the fsq has no terms and no conditions, all fields in the `resultFields` array shall be returned.
     *
     * @todo: this javadoc could benefit from a link to the specification
     */
    private Query buildQueryFromClause( final String idxField, final Operator op, final String value ) throws ParseException
    {

        FedoraFieldName field =  FedoraFieldName.valueOf( idxField.toUpperCase() );

        if( field.equals( FedoraFieldName.PID ) )
        {
            log.info( "Constructing Term or wildCardQuery searching for {} in {}", value, idxField );
            if( value.contains( "*:" ) )
            {
                log.trace( "value '{}' matches .contains( \"*:\" )", value );
                String splitPid = value.split( ":" )[1];
                return new TermQuery( new Term( PID_IDENTIFIER, splitPid ) );
            }
            else if( value.contains( ":*" ) )
            {
                log.trace( "value '{}' matches .contains( \":*\" )", value );
                String splitPid = value.split( ":" )[0];
                return new TermQuery( new Term( PID_NAMESPACE, splitPid ) );
            }
            else if( value.contains( "*" ) )
            {
                log.trace( "value '{}' matches \"*\"", value );
                return new WildcardQuery( new Term( idxField.toLowerCase(), value ) );
            }
            else
            {
                Query pidQuery = new PhraseQuery();
                ( (PhraseQuery) pidQuery ).add( new Term( idxField.toLowerCase(), value ) );
                return pidQuery;
            }
        }
        else if( (! op.equals( Operator.CONTAINS ) ) && ( value.equals( "?" ) || (value.equals( "*" ) ) ) )
        {
            log.debug( "Constructing TermQuery( new Term( {}, {}) )", field.toString(), value );
            return new TermQuery( new Term( idxField, value ) );
        }
	// Search on op.EQUALS can contain * and ? but they should not be interpreted as wildcards:
        else if( ( value.contains( "*" ) || value.contains( "?" ) ) && op.equals( Operator.EQUALS ) )
	{
            log.info( "Constructing TermQuery( new Term( {}, {}) )", idxField, value );
	    return new TermQuery( new Term( idxField, value ) );
	}
        // 2) a) and c)
	else if( ( value.contains( "*" ) || value.contains( "?" ) ) && op.equals( Operator.CONTAINS ) )
        {
            log.info( "Constructing WildCardQuery( new Term( {}, {}) )", idxField, value );
            if( value.startsWith( "*" ) || value.startsWith( "?" ) )
            {
                // http://lucene.apache.org/java/2_9_1/api/all/org/apache/lucene/search/WildcardQuery.html
                log.warn( "In order to prevent extremely slow WildcardQueries, a Wildcard term should not start with one of the wildcards * or ?" );
            }
            if ( field.equals( FedoraFieldName.DATE ) )
            {
                // Wildcard searches for date is done in raw date strinf table
                return new WildcardQuery( new Term( DATE_RAW, value.toLowerCase() ) );
            }
            else
            {
                return new WildcardQuery( new Term( idxField, value.toLowerCase() ) );
            }
        }
        else if( ( op.equals( Operator.EQUALS ) && !field.isDateField() ) || ( value.indexOf( "\"" ) == 0 && value.lastIndexOf( "\"" ) == value.length() - 1 ) )
        {
            String eqField = field.equalsFieldName();
            String eqValue = FIELDSTART + value + FIELDEND;
            log.info( "Constructing TermQuery( new Term( {}, {} ) )", eqField, eqValue );
            return new TermQuery( new Term( eqField, eqValue ) );
        }
        else if( field.equals( FedoraFieldName.DATE ) && op.equals( Operator.EQUALS ))
        {
            String eqField = DATE_RAW_EQ;
            String eqValue = FIELDSTART + value + FIELDEND;
            log.info( "Constructing TermQuery( new Term( {}, {} ) )", eqField, eqValue );
            return new TermQuery( new Term( eqField, eqValue ) );
        }
        else if( field.equals( FedoraFieldName.DATE ) && op.equals( Operator.CONTAINS ))
        {
            // Should be handled by wildcard search
            String error = "Operator CONTAINS is not implemented for date field";
            log.error( error );
            throw new IllegalArgumentException( error );
        }
        else if( field.equals( FedoraFieldName.CDATE ) || field.equals( FedoraFieldName.DATE ) || field.equals( FedoraFieldName.DCMDATE ) || field.equals( FedoraFieldName.MDATE ) )
        {
            log.info( "Constructing Date query where {} {} {}", new Object[] { value, op.getSymbol(), field.toString() } );
            long timestamp = 0L;
            try
            {
                timestamp = parseStringAsTimestamp( value );
            }
            catch( java.text.ParseException ex )
            {
                String warning = String.format( "Will not use %s in query for %s: %s", value, field.toString(), ex.getMessage() );
                log.warn( warning );
                throw new IllegalArgumentException( warning, ex );
            }

            log.debug( "{} interpreted as {}", value, timestamp );

            Query dateQuery = null;

            if( op.equals( Operator.GREATER_OR_EQUAL ) )
            {
                log.trace( "Query from >= : {}-{}", timestamp, latest_date_searchable );
                dateQuery = NumericRangeQuery.newLongRange( idxField, timestamp, latest_date_searchable, true, true );
            }
            else if( op.equals( Operator.GREATER_THAN ) )
            {
                log.trace( "Query from > : {}-{}", timestamp, latest_date_searchable );
                dateQuery = NumericRangeQuery.newLongRange( idxField, timestamp, latest_date_searchable, false, true );
            }
            else if( op.equals( Operator.LESS_OR_EQUAL ) )
            {
                log.trace( "Query from <= : {}-{}", earliest_date_searchable, timestamp );
                dateQuery = NumericRangeQuery.newLongRange( idxField, earliest_date_searchable, timestamp, true, true );
            }
            else if( op.equals( Operator.LESS_THAN ) )
            {
                log.trace( "Query from < : {}-{}", earliest_date_searchable, timestamp );
                dateQuery = NumericRangeQuery.newLongRange( idxField, earliest_date_searchable, timestamp, true, false );
            }
            else if( op.equals( Operator.EQUALS ) )
            {
                log.trace( "Query from = : {}-{}", timestamp, timestamp );
                dateQuery = NumericRangeQuery.newLongRange( idxField, timestamp, timestamp, true, true );
            }
            else if( op.equals( Operator.CONTAINS ) )
            {
                String error = "Operator CONTAINS cannot be used with searches on date fields";
                log.error( error );
                throw new IllegalArgumentException( error );
            }

            return dateQuery;
        }
        else
        {
            log.info( "Constructing default query searching for {} in {}", value, field );
            return getDefaultQuery( field.toString(), value );
        }
    }


    private Query getDefaultQuery( final String field, final String queryString ) throws ParseException
    {
        log.debug( "Query string '{}' will be tokenized in a PhraseQuery", queryString );
        PhraseQuery phraseQuery = new PhraseQuery();
        phraseQuery.setSlop( 0 );
        String[] split = queryString.split( "\\s" );
        for( String queryTerm : split )
        {
            phraseQuery.add( new Term( field, queryTerm.toLowerCase() ) );
        }

        return phraseQuery;
    }

    /**
     * Attempt to parse the given string of form: yyyy-MM-dd[THH:mm:ss[.SSS][Z]]
     * as a epoch timestamp. No timezone conversions are performed
     *
     * @param dateString the date string to parse
     * @return a Date representation of the dateString
     * @throws ParseException if dateString is null, empty or is otherwise
     * unable to be parsed.
     */
    private static long parseStringAsTimestamp( String dateString ) throws java.text.ParseException
    {
        if( dateString == null )
        {
            String error = "datestring is null and cannot be parsed as a long or a date, skipping parsing";
            log.error( error );
            throw new IllegalArgumentException( error );
        }
        else if( dateString.isEmpty() )
        {
            String error = String.format( "%s cannot be parsed as a long or a date, skipping parsing", dateString );
            log.error( error );
            throw new IllegalArgumentException( error );

        }
        else if( dateString.endsWith( "." ) )
        {
            String error = String.format( "%s cannot be parsed as a long or a date, skipping parsing", dateString );
            log.error( error );
            throw new IllegalArgumentException( error );
        }
        SimpleDateFormat formatter = new SimpleDateFormat();
        formatter.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
        int length = dateString.length();
        if( dateString.startsWith( "-" ) )
        {
            length--;
        }

        log.debug( "\"{}\".length() == {}", dateString, length );

        if( dateString.endsWith( "Z" ) )
        {

            if( length == 11 )
            {
                formatter.applyPattern( "yyyy-MM-dd'Z'" );
            }
            else if( length == 20 )
            {
                formatter.applyPattern( "yyyy-MM-dd'T'HH:mm:ss'Z'" );
            }
            else if( length > 21 && length < 24 )
            {
                // right-pad the milliseconds with 0s up to three places
                StringBuilder sb = new StringBuilder( dateString.substring( 0, dateString.length() - 1 ) );
                int dotIndex = sb.lastIndexOf( "." );
                int endIndex = sb.length() - 1;
                int padding = 3 - ( endIndex - dotIndex );
                for( int i = 0; i < padding; i++ )
                {
                    sb.append( "0" );
                }
                sb.append( "Z" );
                dateString = sb.toString();
                formatter.applyPattern( "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" );
            }
            else if( length == 24 )
            {
                formatter.applyPattern( "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" );
            }
        }
        else
        {
            if( length == 10 )
            {
                formatter.applyPattern( "yyyy-MM-dd" );
            }
            else if( length == 19 )
            {
                formatter.applyPattern( "yyyy-MM-dd'T'HH:mm:ss" );
            }
            else if( length > 20 && length < 23 )
            {
                // right-pad millis with 0s
                StringBuilder sb = new StringBuilder( dateString );
                int dotIndex = sb.lastIndexOf( "." );
                int endIndex = sb.length() - 1;
                int padding = 3 - ( endIndex - dotIndex );
                for( int i = 0; i < padding; i++ )
                {
                    sb.append( "0" );
                }
                dateString = sb.toString();
                formatter.applyPattern( "yyyy-MM-dd'T'HH:mm:ss.SSS" );
            }
            else if( length == 23 )
            {
                formatter.applyPattern( "yyyy-MM-dd'T'HH:mm:ss.SSS" );
            }
            else if( dateString.endsWith( "GMT" ) || dateString.endsWith( "UTC" ) )
            {
                formatter.applyPattern( "EEE, dd MMMM yyyyy HH:mm:ss z" );
            }
        }
        return formatter.parse( dateString ).getTime();
    }


    private boolean isSpecialCaseQuery( final String idxField, final String value )
    {
        if( null == idxField
            || idxField.trim().isEmpty()
            || null == value
            || value.trim().isEmpty()
            || value.trim().equals( "*" )
            || value.trim().equals( "?" ) )
        {
            return true;
        }

        return false;
    }

    /**
     * Query class that just retrieves all fields in the index.
     */
    private static class AllFieldsQuery extends Query
    {
        private final String term;
        public AllFieldsQuery( final String specialTerm )
        {
            this.term = specialTerm;
        }

        @Override
        public String toString( final String field )
        {
            return String.format( "AllFieldsQuery<%s>", this.term );
        }
    }
}
