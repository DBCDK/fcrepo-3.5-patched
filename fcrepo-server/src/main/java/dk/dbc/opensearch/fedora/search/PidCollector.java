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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import org.apache.lucene.index.AtomicReaderContext;

/**
 * This class is an implementation of the
 * {@link org.apache.lucene.search.Collector} abstract class for providing
 * the entire collection of search result PIDs unsorted.
 */
public class PidCollector extends Collector
{
    private final static Logger log = LoggerFactory.getLogger( PidCollector.class );

    private IPidList pidList;
    private long pidsCollected = 0;
    private IndexReader currentReader = null;
    private final static String pidFieldName = "pid";
    private final int maxInMemory;
    private final File tmpDir;

    public PidCollector( int maxInMemory, File tmpDir )
    {
        this.maxInMemory = maxInMemory;
        this.tmpDir = tmpDir;
        pidList = new PidListInMemory();
    }

    @Override
    public void setScorer( final Scorer scorer ) throws IOException
    {
        /* We don't use this explicitly, but this method is called from the
           lucene framework and therefore we'll not throw an
           UnsupportedOperationException either. */
    }

    @Override
    public void collect( final int docId ) throws IOException
    {
        log.trace( "Collecting docId: {}", docId );

        Document doc = currentReader.document( docId );
        if( doc == null )
        {
            log.warn( "Failed to retrieve Document for id {}", docId );
        }
        else
        {
            IndexableField pidField = doc.getField( pidFieldName );
            if( pidField == null )
            {
                log.warn( "Unable to retrieve PID field '{}' from the index Document", pidFieldName );
            }
            else
            {
                String pidFieldValue = pidField.stringValue();
                if( pidFieldValue.isEmpty() )
                {
                    log.warn( "Empty value for PID from field '{}' will not be in result set", pidFieldName );
                }
                else
                {
                    if( pidsCollected == maxInMemory )
                    {
                        IPidList tmpPidList = new PidListInFile( File.createTempFile( "pids", ".bin", tmpDir ), pidList );
                        pidList.dispose();
                        pidList = tmpPidList;
                    }

                    log.debug( "Adding PID '{}' to result set", pidFieldValue );
                    pidList.addPid( pidFieldValue );
                    pidsCollected++;
                }
            }
        }
    }

    /**
     * Gets PIDs in result set as IPidList object
     *
     * @return result set PID list
     */
    public IPidList getResults() throws IOException
    {
        pidList.commit();
        return pidList;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException
    {
        currentReader = context.reader();
    }

    @Override
    public boolean acceptsDocsOutOfOrder()
    {
        return true;
    }
}
