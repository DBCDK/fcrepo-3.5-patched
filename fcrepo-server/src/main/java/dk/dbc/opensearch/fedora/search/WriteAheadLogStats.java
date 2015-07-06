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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteAheadLogStats implements WriteAheadLogStatsMBean
{
    private static final Logger log = LoggerFactory.getLogger( WriteAheadLogStats.class );

    protected final AtomicInteger numberOfUpdatedDocuments = new AtomicInteger();

    protected final AtomicInteger numberOfUncomittedDocuments = new AtomicInteger();

    protected final AtomicInteger numberOfCommits = new AtomicInteger();
    
    protected final AtomicInteger numberOfObtainTLogFiles = new AtomicInteger();

    protected final AtomicLong totalUpdateTimeMicroS = new AtomicLong();

    protected final AtomicLong totalWriteToFileTimeMicroS = new AtomicLong();

    protected final AtomicLong totalUpdateInLuceneTimeMicroS = new AtomicLong();

    protected final AtomicLong totalCommitToLuceneTimeMicroS = new AtomicLong();
    
    protected final AtomicLong totalObtainTLogFilesTimeMicroS = new AtomicLong();

    protected final int commitSize;


    public WriteAheadLogStats( int commitSize )
    {
        if ( commitSize < 1 )
        {
            String error = String.format( "Parameter commitSize must be positive: %d", commitSize );
            log.error( error );
            throw new IllegalArgumentException( error );
        }
        this.commitSize = commitSize;
    }


    @Override
    public long getAverageCommitToLuceneTimeMicroS()
    {
        int commits = getNumberOfCommits();
        return commits == 0 ? 0 : totalCommitToLuceneTimeMicroS.get() / commits;
    }


    @Override
    public long getAverageUpdateInLuceneTimeMicroS()
    {
        int updatedDocs = getNumberOfUpdatedDocuments();
        return updatedDocs == 0 ? 0 : totalUpdateInLuceneTimeMicroS.get() / updatedDocs;
    }


    @Override
    public long getAverageUpdateTimeMicroS()
    {
        int updatedDocs = getNumberOfUpdatedDocuments();
        return updatedDocs == 0 ? 0 : totalUpdateTimeMicroS.get() / updatedDocs;
    }


    @Override
    public long getAverageWriteToFileTimeMicroS()
    {
        int updatedDocs = getNumberOfUpdatedDocuments();
        return updatedDocs == 0 ? 0 : totalWriteToFileTimeMicroS.get() / updatedDocs;
    }
    
    @Override
    public long getAverageObtainTLogFilesTimeMicroS() {
        int obtainLogFiles = getNumberOfObtainTLogFiles();
        return obtainLogFiles == 0 ? 0 : totalObtainTLogFilesTimeMicroS.get() / obtainLogFiles;
    }

    @Override
    public int getCommitSize()
    {
        return commitSize;
    }


    @Override
    public int getNumberOfCommits()
    {
        return numberOfCommits.get();
    }


    @Override
    public int getNumberOfUncomittedDocuments()
    {
        return numberOfUncomittedDocuments.get();
    }


    @Override
    public int getNumberOfUpdatedDocuments()
    {
        return numberOfUpdatedDocuments.get();
    }
    
    @Override
    public int getNumberOfObtainTLogFiles() {
        return numberOfObtainTLogFiles.get();
    }


    @Override
    public long getTotalCommitToLuceneTimeMicroS()
    {
        return totalCommitToLuceneTimeMicroS.get();
    }


    @Override
    public long getTotalUpdateInLuceneTimeMicroS()
    {
        return totalUpdateInLuceneTimeMicroS.get();
    }


    @Override
    public long getTotalUpdateTimeMicroS()
    {
        return totalUpdateTimeMicroS.get();
    }


    @Override
    public long getTotalWriteToFileTimeMicroS()
    {
        return totalWriteToFileTimeMicroS.get();
    }

    @Override
    public long getTotalObtainTLogFilesTimeMicroS() {
        return totalObtainTLogFilesTimeMicroS.get();
    }

    @Override
    public int getTLogSize() {
        return 0;
    }

}
