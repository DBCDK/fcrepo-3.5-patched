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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class PidListInFile implements IPidList
{
    private static final Logger log = LoggerFactory.getLogger( PidListInFile.class );

    private long readOffset = 0;

    private int size = 0;

    private final File pidFile;

    public PidListInFile( File pidFile ) throws IOException
    {
        log.debug( "Creating PID list with storage in file '{}'", pidFile);
        if ( pidFile.exists() )
        {
            String error = String.format( "File '%s' already exists. Will not be overwritten", pidFile );
            log.error( error );
            throw new IOException( error );
        }

        this.pidFile = pidFile;
    }

    public PidListInFile( File pidFile, IPidList pids ) throws IOException
    {
        this( pidFile );
        initializePidFile( pidFile, pids );
    }

    final void initializePidFile( File pidFile, IPidList pids ) throws IOException
    {
        log.debug( "Copying {} PIDs from source list", pids.size());

        Writer bufferedWriter = new BufferedWriter ( new FileWriter ( pidFile ) );

        try
        {
            String pid;
            while ( ( pid = pids.getNextPid()) != null )
            {
                appendPid( bufferedWriter, pid );
            }
        }
        finally
        {
            bufferedWriter.close();
        }
    }


    @Override
    public void addPid( String pid ) throws IOException
    {
        Writer bufferedWriter = new BufferedWriter ( new FileWriter ( pidFile, true ) );

        try
        {
            appendPid( bufferedWriter, pid );
        }
        finally
        {
            bufferedWriter.close();
        }
    }


    private void appendPid( Writer bufferedWriter, String pid ) throws IOException
    {
        log.trace( "Appending PID '{}'", pid );
        if ( size > 0 )
        {
            bufferedWriter.append( ',' );
        }
        bufferedWriter.append( pid );
        size++;
        log.debug( "Appended PID '{}'. Size is now", pid, size );
    }


    private String readPid( Reader reader ) throws IOException
    {
        String pid = null;

        if ( reader.ready() )
        {
            int character;
            StringBuilder buffer = new StringBuilder();

            // Stop reading when encountering a separator character or EOF
            while ( (character = reader.read() ) != ',' && character != -1)
            {
                readOffset++;
                buffer.append( (char) character );
            }
            // Skip ,
            readOffset++;
            pid = buffer.toString();
        }
        log.debug( "Next PID is '{}'. Offset is now", pid, readOffset );

        return pid;
    }

    @Override
    public String getNextPid() throws IOException
    {
        log.trace( "Getting next PID from offset {}", readOffset);
        Reader reader = new BufferedReader ( new FileReader ( pidFile ) );
        try
        {
            log.trace( "Skipping {} characters", readOffset);
            reader.skip( readOffset );

            String pid = readPid( reader );
            if ( pid == null )
            {
                // End of file
                log.debug( "Exhausted pid list in file '{}'", pidFile);
                reader.close();
                if ( !pidFile.delete() )
                {
                    log.warn( "File '{}' could not be deleted", pidFile );
                }
            }
            return pid;
        }
        finally
        {
            reader.close();
        }
    }


    public Collection< String > getNextPids( int wanted ) throws IOException
    {
        log.trace( "Getting next {} PIDs from offset {}", wanted, readOffset);

        ArrayList< String > pids = new ArrayList< String >();

        Reader reader = new BufferedReader ( new FileReader ( pidFile ) );
        try
        {
            log.trace( "Skipping {} characters", readOffset);
            reader.skip( readOffset );

            String pid;
            do
            {
                pid = readPid( reader );
                if ( pid != null )
                {
                    pids.add( pid ) ;
                }
            }
            while ( pid != null && pids.size() < wanted );

        }
        finally
        {
            reader.close();
        }
        log.debug( "Returning {} pids", pids.size() );
        return pids;
    }


    @Override
    public long getCursor()
    {
        log.trace( "Getting cursor {}", readOffset );
        return readOffset;
    }


    @Override
    public int size()
    {
        log.trace( "Getting size {}", size );
        return size;
    }

}
