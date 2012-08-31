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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

    private File pidFile;

    private final static Charset ENCODING = Charset.forName( "UTF-8" );

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

        OutputStream stream = new BufferedOutputStream ( new FileOutputStream ( pidFile ) );

        try
        {
            String pid;
            while ( ( pid = pids.getNextPid()) != null )
            {
                appendPid( stream, pid );
            }
        }
        finally
        {
            stream.close();
        }
    }


    @Override
    public void addPid( String pid ) throws IOException
    {
        OutputStream stream = new BufferedOutputStream ( new FileOutputStream ( pidFile, true) );

        try
        {
            appendPid( stream, pid );
        }
        finally
        {
            stream.close();
        }
    }


    private void appendPid( OutputStream stream, String pid ) throws IOException
    {
        log.trace( "Appending PID '{}'", pid );
        byte[] bytes = pid.getBytes( ENCODING );
        // Write length field
        stream.write( bytes.length );
        // Write bytes
        stream.write( bytes );
        size++;
        log.debug( "Appended PID '{}'. Size is now", pid, size );
    }


    private String readPid( RandomAccessFile file ) throws IOException
    {
        String pid = null;

        // Read next length field
        int length = file.read();
        readOffset++;

        log.debug( "Length field: {} read from '{}'", length, pidFile );

        if ( length != -1 )
        {
            log.trace( "Reading {} bytes from '{}'", length, pidFile);
            byte[] pidData = new byte[length];
            int read = file.read( pidData );
            readOffset+= read;
            if ( read != length )
            {
                String error = String.format( "Requested %d bytes, got %d, from file '%s'", length, read, pidFile );
                log.error( error );
                throw new IOException( error );
            }
            pid = new String( pidData, ENCODING );
        }
        log.debug( "Next PID is '{}'. Offset is now", pid, readOffset );
        return pid;
    }

    @Override
    public String getNextPid() throws IOException
    {
        if ( !pidFile.exists() )
        {
            log.debug( "PID list has been exhausted");
            return null;
        }

        log.trace( "Getting next PID from offset {}", readOffset);

        RandomAccessFile file = new RandomAccessFile( pidFile, "r" );
        String pid;
        try
        {
            log.trace( "Skipping {} bytes", readOffset);
            file.seek( readOffset );

            pid = readPid( file );
            if ( pid == null )
            {
                // End of file
                log.debug( "Exhausted pid list in file '{}'", pidFile);
                file.close();
                if ( !pidFile.delete() )
                {
                    log.warn( "File '{}' could not be deleted", pidFile );
                }
            }
        }
        finally
        {
            file.close();
        }
        return pid;
    }

    @Override
    public Collection< String > getNextPids( int wanted ) throws IOException
    {
        log.trace( "Getting next {} PIDs from offset {}", wanted, readOffset);

        if ( !pidFile.exists() )
        {
            log.debug( "PID list has been exhausted");
            return Collections.emptyList();
        }

        ArrayList< String > pids = new ArrayList< String >( wanted );

        RandomAccessFile file = new RandomAccessFile( pidFile, "r" );
        try
        {
            log.trace( "Skipping {} bytes", readOffset);
            file.seek( readOffset );

            String pid;
            do
            {
                pid = readPid( file );
                if ( pid != null )
                {
                    pids.add( pid ) ;
                }
            }
            while ( pid != null && pids.size() < wanted );
        }
        finally
        {
            file.close();
        }
        if ( pids.size() < wanted )
        {
            // End of file
            log.debug( "Exhausted pid list in file '{}'", pidFile);
            if ( !pidFile.delete() )
            {
                log.warn( "File '{}' could not be deleted", pidFile );
            }
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
