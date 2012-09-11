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
 * PID list backed by a file.
 * File stores binary data with a one byte length field and then the PID encoded ad UTF-8
 * Binary file is used for faster seek time, since the file is not kept open between PID
 * retrievals
 */
public class PidListInFile implements IPidList
{
    private static final Logger log = LoggerFactory.getLogger( PidListInFile.class );

    private long readOffset = 0;

    private int size = 0;

    private int cursor = 0;

    private File pidFile;

    private final static Charset ENCODING = Charset.forName( "UTF-8" );

    private OutputStream outputStream = null;

    public PidListInFile( File pidFile )
    {
        log.debug( "Creating PID list with storage in file '{}'", pidFile);
        if ( pidFile == null )
        {
            throw new NullPointerException( "pidFile parameter must not be null" );
        }

        this.pidFile = pidFile;
    }

    /**
     * Copy constructor. Copy pids from an existing list
     * @param pidFile The file to store the PIDs in.
     * @param pids Source list to copy PIDs from
     * @throws IOException
     */
    public PidListInFile( File pidFile, IPidList pids ) throws IOException
    {
        this( pidFile );
        initializePidFile( pidFile, pids );
    }


    /**
     * Copy PIDs from another PID list
     * @param pidFile File to store PIDs in
     * @param pids Source PID list
     * @throws IOException
     */
    private void initializePidFile( File pidFile, IPidList pids ) throws IOException
    {
        log.debug( "Copying {} PIDs from source list", pids.size());

        outputStream = new BufferedOutputStream ( new FileOutputStream ( pidFile ) );

        Collection<String> pidCollection = pids.getNextPids(pids.size());
        for( String pid : pidCollection )
        {
            appendPid( outputStream, pid );
        }
    }


    @Override
    public void addPid( String pid ) throws IOException
    {
        if ( outputStream == null )
        {
            outputStream = new BufferedOutputStream ( new FileOutputStream ( pidFile, true) );
        }

        appendPid( outputStream, pid );
    }

    /**
     * Append a single PID to an already open stream
     * @param stream Stream to append to
     * @param pid PID to append
     * @throws IOException
     */
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


    /**
     * Read a PID from an already open file
     * @param file The file to read from
     * @return The read PID
     * @throws IOException if reading the next PID fails
     */

    private String readPid( RandomAccessFile file ) throws IOException
    {
        if ( outputStream != null )
        {
            throw new IOException( "Reading from uncommitted PidList is not allowed");
        }

        // Read byte with the next length field
        int length = file.read();
        readOffset++;

        log.debug( "Length field: {} read from '{}'", length, pidFile );

        if ( length == -1 )
        {
            String error = String.format( "EOF reading next pid from '%s'. Size: %d, cursor %d, Read offset %d",
                    pidFile, size, cursor, readOffset );
            log.error( error );
            throw new IOException( error );
        }

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
        String pid = new String( pidData, ENCODING );
        log.debug( "Next PID is '{}'. Offset is now", pid, readOffset );
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

            do
            {
                String pid = readPid( file );
                if ( pid != null )
                {
                    pids.add( pid ) ;
                }
            }
            while ( ++cursor < size && pids.size() < wanted );
        }
        finally
        {
            file.close();
        }
        if ( cursor >= size )
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
    public int size()
    {
        log.trace( "Getting size {}", size );
        return size;
    }

    @Override
    public void commit() throws IOException
    {
        log.debug( "Committing search result in file '{}'", pidFile );
        if ( outputStream != null )
        {
            outputStream.close();
            outputStream = null;
        }
        readOffset = 0;
    }

    @Override
    public void dispose()
    {
        log.debug( "Disposing search result in file '{}'", pidFile );
        if ( outputStream != null )
        {
            try
            {
                outputStream.close();
            }
            catch( IOException ex )
            {
                // We don't want the exception to propagate upwards, so just log it.
                log.warn( "Closing stream for file '{}' failed", pidFile, ex );
            }
            outputStream = null;
        }
        pidFile.delete();
        readOffset = 0;
        size = 0;
    }

}
