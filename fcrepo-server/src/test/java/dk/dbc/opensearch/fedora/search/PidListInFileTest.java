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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 *
 * @author thp
 */
public class PidListInFileTest
{
    File tempFile;

    String[] pidsArray = { "obj:1","obj:2","obj:3","obj:4","obj:5"};
    private final String pidsStr = "obj:1,obj:2,obj:3,obj:4,obj:5";

    public PidListInFileTest()
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
        tempFile = File.createTempFile( "PidListInFile", ".csv", new File("target/test-classes") );
    }

    @After
    public void tearDown()
    {
        if ( !tempFile.delete() )
        {
            tempFile.deleteOnExit();
        }
        tempFile = null;
    }

    @Test
    public void testConstructor() throws Exception
    {
        // File must not exist at test start time
        tempFile.delete();

        PidListInFile pidList = new PidListInFile( tempFile );
    }

    @Test( expected = IOException.class )
    public void testConstructorExistingFile() throws IOException
    {
        // tempFile has not been deleted. Existing file will cause an exception
        PidListInFile pidList = new PidListInFile( tempFile );
    }

    @Test
    public void testCopyConstructor() throws IOException
    {
        PidListInMemory memPidList = new PidListInMemory();

        for( String pid : pidsArray )
        {
            memPidList.addPid( pid );
        }

        // File must not exist at test start time
        tempFile.delete();

        PidListInFile pidList = new PidListInFile( tempFile, memPidList );

        assertEquals( 5, pidList.size() );
        assertEquals( pidsStr, FileUtils.readFileToString( tempFile ) );
    }

    @Test
    public void testAddPid() throws IOException
    {
        // File must not exist at test start time
        tempFile.delete();
        PidListInFile pidList = new PidListInFile( tempFile );
        for( String pid : pidsArray )
        {
            pidList.addPid( pid );
        }

        assertEquals( 5, pidList.size() );
        assertEquals( pidsStr, FileUtils.readFileToString( tempFile ) );
    }


    @Test
    public void testGetNextPid() throws IOException
    {
        // File must not exist at test start time
        tempFile.delete();
        PidListInFile pidList = new PidListInFile( tempFile );
        for( String pid : pidsArray )
        {
            pidList.addPid( pid );
        }

        assertEquals( 5, pidList.size() );
        assertEquals( pidsStr, FileUtils.readFileToString( tempFile ) );

        for( int i = 0; i < pidsArray.length; i++ )
        {
            assertEquals( pidsArray[i], pidList.getNextPid() );
        }
        // There must be no more pids in list
        assertNull( pidList.getNextPid() );
        // File must be deleted when pid list is exhausted
        assertFalse( tempFile.exists() );
    }


    @Test
    public void testGetNextPids() throws IOException
    {
        // File must not exist at test start time
        tempFile.delete();
        PidListInFile pidList = new PidListInFile( tempFile );
        for( String pid : pidsArray )
        {
            pidList.addPid( pid );
        }

        assertEquals( 5, pidList.size() );

        Collection<String> nextPids = pidList.getNextPids( 10 );

        assertEquals( 5, nextPids.size() );
        Iterator<String> iterator = nextPids.iterator();
        for( int i = 0; i < nextPids.size(); i++ )
        {
            assertEquals( pidsArray[i], iterator.next() );
        }
        // There must be no more pids in list
        assertNull( pidList.getNextPid() );
        // File must be deleted when pid list is exhausted
        assertFalse( tempFile.exists() );
    }


    @Test
    public void testGetNextPidsMultipleCallsExactSize() throws IOException
    {
        // File must not exist at test start time
        tempFile.delete();
        PidListInRandomAccessFile pidList = new PidListInRandomAccessFile( tempFile );
        for( String pid : pidsArray )
        {
            pidList.addPid( pid );
        }

        assertEquals( 5, pidList.size() );

        Collection<String> nextPids = pidList.getNextPids( 5 );
        // First time there must be 5 in returned list
        assertEquals( 5, nextPids.size() );

        nextPids = pidList.getNextPids( 5 );
        // Second time there must be 0 in returned list
        assertEquals( 0, nextPids.size() );

        // There must be no more pids in list
        assertNull( pidList.getNextPid() );
        // File must be deleted when pid list is exhausted
        assertFalse( tempFile.exists() );
    }


    @Test
    public void testGetNextPidsMultipleCallsOddSize() throws IOException
    {
        // File must not exist at test start time
        tempFile.delete();
        PidListInRandomAccessFile pidList = new PidListInRandomAccessFile( tempFile );
        for( String pid : pidsArray )
        {
            pidList.addPid( pid );
        }

        assertEquals( 5, pidList.size() );

        Collection<String> nextPids = pidList.getNextPids( 3 );
        // First time there must be 3 in returned list
        assertEquals( 3, nextPids.size() );

        nextPids = pidList.getNextPids( 3 );
        // Second time there must be 2 in returned list
        assertEquals( 2, nextPids.size() );

        // There must be no more pids in list
        assertNull( pidList.getNextPid() );
        // File must be deleted when pid list is exhausted
        assertFalse( tempFile.exists() );
    }


    @Test
    public void testGetCursor() throws IOException
    {
        // File must not exist at test start time
        tempFile.delete();
        PidListInFile pidList = new PidListInFile( tempFile );
        for( String pid : pidsArray )
        {
            pidList.addPid( pid );
        }

        assertEquals( 5, pidList.size() );
        assertEquals( pidsStr, FileUtils.readFileToString( tempFile ) );

        for( int i = 0; i < pidsArray.length; i++ )
        {
            // Verify position in file
            assertEquals( i*6, pidList.getCursor() );

            pidList.getNextPid();

            assertEquals( (i+1)*6, pidList.getCursor() );
        }
    }


    @Test
    public void testSize() throws IOException
    {
        // File must not exist at test start time
        tempFile.delete();
        PidListInFile pidList = new PidListInFile( tempFile );

        assertEquals( 0, pidList.size() );
    }

    // Small performance test. Should normally be disabled
    @Test
    @Ignore
    public void testCopyLargeList() throws IOException
    {
        // File must not exist at test start time
        tempFile.delete();

        PidListInMemory memPidList = new PidListInMemory();
        int size = 100000;

        System.out.println( "Create memory list with size: " + size );

        long start = System.currentTimeMillis();

        for( int i = 1; i <= size; i++ )
        {
            String pid = String.format( "obj:%08d", i );
            memPidList.addPid( pid );
        }

        long end = System.currentTimeMillis();

        System.out.println( "Time: " + ( end - start ) );

        System.out.println( "Copy memory list to file list" );

        start = System.currentTimeMillis();

        PidListInFile pidList = new PidListInFile( tempFile, memPidList );

        end = System.currentTimeMillis();

        System.out.println( "File size is:" + tempFile.length() );
        System.out.println( "Time: " + ( end - start ) );

        assertEquals( size, pidList.size() );
    }


    // Small performance test. Should normally be disabled
    @Test
    @Ignore
    public void testCreateLargeListIterateSingleStep() throws IOException
    {
        // File must not exist at test start time
        tempFile.delete();

        PidListInFile pidList = new PidListInFile( tempFile );
        int size = 100000;

        System.out.println( "Create file list with size: " + size );

        long start = System.currentTimeMillis();

        for( int i = 1; i <= size; i++ )
        {
            String pid = String.format( "obj:%08d", i );
            pidList.addPid( pid );
        }

        long end = System.currentTimeMillis();

        System.out.println( "File size is:" + tempFile.length() );
        System.out.println( "Time: " + ( end - start ) );

        assertEquals( size, pidList.size() );

        System.out.println( "Iterate file list with size " + size + ", single step" );
        start = System.currentTimeMillis();

        int total = 0;

        while ( pidList.getNextPid() != null )
        {
            total ++;
        }
        end = System.currentTimeMillis();

        System.out.println( "Time: " + ( end - start ) );
        assertEquals( size, total );
    }


    // Small performance test. Should normally be disabled
    @Test
    @Ignore
    public void testCreateLargeListIterateBatch() throws IOException
    {
        // File must not exist at test start time
        tempFile.delete();

        PidListInFile pidList = new PidListInFile( tempFile );
        int size = 100000;

        System.out.println( "Create file list with size: " + size );

        long start = System.currentTimeMillis();

        for( int i = 1; i <= size; i++ )
        {
            String pid = String.format( "obj:%08d", i );
            pidList.addPid( pid );
        }

        long end = System.currentTimeMillis();

        System.out.println( "File size is:" + tempFile.length() );
        System.out.println( "Time: " + ( end - start ) );

        assertEquals( size, pidList.size() );

        //int batchSize = 13;
        //int batchSize = 123;
        int batchSize = 973;

        System.out.println( "Iterate file list with size " + size + ", " + batchSize + " at a time" );
        start = System.currentTimeMillis();

        int got;
        int total = 0;
        do
        {
            got = pidList.getNextPids( batchSize).size();
            //System.out.println( "Got " + got );
            total += got;
        }
        while ( got == batchSize );

        end = System.currentTimeMillis();

        System.out.println( "Time: " + ( end - start ) );

        assertEquals( size, total );
    }

}