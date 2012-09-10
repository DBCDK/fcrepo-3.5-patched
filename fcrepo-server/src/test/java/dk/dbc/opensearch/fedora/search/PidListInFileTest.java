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
        tempFile = File.createTempFile( "PidListInRandFile", ".bin", new File("target/test-classes") );
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
        PidListInFile pidList = new PidListInFile( tempFile );
    }

    /**
     * Null as file is not allowed. Must cause an exception
     */
    @Test ( expected = NullPointerException.class )
    public void testConstructorThrowsOnNullFile() throws Exception
    {
        PidListInFile pidList = new PidListInFile( null );
    }

    @Test
    public void testCopyConstructor() throws IOException
    {
        PidListInMemory memPidList = new PidListInMemory();

        for( String pid : pidsArray )
        {
            memPidList.addPid( pid );
        }

        PidListInFile pidList = new PidListInFile( tempFile, memPidList );
        pidList.commit();

        assertEquals( 5, pidList.size() );
    }

    /**
     * Null as source is not allowed. Must cause an exception
     */
    @Test ( expected = NullPointerException.class )
    public void testCopyConstructorThrowsonNullSource() throws IOException
    {
        PidListInFile pidList = new PidListInFile( tempFile, null );
    }

    @Test
    public void testAddPid() throws IOException
    {
        PidListInFile pidList = new PidListInFile( tempFile );
        for( String pid : pidsArray )
        {
            pidList.addPid( pid );
        }
        pidList.commit();

        assertEquals( 5, pidList.size() );
    }


    @Test
    public void testGetNextPids() throws IOException
    {
        PidListInFile pidList = new PidListInFile( tempFile );
        for( String pid : pidsArray )
        {
            pidList.addPid( pid );
        }
        pidList.commit();

        assertEquals( 5, pidList.size() );

        Collection<String> nextPids = pidList.getNextPids( 10 );

        assertEquals( 5, nextPids.size() );
        Iterator<String> iterator = nextPids.iterator();
        for( int i = 0; i < nextPids.size(); i++ )
        {
            assertEquals( pidsArray[i], iterator.next() );
        }
        // There must be no more pids in list
        assertTrue( pidList.getNextPids(1).isEmpty() );
        // File must be deleted when pid list is exhausted
        assertFalse( tempFile.exists() );
    }


    @Test
    public void testGetNextPidsMultipleCallsExactSize() throws IOException
    {
        PidListInFile pidList = new PidListInFile( tempFile );
        for( String pid : pidsArray )
        {
            pidList.addPid( pid );
        }
        pidList.commit();

        assertEquals( 5, pidList.size() );

        Collection<String> nextPids = pidList.getNextPids( 5 );
        // First time there must be 5 in returned list
        assertEquals( 5, nextPids.size() );

        // File must be deleted when pid list is exhausted
        assertFalse( tempFile.exists() );

        nextPids = pidList.getNextPids( 5 );
        // Second time there must be 0 in returned list
        assertEquals( 0, nextPids.size() );

        // There must be no more pids in list
        assertTrue( pidList.getNextPids(1).isEmpty() );
    }


    @Test
    public void testGetNextPidsMultipleCallsOddSize() throws IOException
    {
        PidListInFile pidList = new PidListInFile( tempFile );
        for( String pid : pidsArray )
        {
            pidList.addPid( pid );
        }
        pidList.commit();

        assertEquals( 5, pidList.size() );

        Collection<String> nextPids = pidList.getNextPids( 3 );
        // First time there must be 3 in returned list
        assertEquals( 3, nextPids.size() );

        nextPids = pidList.getNextPids( 3 );
        // Second time there must be 2 in returned list
        assertEquals( 2, nextPids.size() );

        // File must be deleted when pid list is exhausted
        assertFalse( tempFile.exists() );
    }


    @Test
    public void testSize() throws IOException
    {
        PidListInFile pidList = new PidListInFile( tempFile );

        assertEquals( 0, pidList.size() );
    }


    @Test
    public void testDisposeEmptyList() throws IOException
    {
        PidListInFile pidList = new PidListInFile( tempFile );

        pidList.dispose();
        assertFalse( tempFile.exists() );
    }


    @Test
    public void testCommitEmptyList() throws IOException
    {
        PidListInFile pidList = new PidListInFile( tempFile );

        pidList.commit();
    }


    @Test ( expected = IOException.class )
    public void testReadFromUncommitedListThrows() throws IOException
    {
        PidListInFile pidList = new PidListInFile( tempFile );
        pidList.addPid( "obj:1" );
        pidList.getNextPids(1);
    }

    @Test
    public void testDisposeNonEmptyList() throws IOException
    {
        PidListInFile pidList = new PidListInFile( tempFile );
        pidList.addPid( "obj:1" );
        pidList.commit();

        pidList.dispose();
        assertFalse( tempFile.exists() );
        assertEquals( 0, pidList.size() );
    }


    @Test
    public void testDisposeClosedList() throws IOException
    {
        PidListInFile pidList = new PidListInFile( tempFile );
        pidList.addPid( "obj:1" );
        pidList.commit();

        String[] nextPidArray = pidList.getNextPids(1).toArray(new String[0]);
        assertEquals(1, nextPidArray.length);
        
        assertEquals( "obj:1", nextPidArray[0] );
        assertTrue( pidList.getNextPids(1).isEmpty() );
        assertFalse( tempFile.exists() );

        pidList.dispose();
        assertFalse( tempFile.exists() );
        assertEquals( 0, pidList.size() );
    }


    // Small performance test. Should normally be disabled
    @Test
    @Ignore
    public void testCopyLargeList() throws IOException
    {
        PidListInMemory memPidList = new PidListInMemory();
        int size = 1000000;

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
        pidList.commit();

        end = System.currentTimeMillis();

        System.out.println( "File size is:" + tempFile.length() );

        System.out.println( "Time: " + ( end - start ) );

        assertEquals( size, pidList.size() );
    }


    // Small performance test. Should normally be disabled
    @Test
    @Ignore
    public void testCreateLargeListIterateBatch() throws IOException
    {
        PidListInFile pidList = new PidListInFile( tempFile );
        int size = 1000000;

        System.out.println( "Create file list with size: " + size );

        long start = System.currentTimeMillis();

        for( int i = 1; i <= size; i++ )
        {
            String pid = String.format( "obj:%08d", i );
            pidList.addPid( pid );
        }
        pidList.commit();

        long end = System.currentTimeMillis();
        System.out.println( "File size is:" + tempFile.length() );

        System.out.println( "Time: " + ( end - start ) );

        assertEquals( size, pidList.size() );

        //int batchSize = 13;
        int batchSize = 123;
        //int batchSize = 973;
        //int batchSize = 1000;

        System.out.println( "Iterate file list with size " + size + ", " + batchSize + " at a time" );
        start = System.currentTimeMillis();

        int got;
        int total = 0;
        do
        {
            got = pidList.getNextPids( batchSize).size();
            System.out.println( "Got " + got );
            total += got;
        }
        while ( got == batchSize );

        end = System.currentTimeMillis();

        System.out.println( "Time: " + ( end - start ) );

        assertEquals( size, total );
    }

}