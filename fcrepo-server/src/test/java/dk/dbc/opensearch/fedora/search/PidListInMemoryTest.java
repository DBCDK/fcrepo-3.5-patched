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

import org.junit.Test;

import java.util.Collection;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;


/**
 * PidListInMemory unit tests
 * <p>
 * The test methods of this class uses the following naming convention:
 *
 *  unitOfWork_stateUnderTest_expectedBehavior
 */
public class PidListInMemoryTest
{
    private final static String PID_1 = "pid_1";
    private final static String PID_2 = "pid_2";
    private final static String PID_3 = "pid_3";

    @Test
    public void constructor_whenCalled_returnsNewEmptyInstance()
    {
        PidListInMemory instance = new PidListInMemory();
        assertEquals( 0, instance.size() );
        assertEquals( 0, instance.getCursor() );
    }

    @Test
    public void addPid_pidArgIsNonNull_increasesListSizeByOne()
    {
        PidListInMemory instance = new PidListInMemory();
        instance.addPid( PID_1 );
        assertEquals( 1, instance.size());
        instance.addPid( PID_2 );
        assertEquals( 2, instance.size());
    }

    @Test
    public void addPid_pidArgIsNull_()
    {
        PidListInMemory instance = new PidListInMemory();
        instance.addPid( null );
        assertEquals( 0, instance.size());
    }

    @Test
    public void getNextPid_listIsEmpty_returnsNull()
    {
        PidListInMemory instance = new PidListInMemory();
        assertNull( instance.getNextPid() );
    }

    @Test
    public void getNextPid_listIsNonEmpty_returnsPidAndAdvancesCursor()
    {
        PidListInMemory instance = new PidListInMemory();
        instance.addPid( PID_1 );
        instance.addPid( PID_2 );
        assertEquals( PID_1, instance.getNextPid() );
        assertEquals( 1, instance.getCursor() );
        assertEquals( PID_2, instance.getNextPid() );
        assertEquals( 2, instance.getCursor() );
    }

    @Test
    public void getNextPids_listIsEmpty_returnsEmptyList()
    {
        PidListInMemory instance = new PidListInMemory();
        Collection<String> result = instance.getNextPids( 42 );
        assertTrue( result.isEmpty() );
    }

    @Test
    public void getNextPids_listIsNonEmptyButWantedExceedsSize_returnsNonEmptyListAndAdvancesCursor()
    {
        PidListInMemory instance = new PidListInMemory();
        instance.addPid( PID_1 );
        instance.addPid( PID_2 );
        Collection<String> result = instance.getNextPids( 42 );
        assertEquals( 2, result.size() );
        assertEquals( 2, instance.getCursor() );
        assertTrue( result.contains( PID_1 ) );
        assertTrue( result.contains( PID_2 ) );
    }

    @Test
    public void getNextPids_listIsNonEmptyAndWantedDoesNotExceedSize_returnsNonEmptyListAndAdvancesCursor()
    {
        PidListInMemory instance = new PidListInMemory();
        instance.addPid( PID_1 );
        instance.addPid( PID_2 );
        instance.addPid( PID_3 );
        Collection<String> result = instance.getNextPids( 2 );
        assertEquals( 2, result.size() );
        assertEquals( 2, instance.getCursor() );
        assertTrue( result.contains( PID_1 ) );
        assertTrue( result.contains( PID_2 ) );
    }

}
