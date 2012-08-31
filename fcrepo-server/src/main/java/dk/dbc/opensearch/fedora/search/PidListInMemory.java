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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * PidListInMemory
 * <p>
 * Implementation of the IPidList interface keeping the list of PIDs
 * entirely in-memory.
 */
public class PidListInMemory implements IPidList
{
    private List< String > pidList;
    private int cursor;

    /**
     * Class default constructor
     * <p>
     * Creates new empty pid list and sets cursor to zero.
     */
    public PidListInMemory()
    {
        pidList = new ArrayList<String>();
        cursor = 0;
    }

    @Override
    public void addPid( String pid )
    {
        if( pid != null )
        {
            pidList.add( pid );
        }
    }

    @Override
    public String getNextPid()
    {
        if( cursor < pidList.size() )
        {
            return pidList.get( cursor++ );
        }
        return null;
    }

    @Override
    public Collection< String > getNextPids( int wanted )
    {
        return null;
    }

    @Override
    public long getCursor()
    {
        return cursor;
    }

    @Override
    public int size()
    {
        return pidList.size();
    }
}
