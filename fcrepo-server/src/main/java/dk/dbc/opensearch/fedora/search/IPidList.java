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

import java.io.IOException;
import java.util.Collection;

/**
 * Allows for a list of PIDs to be built and traversed
 */
public interface IPidList
{
    /**
     * Appends the specified non-null PID to the end of this list, while
     * ignoring null values
     *
     * @param pid to be appended to this list
     * @throws IOException If unable to read PID from the list
     */
    void addPid( String pid ) throws IOException;

    /**
     * Returns the next PID element in the list and advances
     * cursor position to the following element
     *
     * @return PID element or null if list is exhausted
     * @throws IOException If unable to read PID from the list
     */
    String getNextPid() throws IOException;

    /**
     * Get multiple next PID elements in the list and advances
     * cursor position to the following element.
     * If less than the requested number of PID remain, fewer
     * that requested will be returned
     * @param wanted Number of PID wanted
     * @return Collection of PID
     * @throws IOException If unable to read PID from the list
     */
    Collection< String > getNextPids( int wanted ) throws IOException;

    /**
     * Gets the current cursor position in the list
     *
     * @return cursor position
     */
    long getCursor();

    /**
     * Gets the size of the list
     *
     * @return size
     */
    int size();

    /**
     * Dispose of the search result, freeing any underlying resources
     */
    void dispose();

}
