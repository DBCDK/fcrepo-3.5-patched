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

package dk.dbc.opensearch.fedora.storage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.fcrepo.server.Context;
import org.fcrepo.server.errors.ObjectNotFoundException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.storage.DOReader;
import org.fcrepo.server.storage.SimpleServiceDeploymentReader;
import org.fcrepo.server.storage.RepositoryReader;
import org.fcrepo.server.storage.ServiceDefinitionReader;
import org.fcrepo.server.storage.ServiceDeploymentReader;
import org.fcrepo.server.storage.SimpleDOReader;
import org.fcrepo.server.storage.SimpleServiceDefinitionReader;
import org.fcrepo.server.storage.types.DigitalObject;

/**
 * Mock implementation of <code>RepositoryReader</code> for testing. This
 * works by simply keeping a map of <code>DigitalObject</code> instances in
 * memory.
 *
 */
public class MockRepositoryReader
        implements RepositoryReader {

    /**
     * The <code>DigitalObject</code>s in the "repository", keyed by PID.
     */
    private final Map<String, DigitalObject> _objects =
            new HashMap<String, DigitalObject>();

    /**
     * Construct with an empty "repository".
     */
    public MockRepositoryReader() {
    }

    /**
     * Adds/replaces the object into the "repository".
     */
    public synchronized void putObject(DigitalObject obj) {
        _objects.put(obj.getPid(), obj);
    }

    /**
     * Removes the object from the "repository" and returns it (or null if it
     * didn't exist in the first place).
     */
    public synchronized DigitalObject deleteObject(String pid) {
        return _objects.remove(pid);
    }

    /**
     * Get a <code>DigitalObject</code> if it's in the "repository".
     *
     * @throws ObjectNotFoundException
     *         if it's not in the "repository".
     */
    public synchronized DigitalObject getObject(String pid)
            throws ObjectNotFoundException {
        DigitalObject obj = _objects.get(pid);
        if ( null == obj ) {
            throw new ObjectNotFoundException( "No object for pid: " + pid );
        } else {
            return obj;
        }
    }

    // Mock methods from RepositoryReader interface.

    /**
     * {@inheritDoc}
     */
    public synchronized DOReader getReader( boolean cachedObjectRequired,
                                            Context context,
                                            String pid ) throws ServerException
    {
        DigitalObject obj = getObject( pid );
        return new SimpleDOReader(null, this, null, null, null, obj);
    }

    /**
     * {@inheritDoc}
     */
    public synchronized ServiceDeploymentReader getServiceDeploymentReader(boolean cachedObjectRequired,
                                                                           Context context,
                                                                           String pid)
            throws ServerException {
        DigitalObject obj = getObject(pid);
            return new SimpleServiceDeploymentReader(null,
                                                     this,
                                                     null,
                                                     null,
                                                     null,
                                                     obj);
    }

    /**
     * {@inheritDoc}
     */
    public synchronized ServiceDefinitionReader getServiceDefinitionReader(boolean cachedObjectRequired,
                                                                           Context context,
                                                                           String pid)
            throws ServerException {
        DigitalObject obj = getObject(pid);
            return new SimpleServiceDefinitionReader(null,
                                                     this,
                                                     null,
                                                     null,
                                                     null,
                                                     obj);
    }

    /**
     * {@inheritDoc}
     */
    public synchronized String[] listObjectPIDs(Context context)
            throws ServerException {
        String[] pids = new String[_objects.keySet().size()];
        Iterator<String> iter = _objects.keySet().iterator();
        int i = 0;
        while (iter.hasNext()) {
            pids[i++] = iter.next();
        }
        return pids;
    }

}
