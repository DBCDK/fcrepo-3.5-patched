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

package dk.dbc.opensearch.fedora.search;


import org.apache.lucene.queryParser.ParseException;
import org.fcrepo.server.ReadOnlyContext;
import org.fcrepo.server.Server;
import org.fcrepo.server.errors.InvalidStateException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.search.FieldSearchQuery;
import org.fcrepo.server.search.FieldSearchResult;
import org.fcrepo.server.search.ObjectFields;
import org.fcrepo.server.storage.DOReader;
import org.fcrepo.server.storage.RepositoryReader;
import org.fcrepo.server.storage.types.Datastream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;


/**
 * Implementation of the FieldSearchResult interface. This class represents the
 * search results obtained from the underlying lucene indicies.
 * @author stm
 */
class FieldSearchResultLucene implements FieldSearchResult
{

    private final LuceneFieldIndex indexSearcher;
    private final RepositoryReader repoReader;
    /** resultFields is kept a String[] to ensure that values can be sent to this class as they can to FieldSearchResultSQLImpl. */
    private final String[] resultFields;
    private final IPidList searchResultList;

    /** Will hold the current view of results for the client to consume (through the objectFieldsList() method ). */
    private List<ObjectFields> currentResultList;

    /** cursors should keep track of how many results have been put into the list of objectfields per session. */
    private long cursor;     // indicating the start of the current resultset
    private long nextCursor; // indicating the beginning of the next resultset

    private String token;

    private final int maxResults;
    private final int timeout;

    private long completeResultSize;

    private Date expirationDate;

    private static final Logger log = LoggerFactory.getLogger( FieldSearchResultLucene.class );

    protected FieldSearchResultLucene( final LuceneFieldIndex indexController,
                                       final RepositoryReader repositoryReader,
                                       final String[] resultFieldsList,
                                       final FieldSearchQuery query,
                                       final int maximumResults,
                                       final int resultTimeout ) throws InvalidStateException, IOException
    {
        this.indexSearcher = indexController;
        this.repoReader = repositoryReader;
        this.resultFields = resultFieldsList;
        this.cursor = 0;
        this.nextCursor = 0;
        this.maxResults = maximumResults;
        this.timeout = resultTimeout;
        this.searchResultList = searchIndex( query );
        log.trace( "Opening and caching search result" );
        stepAndCacheResult();
    }


    /**
     * Returns a {@code List} of {@link ObjectFields} from which a client can
     * retrieve the results of the search. As the interface contract
     * {@link FieldSearchResult} does not allow for checked exceptions on this
     * method, all exceptions thrown from lower layers on a given pid will result
     * in that pid having no search results.
     *
     * The unfortunate side effect of the contract for this method is that if the
     * exception stems from a fundamental server error (Such as the inability to
     * read objects generally) this method will fill the logs with errors each
     * time it is called. Barring throwing Throwable, this method have no other
     * ways of communicating that the world has gone bonkers.
     *
     * @return a {@code List<fedora.server.search.ObjectFields>} containing search results
     */
    @Override
    public List<ObjectFields> objectFieldsList()
    {
        return this.currentResultList;
    }

    protected final FieldSearchResult stepAndCacheResult() throws IOException
    {
        log.trace( "Entering stepAndCacheResult" );
        cursor = nextCursor;
        log.debug( "Starting retrieval from index {}", cursor );
        long localResultCounter = cursor;
        currentResultList = new LinkedList<ObjectFields>();

        int size = searchResultList.size();

        log.debug( "ResultSet has {} elements, retrieving elements {}-{}",
                new Object[] { size, localResultCounter, localResultCounter + maxResults - 1 } );

        for( int objectsFetched = 0; objectsFetched < maxResults; objectsFetched++, localResultCounter++ ) {
            String pid = searchResultList.getNextPid();
            if( pid == null )
            {
                break;
            }

            log.debug( "Retrieving element {}", localResultCounter );

            try
            {
                log.trace( "Retrieving object fields from object with pid: {}", pid );
                currentResultList.add( getObjectFields( pid ) );
            }
            catch( ServerException ex )
            {
                String error = String.format( "Could not retrieve data from object reader for object pid %s: %s", pid, ex.getMessage() );
                log.error( error, ex );
            }
            catch( ClassCastException ex )
            {
                String error = String.format( "Object identified by %s has a DC datastream, but it's not inline XML. No data can be retrieved from this DigitalObject", pid );
                log.error( error, ex );
            }
        }

        log.debug( "Result set counter points to element at pos {}", localResultCounter );

        if( localResultCounter == size )
        {
            log.info( "Result set exhausted, null'ing token, resetting nextCursor" );
            token = null;
            nextCursor = 0;
        }
        else
        {
            log.info( "Still {} entries in result set", size - localResultCounter );
            long now = System.currentTimeMillis();
            token = hashCode() + "" + Long.toString( now );
            log.debug( "Setting token as {}", token );
            nextCursor = localResultCounter;
            log.debug( "cursor set to {}", cursor );
            log.debug( "nextCursor set to {}", nextCursor );
            Date dt = new Date();
            dt.setTime( now + ( 1000 * this.timeout ) );
            log.debug( "Setting expiration date to {} (1000*{} ms)", dt.toString(), timeout );
            expirationDate = dt;
        }
        log.trace( "Returning FieldSearchResult from stepAndCacheResult" );
        return this;
    }

    @Override
    public String getToken()
    {
        return this.token;
    }


    /**
     *  The {@code getCurser()} returns a cursor to the <i>first</i>
     *  index of the current resultset, and not the first index of the
     *  <i>next</i> resultset, as one would probably think.  A cursor
     *  to the first index of the next resultset is maintained
     *  internally in {@link FieldSearchResultLucene}.
     *
     *  @return a cursor to the first index of the current resultset.
     */
    @Override
    public long getCursor()
    {
        /**
         * Because the fedora logic around FieldSearch is bound to database
         * metaphors, we'll need to make the result from the indicies behave like
         * a java.sql.ResultSet:
         * (http://java.sun.com/javase/6/docs/api/java/sql/ResultSet.html)
         *
         * This is a naïve implementation which simply returns the position in
         * the result list.
         */
        return this.cursor;
    }

    /**
     * The interface for {@code getCompleteListSize} does not give a description
     * of what should be returned from its implementation. The canonical
     * implementation (the FieldSearchResultSQLImpl distributed with fedora)
     * returns -1 under all circumstances.
     *
     * This implementation interprets the "Complete List Size" as the size of
     * the entire search result returned from the index as viewed at the time
     * of calling this method.
     *
     * @return the size of the search result at the time of calling this method
     */
    @Override
    public long getCompleteListSize()
    {
        return this.completeResultSize;
    }

    /*
     * returns a {@link Date} object representing the expiration date for this
     * search result.
     * As we are not allowed to set the expiration date on search results
     * that are smaller than `maxResults`, we return a Date object that
     * has passed if that condition hold.
     */
    @Override
    public Date getExpirationDate()
    {
        if( null == this.expirationDate )
        {
            return new Date();
        }
        return this.expirationDate;
    }


    /**
     * In order to minimize the number of checked exceptions that can rise from
     * use of this method, any error that arises with the {@link fedora.server.Server}
     * will be wrapped and an empty ObjectFields will be returned.
     *
     * This design forces all users of this method to heed the possibility for
     * empty ObjectFields objects when calling this method., but the alternative
     * is to have an illegal state propagate out from the method, which is even
     * worse as not only will the {@link fedora.server.search.ObjectFields}
     * object returned be null, the entire state on which this plugin depends
     * will be in ruins.
     *
     * Therefore this method is made private for obvious reasons.
     *
     * @param pid the pid referencing an {@link fedora.server.storage.types.DigitalObject} from which to retrieve information
     * @return the ObjectFields object representing information on the object identified by {@code pid}. NOTE: Can be null in the event of exceptions arising from trying to retrieve this information
     */
    private ObjectFields getObjectFields( final String pid ) throws ServerException
    {
        log.trace( "Entering getObjectFields" );
        DOReader objectReader;

        log.trace( "Retrieving DigitalObjectReader from RepositoryReader on pid {}", pid );
        objectReader = this.repoReader.getReader( Server.USE_DEFINITIVE_STORE, ReadOnlyContext.EMPTY, pid );

        log.debug( "Trying to retrieve DC datastream from pid {}", pid );
        // If there's a DC record available, use SAX to parse the most
        // recent version of it into fields.
        Datastream dcmd = objectReader.GetDatastream( "DC", null );

        log.debug( "Putting {} result fields into ObjectFields object", this.resultFields.length );

        ObjectFields fields = null;
        if( null != dcmd )
        {
            fields = new ObjectFields( this.resultFields, dcmd.getContentStream() );
            log.debug( "Retrieved ObjectFields from object with pid {}", pid );
            // add dcmDate if wanted
            for ( String element : this.resultFields ) {
                if( element.equals( "dcmDate" ) )
                {
                    Date createddate = dcmd.DSCreateDT;
                    log.trace( "Found {} as DSCreatedDT", createddate.toString() );
                    fields.setDCMDate( createddate );
                }
            }
        } else {
            log.info( "Found no metadata on object with pid {}. No Dublin Core metadata for object", pid );
            fields = new ObjectFields();
        }

        log.debug( "Storing object properties from object {}", pid );
        // add non-dc values from doReader for the others in m_resultFields[]
        for( String resultFieldName : this.resultFields )
        {
            if( resultFieldName.equals( "pid" ) )
            {
                fields.setPid( pid );
            }
            if( resultFieldName.equals( "label" ) )
            {
                fields.setLabel( objectReader.GetObjectLabel() );
            }
            if( resultFieldName.equals( "state" ) )
            {
                fields.setState( objectReader.GetObjectState() );
            }
            if( resultFieldName.equals( "ownerId" ) )
            {
                fields.setOwnerId( objectReader.getOwnerId() );
            }
            if( resultFieldName.equals( "cDate" ) )
            {
                fields.setCDate( objectReader.getCreateDate() );
            }
            if( resultFieldName.equals( "mDate" ) )
            {
                fields.setMDate( objectReader.getLastModDate() );
            }

        }
        log.trace( "Returning ObjectFields from getObjectFields" );
        return fields;
    }

    /**
     * Conducts the search.
     */
    private IPidList searchIndex( final FieldSearchQuery query ) throws InvalidStateException
    {
        log.trace( "Entering searchIndex" );
        IPidList searchResult;
        try
        {
            searchResult = this.indexSearcher.search( query );
        }
        catch( IOException ex )
        {
            throw new InvalidStateException( "", "", new String[]{""}, new String[]{""}, ex );
        }
        catch( ParseException ex )
        {
            throw new InvalidStateException( "", "", new String[]{""}, new String[]{""}, ex );
        }
        this.completeResultSize = searchResult.size();

        log.trace( "Returning search result" );
        return searchResult;
    }

}
