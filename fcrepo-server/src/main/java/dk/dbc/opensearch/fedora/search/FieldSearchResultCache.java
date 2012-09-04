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

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that handles the caching of search results between calls to 'resumeFindObject'
 * If search results are not retrieved before a configured time limit, they are expired,
 * removed from the cache and disposed
 */
class FieldSearchResultCache
{
    private static final Logger log = LoggerFactory.getLogger( FieldSearchResultCache.class );

    private final int resultLifeTimeInSeconds;
    private final ConcurrentHashMap<String, FieldSearchResultLucene> cachedResult;
    private final ScheduledExecutorService cacheSurveillance;


    /**
     * Constructor for the cache.
     * @param resultLifeTimeInSeconds Life time to keep search results alive in the cache
     */
    FieldSearchResultCache( int resultLifeTimeInSeconds )
    {
        this.resultLifeTimeInSeconds = resultLifeTimeInSeconds;

        this.cachedResult = new ConcurrentHashMap<String, FieldSearchResultLucene>();
        this.cacheSurveillance = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Start the mechanism that removes expired results from the cache
     */
    void start()
    {
        log.debug( "Starting cache invalidation thread with interval set to {} seconds", resultLifeTimeInSeconds );
        this.cacheSurveillance.scheduleAtFixedRate( new CacheInvalidationThread(), 0L,
                                                    resultLifeTimeInSeconds, TimeUnit.SECONDS );
    }

    /**
     * Shutdown. Remove and dispose any remaining cached search results
     */
    void shutdown()
    {
        log.info( "Shutting down and removing {} cached search result", cachedResult.size() );
        this.cacheSurveillance.shutdown();

        for ( Map.Entry<String, FieldSearchResultLucene> result : cachedResult.entrySet() )
        {
            log.info( "Removing search result with token {}, Size {}", result.getKey(),
                    result.getValue().getCompleteListSize() );

            cachedResult.remove( result.getKey() );
            result.getValue().dispose();
        }
    }

    void putCachedResult( String token, FieldSearchResultLucene result )
    {
        FieldSearchResultLucene previousValue = cachedResult.put( token, result );
        if( null != previousValue )
        {
            log.debug( "Added search result to cache with token {}, size {}", token, result.getCompleteListSize() );
        }
        else
        {
            log.debug( "Replaced search result to cache with token {}, size {}", token, result.getCompleteListSize() );
        }

    }

    FieldSearchResultLucene getCachedResult( String token )
    {
        FieldSearchResultLucene result = cachedResult.get( token );
        if ( result == null )
        {
            log.debug( "No cached result found for token {}", token);
        }
        else
        {
            log.debug( "Found cached result for token {}, with size", token, result.getCompleteListSize() );
        }
        return result;
    }

    FieldSearchResultLucene removeCachedResult( String token )
    {
        FieldSearchResultLucene result = cachedResult.remove( token );
        if ( result == null )
        {
            log.debug( "No cached result removed for token {}", token);
        }
        else
        {
            log.debug( "Removed cached result for token {}, with size", token, result.getCompleteListSize() );

        }
        return result;
    }

    /**
     * the {@code CacheInvalidationThread} checks the {@code cacheResult} map on
     * a specified interval and removes entries that are expired.
     */
    private final class CacheInvalidationThread implements Runnable
    {
        @Override
        public void run()
        {
            log.debug( "Running cleanup task. {} results in cache", cachedResult.size() );
            Date now = new Date();
            for ( Map.Entry<String, FieldSearchResultLucene> result : cachedResult.entrySet() )
            {
                Date expirationDate = result.getValue().getExpirationDate();
                log.trace( "Checking cached result. Token {}, Size {}, Timestamps: Now [{}], Expiration [{}]",
                        new Object[] { result.getKey(), result.getValue().getCompleteListSize(), now, expirationDate } );
                if ( now.after( expirationDate ) )
                {
                    log.info( "Removing expired search result with token {}, Size {}", result.getKey(),
                            result.getValue().getCompleteListSize() );

                    cachedResult.remove( result.getKey() );
                    result.getValue().dispose();
                }
            }
        }
    }
}
