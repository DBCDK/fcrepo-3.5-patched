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

import java.util.Date;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;
import static org.junit.Assert.*;

public class FieldSearchResultCacheTest
{
    private final static String token = "token";


    @Test ( expected = IllegalArgumentException.class )
    public void testIllegalLifetime()
    {
        FieldSearchResultCache instance = new FieldSearchResultCache( 0 );
        instance.start();
    }


    @Test ( timeout = 1000 )
    public void testStartAndShutdownWithLegalLifetime()
    {
        FieldSearchResultCache instance = new FieldSearchResultCache( 1 );
        instance.start();
        instance.shutdown();
    }


    @Test
    public void testPutGetRemove( final @Mocked FieldSearchResultLucene fslResult )
    {
        FieldSearchResultCache instance = new FieldSearchResultCache( 1 );

        // Result must not be in cache before test
        assertNull( instance.getCachedResult( token ) );

        instance.putCachedResult( token, fslResult );
        assertSame( fslResult, instance.getCachedResult( token ) );
        assertSame( fslResult, instance.removeCachedResult( token ) );

        // Result must no longer be in cache
        assertNull( instance.getCachedResult( token ) );

    }


    @Test
    public void testInvalidationThreadDisposesExpiredResult( final @Mocked FieldSearchResultLucene fslResult ) throws InterruptedException
    {
        // Pick a time before 'now' so it is elligeble for invalidation
        final long timeBeforeStart = System.currentTimeMillis() - 1000;

        FieldSearchResultCache instance = new FieldSearchResultCache( 1 );

        // Result must not be in cache before test
        assertNull( instance.getCachedResult( token ) );

        instance.putCachedResult( token, fslResult );

        new Expectations()
        {
            {
                fslResult.getExpirationDate(); result = new Date ( timeBeforeStart );
                fslResult.dispose();
            }
        };

        instance.start();

        // Wait long enough for initial invalidation to run
        Thread.sleep( 500 );

        // Result must no longer be in cache
        FieldSearchResultLucene cachedResult = instance.getCachedResult( token );

        instance.shutdown();
        assertNull( cachedResult);
    }


    @Test ( timeout = 1000 )
    public void testShutdownDisposesResult( final @Mocked FieldSearchResultLucene fslResult )
    {
        FieldSearchResultCache instance = new FieldSearchResultCache( 1 );

        instance.putCachedResult( token, fslResult );

        // Dispose is expected to be called during shutdown
        new Expectations()
        {
            {
                fslResult.dispose();
            }
        };

        instance.shutdown();

        // And result must not be in cache after shutdown
        assertNull( instance.getCachedResult( token ) );
    }
}