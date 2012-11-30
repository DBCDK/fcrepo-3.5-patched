/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.search;

import java.util.List;
import org.fcrepo.server.errors.QueryParseException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ConditionTest
{

    public ConditionTest()
    {
    }

    @Test
    public void getConditions_whenValueContainsEqualsOperatorInQoutes() throws QueryParseException
    {
        List<Condition> conditions = Condition.getConditions( "title='E=mc²'");
        assertEquals( 1, conditions.size() );
        Condition cond = conditions.get( 0 );
        assertEquals( "title", cond.getProperty() );
        assertEquals( Operator.EQUALS, cond.getOperator() );
        assertEquals( "E=mc²", cond.getValue() );
    }

    @Test ( expected = QueryParseException.class )
    public void getConditions_whenValueContainsEqualsOperatorWithoutQoutes() throws QueryParseException
    {
        Condition.getConditions( "title=E=mc²");
    }

    @Test
    public void getConditions_whenValueContainsGreaterThanOperatorInQoutes() throws QueryParseException
    {
        List<Condition> conditions = Condition.getConditions( "title='2>1'");
        assertEquals( 1, conditions.size() );
        Condition cond = conditions.get( 0 );
        assertEquals( "title", cond.getProperty() );
        assertEquals( Operator.EQUALS, cond.getOperator() );
        assertEquals( "2>1", cond.getValue() );
    }

    @Test ( expected = QueryParseException.class )
    public void getConditions_whenValueContainsGreaterThanOperatorWithoutQoutes() throws QueryParseException
    {
        Condition.getConditions( "title=2>1");
    }

}