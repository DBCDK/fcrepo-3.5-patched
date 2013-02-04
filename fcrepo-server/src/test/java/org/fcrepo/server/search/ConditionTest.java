/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */
package org.fcrepo.server.search;

import java.util.List;
import junit.framework.JUnit4TestAdapter;
import org.fcrepo.server.errors.QueryParseException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ConditionTest
{

    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(ConditionTest.class);
    }

    @Test
    public void getConditions_whenCalledWithDocumentationExample() throws QueryParseException
    {
        List<Condition> conditions = Condition.getConditions( "a=x b~'that\\'s' c>='z'");
        assertEquals( 3, conditions.size() );
        assertEquals( new Condition("a", Operator.EQUALS, "x" ), conditions.get( 0 ) );
        assertEquals( new Condition("b", Operator.CONTAINS, "that's" ), conditions.get( 1 ) );
        assertEquals( new Condition("c", Operator.GREATER_OR_EQUAL, "z" ), conditions.get( 2 ) );
    }

    @Test
    public void getConditions_whenValueContainsEscapedSingleQuoteInQuotes() throws QueryParseException
    {
        List<Condition> conditions = Condition.getConditions( "title='Hello\\'World'");
        assertEquals( 1, conditions.size() );
        Condition cond = conditions.get( 0 );
        assertEquals( "title", cond.getProperty() );
        assertEquals( Operator.EQUALS, cond.getOperator() );
        assertEquals( "Hello'World", cond.getValue() );
    }

    @Test
    public void getConditions_whenValueContainsEqualsOperatorInQuotes() throws QueryParseException
    {
        List<Condition> conditions = Condition.getConditions( "title='E=mc²'");
        assertEquals( 1, conditions.size() );
        Condition cond = conditions.get( 0 );
        assertEquals( "title", cond.getProperty() );
        assertEquals( Operator.EQUALS, cond.getOperator() );
        assertEquals( "E=mc²", cond.getValue() );
    }

    @Test ( expected = QueryParseException.class )
    public void getConditions_whenValueContainsEqualsOperatorWithoutQuotes() throws QueryParseException
    {
        Condition.getConditions( "title=E=mc²");
    }

    @Test
    public void getConditions_whenValueContainsGreaterThanOperatorInQuotes() throws QueryParseException
    {
        List<Condition> conditions = Condition.getConditions( "title='2>1'");
        assertEquals( 1, conditions.size() );
        Condition cond = conditions.get( 0 );
        assertEquals( "title", cond.getProperty() );
        assertEquals( Operator.EQUALS, cond.getOperator() );
        assertEquals( "2>1", cond.getValue() );
    }

    @Test ( expected = QueryParseException.class )
    public void getConditions_whenValueContainsGreaterThanOperatorWithoutQuotes() throws QueryParseException
    {
        Condition.getConditions( "title=2>1");
    }

}