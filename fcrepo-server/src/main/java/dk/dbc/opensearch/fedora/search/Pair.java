/**
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


/**
 * You can add two objects to Pair and retrieve them again.
 * The objects may be different, identical, or the actual same object.
 * After you have added the objects to Pair you can no longer modify it,
 * i.e. Pair is immutable, even though the objects inside Pair may be mutable (see below).
 * <p>
 * Please notice, Pair do not use defensive copying of its two elements.
 * As a consequence if you modify the original objects after adding them to the Pair,
 * the objects inside Pair will also be changed. It is the responibility of the user of Pair
 * to ensure the correct behavior of the objects after adding them to Pair. This is of course only
 * possible if you use mutable objects.
 * <p>
 *
 * @param <E>
 * @param <V>
 */
public final class Pair< E, V >
{
    private final E first;
    private final V second;

    /**
     *  Constructor taking two objects. The Objects may be different objects, 
     *  equal objects, or the same actual object. Neither of the objects may be null.
     * 
     *  @param firstObject The first object.
     *  @param secondObject The second object.
     */
    public Pair( final E firstObject, final V secondObject )
    {
	if ( firstObject == null || secondObject == null )
	{
	    throw new IllegalArgumentException( "null values are not accepted as elements in Pair." );
	}
        this.first = firstObject;
        this.second = secondObject;
    }

    /**
     *  Retrieves the first element of the pair.
     *  
     *  @return The first element of the pair.
     */
    public E getFirst()
    {
        return this.first;
    }

    /**
     *  Retrieves the second element of the pair.
     *  
     *  @return The second element of the pair
     */
    public V getSecond()
    {
        return this.second;
    }
    
    /**
     *  A string representation of the pair in the following format:
     *  <pre>
     *  {@code
     *     Pair< String-representation-of-first-element, String-representaion-of-second-element >
     *  }
     *  </pre>
     *  If the Pair as the first element contains a String with value "FancyPants",
     *  and as its second element the Integer value 42, then the toString will return:
     *  <pre>
     *  {@code
     *     Pair< FancyPants, 42 >
     *  }
     *  </pre>
     *
     *  @return a String representation of the object
     */
    @Override
    public String toString()
    {
        return String.format( "Pair< %s, %s >", this.first.toString(), this.second.toString() );
    }
    
    /**
     *  Returns a unique hashcode for the specific combination of elements in this Pair.
     *  Notice, if you use the same two objects in the same order in two different Pairs, 
     *  then the two Pairs will return the same hashcode.
     *
     * @return the hash code of this object as an int
     */    
    @Override
    public int hashCode()
    {
        return this.first.hashCode() ^ this.second.hashCode();
    }

    /**
     *  Asserts equality of the Pair object and another Pair object,
     *  based on equality of the contained elements. The elements are testeted against each other 
     *  in the same order they appear in the Pair. That is, Pair< E, V > and Pair < V, E >
     *  are not equal even though it is the same objects (E and V) that are contained in the Pair,
     *  assuming E and V are nonequal.
     *
     * @param obj the object to be compared with this object
     * @return true iff {@code obj} equals this object, false otherwise
     */
    @Override
    public boolean equals( final Object obj )
    {
        if( !( obj instanceof Pair<?,?> ) )
        {
            return false;
        }
        else if( !( this.first.equals( ( (Pair<?, ?>)obj ).getFirst() ) ) || !( this.second.equals( ( (Pair<?, ?>)obj ).getSecond() ) ) )
        {
            return false;
        }
        else
        {
            return true;
        }
    }
}
