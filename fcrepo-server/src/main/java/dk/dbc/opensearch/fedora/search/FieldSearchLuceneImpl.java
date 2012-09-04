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

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.lucene.index.CorruptIndexException;

import org.fcrepo.common.rdf.RDFName;
import org.fcrepo.server.storage.types.RelationshipTuple;
import org.fcrepo.server.utilities.DCField;
import org.fcrepo.server.utilities.DCFields;

//
// README
//
// Note: Tests pending! Unittests works, though.
//
// I have refactored FieldSearchLucene in such a way, that it is now
// possible (though untested) to create a LuceneIndex without starting
// a Fedora-instance. It should be enough to implement up against the
// FieldSearchLuceneImpl.
//
// The purpose for this is to be able to test and tweak the lucene
// indexes without the need for the tedious ingesting through
// fedora. Given correct formatted data (which relies on fedora-types)
// an index can be created from an application importing this file
// (and maybe some more FieldSearchLucene files).
//
// Please notice:
// Only methods for creating, updating and deleting indexes have been
// refactored. The search method, findObjects, rely heavily on fedora
// since results from the search are not taken from the indexes but
// rather from the actual fedora documents.
//
// (JDA)
//

public final class FieldSearchLuceneImpl
{
    private static final Logger log = LoggerFactory.getLogger( FieldSearchLuceneImpl.class );

    private final LuceneFieldIndex luceneindexer;

    public FieldSearchLuceneImpl( LuceneFieldIndex luceneindexer ) {

        this.luceneindexer = luceneindexer;

    }


    public boolean delete( final String identifier ) throws CorruptIndexException {
        log.trace( "Entering delete" );
        try
        {
            this.luceneindexer.removeDocument( identifier );
        }
        catch( IOException ex )
        {
            log.warn( "Could not delete object {}: {}", identifier, ex.getMessage() );
            log.debug( "Could not delete object, returning false" );
            return false;
        }

        log.debug( "Successfully deleted object {}", identifier );
        return true;
    }


    public void update( Date fedoraCreateDate,
                        Date fedoraLastModDate,
                        String objectPID,
                        String objectState,
                        String objectLabel,
                        String ownerId,
                        DCFields dcFields,
                        Date dcmCreatedDate,
                        Set< RelationshipTuple > relations )
        throws CorruptIndexException, IOException
    {
        long startTimeNs = System.nanoTime();

        // This creation of SimpleDateFormat has been moved inside this function in order to fix bug#11968.
        // At some point this code may need refactoring. At that point, someone may think that
        // it is rediculous to create a new SimpleDateFormat object on every invocation of the
        // update-function. This "someone" should then sit down and read the documentation
        // for SimpleDateFormat in order to convince him- or herself that SimpleDateFormat
        // is not thread-safe. And since all the overridden functions in this class needs
        // to be thread-safe it would not be a good idea to move the creation of SimpleDateFormat
        // outside this function.
        final SimpleDateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss.SSS Z" );

        List<Pair<FedoraFieldName, String>> indexFieldsList = new ArrayList<Pair<FedoraFieldName, String>>();

        String stringCDate = formatter.format( fedoraCreateDate );
        String stringMDate = formatter.format( fedoraLastModDate );

        formatter.setTimeZone( TimeZone.getDefault() );
        Date localCDate = null;
        try
        {
            localCDate = formatter.parse( stringCDate );
        }
        catch( ParseException ex )
        {
            log.warn( "Could not parse {} as a date for localCDate: {}", stringCDate, ex.getMessage() );
        }

        Date localMDate = null;
        try
        {
            localMDate = formatter.parse( stringMDate );
        }
        catch( ParseException ex )
        {
            log.warn( "Could not parse {} as a date for localMDate: {}", stringMDate, ex.getMessage() );
        }

        formatter.setTimeZone( TimeZone.getTimeZone( "UTC" ) );

        if( null != localCDate )
        {
            String creationDate = formatter.format( localCDate );
            addPairToIndexFieldList( constructIndexableFieldPair( "cdate", creationDate ), indexFieldsList );
        }

        if( null != localMDate )
        {
            String modificationDate = formatter.format( localMDate );
            addPairToIndexFieldList( constructIndexableFieldPair( "mdate", modificationDate ), indexFieldsList );
        }
        formatter.setTimeZone( TimeZone.getDefault() );

        if( null != objectPID )
        {
            addPairToIndexFieldList( constructIndexableFieldPair( "pid", objectPID ), indexFieldsList );
        }

        if( null != objectState )
        {
            addPairToIndexFieldList( constructIndexableFieldPair( "state", objectState ), indexFieldsList );
        }

        if( null != objectLabel )
        {
            addPairToIndexFieldList( constructIndexableFieldPair( "label", objectLabel ), indexFieldsList );
        }

        if( null != ownerId )
        {
            addPairToIndexFieldList( constructIndexableFieldPair( "ownerid", ownerId ), indexFieldsList );
        }

        log.trace( "Preparing dublin core fields for indexing" );


        if( null != dcFields )
        {
            for( RDFName dcType : dcFields.getMap().keySet() )
            {
                for( DCField dcField : dcFields.getMap().get( dcType ) )
                {
                    addPairToIndexFieldList( constructIndexableFieldPair( dcType.getLocalName().toLowerCase(), dcField.getValue() ), indexFieldsList );
                }
            }

            Date createdDate = dcmCreatedDate;
            if( null != createdDate )
            {
                addPairToIndexFieldList( constructIndexableFieldPair( "dcmdate", formatter.format( createdDate ) ), indexFieldsList );
            }
        }

        for( RelationshipTuple relation : relations ) {
            String subject = relation.subject;
            String object = relation.object;
            String predicate = relation.predicate;
            log.debug( "PID [{}] Relation: subj[{}] pred[{}] obj[{}]", new Object[] { objectPID, subject, predicate, object } );

            if( "info:fedora/fedora-system:def/model#hasModel".equals( predicate )
                && "info:fedora/fedora-system:FedoraObject-3.0".equals( object ) ) {

                // Since all digital objects in the current fedora
                // repository have a hasModel relationship to the default
                // content model we skip this to avoid unnecessary clutter
                // in the lucene index.
                log.debug( "Skipping default content model relation: pid [{}]", objectPID );
                continue;
            } else {
                // Add string identifying relationship object to relObj
                // field.
                addPairToIndexFieldList( constructIndexableFieldPair( "relobj", object ), indexFieldsList );
                // When purging a relationship we need to know both
                // subject, predicate and object, we therefore add the
                // predicate and object strings joined by a pipe (|)
                // character to the relPredObj field.
                String predObjMap = String.format("%s|%s", predicate, object);
                addPairToIndexFieldList( constructIndexableFieldPair( "relpredobj", predObjMap ), indexFieldsList );
            }
        }

        log.debug( "{} fields ready for indexing", indexFieldsList.size() );
        log.trace( "Indexing fields" );

        // throws CorruptIndexException and IOException which must be handled by caller
        this.luceneindexer.indexFields( indexFieldsList, System.nanoTime() - startTimeNs );
    }


    /**
     * This helper method constructs indexable fields suitable for the lucene
     * indexer.
     *
     * @param name the name of the field to be indexed
     * @param value the string value of the field.
     * @return a {@link Pair} consisting of the field name (as an FedoraFieldName)
     * and a value.
     */
    private Pair<FedoraFieldName, String> constructIndexableFieldPair( final String name, final String value )
    {
        log.trace( "Constructing indexable fields from <{}, {}>", name, value );
        FedoraFieldName fieldName = FedoraFieldName.valueOf( name.toUpperCase() );
        Pair<FedoraFieldName, String> returnval = new Pair<FedoraFieldName, String>( fieldName, value );
        log.debug( "The field {} associated with the value {}", fieldName.toString(), value );

        return returnval;
    }


    private void addPairToIndexFieldList( final Pair<FedoraFieldName, String> indexablePair, final List<Pair<FedoraFieldName, String>> indexFieldsList )
    {
        log.trace( "Adding {} to list of fields for indexing (has value '{}')", indexablePair.getFirst().toString(), indexablePair.getSecond() );
        indexFieldsList.add( indexablePair );
    }


    public static String[] getValidatedReturnFields( final String[] fields )
    {
        int flength = fields.length;
        String[] validatedFields = new String[ flength ];
        log.trace( "Validating requested fields" );

        for ( int i = 0; i < flength; i++ )
        {
            String field = fields[i];
            try
            {
                FedoraFieldName.valueOf( field.toUpperCase() );
                validatedFields[i] = field;
            }
            catch( IllegalArgumentException ex )
            {
                log.warn( "Will not show results for requested field '{}': it is not indexed", field );
            }
        }

        return validatedFields;
    }

}
