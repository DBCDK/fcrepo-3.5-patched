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


/**
 * This enum represents all the fields that are recognized in the fedora server
 * as well as in the indices.
 */
enum FedoraFieldName
{
    PID( "pid_eq", false ),
    LABEL( "label_eq", false ),
    STATE( "state_eq", false ),
    OWNERID( "ownerid_eq", false ),
    CDATE( "cdate_eq", true ),
    MDATE( "mdate_eq", true ),
    TITLE( "title_eq", false ),
    CREATOR( "creator_eq", false ),
    SUBJECT( "subject_eq", false ),
    DESCRIPTION( "description_eq", false ),
    PUBLISHER( "publisher_eq", false ),
    CONTRIBUTOR( "contributor_eq", false ),
    DATE( "date_eq", true ),
    TYPE( "type_eq", false ),
    FORMAT( "format_eq", false ),
    IDENTIFIER( "identifier_eq", false ),
    SOURCE( "source_eq", false ),
    LANGUAGE( "language_eq", false ),
    RELATION( "relation_eq", false ),
    COVERAGE( "coverage_eq", false ),
    RIGHTS( "rights_eq", false ),
    DCMDATE( "dcm_date_eq", true ),
    RELOBJ( "relobj_eq", false ),
    RELPREDOBJ( "relpredobj_eq", false );

    private final String eq;
    private final boolean isDateField;
    private FedoraFieldName( final String equalsFieldName, final boolean isFieldDateField )
    {
        this.eq = equalsFieldName;
        this.isDateField = isFieldDateField;
    }

    public String equalsFieldName()
    {
        return this.eq;
    }

    public boolean isDateField()
    {
        return this.isDateField;
    }

    /**
     *
     * @return the lowercase name of the indexable (ie. the searchable) field
     */
    @Override
    public String toString()
    {
        return this.name().toLowerCase();
    }
}
