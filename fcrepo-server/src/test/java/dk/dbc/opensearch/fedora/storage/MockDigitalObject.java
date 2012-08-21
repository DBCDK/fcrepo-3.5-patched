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

import org.fcrepo.server.storage.types.AuditRecord;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.storage.types.DigitalObject;
import org.fcrepo.server.storage.types.RelationshipTuple;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;


/**
 *
 * @author stm
 */
@SuppressWarnings( "deprecation" )
public class MockDigitalObject implements DigitalObject{

    private String pid;
    private Date date;
    private final LinkedHashMap<String, List<Datastream>> datastreams;

    public MockDigitalObject()
    {
        this.datastreams = new LinkedHashMap<String, List<Datastream>>();
    }



    @Override
    public boolean isNew()
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    @Override
    public void setNew( boolean bln )
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    @Override
    public String getPid()
    {
        return this.pid;
    }


    @Override
    public void setPid( String string )
    {
        this.pid = string;
    }


    @Override
    public String getState()
    {
        return "A";
    }


    @Override
    public void setState( String string )
    {
    }


    @Override
    public String getOwnerId()
    {
        return "demo";
    }


    @Override
    public void setOwnerId( String string )
    {
    }


    @Override
    public String getLabel()
    {
        return "Label";
    }


    @Override
    public void setLabel( String string )
    {
    }


    @Override
    public Date getCreateDate()
    {
        if( null == this.date )
        {
            return new Date( System.currentTimeMillis() );
        }
        return this.date;
    }


    @Override
    public void setCreateDate( Date date )
    {
        this.date = date;
    }


    @Override
    public Date getLastModDate()
    {
        if( null == this.date )
        {
            return new Date( System.currentTimeMillis() );
        }
        return this.date;
    }


    @Override
    public void setLastModDate( Date date )
    {
    }


    @Override
    public List<AuditRecord> getAuditRecords()
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    @Override
    public Iterator<String> datastreamIdIterator()
    {
        return new ArrayList<String>( datastreams.keySet() ).iterator();
    }


    @Override
    public Iterable<Datastream> datastreams( String id )
    {
        if( ! datastreams.containsKey( id ) )
        {
            return new ArrayList<Datastream>();
        }
        return Collections
                .unmodifiableList(new ArrayList<Datastream>( datastreams.get(id)));
    }


    @Override
    public void addDatastreamVersion( Datastream dtstrm, boolean bln )
    {
        ArrayList<Datastream> list = new ArrayList<Datastream>();
        list.add( dtstrm );
        this.datastreams.put( dtstrm.DatastreamID, list);
    }


    @Override
    public void removeDatastreamVersion( Datastream dtstrm )
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    @Override
    @SuppressWarnings( "deprecation" )
    public Iterator<String> disseminatorIdIterator()
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    @Override
    @SuppressWarnings( "deprecation" )
    public List<org.fcrepo.server.storage.types.Disseminator> disseminators( String string )
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    @Override
    public String newDatastreamID()
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    @Override
    public String newDatastreamID( String string )
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    @Override
    public String newAuditRecordID()
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    @Override
    public void setExtProperty( String string, String string1 )
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    @Override
    public String getExtProperty( String string )
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    @Override
    public Map<String, String> getExtProperties()
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    @Override
    public boolean hasRelationship( PredicateNode pn, ObjectNode on )
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    @Override
    public Set<RelationshipTuple> getRelationships( PredicateNode pn, ObjectNode on )
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public boolean hasRelationship( SubjectNode sn, PredicateNode pn, ObjectNode on )
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    public Set<RelationshipTuple> getRelationships()
    {
        return new LinkedHashSet< RelationshipTuple >();
    }


    public Set<RelationshipTuple> getRelationships( SubjectNode sn, PredicateNode pn, ObjectNode on )
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    public List<String> getContentModels()
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }


    public boolean hasContentModel( ObjectNode on )
    {
        throw new UnsupportedOperationException( "Not supported yet." );
    }

}
