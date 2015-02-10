/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also 
 * available online at http://fedora-commons.org/license/).
 */

package org.fcrepo.server.storage.translation;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class NamespaceHandler {
    private final Map<String, Deque<String>> prefixes = new HashMap<String, Deque<String>>();
            
    public String getNamespace(String prefix){
        Deque<String> namespaceStack = prefixes.get(prefix);
        return namespaceStack.getLast();
    }
    
    public void removeNamespace(String prefix){
        Deque<String> namespaceStack = prefixes.get(prefix);
        if (namespaceStack != null) {
            namespaceStack.removeLast();
        }
    }
    
    public void addNamespace(String prefix, String uri){
        Deque<String> namespaceStack = prefixes.get(prefix);
        if(namespaceStack == null){
            namespaceStack = new ArrayDeque<String>();
            prefixes.put(prefix, namespaceStack);
        }
        namespaceStack.add(uri);
    }
    
    public void clear(){
        prefixes.clear();
    }
}
