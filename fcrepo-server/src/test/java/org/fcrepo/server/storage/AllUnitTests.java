/* The contents of this file are subject to the license and copyright terms
 * detailed in the license directory at the root of the source tree (also
 * available online at http://fedora-commons.org/license/).
 */

package org.fcrepo.server.storage;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses( {org.fcrepo.server.storage.DefaultDOManagerTest.class,
                      org.fcrepo.server.storage.DefaultExternalContentManagerTest.class,
                      org.fcrepo.server.storage.translation.AllUnitTests.class,
                      org.fcrepo.server.storage.lowlevel.akubra.AllUnitTests.class})
public class AllUnitTests {

    // Supports legacy tests runners
    public static junit.framework.Test suite() throws Exception {

        junit.framework.TestSuite suite =
                new junit.framework.TestSuite(AllUnitTests.class.getName());

        suite.addTest(org.fcrepo.server.storage.DefaultDOManagerTest.suite());
        suite.addTest(org.fcrepo.server.storage.DefaultExternalContentManagerTest.suite());
        suite.addTest(org.fcrepo.server.storage.translation.AllUnitTests.suite());
        suite.addTest(org.fcrepo.server.storage.lowlevel.akubra.AllUnitTests.suite());
        suite.addTest(org.fcrepo.server.storage.DefaultDOManagerTest.suite());

        return suite;
    }
}
