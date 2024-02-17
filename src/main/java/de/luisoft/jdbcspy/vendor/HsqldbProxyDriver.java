package de.luisoft.jdbcspy.vendor;

import de.luisoft.jdbcspy.AbstractProxyDriver;
import de.luisoft.jdbcspy.ClientProperties;

public class HsqldbProxyDriver extends AbstractProxyDriver {

    static {
        new HsqldbProxyDriver();
    }

    public HsqldbProxyDriver() {
        super((String) ClientProperties.getProperty(ClientProperties.DB_HSQLDB_DRIVER_CLASS));
    }
}
