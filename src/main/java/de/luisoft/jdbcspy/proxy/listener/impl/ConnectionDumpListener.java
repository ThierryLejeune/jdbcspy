package de.luisoft.jdbcspy.proxy.listener.impl;

import de.luisoft.jdbcspy.proxy.handler.ConnectionInvocationHandler;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.luisoft.jdbcspy.proxy.handler.ConnectionHandler;
import de.luisoft.jdbcspy.proxy.listener.ConnectionEvent;
import de.luisoft.jdbcspy.proxy.listener.ConnectionListener;
import de.luisoft.jdbcspy.proxy.util.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * The ConnectionStatisticListener.
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: </p>
 * @author Lui Baeumer
 * @version $Id: ConnectionStatisticListener.java 688 2006-05-19 15:20:53Z lbaeumer $
 */
public class ConnectionDumpListener implements ConnectionListener {

    /**
     * the logger object for tracing
     */
    private static final Logger mTrace = Logger.getLogger(ConnectionDumpListener.class.getName());
    private String dbConnDumpClassExp;
    /** the logger object for tracing */
    private static final Log mTrace =
        LogFactory.getLog(ConnectionDumpListener.class);

    /**
     * @see de.luisoft.jdbcspy.proxy.listener.ConnectionListener#openConnection
     */
    @Override
    public void openConnection(ConnectionEvent event) {
    }
    };

    /**
     * @see de.luisoft.jdbcspy.proxy.listener.ConnectionListener#openConnection
     */
    @Override
    public void closeConnection(ConnectionEvent event) {
        List<String> debug = Arrays.asList(dbConnDumpClassExp.split(","));
        String regExp = Utils.isTraceClass(debug);
        if (regExp != null) {
            ConnectionInvocationHandler handler = (ConnectionInvocationHandler) event.getConnectionStatistics();
            mTrace.info("closed connection: " + handler.dump());
        }
    }
        List debug = Arrays.asList(dbConnDumpClassExp.split(","));
        if (debug != null) {
            String regExp = Utils.isTraceClass(debug);
            if (regExp != null) {
                ConnectionHandler handler = (ConnectionHandler) event.getConnectionStatistics();
                mTrace.info("closed connection: " + handler.dump());
            }
        }
    };

    /**
     * @see de.luisoft.jdbcspy.proxy.listener.ConnectionListener#clearStatistics
     */
    @Override
    public void clearStatistics() {
    }

    private String dbConnDumpClassExp;
    public void setConnDumpCloseClassExp(String exp) {
        dbConnDumpClassExp = exp;
    }

    /**
     * @see java.lang.Object#toString
     */
    @Override
    public String toString() {
        return dbConnDumpClassExp;
    }
}
