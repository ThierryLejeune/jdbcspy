package de.luisoft.jdbcspy.proxy.listener.impl;

import de.luisoft.jdbcspy.proxy.ConnectionStatistics;
import de.luisoft.jdbcspy.proxy.handler.ConnectionInvocationHandler;
import de.luisoft.jdbcspy.proxy.listener.ConnectionEvent;
import de.luisoft.jdbcspy.proxy.listener.ConnectionListener;
import de.luisoft.jdbcspy.proxy.util.Utils;

import javax.sql.XAConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

/**
 * The ConnectionStatisticListener.
 */
public class ConnectionStatisticListener implements ConnectionListener {

    /**
     * the logger object for tracing
     */
    private static final Logger mTrace = Logger.getLogger(ConnectionStatisticListener.class.getName());

    /**
     * the connections
     */
    private final Set<ConnectionStatistics> mlistConStat = new HashSet<>();

    /**
     * the count
     */
    private int miCount;

    /**
     * the max current count
     */
    private int miMaxCurrentCount;

    /**
     * the max stmt count
     */
    private int miMaxStmtCount;

    /**
     * @see de.luisoft.jdbcspy.proxy.listener.ConnectionListener#openConnection
     */
    @Override
    public void openConnection(ConnectionEvent event) {
        miCount++;
        synchronized (mlistConStat) {
            mlistConStat.add(event.getConnectionStatistics());
            if (mlistConStat.size() > miMaxCurrentCount) {
                miMaxCurrentCount = mlistConStat.size();
            }
        }
    }

    /**
     * @see de.luisoft.jdbcspy.proxy.listener.ConnectionListener#openConnection
     */
    @Override
    public void closeConnection(ConnectionEvent event) {
        if (event.getConnectionStatistics().getItemCount() > miMaxStmtCount) {
            miMaxStmtCount = event.getConnectionStatistics().getItemCount();
        }

        synchronized (mlistConStat) {
            mlistConStat.remove(event.getConnectionStatistics());
        }
    }

    /**
     * @see de.luisoft.jdbcspy.proxy.listener.ConnectionListener#clearStatistics
     */
    @Override
    public void clearStatistics() {
        synchronized (mlistConStat) {
            mlistConStat.clear();
        }
        miCount = 0;
        miMaxCurrentCount = 0;
        miMaxStmtCount = 0;
    }

    /**
     * @see java.lang.Object#toString
     */
    @Override
    public String toString() {
        int             iX = 0;
        StringBuilder   sbText = new StringBuilder("[ConnectionStatisticListener[\n");

        sbText.append("  #conn=").append(miCount)
              .append("; #max open conns=").append(miMaxCurrentCount)
              .append("; #max stmts/conn=").append( miMaxStmtCount);

        synchronized (mlistConStat) {
            for (Iterator<ConnectionStatistics> it = mlistConStat.iterator(); it.hasNext(); iX++) {
                ConnectionInvocationHandler hndlr = (ConnectionInvocationHandler) it.next();
                Object c = hndlr.getUnderlyingConnection();

                if (iX == 1) {
                    sbText.append("; current:");
                }
                sbText.append("\n  ").append(iX).append(": ");
                sbText.append(hndlr.toString());

                try {
                    if (c instanceof Connection && ((Connection) c).getAutoCommit()
                            || c instanceof XAConnection && ((XAConnection) c).getConnection().getAutoCommit()) {
                        sbText.append("; autocommit");
                    }
                    sbText.append("; isolation=").append(Utils.getIsolationLevel(
                            (c instanceof Connection
                                    ? ((Connection) c).getTransactionIsolation()
                                    : ((XAConnection) c).getConnection().getTransactionIsolation())
                    ));
                    if (c instanceof Connection && ((Connection) c).isReadOnly()
                            || c instanceof XAConnection && ((XAConnection) c).getConnection().isReadOnly()) {
                        sbText.append("; readonly");
                    }
                } catch (SQLException e) {
                    sbText.append("; no connection properties");
                    mTrace.info("property reading failed, but ignored " + e);
                    it.remove();
                }
            }
            sbText.append("\n");
        }
        sbText.append("]]");

        return sbText.toString();
    }
}
