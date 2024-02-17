package de.luisoft.jdbcspy.proxy.listener;

import de.luisoft.jdbcspy.proxy.ConnectionStatistics;

/**
 * The Connection Event class.
 */
public class ConnectionEvent {

    /**
     * the proxy object
     */
    private final ConnectionStatistics moConnexionStat;

    /**
     * Constructor.
     *
     * @param inoConStat the proxy object
     */
    public ConnectionEvent(ConnectionStatistics inoConStat) {
        moConnexionStat = inoConStat;
    }

    /**
     * Get the connection statistics object.
     *
     * @return Object
     */
    public ConnectionStatistics getConnectionStatistics() {
        return moConnexionStat;
    }
}
