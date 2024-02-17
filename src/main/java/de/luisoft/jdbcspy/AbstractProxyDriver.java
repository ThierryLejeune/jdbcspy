package de.luisoft.jdbcspy;

import de.luisoft.jdbcspy.proxy.ConnectionFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * This class wraps the functionality of the underlying driver.
 * <p>
 * Usage:
 * </p>
 * <ol>
 * <li>define the class of the underlying driver in the file dbinit.xml
 * <li>a file dbproxy.xml in your home folder will overwrite the dbinit.xml
 * settings
 * <li>Enter the example code
 * </ol>
 *
 * <pre>
 * // instanciate the driver wrapper
 * Class.forName("de.luisoft.jdbcspy.DBProxyDriver");
 *
 * // get the connection; the url has the suffix dbproxy://
 * ProxyConnection c = (ProxyConnection) DriverManager.getConnection("dbproxy://&lt;underlying driver URL&gt;");
 * PreparedStatement stmt;
 * ResultSet rs;
 *
 * // execute the 1st statement
 * stmt = c.prepareStatement("select * from users");
 * rs = stmt.executeQuery();
 * while (rs.next()) {
 * 	// System.out.println(rs.getString(1) + ";" + rs.getString(2)); // loop
 * 	// over 10 entries
 * }
 * rs.close();
 * stmt.close();
 *
 * // execute a 2nd statement
 * stmt = c.prepareStatement("select * from rights");
 * rs = stmt.executeQuery();
 * while (rs.next()) {
 * 	// System.out.println(rs.getString(1) + ";" + rs.getString(2)); // loop
 * 	// over 136 entries
 * }
 * rs.close();
 * stmt.close();
 *
 * // print out the connection including all statements
 * System.out.println("connection, including all statements=" + c.getStatements());
 * c.close();
 *
 * // print out the statement
 * System.out.println("stmt=" + stmt);
 *
 * // The connection object after the close does not contain the statements any
 * // more
 * System.out.println("connection after having been closed=" + c);
 *
 * // full dump, including the currently running statements, the long running
 * // statements, ...
 * System.out.println("c=" + c.getProxyConnectionMetaData().dumpStatistics());
 * </pre>
 *
 * <p>
 * The output will look like:
 * </p>
 *
 * <pre>
 * 	connection, including all statements=["select * from users" (0ms + 0ms; #=10) executed since 21:16:30.040 in TestCase.runTest:166, "select * from rights" (2ms + 4ms; #=136) executed since 21:16:30.088 in TestCase.runTest:166]
 *
 * 	stmt="select * from rights" (2ms + 4ms; #=136) executed since 21:16:30.088 in TestCase.runTest:166
 *
 * 	connection after having been closed=Connection[#stmt=2; #rs=10+136; duration=6ms, opened in TestCase.runTest:166]
 * 	c=[ExecutionTimeListener[
 * 	currently executing:
 *
 * 	executed but waiting to be closed:
 *
 * 	long running history (execTime + iterTime):
 * 	1: "select * from rights" (2ms + 4ms; #=136) executed since 21:16:30.088 in TestCase.runTest:166
 * 	2: "select * from users" (0ms + 0ms; #=10) executed since 21:16:30.040 in TestCase.runTest:166
 * 	]]
 *
 * 	[ExecutionLastStatementListener[
 * 	0: "select * from rights" (#=1, TestCase.runTest:166)
 * 	1: "select * from users" (#=1, TestCase.runTest:166)
 * 	]]
 *
 * 	[ExecutionRepeatCountListener[
 * 	1: #=1: "select * from users"
 * 	2: #=1: "select * from rights"
 * 	]]
 *
 * 	[ExecutionStatisticListener[online since 21:16:29 (280ms)
 * 	#stmt=2; #rs=146; duration=6ms; avgDuration=3ms; maxLength=20; avgLength=19;
 * 	time={<=3ms}=1, {5-10ms}=1;
 * 	length={<=20}=2
 * 	]]
 *
 * 	[ConnectionStatisticListener[
 * 	#conn=1; #max open conns=1; #max stmts/conn=2
 * 	]]
 *
 * </pre>
 */
public class AbstractProxyDriver implements Driver {
    private static final String csPROXY = "proxy:";
    private Driver moDriver;
    private ConnectionFactory moConnexionFactory;

    protected AbstractProxyDriver(String insDriverClass) {
        System.out.println("jdbcspy: register wrapper for driver " + insDriverClass);
        try {
            Class.forName(insDriverClass);
            Enumeration<Driver> itDriver = DriverManager.getDrivers();
            Driver              oDriver;

            while (itDriver.hasMoreElements()) {
                oDriver = itDriver.nextElement();
                if (oDriver.getClass().getName().equals(insDriverClass)) {
                    moDriver = oDriver;
                    System.out.println("register " + oDriver);
                    DriverManager.registerDriver(this);
                }
            }
            if (moDriver == null) {
                throw new IllegalArgumentException("did not find the driver " + insDriverClass);
            }
            moConnexionFactory = new ConnectionFactory();
        } catch (Exception exError) {
            exError.printStackTrace();
        }
    }

    @Override
    public boolean acceptsURL(String insUrl) throws SQLException {
        if (insUrl.startsWith(csPROXY)) {
            return moDriver.acceptsURL(insUrl.substring(csPROXY.length()));
        }
        return false;
    }

    @Override
    public Connection connect(String insUrl, Properties inlistInfo) throws SQLException {
        Connection oConnexion = moDriver.connect(insUrl.substring(csPROXY.length()), inlistInfo);
        return moConnexionFactory.getProxyConnection(oConnexion);
    }

    @Override
    public int getMajorVersion() {
        return moDriver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return moDriver.getMinorVersion();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String insUrl, Properties inlistInfo) throws SQLException {
        return moDriver.getPropertyInfo(insUrl, inlistInfo);
    }

    @Override
    public boolean jdbcCompliant() {
        return moDriver.jdbcCompliant();
    }

    @Override
    public String toString() {
        return ConnectionFactory.dumpStatistics();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return moDriver.getParentLogger();
    }
}
