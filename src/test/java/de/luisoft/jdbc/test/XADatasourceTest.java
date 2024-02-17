package de.luisoft.jdbc.test;

import de.luisoft.jdbcspy.ProxyXADatasource;
import de.luisoft.jdbcspy.proxy.ConnectionFactory;
import de.luisoft.jdbcspy.vendor.DerbyProxyXADatasource;
import de.luisoft.jdbcspy.vendor.HsqldbProxyXADatasource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.XAConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Some simple tests.
 */
public class XADatasourceTest {

    @Before
    public void setUp() throws SQLException, ClassNotFoundException {
//        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
//        initDb("jdbc:derby:memory:booksDerby;create=true");
        Class.forName("org.hsqldb.jdbc.JDBCDriver");
        initDb("jdbc:hsqldb:mem:booksHsql;create=true");
    }

   private void initDb(String insUrl) throws SQLException {
        Connection  conn = DriverManager.getConnection(insUrl);
        Statement   stmt = conn.createStatement();
        String      sSql = "CREATE TABLE book (book_id int primary key, title varchar(128))";

        try {
            stmt.execute(sSql);
            sSql = "INSERT INTO book VALUES (1, 'Effective Java'), (2, 'Core Java'), (3, 'Agility')";
            stmt.execute(sSql);
            stmt.close();
            conn.close();
        } catch (SQLException exError) {
            Assert.assertEquals(exError.getSQLState(), exError.getSQLState(), "initDb error: X0Y32");
        }
    }

    @Test
    public void minimalDerbyDriverURL() throws Exception {
        Class.forName("de.luisoft.jdbcspy.vendor.DerbyProxyDriver");
        System.out.println("starting ...");
        int iFound = getBooksWithDriverURL("proxy:jdbc:derby:booksdb;create=true");
        Assert.assertEquals(iFound, 2);
    }

    @Test
    public void minimalHsqldbDriverURL() throws Exception {
        Class.forName("de.luisoft.jdbcspy.vendor.HsqldbProxyDriver");
        System.out.println("starting ...");
        int iFound = getBooksWithDriverURL("proxy:jdbc:hsqldb:mem:booksdb;create=true");
        Assert.assertEquals(iFound, 2);
    }

    @Test
    public void minimalDerbyXA() throws Exception {
        DerbyProxyXADatasource datasource = new DerbyProxyXADatasource();
        datasource.setDatabaseName("booksdb");
        int iFound = getBooks(datasource, "select * from book");
        Assert.assertEquals(iFound, 2);

        iFound = getBooks(datasource, "select book_id, title from book");
        Assert.assertEquals(iFound, 2);
        System.out.println("connection dump:\n" + ConnectionFactory.dumpStatistics());
    }

    @Test
    public void minimalHsqldbXA() throws Exception {
        HsqldbProxyXADatasource datasource = new HsqldbProxyXADatasource();
        datasource.setDatabaseName("booksdb");
        int iFound = getBooks(datasource, "select * from book");
        Assert.assertEquals(iFound, 2);

        iFound = getBooks(datasource, "select book_id, title from book");
        Assert.assertEquals(iFound, 2);
        System.out.println("connection dump:\n" + ConnectionFactory.dumpStatistics());
    }

    private int getBooksWithDriverURL(String insUrl) throws Exception {
        System.out.println("starting ...");
        Connection con = DriverManager.getConnection(insUrl);
        PreparedStatement pstmt = con.prepareStatement("select * from book");
        ResultSet rs = pstmt.executeQuery();
        int iFound = 0;
        int iId = 0;

        while (rs.next()) {
            iId = rs.getInt(1);
            System.out.println(rs.getInt(1) + "/" + rs.getString(2));
            Assert.assertEquals(iId, ++iFound);
        }
        rs.close();
        pstmt.close();
        con.close();
        return iFound;
    }

    private int getBooks(ProxyXADatasource datasource, String insSql) throws SQLException {
        XAConnection xaConnection = datasource.getXAConnection();
        Connection con = xaConnection.getConnection();
        PreparedStatement stmt = con.prepareStatement(insSql);
        ResultSet rs = stmt.executeQuery();
        int iFound = 0;

        while (rs.next()) {
            iFound++;
        }
        rs.close();
        stmt.close();
        con.close();
        xaConnection.close();
        return iFound;
    }

}
