package de.luisoft.jdbcspy;

import org.hsqldb.jdbc.JDBCDriver;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Some simple tests.
 */
public class DatasourceTest {
    private static final String csDB = "booksdb";
    private static final String csURL = "jdbc:hsqldb:mem:" + csDB + ";sql.syntax_pgs=true";

    @BeforeClass
    public static void setUp() throws SQLException {
        DriverManager.registerDriver(new JDBCDriver());
        Connection  oConnexion = DriverManager.getConnection(csURL);
        Statement   stStmt = oConnexion.createStatement();
        String      sSql;

        try {
            sSql = "CREATE TABLE BOOK (book_id int primary key, title varchar(128))";
            stStmt.execute(sSql);
            sSql = "INSERT INTO BOOK VALUES (1, 'Effective Java'), (2, 'Core Java'), (3, 'Agility')";
            stStmt.execute(sSql);
            ResultSet      rsResult = stStmt.executeQuery("SELECT * FROM BOOK");
            int            iFound = 0;

            while (rsResult.next()) {
                iFound++;
            }
            Assert.assertEquals(iFound, 3);
            rsResult.close();
            stStmt.close();
            oConnexion.close();
        } catch (SQLException exError) {
            Assert.assertEquals(exError.getMessage(), exError.getSQLState(), "X0Y32");
        }
    }

    @Test
    public void testDump() throws Exception {
        ProxyDatasource dsProxy = new ProxyDatasource(ClientProperties.DB_HSQLDB_DATASOURCE_CLASS);
        dsProxy.setDriverUrl(csURL);
//        dsProxy.setDatabaseName(csDB);

        Connection          oConnexion = dsProxy.getConnection();
        PreparedStatement   psStmt = oConnexion.prepareStatement("SELECT * FROM BOOK");
        ResultSet           rsResult = psStmt.executeQuery();
        int                 iFound = 0;

        while (rsResult.next()) {
            iFound++;
        }
        Assert.assertEquals(iFound, 3);
        rsResult.close();
        psStmt.close();
        oConnexion.close();
        System.out.println("connexion=" + oConnexion);
    }

    @Test
    public void testDumpWithDefault() throws Exception {
        ProxyDatasource dsProxy = new ProxyDatasource();
        dsProxy.setDriverUrl(csURL);

        Connection          oConnexion = dsProxy.getConnection();
        PreparedStatement   psStmt = oConnexion.prepareStatement("SELECT * FROM BOOK");
        ResultSet           rsResult = psStmt.executeQuery();
        int                 iFound = 0;

        while (rsResult.next()) {
            iFound++;
        }
        Assert.assertEquals(iFound, 3);
        rsResult.close();
        psStmt.close();
        oConnexion.close();
        System.out.println("connexion=" + oConnexion);
    }
}
