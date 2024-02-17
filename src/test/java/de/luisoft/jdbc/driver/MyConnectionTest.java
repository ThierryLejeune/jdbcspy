package de.luisoft.jdbc.driver;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Some simple tests.
 */
public class MyConnectionTest {

    private static final boolean debug = false;

    @Test
    public void testExecute() throws Exception {
        long start = System.currentTimeMillis();
        Connection conn = new MyConnection(100, 1000, 3000);
        PreparedStatement s = conn.prepareStatement("select * from x");
        ResultSet rs = s.executeQuery();
        long afterExec = System.currentTimeMillis();
        int iFound = 0;

        while (rs.next()) {
            iFound++;
        }
        long afterIter = System.currentTimeMillis();
        rs.close();
        s.close();
        conn.close();

        Assert.assertEquals(iFound, 100);
        Assert.assertTrue("t=" + (afterExec - start), afterExec - start >= 3000 && afterExec - start < 3050);
        Assert.assertTrue("t=" + (afterIter - afterExec), afterIter - afterExec >= 1000 && afterIter - afterExec < 1050);
    }
}
