package de.luisoft.jdbcspy;

import de.luisoft.jdbcspy.proxy.listener.ConnectionListener;
import de.luisoft.jdbcspy.proxy.listener.ExecutionFailedListener;
import de.luisoft.jdbcspy.proxy.listener.ExecutionListener;
import de.luisoft.jdbcspy.proxy.util.Utils;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import de.luisoft.jdbcspy.proxy.listener.ConnectionListener;
import de.luisoft.jdbcspy.proxy.listener.ExecutionFailedListener;
import de.luisoft.jdbcspy.proxy.listener.ExecutionListener;
import de.luisoft.jdbcspy.proxy.util.Utils;

/**
 * The Properties class.
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: </p>
 * @author Lui Baeumer
 * @version $Id: ClientProperties.java,v 1.2 2010/03/23 20:01:04 d292734 Exp $
 */
public final class ClientProperties {

    /**
     * enable the proxy
     */
    /** A Logger. */
    private static final Log mTrace = LogFactory.getLog(ClientProperties.class);

    /** the db init file */
    private static final String DBINIT_FILE = "/de/luisoft/jdbcspy/dbinit.xml";

    /** enable the proxy */
    public static final String DB_ENABLE_PROXY_INITIALLY = "EnableProxyInitially";
    /**
     * throw warnings
     */

    /** throw warnings */
    public static final String DB_THROW_WARNINGS = "ThrowWarnings";
    /**
     * driver class
     */

    /** driver class */
    public static final String DB_DRIVER_CLASS = "DriverClass";
    /**
     * driver class
     */
    public static final String DB_DATASOURCE_CLASS = "DatasourceClass";
    /**
     * driver class
     */
    public static final String DB_XA_DATASOURCE_CLASS = "XADatasourceClass";

    public static final String DB_DERBY_DRIVER_CLASS = "Derby_DriverClass";
    public static final String DB_DERBY_XA_DATASOURCE_CLASS = "Derby_XADatasourceClass";
    public static final String DB_DERBY_DATASOURCE_CLASS = "Derby_DatasourceClass";
    public static final String DB_HSQLDB_DRIVER_CLASS = "Hsqldb_DriverClass";
    public static final String DB_HSQLDB_XA_DATASOURCE_CLASS = "Hsqldb_XADatasourceClass";
    public static final String DB_HSQLDB_DATASOURCE_CLASS = "Hsqldb_DatasourceClass";
    public static final String DB_DB2_DRIVER_CLASS = "DB2_DriverClass";
    public static final String DB_DB2_XA_DATASOURCE_CLASS = "DB2_XADatasourceClass";
    public static final String DB_DB2_DATASOURCE_CLASS = "DB2_DatasourceClass";
    public static final String DB_MSSQL_DRIVER_CLASS = "Mssql_DriverClass";
    public static final String DB_MSSQL_XA_DATASOURCE_CLASS = "Mssql_XADatasourceClass";
    public static final String DB_MSSQL_DATASOURCE_CLASS = "Mssql_DatasourceClass";
    public static final String DB_MYSQL_DRIVER_CLASS = "Mysql_DriverClass";
    public static final String DB_MYSQL_XA_DATASOURCE_CLASS = "Mysql_XADatasourceClass";
    public static final String DB_MYSQL_DATASOURCE_CLASS = "Mysql_DatasourceClass";
    public static final String DB_POSTGRESQL_DRIVER_CLASS = "PostgreSql_DriverClass";
    public static final String DB_POSTGRESQL_XA_DATASOURCE_CLASS = "PostgreSql_XADatasourceClass";
    public static final String DB_POSTGRESQL_DATASOURCE_CLASS = "PostgreSql_DatasourceClass";
    public static final String DB_ORACLE_DRIVER_CLASS = "Oracle_DriverClass";
    public static final String DB_ORACLE_XA_DATASOURCE_CLASS = "Oracle_XADatasourceClass";
    public static final String DB_ORACLE_DATASOURCE_CLASS = "Oracle_DatasourceClass";
    /** the threshold for the next method */
    public static final String DB_RESULTSET_NEXT_TIME_THRESHOLD =
        "ResultSetNextTimeThreshold";

    /** the threshold for the resultset iteration */
    public static final String DB_RESULTSET_TOTAL_TIME_THRESHOLD =
        "ResultSetTotalTimeThreshold";

    /**
     * the threshold for the next method
     */
    public static final String DB_RESULTSET_NEXT_TIME_THRESHOLD = "ResultSetNextTimeThreshold";
    /**
     * the threshold for the resultset iteration
     */
    public static final String DB_RESULTSET_TOTAL_TIME_THRESHOLD = "ResultSetTotalTimeThreshold";
    /**
     * the threshold for the resultset iteration
     */
    public static final String DB_RESULTSET_TOTAL_SIZE_THRESHOLD = "ResultSetTotalSizeThreshold";
    /**
     * the threshold
     */
    public static final String DB_STMT_EXECUTE_TIME_THRESHOLD = "StmtExecuteTimeThreshold";
    /**
     * the threshold
     */
    public static final String DB_STMT_TOTAL_TIME_THRESHOLD = "StmtTotalTimeThreshold";
    /**
     * the threshold
     */
    public static final String DB_STMT_TOTAL_SIZE_THRESHOLD = "StmtTotalSizeThreshold";
    /**
     * the threshold
     */
    public static final String DB_CONN_TOTAL_TIME_THRESHOLD = "ConnTotalTimeThreshold";
    /**
     * the threshold
     */
    public static final String DB_CONN_TOTAL_SIZE_THRESHOLD = "ConnTotalSizeThreshold";
    /**
     * maximum number of characters to be displayed of sql string
     */
    public static final String DB_DISPLAY_SQL_STRING_MAXLEN = "DisplaySqlStringMaxlen";
    /**
     * remove hints
     */
    /** the threshold for the resultset iteration */
    public static final String DB_RESULTSET_TOTAL_SIZE_THRESHOLD =
        "ResultSetTotalSizeThreshold";

    /** the threshold */
    public static final String DB_STMT_EXECUTE_TIME_THRESHOLD =
        "StmtExecuteTimeThreshold";

    /** the threshold */
    public static final String DB_STMT_TOTAL_TIME_THRESHOLD =
        "StmtTotalTimeThreshold";

    /** the threshold */
    public static final String DB_STMT_TOTAL_SIZE_THRESHOLD =
        "StmtTotalSizeThreshold";

    /** the threshold */
    public static final String DB_CONN_TOTAL_TIME_THRESHOLD =
        "ConnTotalTimeThreshold";

    /** the threshold */
    public static final String DB_CONN_TOTAL_SIZE_THRESHOLD =
        "ConnTotalSizeThreshold";

    /** maximum number of characters to be displayed of sql string */
    public static final String DB_DISPLAY_SQL_STRING_MAXLEN =
        "DisplaySqlStringMaxlen";

    /** display entity beans */
    public static final String DB_DISPLAY_ENTITY_BEANS = "DisplayEntityBeans";

    /** remove hints */
    public static final String DB_REMOVE_HINTS = "RemoveHints";
    /**
     * ignore not closed objects
     */
    public static final String DB_IGNORE_NOT_CLOSED_OBJECTS = "IgnoreNotClosedObjects";
    /**
     * enable size evaluation
     */
    public static final String DB_ENABLE_SIZE_EVALUATION = "EnableSizeEvaluation";
    /**
     * debug beans
     */

    /** ignore not closed objects */
    public static final String DB_IGNORE_NOT_CLOSED_OBJECTS =
        "IgnoreNotClosedObjects";

    /** ignore double closed objects */
    public static final String DB_IGNORE_DOUBLE_CLOSED_OBJECTS =
        "IgnoreDoubleClosedObjects";

    /** enable size evaluation */
    public static final String DB_ENABLE_SIZE_EVALUATION =
        "EnableSizeEvaluation";

    /** debug beans */
    public static final String DB_STMT_DEBUG_CLASS_EXP = "StmtDebugClassExp";
    /**
     * historize statement
     */
    public static final String DB_STMT_HISTORIZE_CLASS_EXP = "StmtHistorizeClassExp";
    /**
     * debug beans
     */

    /** historize statement */
    public static final String DB_STMT_HISTORIZE_CLASS_EXP =
        "StmtHistorizeClassExp";

    /** debug beans */
    public static final String DB_STMT_DEBUG_SQL_EXP = "StmtDebugSQLExp";
    /**
     * historize beans
     */

    /** historize beans */
    public static final String DB_STMT_HISTORIZE_SQL_EXP = "StmtHistorizeSQLExp";
    /**
     * the trace depth
     */

    /** the trace depth */
    public static final String DB_TRACE_DEPTH = "TraceDepth";
    public static final String DB_TRACE_CLASS_IGNORE_REGEXP = "TraceClassIgnoreRegExp";

    /**
     * dump after shutdown
     */
    /** dump after shutdown */
    public static final String DB_DUMP_AFTER_SHUTDOWN = "DumpAfterShutdown";
    /**
     * dump interval in s
     */

    /** dump interval in s */
    public static final String DB_DUMP_INTERVAL = "DumpInterval";
    /**
     * dump interval in s
     */
    public static final String VERBOSE = "Verbose";

    /**
     * A Logger.
     */
    private static final Logger mTrace = Logger.getLogger("jdbcspy.properties");
    /**
     * the db init file
     */
    private static final String DBINIT_FILE = "/de/luisoft/jdbcspy/dbinit.xml";
    /**
     * all int values
     */
    private static final List<String> mIntValues = Arrays.asList(DB_RESULTSET_NEXT_TIME_THRESHOLD,
            DB_RESULTSET_TOTAL_TIME_THRESHOLD, DB_RESULTSET_TOTAL_SIZE_THRESHOLD, DB_STMT_EXECUTE_TIME_THRESHOLD,
            DB_STMT_TOTAL_TIME_THRESHOLD, DB_STMT_TOTAL_SIZE_THRESHOLD, DB_CONN_TOTAL_TIME_THRESHOLD,
            DB_CONN_TOTAL_SIZE_THRESHOLD, DB_DISPLAY_SQL_STRING_MAXLEN, DB_TRACE_DEPTH, DB_DUMP_INTERVAL);
    /**
     * all boolean values
     */
    private static final List<String> mBoolValues = Arrays.asList(DB_ENABLE_PROXY_INITIALLY, DB_THROW_WARNINGS,
            DB_REMOVE_HINTS, DB_IGNORE_NOT_CLOSED_OBJECTS,
            DB_ENABLE_SIZE_EVALUATION, DB_DUMP_AFTER_SHUTDOWN, VERBOSE);
    /**
     * all string values
     */
    private static final List<String> mStringValues = Arrays.asList(
            DB_DRIVER_CLASS,
            DB_DATASOURCE_CLASS, DB_XA_DATASOURCE_CLASS,
            DB_DB2_DRIVER_CLASS, DB_DB2_DATASOURCE_CLASS, DB_DB2_XA_DATASOURCE_CLASS,
            DB_DERBY_DRIVER_CLASS, DB_DERBY_DATASOURCE_CLASS, DB_DERBY_XA_DATASOURCE_CLASS,
            DB_HSQLDB_DRIVER_CLASS, DB_HSQLDB_DATASOURCE_CLASS, DB_HSQLDB_XA_DATASOURCE_CLASS,
            DB_MSSQL_DRIVER_CLASS, DB_MSSQL_DATASOURCE_CLASS, DB_MSSQL_XA_DATASOURCE_CLASS,
            DB_MYSQL_DRIVER_CLASS, DB_MYSQL_DATASOURCE_CLASS, DB_MYSQL_XA_DATASOURCE_CLASS,
            DB_ORACLE_DRIVER_CLASS, DB_ORACLE_DATASOURCE_CLASS, DB_ORACLE_XA_DATASOURCE_CLASS,
            DB_TRACE_CLASS_IGNORE_REGEXP);
    /**
     * all list values
     */
    private static final List<String> mListValues = Arrays.asList(DB_STMT_DEBUG_CLASS_EXP,
            DB_STMT_HISTORIZE_CLASS_EXP, DB_STMT_DEBUG_SQL_EXP, DB_STMT_HISTORIZE_SQL_EXP);

    /**
     * the instance
     */
    /** the instance */
    private static ClientProperties instance;
    /**
     * the values
     */
    private final Map<String, Object> values;
    /**
     * the listener list
     */
    private final List<ExecutionListener> mListener = new ArrayList<>();
    /**
     * the listener list
     */
    private final List<ExecutionFailedListener> mFailedListener = new ArrayList<>();
    /**
     * the connection listener list
     */
    private final List<ConnectionListener> mConnectionListener = new ArrayList<>();
    /**
     * The XML handler.
     */
    private final DefaultHandler mHandler = new DefaultHandler() {
        private ConnectionListener connectionListener;
        private ExecutionListener executionListener;
        private ExecutionFailedListener executionFailedListener;

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) {
            if ("property".equals(qName)) {
                String name = attributes.getValue("name").trim();
                String value = attributes.getValue("value").trim();

                if (connectionListener == null && executionFailedListener == null && executionListener == null) {
                    boolean found = false;
                    if (mBoolValues.contains(name)) {
                        values.put(name, Boolean.parseBoolean(value));
                        found = true;
                    } else if (mIntValues.contains(name)) {
                        values.put(name, Integer.parseInt(value));
                        found = true;
                    } else if (mStringValues.contains(name)) {
                        values.put(name, value);
                        found = true;
                    } else if (mListValues.contains(name)) {
                        String[] s = value.split(",");
                        List<String> l = new ArrayList<>(Arrays.asList(s));
                        values.put(name, l);
                        found = true;
                    }

                    if (!found) {
                        mTrace.warning("Could not find the property " + name + ".");
                    }
                } else {
                    // this must be a value assigned by a listener
                    if (connectionListener != null) {
                        Utils.setProperty(connectionListener, name, value);
                    } else if (executionFailedListener != null) {
                        Utils.setProperty(executionFailedListener, name, value);
                    } else {
                        Utils.setProperty(executionListener, name, value);
                    }
                }
            } else if (qName.endsWith("listener")) {

                String classname = attributes.getValue("class");
                try {
                    Class<?> c = Class.forName(classname);
                    Object cl = c.getDeclaredConstructor().newInstance();
    /** all int values */
    private static List mIntValues = Arrays.asList(new String[] {
        DB_RESULTSET_NEXT_TIME_THRESHOLD,
        DB_RESULTSET_TOTAL_TIME_THRESHOLD,
        DB_RESULTSET_TOTAL_SIZE_THRESHOLD,
        DB_STMT_EXECUTE_TIME_THRESHOLD,
        DB_STMT_TOTAL_TIME_THRESHOLD,
        DB_STMT_TOTAL_SIZE_THRESHOLD,
        DB_CONN_TOTAL_TIME_THRESHOLD,
        DB_CONN_TOTAL_SIZE_THRESHOLD,
        DB_DISPLAY_SQL_STRING_MAXLEN,
        DB_TRACE_DEPTH,
        DB_DUMP_INTERVAL
    });

                    switch (qName) {
                        case "connectionlistener":
                            mConnectionListener.add((ConnectionListener) cl);
                            connectionListener = (ConnectionListener) cl;
                            executionFailedListener = null;
                            executionListener = null;
                            break;
                        case "executionfailedlistener":
                            mFailedListener.add((ExecutionFailedListener) cl);
                            executionFailedListener = (ExecutionFailedListener) cl;
                            connectionListener = null;
                            executionListener = null;
                            break;
                        case "executionlistener":
                            mListener.add((ExecutionListener) cl);
                            executionListener = (ExecutionListener) cl;
                            connectionListener = null;
                            executionFailedListener = null;
                            break;
                        default:
                            throw new IllegalArgumentException("The listener " + qName + " does not exist.");
                    }
    /** all boolean values */
    private static List mBoolValues = Arrays.asList(new String[] {
        DB_ENABLE_PROXY_INITIALLY,
        DB_THROW_WARNINGS,
        DB_DISPLAY_ENTITY_BEANS,
        DB_REMOVE_HINTS,
        DB_IGNORE_NOT_CLOSED_OBJECTS,
        DB_IGNORE_DOUBLE_CLOSED_OBJECTS,
        DB_ENABLE_SIZE_EVALUATION,
        DB_DUMP_AFTER_SHUTDOWN
    });

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    /** all string values */
    private static List mStringValues = Arrays.asList(new String[] {
        DB_DRIVER_CLASS
    });

        @Override
        public void endElement(String uri, String localName, String qName) {
    /** all list values */
    private static List mListValues = Arrays.asList(new String[] {
        DB_STMT_DEBUG_CLASS_EXP,
        DB_STMT_HISTORIZE_CLASS_EXP,
        DB_STMT_DEBUG_SQL_EXP,
        DB_STMT_HISTORIZE_SQL_EXP
    });

            if (qName.endsWith("listener")) {

                connectionListener = null;
                executionFailedListener = null;
                executionListener = null;
            }
        }
    };
    /** the values */
    private Map values;

    /**
     * Constructor.
     */
    private ClientProperties() {

        values = new LinkedHashMap<>();
        Properties p = new Properties();
        values = new LinkedHashMap();

        InputStream input =
        	ClientProperties.class.getResourceAsStream(DBINIT_FILE);

        try {
            p.load(ClientProperties.class.getResourceAsStream("/spyversion.properties"));
        } catch (IOException e) {
            // ignore
            SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            parser.parse(input, mHandler);
            input.close();
        }
        catch (Exception ex) {
            mTrace.info("Parsing the dbinit file " + DBINIT_FILE + " failed.",
                        ex);
            ex.printStackTrace();
        }

        mTrace.info("init jdbcspy " + p.get("version"));

        InputStream input = ClientProperties.class.getResourceAsStream(DBINIT_FILE);

        try {
            SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            parser.parse(input, mHandler);
            input.close();
        } catch (Exception ex) {
            throw new IllegalStateException("something's wrong here with dbinit.xml");
            input =	ClientProperties.class.getResourceAsStream("/dbproxy.xml");
            if (input != null) {
            	System.out.println("Reading dbproxy.xml configuration from classpath.");
	            SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
	            parser.parse(input, mHandler);
	            input.close();
            }
        }
        mTrace.info("initialized " + values);
        catch (Exception ex) {
            mTrace.info("Parsing the dbinit file dbproxy.xml failed.",
                        ex);
            ex.printStackTrace();
        }
    }

    /**
     * Get the instance.
     *
     * @return instance
     */
    private static ClientProperties getInstance() {
    public static ClientProperties getInstance() {
        if (instance == null) {
            instance = new ClientProperties();
            instance.init();
        }
        return instance;
    }

    /**
     * Init the client properties.
     */
    public void init() {

        File f = new File(System.getProperty("user.home") + "/dbproxy.xml");
        mTrace.debug("Reading properties from " + f);

        try {
            SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            parser.parse(f, mHandler);
        }
        catch (FileNotFoundException ex) {
            mTrace.info("The dbproxy configuration file " + f
                        + " does not exist.");
        }
        catch (Exception ex) {
            mTrace.error("parsing the file " + f + " failed", ex);
        }

        mTrace.debug("Reading system properties");
        readSystemProperties();
    }

    /**
     * Get the int value.
     *
     * @param flag the flag
     * @return the value
     */
    public static int getInt(String flag) {
        return (Integer) getInstance().values.get(flag);
    public int getInt(String flag) {
        return ((Integer) values.get(flag)).intValue();
    }

    /**
     * Get the boolean value.
     *
     * @param flag the flag
     * @return the value
     */
    public static boolean getBoolean(String flag) {
        return (Boolean) getInstance().values.get(flag);
    public boolean getBoolean(String flag) {
        return ((Boolean) values.get(flag)).booleanValue();
    }

    /**
     * Get the list value.
     *
     * @param flag the flag
     * @return the value
     */
    public static List<String> getList(String flag) {
        return (List<String>) getInstance().values.get(flag);
    public List getList(String flag) {
        return (List) values.get(flag);
    }

    /**
     * Get the property.
     *
     * @param flag the flag
     * @return the value
     */
    public static Object getProperty(String flag) {
        return getInstance().values.get(flag);
    public Object getProperty(String flag) {
        return values.get(flag);
    }

    /**
     * Get the int keys.
     *
     * @return String[]
     */
    public static List<String> getIntKeys() {
        return getInstance().mIntValues;
    public List getIntKeys() {
        return mIntValues;
    }

    /**
     * Get the boolean keys.
     *
     * @return String[]
     */
    public static List<String> getBooleanKeys() {
        return getInstance().mBoolValues;
    public List getBooleanKeys() {
        return mBoolValues;
    }

    /**
     * Get the string keys.
     * @return String[]
     */
    public List getStringKeys() {
        return mStringValues;
    }

    /**
     * Get the list keys.
     *
     * @return String[]
     */
    public static List<String> getListKeys() {
        return getInstance().mListValues;
    public List getListKeys() {
        return mListValues;
    }

    /**
     * @see java.lang.Object
     */
    public String toString() {
        return values.toString();
    }

    /**
     * Is the proxy enabled?
     *
     * @return boolean
     */
    public static boolean isInitiallyEnabled() {
        return (Boolean) getInstance().values.get(DB_ENABLE_PROXY_INITIALLY);
    public boolean isInitiallyEnabled() {
        return ((Boolean) values.get(DB_ENABLE_PROXY_INITIALLY)).booleanValue();
    }

    /**
     * Get the driver class
     * @return the driver class
     */
    public String getDriverClass() {
        return (String) values.get(DB_DRIVER_CLASS);
    }

    /**
     * Set am integer property.
     *
     * @param property String
     * @param value    the value
     */
    public static void setProperty(String property, Object value) {
    public void setProperty(String property, Object value) {
        if (value instanceof Boolean) {
            if (!mBoolValues.contains(property)) {
                throw new IllegalArgumentException("the boolean property " + property + " does not exist.");
                throw new IllegalArgumentException("the boolean property "
                    + property
                    + " does not exist.");
            }
            getInstance().values.put(property, value);
            mTrace.debug("set " + property + " to " + value);
            values.put(property, value);
            return;
        }
        if (value instanceof Integer) {
            if (!mIntValues.contains(property)) {
                throw new IllegalArgumentException("the int property " + property + " does not exist.");
                throw new IllegalArgumentException("the int property "
                    + property
                    + " does not exist.");
            }
            getInstance().values.put(property, value);
            mTrace.debug("set " + property + " to " + value);
            values.put(property, value);
            return;
        }
        if (value instanceof String) {
            if (!mStringValues.contains(property)) {
                throw new IllegalArgumentException("the string property " + property + " does not exist.");
                throw new IllegalArgumentException("the string property "
                    + property
                    + " does not exist.");
            }
            getInstance().values.put(property, value);
            mTrace.debug("set " + property + " to " + value);
            values.put(property, value);
            return;
        }
        if (value instanceof List) {
            if (!mListValues.contains(property)) {
                throw new IllegalArgumentException("the list property " + property + " does not exist.");
                throw new IllegalArgumentException("the list property "
                    + property
                    + " does not exist.");
            }
            getInstance().values.put(property, value);
            mTrace.debug("set " + property + " to " + value);
            values.put(property, value);
            return;
        }

        throw new IllegalArgumentException("the argument " + value + " is illegal");
    }

    public static List<ExecutionListener> getListener() {
        return getInstance().mListener;
    }

    public static List<ExecutionFailedListener> getFailedListener() {
        return getInstance().mFailedListener;
    }

    public static List<ConnectionListener> getConnectionListener() {
        return getInstance().mConnectionListener;
    }

    /**
     * Init the client properties.
     */
    private void init() {

        boolean init = false;

        InputStream input;
        try {
            input = ClientProperties.class.getResourceAsStream("/dbproxy.xml");
            if (input != null) {
                mTrace.info("jdbcspy: reading dbproxy.xml configuration from classpath.");
                SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
                parser.parse(input, mHandler);
                input.close();
                init = true;
            }
        } catch (Exception ex) {
            throw new IllegalArgumentException("Parsing the dbinit file dbproxy.xml failed.", ex);
        }

        File f = new File(System.getProperty("user.home") + "/dbproxy.xml");
        if (f.exists()) {
            mTrace.info("jdbcspy: reading properties from " + f);
            try {
                SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
                parser.parse(f, mHandler);
                init = true;
            } catch (FileNotFoundException ex) {
                mTrace.info("The dbproxy configuration file " + f + " does not exist.");
            } catch (Exception ex) {
                mTrace.log(Level.SEVERE, "parsing the file " + f + " failed", ex);
            }
        }

        if (!init) {
            System.out.println("jdbcspy: neither a file " + f + " nor a file dbproxy.xml in the classpath exists."
                    + " You can download an example of the file here https://github.com/lbaeumer/jdbcspy/blob/master/src/test/resources/dbproxy.xml");
        }

        readSystemProperties();
    }

    /**
     * @see java.lang.Object
     */
    @Override
    public String toString() {
        return values.toString();
    }

    private void readSystemProperties() {
        Properties p = System.getProperties();
        for (String key : mIntValues) {
        for (Iterator it = mIntValues.iterator(); it.hasNext(); ) {
            String key = (String) it.next();
            Object obj = p.get(key);
            if (obj != null) {
                try {
                    mTrace.debug("set " + key + " to " + obj);
                    values.put(key, Integer.valueOf((String) obj));
                } catch (Exception e) {
                    System.err.println("property for " + key + "=" + obj + " is not convertable to type int");
                    System.err.println("property for " + key + "="
                        + obj + " is not convertable to type int");
                }
            }
        }
        for (String key : mBoolValues) {
        for (Iterator it = mBoolValues.iterator(); it.hasNext(); ) {
            String key = (String) it.next();
            Object obj = p.get(key);
            if (obj != null) {
                try {
                    mTrace.debug("set " + key + " to " + obj);
                    values.put(key, Boolean.valueOf((String) obj));
                } catch (Exception e) {
                    System.err.println("property for " + key + "=" + obj + " is not convertable to type boolean");
                    System.err.println("property for " + key + "="
                        + obj + " is not convertable to type boolean");
                }
            }
        }
        for (String key : mStringValues) {
        for (Iterator it = mStringValues.iterator(); it.hasNext(); ) {
            String key = (String) it.next();
            Object obj = p.get(key);
            if (obj != null) {
                mTrace.debug("set " + key + " to " + obj);
                values.put(key, obj);
            }
        }
    }

    /** the listener list */
    private List mListener = new ArrayList();

    /** the listener list */
    private List mFailedListener = new ArrayList();

    /** the connection listener list */
    private List mConnectionListener = new ArrayList();

    public List getListener() {
    	return mListener;
    }
    public List getFailedListener() {
    	return mFailedListener;
    }
    public List getConnectionListener() {
    	return mConnectionListener;
    }
    /**
     * The XML handler.
     */
    private DefaultHandler mHandler = new DefaultHandler() {
    	private ConnectionListener connectionListener;
    	private ExecutionListener executionListener;
    	private ExecutionFailedListener executionFailedListener;

        public void startElement(String uri, String localName,
                                 String qName, Attributes attributes) {
            if ("property".equals(qName)) {
                String name = attributes.getValue("name");
                String value = attributes.getValue("value");

                if (connectionListener == null
                		&& executionFailedListener == null
                		&& executionListener == null) {
	                boolean found = false;
	                if (mBoolValues.contains(name)) {
	                    setProperty(name, Boolean.valueOf(value));
	                    found = true;
	                } else if (mIntValues.contains(name)) {
	                    setProperty(name, Integer.valueOf(value));
	                    found = true;
	                } else if (mStringValues.contains(name)) {
	                    setProperty(name, value);
	                    found = true;
	                } else if (mListValues.contains(name)) {
	                    List l = new ArrayList();
	                    String[] s = value.split(",");
	                    for (int j = 0; j < s.length; j++) {
	                        l.add(s[j]);
	                    }
	                    setProperty(name, l);
	                    found = true;
	                }

	                if (!found) {
	                    mTrace.warn("Could not find the property " + name + ".");
	                }
                } else {
                	// this must be a value assigned by a listener
                	if (connectionListener != null) {
                		Utils.setProperty(connectionListener, name, value);
                	} else if (executionFailedListener != null) {
                		Utils.setProperty(executionFailedListener, name, value);
                    } else if (executionListener != null) {
                    	Utils.setProperty(executionListener, name, value);
                    } else {
                    	mTrace.error("illegal state");
                    }
                }
            } else if (qName.endsWith("listener")) {

                String classname = attributes.getValue("class");
                try {
                	Class c = Class.forName(classname);
                	Object cl = c.newInstance();

                	if (qName.equals("connectionlistener")) {
                		mConnectionListener.add(cl);
                		connectionListener = (ConnectionListener) cl;
                	} else if (qName.equals("executionfailedlistener")) {
                		mFailedListener.add(cl);
                		executionFailedListener = (ExecutionFailedListener) cl;
                	} else if (qName.equals("executionlistener")) {
                		mListener.add(cl);
                		executionListener = (ExecutionListener) cl;
                	} else {
                		throw new IllegalArgumentException("The listener " + qName + " does not exist.");
                	}

                } catch (Exception e) {
                	e.printStackTrace();
                }

            }
        }

        public void endElement(String uri, String localName, String qName)
				throws SAXException {

        	if (qName.endsWith("listener")) {

            	connectionListener = null;
            	executionFailedListener = null;
            	executionListener = null;
        	}
		}
    };
}
