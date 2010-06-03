package com.yahoo.ycsb.db;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * 
 * 
 * mysql.driver=com.mysql.jdbc.Driver <br/>
 * 
 * mysql.url=jdbc:mysql://localhost:3306/ycsb <br/>
 * mysql.user=user <br/>
 * mysql.password=password <br/>
 * 
 * mysql.pool.min=10 <br/>
 * mysql.pool.max=20 <br/>
 * mysql.pool.acquireIncrement=5 <br/>
 * 
 * @author ypai
 * 
 */
public class MysqlClient extends DB {

    private static ComboPooledDataSource dataSource;

    private static final ReentrantLock _lock = new ReentrantLock();

    private String tableName = "usertable";

    @Override
    public void init() throws DBException {
        Properties props = getProperties();

        _lock.lock();
        try {
            if (dataSource == null) {

                log("info", "Initializing connection pool...", null);

                // create db pool
                dataSource = new ComboPooledDataSource();
                dataSource.setDriverClass(props.getProperty("mysql.driver"));

                // driver
                dataSource.setJdbcUrl(props.getProperty("mysql.url"));
                dataSource.setUser(props.getProperty("mysql.user"));
                dataSource.setPassword(props.getProperty("mysql.password"));

                // the settings below are optional -- c3p0 can work with
                // defaults
                dataSource.setMinPoolSize(Integer.parseInt(props
                        .getProperty("mysql.pool.min")));
                dataSource.setAcquireIncrement(Integer.parseInt(props
                        .getProperty("mysql.pool.acquireIncrement")));
                dataSource.setMaxPoolSize(Integer.parseInt(props
                        .getProperty("mysql.pool.max")));

            }
        } catch (PropertyVetoException e) {
            throw new DBException(e + "", e);
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public int delete(String table, String key) {
        Connection conn = null;
        int rowsAffected = 0;
        try {
            conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement("DELETE FROM "
                    + tableName + " WHERE id=?");

            rowsAffected = stmt.executeUpdate();

        } catch (SQLException e) {
            log("error", e + "", e);

        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                log("error", e + "", e);
            }
        }

        return rowsAffected > 0 ? 0 : 1;
    }

    @Override
    public int insert(String table, String key, HashMap<String, String> values) {
        Connection conn = null;
        PreparedStatement stmt = null;
        int rowsAffected = 0;
        try {
            conn = dataSource.getConnection();

            StringBuilder sb = new StringBuilder();
            Iterator<String> iter = values.keySet().iterator();
            String k = null;
            int valuesSize = values.size();
            String[] vals = new String[valuesSize];
            int i = 0;
            while (iter.hasNext()) {
                k = iter.next();
                sb.append(k);
                sb.append("=?,");
                vals[i] = values.get(k);
                i++;
            }
            // sb.setLength(sb.length() - 1);
            String sql = "INSERT INTO " + tableName + " SET " + sb.toString()
                    + "id=?";
            stmt = conn.prepareStatement(sql);

            // logger.info(sql);

            for (i = 1; i <= valuesSize; i++) {
                stmt.setString(i, vals[i - 1]);
            }
            stmt.setString(i, key);

            rowsAffected = stmt.executeUpdate();

        } catch (SQLException e) {
            log("error", e + "", e);

        } finally {
            try {
                if (stmt != null)
                    stmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                log("error", e + "", e);
            }
        }

        return rowsAffected > 0 ? 0 : 1;

    }

    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, String> result) {
        Connection conn = null;
        PreparedStatement stmt = null;
        boolean success = false;
        try {
            conn = dataSource.getConnection();

            StringBuilder sb = new StringBuilder();
            Iterator<String> iter = null;
            if (fields != null) {
                iter = fields.iterator();
                while (iter.hasNext()) {
                    sb.append(iter.next());
                    sb.append(',');
                }
                sb.setLength(sb.length() - 1);
            } else {
                sb.append("*");
            }
            stmt = conn.prepareStatement("SELECT " + sb.toString() + " FROM "
                    + tableName + " WHERE id=?");
            stmt.setString(1, key);
            success = stmt.execute();

            ResultSet rs = stmt.getResultSet();
            if (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int numCols = rsmd.getColumnCount();
                String k = null;
                for (int i = 1; i <= numCols; i++) {
                    k = rsmd.getColumnLabel(i);
                    result.put(k, rs.getString(k));
                }
            }

        } catch (SQLException e) {
            log("error", e + "", e);

        } finally {
            try {
                if (stmt != null)
                    stmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                log("error", e + "", e);
            }
        }

        return success ? 0 : 1;
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, String>> result) {
        throw new UnsupportedOperationException(
                "MySql YCSB client does not support range scans!");
    }

    @Override
    public int update(String table, String key, HashMap<String, String> values) {
        Connection conn = null;
        PreparedStatement stmt = null;
        int rowsAffected = 0;
        try {
            StringBuilder sb = new StringBuilder();
            Iterator<String> iter = values.keySet().iterator();
            int numValues = values.size();
            String[] vals = new String[numValues];
            String k = null;
            int i = 0;
            while (iter.hasNext()) {
                k = iter.next();
                sb.append(k);
                sb.append("=?,");
                vals[i] = values.get(k);
                i++;
            }
            sb.setLength(sb.length() - 1);

            conn = dataSource.getConnection();
            stmt = conn.prepareStatement("UPDATE " + tableName + " SET "
                    + sb.toString() + " WHERE id=?");

            for (i = 1; i <= numValues; i++) {
                stmt.setString(i, vals[i - 1]);
            }
            stmt.setString(i, key);
            rowsAffected = stmt.executeUpdate();

        } catch (SQLException e) {
            log("error", e + "", e);

        } finally {
            try {
                if (stmt != null)
                    stmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                log("error", e + "", e);
            }
        }

        return rowsAffected > 0 ? 0 : 1;
    }

    /**
     * Simple logging method.
     * 
     * @param level
     * @param message
     * @param e
     */
    protected void log(String level, String message, Exception e) {
        System.out.println(message);
        if ("error".equals(level)) {
            System.err.println(message);
        }
        if (e != null) {
            e.printStackTrace(System.out);
            e.printStackTrace(System.err);
        }
    }
}
