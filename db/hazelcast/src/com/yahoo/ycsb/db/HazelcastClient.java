package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.Hazelcast;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * 
 * @author ypai
 * 
 */
public class HazelcastClient extends DB {

    private static final int MAP = 1;
    private static final int QUEUE = 2;

    private boolean debug = false;
    private boolean superclient = false;
    private int dataStructureType = 1;

    private int pollTimeoutMs = 100;

    private com.hazelcast.client.HazelcastClient client;

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#init()
     */
    @Override
    public void init() throws DBException {
        super.init();
        if (System.getProperty("debug") != null) {
            log("info", "Debug mode:  using data structure name 'default'", null);
            this.debug = true;
        }
        Properties conf = this.getProperties();

        // check for datastructure type
        String dataStructureType = conf.getProperty("hc.dataStructureType");
        if ("queue".equalsIgnoreCase(dataStructureType)) {
            this.dataStructureType = QUEUE;
            log("info", "Testing QUEUE", null);

            String pollTimeoutMs = conf.getProperty("hc.queuePollTimeoutMs");
            if (pollTimeoutMs != null) {
                this.pollTimeoutMs = Integer.parseInt(pollTimeoutMs);
            }
            log("info", "QUEUE.poll timeout = " + this.pollTimeoutMs + " ms", null);
        } else if ("map".equalsIgnoreCase(dataStructureType)) {
            this.dataStructureType = MAP;
            log("info", "Testing MAP", null);
        } else {
            log("error", "Unknown data structure type:  " + dataStructureType + "; please specify with 'hc.dataStructureType' property!", null);
            System.exit(1);
        }

        // check if we are using superclient mode
        this.superclient = "true".equals(System.getProperty("hazelcast.super.client"));

        // not using superclient mode, so set up java client
        if (!superclient) {
            log("info", "Using regular Java client", null);
            String groupName = conf.getProperty("hc.groupName");
            String groupPassword = conf.getProperty("hc.groupPassword");
            String address = conf.getProperty("hc.address");
            if (address == null) {
                log("error", "No cluster address specified for client!  Use 'hc.address'!", null);
                System.exit(1);
            }
            this.client = com.hazelcast.client.HazelcastClient.newHazelcastClient(groupName, groupPassword, address);
        } else {
            log("info", "Using super client", null);
        }

    }

    protected ConcurrentMap<String, Map<String, String>> getMap(String table) {
        ConcurrentMap<String, Map<String, String>> retval = null;
        if (this.superclient) {
            retval = Hazelcast.getMap(table);
        } else {
            retval = this.client.getMap(table);
        }
        return retval;
    }

    protected BlockingQueue<Map<String, String>> getQueue(String table) {
        BlockingQueue<Map<String, String>> retval = null;
        if (this.superclient) {
            retval = Hazelcast.getQueue(table);
        } else {
            retval = this.client.getQueue(table);
        }
        return retval;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#delete(java.lang.String, java.lang.String)
     */
    @Override
    public int delete(String table, String key) {
        if (debug)
            table = "default";
        try {
            switch (this.dataStructureType) {
            case MAP:
                ConcurrentMap<String, Map<String, String>> distributedMap = getMap(table);
                distributedMap.remove(key);
                break;
            }
        } catch (Exception e1) {
            log("error", e1 + "", e1);
            return 1;
        }
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#insert(java.lang.String, java.lang.String,
     * java.util.HashMap)
     */
    @Override
    public int insert(String table, String key, HashMap<String, String> values) {
        if (debug)
            table = "default";
        try {
            switch (this.dataStructureType) {
            case MAP:
                ConcurrentMap<String, Map<String, String>> distributedMap = getMap(table);
                distributedMap.put(key, values);
                break;
            case QUEUE:
                BlockingQueue<Map<String, String>> distributedQueue = getQueue(table);
                if (!distributedQueue.offer(values)) {
                    throw new RuntimeException("Unable to insert into queue!");
                }
                break;
            }
        } catch (Exception e1) {
            log("error", e1 + "", e1);
            return 1;
        }
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#read(java.lang.String, java.lang.String,
     * java.util.Set, java.util.HashMap)
     */
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, String> result) {
        if (debug)
            table = "default";
        try {
            switch (this.dataStructureType) {
            case MAP:
                ConcurrentMap<String, Map<String, String>> distributedMap = getMap(table);
                Map<String, String> resultMap = distributedMap.get(key);
                result.putAll(resultMap);
                break;
            case QUEUE:
                BlockingQueue<Map<String, String>> distributedQueue = getQueue(table);
                resultMap = distributedQueue.poll(this.pollTimeoutMs, TimeUnit.MILLISECONDS);
                if (resultMap != null)
                    result.putAll(resultMap);
                break;
            }
        } catch (Exception e1) {
            log("error", e1 + "", e1);
            return 1;
        }
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#scan(java.lang.String, java.lang.String, int,
     * java.util.Set, java.util.Vector)
     */
    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, String>> result) {
        throw new UnsupportedOperationException("scan() is not supported at this time!");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yahoo.ycsb.DB#update(java.lang.String, java.lang.String,
     * java.util.HashMap)
     */
    @Override
    public int update(String table, String key, HashMap<String, String> values) {
        if (debug)
            table = "default";
        try {
            switch (this.dataStructureType) {
            case MAP:
                ConcurrentMap<String, Map<String, String>> distributedMap = getMap(table);
                if (values != null && values.size() > 0) {
                    Map<String, String> resultMap = distributedMap.get(key);
                    Iterator<String> iter = values.keySet().iterator();
                    String k = null;
                    while (iter.hasNext()) {
                        k = iter.next();
                        resultMap.put(k, values.get(k));
                    }
                    distributedMap.put(key, resultMap);
                }
                break;
            }
        } catch (Exception e1) {
            log("error", e1 + "", e1);
            return 1;
        }
        return 0;
    }

    /**
     * Simple logging method.
     * 
     * @param level
     * @param message
     * @param e
     */
    protected void log(String level, String message, Exception e) {
        message = Thread.currentThread().getName() + ":  " + message;
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
