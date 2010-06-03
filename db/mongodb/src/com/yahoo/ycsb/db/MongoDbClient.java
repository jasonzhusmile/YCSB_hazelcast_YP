/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */

package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.mongodb.BasicDBObject;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.DB.WriteConcern;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * MongoDB client for YCSB framework for Mongo DB Java driver 1.4. <br/>
 * 
 * Properties to set:<br/>
 * 
 * mongodb.url=mongodb://localhost:27017 <br/>
 * mongodb.database=ycsb <br/>
 * 
 * @author ypai
 * 
 */
public class MongoDbClient extends DB {

    private Mongo mongo;
    private WriteConcern writeConcern = WriteConcern.NORMAL;
    private String database;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        // initialize MongoDb driver
        Properties props = getProperties();
        String url = props.getProperty("mongodb.url");
        database = props.getProperty("mongodb.database");
        String writeConcernType = props.getProperty("mongodb.writeConcern");

        if ("none".equals(writeConcernType)) {
            writeConcern = WriteConcern.NONE;
        } else if ("strict".equals(writeConcernType)) {
            writeConcern = WriteConcern.STRICT;
        } else if ("normal".equals(writeConcernType)) {
            writeConcern = WriteConcern.NORMAL;
        }

        try {

            if (url == null || "".equals(url) || database == null
                    || "".equals(database)) {
                throw new RuntimeException(
                        "mongodb.url or mongodb.database not specified!");
            }

            // strip out prefix since Java driver doesn't currently support
            // standard
            // connection format URL yet
            // http://www.mongodb.org/display/DOCS/Connections
            if (url.startsWith("mongodb://")) {
                url = url.substring(10) + "/" + database;
            }

            mongo = new Mongo(new DBAddress(url));
        } catch (Exception e) {
            log("error", "Could not initialize MongoDb connection:  " + e, e);
            throw new DBException("Could not initialize MongoDb connection:  "
                    + e, e);
        }

    }

    @Override
    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int delete(String table, String key) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            if (writeConcern.equals(WriteConcern.STRICT)) {
                q.put("$atomic", true);
            }
            collection.remove(q);

            // see if record was deleted
            DBObject errors = db.getLastError();

            return (Integer) errors.get("n") == 1 ? 0 : 1;
        } catch (Exception e) {
            log("error", e + "", e);
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    @Override
    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int insert(String table, String key, HashMap<String, String> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject r = new BasicDBObject().append("_id", key);
            r.putAll(values);

            collection.setWriteConcern(writeConcern);

            collection.insert(r);

            // determine if record was inserted, does not seem to return
            // n=<records affected> for insert
            DBObject errors = db.getLastError();

            return (Boolean) errors.get("ok") && errors.get("err") == null ? 0
                    : 1;
        } catch (Exception e) {
            log("error", e + "", e);
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }

    }

    @Override
    @SuppressWarnings("unchecked")
    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int read(String table, String key, Set<String> fields,
            HashMap<String, String> result) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject fieldsToReturn = new BasicDBObject();
            boolean returnAllFields = fields == null;

            DBObject queryResult = null;
            if (!returnAllFields) {
                for (String fieldName : fields) {
                    fieldsToReturn.put(fieldName, 1);
                }
                queryResult = collection.findOne(q, fieldsToReturn);
            } else {
                queryResult = collection.findOne(q);
            }

            if (queryResult != null) {
                result.putAll(queryResult.toMap());
            }
            return queryResult != null ? 0 : 1;
        } catch (Exception e) {
            log("error", e + "", e);
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }

    }

    @Override
    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int update(String table, String key, HashMap<String, String> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject u = new BasicDBObject();
            DBObject fieldsToSet = new BasicDBObject();
            Iterator<String> keys = values.keySet().iterator();
            String tmpKey = null, tmpVal = null;
            while (keys.hasNext()) {
                tmpKey = keys.next();
                tmpVal = values.get(tmpKey);
                fieldsToSet.put(tmpKey, tmpVal);

            }
            u.put("$set", fieldsToSet);

            collection.setWriteConcern(writeConcern);

            collection.update(q, u);

            // determine if record was inserted
            DBObject errors = db.getLastError();

            return (Integer) errors.get("n") == 1 ? 0 : 1;
        } catch (Exception e) {
            log("error", e + "", e);
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }

    }

    @Override
    @SuppressWarnings("unchecked")
    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, String>> result) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            DBObject scanRange = new BasicDBObject().append("$gte", startkey);
            DBObject q = new BasicDBObject().append("_id", scanRange);

            DBCursor cursor = null;

            DBObject fieldsToReturn = new BasicDBObject();
            boolean returnAllFields = fields == null;
            if (returnAllFields) {
                cursor = collection.find(q).limit(recordcount);
            } else {
                for (String fieldName : fields) {
                    fieldsToReturn.put(fieldName, 1);
                }
                cursor = collection.find(q, fieldsToReturn).limit(recordcount);
            }

            while (cursor.hasNext()) {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                result.add((HashMap<String, String>) cursor.next().toMap());
            }

            return 0;
        } catch (Exception e) {
            log("error", e + "", e);
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }

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
