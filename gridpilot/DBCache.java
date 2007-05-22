package gridpilot;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;


import gridpilot.GridPilot;
import gridpilot.Debug;

/**
 * This class provides methods for caching JDBC queries.
 *
 */
public class DBCache{

  protected String dbName;
  // HashMap (key sql) of queries and results
  private HashMap queryResults = new HashMap();
  protected boolean useCaching = false;
  
  public DBCache(){
  }
  
  public void clearCache(){
    queryResults.clear();
  }
  
  public DBResult executeQuery(String sql) throws SQLException{
    return executeQuery(null, sql);
  }
  
  public DBResult executeQuery(String _dbName, String sql) throws SQLException{
    String thisDbName = dbName;
    if(_dbName!=null){
      thisDbName = _dbName;
    }
    Debug.debug("Caching: "+thisDbName+":"+useCaching, 3);
    if(useCaching && queryResults.containsKey(sql)){
      Debug.debug("Returning cached result", 2);
      DBResult rset = (DBResult) queryResults.get(sql);
      rset.beforeFirst();
      return rset;
    }
    String [] row = null;
    Vector valuesVector = new Vector();
    Connection conn = GridPilot.getClassMgr().getDBConnection(thisDbName);
    Statement stmt = conn.createStatement();
    ResultSet rset = stmt.executeQuery(sql);
    ResultSetMetaData md = rset.getMetaData();
    String [] fields = new String[md.getColumnCount()];
    for(int i=1; i<=md.getColumnCount(); ++i){
      fields[i-1] = md.getColumnName(i);
    }
    while(rset.next()){
      row = new String[fields.length];
      for(int i=0; i<fields.length; i++){
        if(fields[i].endsWith("FK") || fields[i].endsWith("ID") &&
            !fields[i].equalsIgnoreCase("grid") && !fields[i].equalsIgnoreCase("jobid") ||
            fields[i].endsWith("COUNT")){
          row[i] = Integer.toString(rset.getInt(i+1));
        }
        else{
          row[i] = rset.getString(i+1);
          Debug.debug(i+"->"+row[i], 3);
        }
      }
      valuesVector.add(row);
    }
    rset.close();
    conn.close();
    String [][] values = new String [valuesVector.size()][fields.length];
    for(int i=0; i<valuesVector.size(); ++i){
      for(int j=0; j<fields.length; ++j){
        values[i][j] = ((String []) valuesVector.get(i))[j];
        Debug.debug(i+":"+j+"-->"+values[i][j], 3);
      }
    }
    DBResult res = new DBResult(fields, values);
    Debug.debug("Adding to cache", 2);
    queryResults.put(sql, res);
    return res;
  }
  
  public int executeUpdate(String sql) throws SQLException{
    int execok = -1;
    Connection conn = null;
    if(useCaching){
      String table = Util.getTableName(sql);
      String thisSql = null;
      String thisTableName = null;
      HashSet deleteKeys = new HashSet();
      for(Iterator it=queryResults.keySet().iterator(); it.hasNext();){
        thisSql = (String) it.next();
        thisTableName = Util.getTableName(thisSql);
        Debug.debug("Checking cache: "+thisTableName+"<->"+table+":", 2);
        if(thisTableName.equalsIgnoreCase(table)){
          Debug.debug("Removing from cache: "+thisSql, 2);
          deleteKeys.add(thisSql);
        }
      }
      Debug.debug("Clearing cache entries", 2);
      for(Iterator it=deleteKeys.iterator(); it.hasNext();){
        thisSql = (String) it.next();
        Debug.debug("--> "+thisSql, 2);
        queryResults.remove(thisSql);
      }
    }
    try{
      conn = GridPilot.getClassMgr().getDBConnection(dbName);
      Statement stmt = conn.createStatement();
      execok = stmt.executeUpdate(sql);
      conn.close();
    }
    catch(SQLException e){
      try{
        conn.close();
      }
      catch(Exception ee){
      }
      throw e;
    }
    return execok;
  }
  
}