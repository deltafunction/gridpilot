package gridpilot;

import java.util.HashMap;

import javax.swing.JOptionPane;

import gridpilot.GridPilot;
import gridpilot.ConfigFile;
import gridpilot.Debug;
import gridpilot.LogFile;
import gridpilot.Database;
import gridpilot.MyThread;
import gridpilot.MyClassLoader;


/**
 * This class manages access to job databases.
 *
 */
public class DBPluginMgr implements Database{

  private ConfigFile configFile;
  private LogFile logFile;
  private Database db ;
  private String dbName ;
  
  private String database ;
  private String user;
  private String passwd;
  private String dbprefix; //prepend to tables
// TODO: cache here??
  private HashMap partInfoCacheId = null ;

  /** time out in ms used if neither the specific time out nor "default timeout" is
   * defined in configFile */
  private int dbTimeOut = 60*1000;
  
  private boolean askBeforeInterrupt = true;

  public DBPluginMgr(String _dbName, String _database, String _user, String _passwd){
    dbName = _dbName;
    database = _database;
    user = _user;
    passwd = _passwd;
  }

  public String getDBName(){
    return dbName;
  }
  public String getDatabase(){
    return database;
  }
  public String getUser(){
    return user;
  }
  public String getPasswd(){
    return passwd;
  }
  /**
   * Constructs a DBPluginMgr.
   * Looks after plug-in names and class in configFile, load them, and read time out values.
   * @throws Throwable if
   * - There is no Database specified in configuration file
   * - One of these databases hasn't a class name defined
   * - An Exception occurs when gridpilot tries to load these classes (for example because
   * the constructor with one parameter (String) is not defined)
   */
  public void init() throws Throwable{
  
    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
  
    loadClass();
    loadValues();
  }

  /**
     * Loads plug-in.
     * @throws Throwable if an exception or an error occurs during plug-in loading
     */
    public void loadClass() throws Throwable{//Exception{
        Debug.debug("Loading plugin: "+dbName, 2);
        // Arguments and class name for <DatabaseName>Database
        //  AMI ****
        String driver = configFile.getValue(dbName, "driver");
        String transDB = configFile.getValue(dbName, "transDB");
        String project = configFile.getValue(dbName, "project");
        String level = configFile.getValue(dbName, "level");
        String site = configFile.getValue(dbName, "site");
        // ****
        database = configFile.getValue(dbName, "database");
        user = configFile.getValue(dbName, "user");
        passwd = configFile.getValue(dbName, "passwd");
        dbprefix = configFile.getValue(dbName, "dbprefix");
        String dbClass = configFile.getValue(dbName, "Database class");
        if(dbClass == null){
          throw new Exception("Cannot load class for system " + dbName + " : \n"+
                              configFile.getMissingMessage(dbName, "Database class"));
        }
  
        Class [] dbArgsType = {String.class, String.class, String.class,
            String.class, String.class, String.class, String.class, String.class, String.class};
        Object [] dbArgs = {/*AMI*/project, level, site, transDB,/**/
            driver, database, user, passwd, dbprefix};
        boolean loadfailed = false;
        try {
        	Class dbclass = this.getClass().getClassLoader().loadClass(dbClass);
            db = (Database)(dbclass.getConstructor(dbArgsType).newInstance(dbArgs));
        } catch (Exception e) {
        	loadfailed = true;
        	//do nothing, will try with MyClassLoader.
        }
        if (loadfailed == false) return;
        try{
           // loading of this plug-in
          MyClassLoader mcl = new MyClassLoader();
  
          db = (Database)(mcl.findClass(dbClass).getConstructor(dbArgsType).newInstance(dbArgs));
  
          Debug.debug("plugin " + dbName + "(" + dbClass + ") loaded", 2);
  
        }catch(IllegalArgumentException iae){
          logFile.addMessage("Cannot load class for " + dbName + ".\nThe plugin constructor " +
                             "must have one parameter (String)", iae);
          throw iae;
        }catch (Exception e){
          logFile.addMessage("Cannot load class for " + dbName, e);
          //throw e;
        }
    }

  /**
   * Reads time out values in configuration file.
   */
  public void loadValues(){
  
    String tmp;
  
    /**
     * default timeout
     */
  
    tmp = configFile.getValue("gridpilot", "db timeout");
    if(tmp!=null){
      try{
        dbTimeOut = new Integer(tmp).intValue();
      }catch(NumberFormatException nfa){
        logFile.addMessage("value of default timeout (" + tmp +") is not an integer");
      }
    }
  }

  public synchronized String [] getFieldNames(final String table){
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getFieldNames(table);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                              table, t);
        }
      }
      public String [] getString2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getFieldNames"))
      return t.getString2Res();
    else
      return null;
  }

  public synchronized String getTransId(final int datasetIdentifier, final String version){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getTransId(datasetIdentifier, version);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetIdentifier, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTransId"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getPartOutLogicalName(final int partID, final String outpar){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getPartOutLogicalName(partID, outpar);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             partID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getPartOutLogicalName"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getPackInitText(final String pack, final String cluster){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getPackInitText(pack, cluster);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             pack + " " + cluster, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getPackInitText"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getDatasetName(final int datasetID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getDatasetName(datasetID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getDatasetName"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getPartOutLocalName(final int partID, final String outpar){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getPartOutLocalName(partID, outpar);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             partID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getPartOutLocalName"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getPartValue(final int partID, final String key){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getPartValue(partID, key);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             partID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getPartValue"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getPartTransValue(final int partID, final String key){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getPartTransValue(partID, key);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             partID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getPartTransValue"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String [] getOutputMapping(final int datasetIdentifier, final String version){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getOutputMapping(datasetIdentifier, version);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetIdentifier, t);
        }
      }
      public String [] getString2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getOutputMapping"))
      return t.getString2Res();
    else
      return null;
  }

  public synchronized String [] getDefVals(final int datasetIdentifier){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getDefVals(datasetIdentifier);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetIdentifier, t);
        }
      }
      public String [] getString2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getDefVals"))
      return t.getString2Res();
    else
      return null;
  }

  public synchronized String [] getJobParameters(final int datasetIdentifier, final String version){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getJobParameters(datasetIdentifier, version);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetIdentifier, t);
        }
      }
      public String [] getString2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobParameters"))
      return t.getString2Res();
    else
      return null;
  }

  public synchronized String [] getTransformationVersions(final int datasetIdentifier){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getTransformationVersions(datasetIdentifier);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetIdentifier, t);
        }
      }
      public String [] getString2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTransformationVersions"))
      return t.getString2Res();
    else
      return null;
  }

//  public synchronized JobTrans [] getJobTrans(final int taskID){
  
//    MyThread t = new MyThread(){
//      JobTrans [] res = null;
//      public void run(){
//        try{
//          res = db.getJobTrans(taskID);
//        }catch(Throwable t){
//          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
//                             " from plugin " + dbName + " " +
//                             taskID, t);
//        }
//      }
//      public JobTrans [] getJTRes(){return res;}
//    };
  
//    t.start();
  
//    if(waitForThread(t, dbName, dbTimeOut, "getJobTrans"))
//      return t.getJTRes();
//    else
//      return null;
//  }

  public synchronized int createPart (final int datasetID, final String lfn,
      final String partNr,
      final String evMin, final String evMax,
      final String transID, final String [] trpars,
      final String [] [] ofmap, final String odest, final String edest) {
  
    MyThread t = new MyThread(){
      int res = -1;
      public void run(){
        try{
          res = db.createPart(datasetID, lfn, partNr,
              evMin, evMax, transID, trpars,
              ofmap, odest, edest);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                              lfn, t);
        }
      }
      public int getIntRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "createPart"))
      return t.getIntRes();
    else
      return -1;
  }

  public synchronized boolean saveDefVals (final int datasetID,
      final String[] defvals) {
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.saveDefVals(datasetID, defvals);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetID, t);
        }
      }
      public boolean getBoolRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "saveDefVals"))
      return t.getBoolRes();
    else
      return false;
  }

  public synchronized boolean createJobDefinition (final JobDefinition jobDef) {
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.createJobDefinition(jobDef);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDef.toString(), t);
        }
      }
      public boolean getBoolRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "createJobDefinition"))
      return t.getBoolRes();
    else
      return false;
  }

  public synchronized boolean updateJobDefinition (final JobDefinition jobDef) {
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.updateJobDefinition(jobDef);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDef.toString(), t);
        }
      }
      public boolean getBoolRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "updateJobDefinition"))
      return t.getBoolRes();
    else
      return false;
  }

  public synchronized boolean deleteJobDefinition (final JobDefinition jobDef) {
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.deleteJobDefinition(jobDef);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               jobDef.toString(), t);
          }
        }
        public boolean getBoolRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "deleteJobDefinition"))
        return t.getBoolRes();
      else
        return false;
    }

  public synchronized boolean deletePart (final int partID) {
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.deletePart(partID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             partID, t);
        }
      }
      public boolean getBoolRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "deletePart"))
      return t.getBoolRes();
    else
      return false;
  }

  public synchronized boolean reservePart (final int partID, final String user) {
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.reservePart(partID, user);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             partID, t);
        }
      }
      public boolean getBoolRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "reservePart"))
      return t.getBoolRes();
    else
      return false;
  }

  public synchronized boolean createRunRecord (final int partID, final String user, final String cluster, final String jobID,
      final String jobName, final String outTmp, final String errTmp) {
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.createRunRecord(partID,  user, cluster, jobID,
              jobName, outTmp, errTmp);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             partID, t);
        }
      }
      public boolean getBoolRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "createRunRecord"))
      return t.getBoolRes();
    else
      return false;
  }

  public synchronized boolean dereservePart (final int partID) {
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.dereservePart(partID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             partID, t);
        }
      }
      public boolean getBoolRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "dereservePart"))
      return t.getBoolRes();
    else
      return false;
  }

  public synchronized boolean updatePartition (final int partID,
      final HashMap attrVals) {
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.updatePartition(partID, attrVals);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             partID, t);
        }
      }
      public boolean getBoolRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "updatePartition"))
      return t.getBoolRes();
    else
      return false;
  }

  public synchronized String getDatasetTableName(){
  
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getDatasetTableName();
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName,
                              t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getDatasetTableName"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getPartitionTableName(){
  
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getPartitionTableName();
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName,
                              t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getPartitionTableName"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized DBResult select(final String selectQuery, final String identifier) {
  
    MyThread t = new MyThread(){
      DBResult res = null;
      public void run(){
        try{
          res = db.select(selectQuery, identifier);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                              selectQuery, t);
        }
      }
      public DBResult getDB2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "select"))
      return t.getDB2Res();
    else
      return null;
  }

  public synchronized DBResult getAllPartJobInfo (final int datasetID) {
  
    MyThread t = new MyThread(){
      DBResult res = null;
      public void run(){
        try{
          res = db.getAllPartJobInfo(datasetID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetID, t);
        }
      }
      public DBResult getDB2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getAllPartJobInfo"))
      return t.getDB2Res();
    else
      return null;
  }

  public synchronized DBResult getAllJobTransRecords (final int taskID) {
  
    MyThread t = new MyThread(){
      DBResult res = null;
      public void run(){
        try{
          res = db.getAllJobTransRecords(taskID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             taskID, t);
        }
      }
      public DBResult getDB2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getAllPartJobTransRecords"))
      return t.getDB2Res();
    else
      return null;
  }

  public synchronized DBRecord getTaskTransRecord (final int taskID) {
    
      MyThread t = new MyThread(){
        DBRecord res = null;
        public void run(){
          try{
            res = db.getTaskTransRecord(taskID);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               taskID, t);
          }
        }
        public DBRecord getDBRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "getTaskTransRecord"))
        return t.getDBRes();
      else
        return null;
    }

  public synchronized DBRecord getTask (final int taskID) {
  
    MyThread t = new MyThread(){
      DBRecord res = null;
      public void run(){
        try{
          res = db.getTask(taskID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             taskID, t);
        }
      }
      public DBRecord getDBRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTask"))
      return t.getDBRes();
    else
      return null;
  }

  public synchronized DBResult getAllJobDefinitions (final int taskID, final String [] fieldNames) {
  
    MyThread t = new MyThread(){
      DBResult res = null;
      public void run(){
        try{
          res = db.getAllJobDefinitions(taskID, fieldNames);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             taskID, t);
        }
      }
      public DBResult getDB2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getAllJobDefinitions"))
      return t.getDB2Res();
    else
      return null;
  }

  public synchronized DBRecord getJobDefinition (final int jobDefinitionID) {
  
    MyThread t = new MyThread(){
      DBRecord res = null;
      public void run(){
        try{
          res = db.getJobDefinition(jobDefinitionID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public DBRecord getDBRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefinition"))
      return t.getDBRes();
    else
      return null;
  }

  public String connect() {
  
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.connect();
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             user + " " + passwd, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "connect"))
      return t.getStringRes();
    else
      return null;
  }

  public void disconnect() {
  
    MyThread t = new MyThread(){
      public void run(){
        try{
           db.disconnect();
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             user + " " + passwd, t);
        }
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "disconnect"))
      return;
    else
      return;
  }

  public synchronized void clearCaches() {
  
    MyThread t = new MyThread(){
      public void run(){
        try{
           db.clearCaches();
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             user + " " + passwd, t);
        }
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "clearCaches"))
      return;
    else
      return;
  }

  /**
   * Waits the specified MyThread during maximum timeOut ms.
   * @return true if t ended normally, false if t has been interrupted
   */
  private boolean waitForThread(MyThread t, String dbName, int timeOut, String function){
    do{
      try{t.join(timeOut);}catch(InterruptedException ie){}
  
      if(t.isAlive()){
        if(!askBeforeInterrupt || askForInterrupt(dbName, function)){
          logFile.addMessage("No response from plugin " +
                             dbName + " for " + function);
          t.interrupt();
          return false;
        }
      }
      else
        break;
    }while(true);
    return true;
  }

  /**
   * Asks the user if he wants to interrupt a plug-in
   */
  private boolean askForInterrupt(String csName, String fct){
    String msg = "No response from plugin " + csName +
                 " for " + fct + "\n"+
                 "Do you want to interrupt it ?";
    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), msg, "No response from plugin",
        JOptionPane.YES_NO_OPTION);
    if(choice == JOptionPane.YES_OPTION)
      return true;
    else
      return false;
  }
  public static String getStatusName(int status){
    switch(status){
      case DEFINED : return "Defined";
      case SUBMITTED : return "Submitted";
      case VALIDATED : return "Validated";
      case FAILED : return "Failed";
      case UNDECIDED : return "Undecided";
      case ABORTED : return "Aborted";
      default : return "status not found";
    }
  }

  public static int getStatusId(String status){
    for(int i=1; i<= 6 ; ++i){
      if(status.compareToIgnoreCase(getStatusName(i)) == 0)
        return i;
    }
    return -1;
  }

  public static String [] getSdtOutput(){
    String [] res = {"stdout", "stderr"};
    return res;
  }
  
  private HashMap dbDefFields = new HashMap();
  public synchronized String [] getDBDefFields(String dbName, String tableName){
   String [] ret;
    if(dbDefFields.get(tableName) != null){
      return (String []) dbDefFields.get(tableName);
    }
    else{
      try{
          ret = GridPilot.split((String)
          GridPilot.getClassMgr().getConfigFile().getValue(dbName, "default "+tableName+" fields"));
      dbDefFields.put(tableName, ret);
      return ret;
      }catch(Exception e){
        // hard default
        String [] ret1 = {"dataset.logicalDatasetName"};
        return ret1;
        }
    }
  }

  public synchronized int getTaskTransId(final int taskID) {
  
    MyThread t = new MyThread(){
      int res = -1;
      public void run(){
        try{
          res = db.getTaskTransId(taskID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             taskID, t);
        }
      }
      public int getIntRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTaskTransId"))
      return t.getIntRes();
    else
      return -1;
  }

  public synchronized String [] getHomePackages(){
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getHomePackages();
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName , t);
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getHomePackages"))
      return t.getString2Res();
    else
      return new String [] {};
   }

  public synchronized String [] getVersions(final String homePackage){
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getVersions(homePackage);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName , t);
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getVersions"))
      return t.getString2Res();
    else
      return new String [] {};
   }

}