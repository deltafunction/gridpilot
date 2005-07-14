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

  public DBPluginMgr(String _dbName, String _user, String _passwd){
    dbName = _dbName;
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
        }
        catch(Exception e){
        	loadfailed = true;
        	//do nothing, will try with MyClassLoader.
        }
        if(loadfailed == false) return;
        try{
           // loading of this plug-in
          MyClassLoader mcl = new MyClassLoader();
  
          db = (Database)(mcl.findClass(dbClass).getConstructor(dbArgsType).newInstance(dbArgs));
  
          Debug.debug("plugin " + dbName + "(" + dbClass + ") loaded", 2);
  
        }
        catch(IllegalArgumentException iae){
          logFile.addMessage("Cannot load class for " + dbName + ".\nThe plugin constructor " +
                             "must have one parameter (String)", iae);
          throw iae;
        }
        catch(Exception e){
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

  public boolean updateJobDef(int jobDefID, String [] fields, String [] values) {
    if(fields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          fields.length+"!="+values.length, 1);
      return false;
    }
    if(fields.length>JobDefinition.Fields.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+JobDefinition.Fields.length, 1);
    }
    String [] vals = new String[JobDefinition.Fields.length];
    for(int i=0; i<JobDefinition.Fields.length; ++i){
      vals[i] = "";
      for(int j=0; i<fields.length; ++j){
        if(fields[j].equalsIgnoreCase(JobDefinition.Fields[i])){
          vals[i] = values[j];
          break;
        }
        if(fields[j].equalsIgnoreCase(JobDefinition.Identifier)){
          vals[i] = Integer.toString(jobDefID);
          break;
        }
      }
    }
    JobDefinition jobDef = new JobDefinition(vals);
    return GridPilot.getClassMgr().getDBPluginMgr(dbName).updateJobDefinition(jobDef) ;
  }

  public boolean updateJobStdoutErr(int jobDefID, String stdOut, String stdErr){
    return updateRunInf(jobDefID, new String [] {"outVal", "errVal"}, new String [] {stdOut, stdErr});
  }
  
  public boolean updateRunInf(int jobDefID, String [] fields, String [] values){
    if(fields.length!=values.length){
      Debug.debug("The number of fields and values do not agree, "+
          fields.length+"!="+values.length, 1);
      return false;
    }
    if(fields.length>JobInfo.Fields.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+JobInfo.Fields.length, 1);
    }
    String [] vals = new String[JobInfo.Fields.length];
    for(int i=0; i<JobInfo.Fields.length; ++i){
      vals[i] = "";
      for(int j=0; i<fields.length; ++j){
        if(fields[j].equalsIgnoreCase(JobInfo.Fields[i])){
          vals[i] = values[j];
          break;
        }
        if(fields[j].equalsIgnoreCase(JobInfo.Identifier)){
          vals[i] = Integer.toString(jobDefID);
          break;
        }
      }
    }
    JobInfo job = (JobInfo) GridPilot.getClassMgr().getDBPluginMgr(dbName).getRunInfo(jobDefID);

    for(int i=0; i<fields.length; ++i){
      try{
        job.setValue(fields[i], values[i]);
      }
      catch(Throwable e){
        Debug.debug("Could not set "+fields[i]+" to "+values[i], 1);
      }
    }
    return GridPilot.getClassMgr().getDBPluginMgr(dbName).updateRunInfo(job);
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

  public synchronized String getStdOutFinalDest(final int jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getStdOutFinalDest(jobDefinitionID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getStdOutFinalDest"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getStdErrFinalDest(final int jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getStdErrFinalDest(jobDefinitionID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getStdErrFinalDest"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getJobDefValue(final int jobDefID, final String key){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getJobDefValue(jobDefID, key);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefValue"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getJobRunInfo(final int jobDefID, final String key){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getJobRunInfo(jobDefID, key);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobRunInfo"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getUserLabel(){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getUserLabel();
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getUserLabel"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getJobTransValue(final int jobDefID, final String key){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getJobTransValue(jobDefID, key);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobTransValue"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String [] getOutputs(final int transformationID){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getOutputs(transformationID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             transformationID, t);
        }
      }
      public String [] getString2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getOutputs"))
      return t.getString2Res();
    else
      return null;
  }

  public synchronized String [] getInputs(final int transformationID){
    
      MyThread t = new MyThread(){
        String [] res = null;
        public void run(){
          try{
            res = db.getInputs(transformationID);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               transformationID, t);
          }
        }
        public String [] getString2Res(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "getInputs"))
        return t.getString2Res();
      else
        return null;
    }

  public synchronized String getJobDefOutLocalName(final int jobDefID, final String outpar){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getJobDefOutLocalName(jobDefID, outpar);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefOutLocalName"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getJobDefInLocalName(final int jobDefID, final String outpar){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getJobDefInLocalName(jobDefID, outpar);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefInLocalName"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getJobDefOutRemoteName(final int jobDefinitionID, final String outpar){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getJobDefOutRemoteName(jobDefinitionID, outpar);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefOutRemoteName"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getJobDefInRemoteName(final int jobDefinitionID, final String outpar){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getJobDefInRemoteName(jobDefinitionID, outpar);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefInRemoteName"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String [] getDefVals(final int taskId, final String user){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getDefVals(taskId, user);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             taskId, t);
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

  public synchronized String [] getJobParameters(final int transformationID){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getJobParameters(transformationID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             transformationID, t);
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

  public synchronized boolean saveDefVals(final int taskID,
      final String[] defvals, final String user){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.saveDefVals(taskID, defvals, user);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             taskID, t);
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

  public synchronized boolean createJobDefinition(final JobDefinition jobDef){
  
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

  public synchronized boolean createRunInfo(final JobInfo jobInfo){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.createRunInfo(jobInfo);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               jobInfo.toString(), t);
          }
        }
        public boolean getBoolRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "createRunInfo"))
        return t.getBoolRes();
      else
        return false;
    }

  public synchronized boolean createTask(final Task task){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.createTask(task);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               task.toString(), t);
          }
        }
        public boolean getBoolRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "createTask"))
        return t.getBoolRes();
      else
        return false;
    }

  public synchronized boolean createJobTransRecord(final JobTrans jobTrans){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.createJobTransRecord(jobTrans);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               jobTrans.toString(), t);
          }
        }
        public boolean getBoolRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "createJobTrans"))
        return t.getBoolRes();
      else
        return false;
    }

  public synchronized boolean setJobDefsField(final int [] identifiers,
      final String field, final String value){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.setJobDefsField(identifiers, field, value);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               field, t);
          }
        }
        public boolean getBoolRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "setJobDefinitionField"))
        return t.getBoolRes();
      else
        return false;
    }

  public synchronized boolean updateJobDefinition(final JobDefinition jobDef){
  
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

  public synchronized boolean updateRunInfo(final JobInfo jobInfo){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.updateRunInfo(jobInfo);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               jobInfo.toString(), t);
          }
        }
        public boolean getBoolRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "updateRunInfo"))
        return t.getBoolRes();
      else
        return false;
    }

  public synchronized boolean updateTask(final Task task){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.updateTask(task);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               task.toString(), t);
          }
        }
        public boolean getBoolRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "updateTask"))
        return t.getBoolRes();
      else
        return false;
    }

  public synchronized boolean updateJobTransRecord(final JobTrans jobTrans){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.updateJobTransRecord(jobTrans);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               jobTrans.toString(), t);
          }
        }
        public boolean getBoolRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "updateJobTransRecord"))
        return t.getBoolRes();
      else
        return false;
    }

  public synchronized boolean deleteJobDefinition(final int jobDefID){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.deleteJobDefinition(jobDefID);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               jobDefID, t);
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

  public synchronized boolean deleteTask(final int taskID){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.deleteTask(taskID);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               taskID, t);
          }
        }
        public boolean getBoolRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "deleteTask"))
        return t.getBoolRes();
      else
        return false;
    }

  public synchronized boolean deleteJobTransRecord(final int taskID){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.deleteJobTransRecord(taskID);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               taskID, t);
          }
        }
        public boolean getBoolRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "deleteJobTransRecord"))
        return t.getBoolRes();
      else
        return false;
    }

  public synchronized boolean reserveJobDefinition(final int jobDefID, final String user){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.reserveJobDefinition(jobDefID, user);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
        }
      }
      public boolean getBoolRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "reserveJobDefinition"))
      return t.getBoolRes();
    else
      return false;
  }

  public synchronized boolean dereserveJobDefinition(final int jobDefID){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.dereserveJobDefinition(jobDefID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
        }
      }
      public boolean getBoolRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "jobDefID"))
      return t.getBoolRes();
    else
      return false;
  }

  public synchronized DBResult select(final String selectQuery, final String identifier){
  
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

  public synchronized DBResult getJobTransRecords(final int taskID){
  
    MyThread t = new MyThread(){
      DBResult res = null;
      public void run(){
        try{
          res = db.getJobTransRecords(taskID);
        }catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             taskID, t);
        }
      }
      public DBResult getDB2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getAllJobTransRecords"))
      return t.getDB2Res();
    else
      return null;
  }

  public synchronized DBResult getAllTaskTransRecords(){
    
      MyThread t = new MyThread(){
        DBResult res = null;
        public void run(){
          try{
            res = db.getAllTaskTransRecords();
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName, t);
          }
        }
        public DBResult getDB2Res(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "getAllTaskTransRecords"))
        return t.getDB2Res();
      else
        return null;
    }
  
  public synchronized DBRecord getTaskTransRecord(final int taskID){
    
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

  public synchronized DBRecord getTask(final int taskID){
  
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

  public synchronized DBRecord getJobTransRecord(final int jobTransID){
    
      MyThread t = new MyThread(){
        DBRecord res = null;
        public void run(){
          try{
            res = db.getJobTransRecord(jobTransID);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               jobTransID, t);
          }
        }
        public DBRecord getDBRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "jobTransID"))
        return t.getDBRes();
      else
        return null;
    }

  public synchronized DBRecord getRunInfo(final int jobDefID){
    
      MyThread t = new MyThread(){
        DBRecord res = null;
        public void run(){
          try{
            res = db.getRunInfo(jobDefID);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               jobDefID, t);
          }
        }
        public DBRecord getDBRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "getRunInfo"))
        return t.getDBRes();
      else
        return null;
    }

  public synchronized DBResult getJobDefinitions(final int taskID, final String [] fieldNames){
  
    MyThread t = new MyThread(){
      DBResult res = null;
      public void run(){
        try{
          res = db.getJobDefinitions(taskID, fieldNames);
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

  public synchronized DBRecord getJobDefinition(final int jobDefinitionID){
  
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

  public String connect(){
  
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

  public void disconnect(){
  
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

  public synchronized void clearCaches(){
  
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
      case UNEXPECTED : return "UnexpectedErrors";
      default : return "status not found";
    }
  }

  public static int getStatusId(String status){
    for(int i=1; i<=7 ; ++i){
      if(status.compareToIgnoreCase(getStatusName(i)) == 0)
        return i;
    }
    return -1;
  }

  public static String [] getSdtOutput(){
    String [] res = {"stdout", "stderr"};
    return res;
  }
    
  public synchronized String [] getDBDefFields(String dbName, String tableName){
    HashMap dbDefFields = new HashMap();
    String [] ret;
    try{
      ret = GridPilot.split((String)
      GridPilot.getClassMgr().getConfigFile().getValue(dbName, "default "+tableName+" fields"));
      dbDefFields.put(tableName, ret);
      return ret;
    }catch(Exception e){
      // hard default
      return new String []  {"*"};
    }
  }

  public synchronized String [] getDBHiddenFields(String dbName, String tableName){
    HashMap dbDefFields = new HashMap();
    String [] ret;
    try{
      ret = GridPilot.split((String)
      GridPilot.getClassMgr().getConfigFile().getValue(dbName, "hidden "+tableName+" fields"));
      dbDefFields.put(tableName, ret);
      return ret;
    }catch(Exception e){
      // hard default
      return new String []  {"actualPars"};
    }
  }

  public synchronized int getTaskTransId(final int taskID){
  
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

  public synchronized int getTaskId(final int jobDefID){
    
      MyThread t = new MyThread(){
        int res = -1;
        public void run(){
          try{
            res = db.getTaskId(jobDefID);
          }catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               jobDefID, t);
          }
        }
        public int getIntRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "getTaskId"))
        return t.getIntRes();
      else
        return -1;
    }

  public synchronized String [] getJobTransNames(){
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getJobTransNames();
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