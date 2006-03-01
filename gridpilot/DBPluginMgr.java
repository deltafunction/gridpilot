package gridpilot;

import java.awt.Color;
import java.util.HashMap;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JComponent;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

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
public class DBPluginMgr implements Database, PanelUtil{

  private ConfigFile configFile;
  private LogFile logFile;
  private Database db;
  private String dbName;
  private PanelUtil pu;
  
// TODO: cache here??
  private HashMap partInfoCacheId = null ;

  /** time out in ms used if neither the specific time out nor "default timeout" is
   * defined in configFile */
  private int dbTimeOut = 60*1000;
  
  private boolean askBeforeInterrupt = true;

  public DBPluginMgr(String _dbName){
    dbName = _dbName;
  }

  public String getDBName(){
    return dbName;
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
  
    loadValues();

    String dbClass = configFile.getValue(dbName, "class");
    if(dbClass == null){
      throw new Exception("Cannot load class for system " + dbName + " : \n"+
                          configFile.getMissingMessage(dbName, "class"));
    }

    String [] parameters = configFile.getValues(dbName, "parameters");
    Class [] dbArgsType = new Class [parameters.length];
    Object [] dbArgs = new String [parameters.length];
    for(int i=0; i<parameters.length; ++i){
      dbArgsType[i] = String.class;
      dbArgs[i] = configFile.getValue(dbName, parameters[i]);
      if(dbArgs[i]==null){
        dbArgs[i] = "";
      }
    }

    db = (Database) loadClass(dbClass, dbArgsType, dbArgs);

  }

  /**
   * Initializes a JobDefCreationPanel
   */
  public void initJobDefCreationPanel(JobDefCreationPanel panel) throws Throwable{
    
    String puClass = getPanelUtilClass();

    Class [] puArgsType = {JobDefCreationPanel.class};
    
    Object [] puArgs = {panel};

    pu = (PanelUtil) loadClass(puClass, puArgsType, puArgs);
    
  }
  
  /**
   * Loads plug-in.
   * @throws Throwable if an exception or an error occurs during plug-in loading
   */
  public Object loadClass(String dbClass, Class [] dbArgsType,
     Object [] dbArgs) throws Throwable{
    Debug.debug("Loading plugin: "+dbName+" : "+dbClass, 2);
    // Arguments and class name for <DatabaseName>Database
    boolean loadfailed = false;
    Object ret = null;
    Debug.debug("argument types: "+Util.arrayToString(dbArgsType), 3);
    Debug.debug("arguments: "+Util.arrayToString(dbArgs), 3);
    try{
      Class newClass = this.getClass().getClassLoader().loadClass(dbClass);
      ret = (newClass.getConstructor(dbArgsType).newInstance(dbArgs));
      Debug.debug("plugin " + dbName + "(" + dbClass + ") loaded, "+ret.getClass(), 2);
    }
    catch(Exception e){
      e.printStackTrace();
      loadfailed = true;
      //do nothing, will try with MyClassLoader.
    }
    if(loadfailed){
      try{
        // loading of this plug-in
       MyClassLoader mcl = new MyClassLoader();
       ret = (mcl.findClass(dbClass).getConstructor(dbArgsType).newInstance(dbArgs)); 
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
    return ret;
  }

  /**
   * Reads time-out values in configuration file.
   */
  public void loadValues(){
    // default timeout  
    String tmp = configFile.getValue("GridPilot", "db timeout");
    if(tmp!=null){
      try{
        dbTimeOut = new Integer(tmp).intValue();
      }
      catch(NumberFormatException nfa){
        logFile.addMessage("value of default timeout (" + tmp +") is not an integer");
      }
    }
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

  /** 
   * Construct the name of the target dataset when creating a new dataset
   * from an input dataset.
   */ 
  public String getTargetDatasetName(String targetDB, String sourceDatasetName,
      String transformation, String version){
    Debug.debug("finding target dataset for "+sourceDatasetName+" in "+targetDB, 3);
    String findString = "";
    String replaceString = "";
    String matchString = "";
    Pattern p = null;
    Matcher m = null;
    String s = "";
    String ret = "";
    if(targetDB!=null){
      // TODO: make these patterns configurable
      s = ".*simulation.*";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(targetDB);
      if(m.matches()){
        replaceString = "SimProd";
      }
      s = ".*digitization.*";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(targetDB);
      if(m.matches()){
        replaceString = "DigitProd";
      }
      s = ".*reconstruction.*";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(targetDB);
      if(m.matches()){
        replaceString = "ReconProd";
      }
      
      findString = "SimProd";
      Debug.debug("replacing "+findString+" -> "+replaceString, 3);
      ret = sourceDatasetName.replaceFirst(findString, replaceString);
      
      findString = "DigitProd"; 
      Debug.debug("replacing "+findString+" -> "+replaceString, 3);
      ret = sourceDatasetName.replaceFirst(findString, replaceString);
      
      findString = "ReconProd";  
      Debug.debug("replacing "+findString+" -> "+replaceString, 3);
      ret = sourceDatasetName.replaceFirst(findString, replaceString);
    }
    else{
      ret = sourceDatasetName;
    }
    
    // Get rid of redundant .simul extension
    s = "\\.simul$";
    p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
    m = p.matcher(ret);
    ret = m.replaceAll("");    

    boolean matched = false;
    String ret1 = ret;
    if(version!=null && !version.equals("")){
      // Change the version to match the transformation version
      s = "\\.v\\w*\\.\\w*$";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(ret);
      if(!matched){
        Debug.debug("replacing version", 3);
        ret1 = m.replaceAll("."+version);
        if(!ret.equals(ret1)){
          matched = true;
        }
      }
      s = "\\.\\w*\\.v\\w*$";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(ret);
      if(!matched){
        Debug.debug("replacing version", 3);
        ret1 = m.replaceAll("."+version);
        if(!ret.equals(ret1)){
          matched = true;
        }
      }
      s = "\\.v\\w*$";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(ret);
      if(!matched){
        Debug.debug("replacing version", 3);
        ret1 = m.replaceAll("."+version);
        if(!ret.equals(ret1)){
          matched = true;
        }
      }
    }
    
    return ret1;
  }

  public synchronized String getPanelUtilClass(){
    return db.getPanelUtilClass();
  }

  public synchronized String [] getFieldNames(final String table){
    Debug.debug("Getting field names for table "+table, 3);
   
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getFieldNames(table);
        }
        catch(Throwable t){
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
        }
        catch(Throwable t){
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
        }
        catch(Throwable t){
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
        }
        catch(Throwable t){
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

  public synchronized String getExtractScript(final int jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getExtractScript(jobDefinitionID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getExtractScript"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getValidationScript(final int jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getValidationScript(jobDefinitionID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getValidationScript"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getTransformationScript(final int jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getTransformationScript(jobDefinitionID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobTransDefinition"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String [] getTransformationPackages(final int jobDefinitionID){
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getTransformationPackages(jobDefinitionID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String [] getString2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobTransPackages"))
      return t.getString2Res();
    else
      return null;
  }

  public synchronized String [] getTransformationSignature(final int jobDefinitionID){
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getTransformationSignature(jobDefinitionID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String [] getString2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobTransSignature"))
      return t.getString2Res();
    else
      return null;
  }

  public synchronized String getTransNameColumn(){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getTransNameColumn();
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName , t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTransNameColumn"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getJobDefUser(final int jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getJobDefUser(jobDefinitionID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefUser"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getJobDefName(final int jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getJobDefName(jobDefinitionID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefName"))
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
        }
        catch(Throwable t){
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

  public synchronized String getRunNumber(final int datasetID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getRunNumber(datasetID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getRunNumber"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized int getJobDefDatasetID(final int jobDefinitionID){
    MyThread t = new MyThread(){
      int res = -1;
      public void run(){
        try{
          res = db.getJobDefDatasetID(jobDefinitionID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public int getIntRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefDatasetID"))
      return t.getIntRes();
    else
      return -1;
  }

  public synchronized String getJobStatus(final int jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getJobStatus(jobDefinitionID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobStatus"))
      return t.getStringRes();
    else
      return null;
  }

 public synchronized String getJobRunUser(final int jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getJobRunUser(jobDefinitionID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobRunUser"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getJobDefValue(final int jobDefID, final String key){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getJobDefinition(jobDefID).getValue(key).toString();
        }
        catch(Throwable t){
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

  public synchronized String getJobRunValue(final int jobDefID, final String key){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getRunInfo(jobDefID).getValue(key).toString();
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobRunValue"))
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
        }
        catch(Throwable t){
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

  public synchronized String getTransformationID(final int jobDefID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getTransformationID(jobDefID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobTransID"))
      return t.getStringRes();
    else
      return null;
  }

  public synchronized String getJobTransValue(final int jobDefID, final String key){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getTransformation(
              Integer.parseInt(db.getTransformationID(jobDefID))
              ).getValue(key).toString();
        }
        catch(Throwable t){
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

  public synchronized String [] getOutputs(final int jobDefID){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getOutputs(jobDefID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
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

  public synchronized String [] getInputs(final int jobDefID){
    
      MyThread t = new MyThread(){
        String [] res = null;
        public void run(){
          try{
            res = db.getInputs(jobDefID);
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               jobDefID, t);
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

  public synchronized String [] getJobDefTransPars(final int jobDefID){
    
      MyThread t = new MyThread(){
        String [] res = null;
        public void run(){
          try{
            res = db.getJobDefTransPars(jobDefID);
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               jobDefID, t);
          }
        }
        public String [] getString2Res(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "getJobDefTransPars"))
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
        }
        catch(Throwable t){
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
        }
        catch(Throwable t){
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
        }
        catch(Throwable t){
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
        }
        catch(Throwable t){
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

  public synchronized String [] getDefVals(final int datasetID, final String user){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getDefVals(datasetID, user);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetID, t);
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

  public synchronized String [] getTransJobParameters(final int transformationID){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getTransJobParameters(transformationID);
        }
        catch(Throwable t){
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

  public synchronized boolean saveDefVals(final int datasetID,
      final String[] defvals, final String user){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.saveDefVals(datasetID, defvals, user);
        }
        catch(Throwable t){
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

  public synchronized boolean createJobDefinition(final String [] values){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.createJobDefinition(values);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             values.toString(), t);
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
          }
          catch(Throwable t){
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

  // Here, in contrast to updateJobDef (in DBPluginMgr), because it is not needed by other
  // classes and it needs to update display.
  public DBRecord createJobDef(String [] fields, String [] values) throws Exception {
    
    String [] jobDefFieldNames = getFieldNames("jobDefinition");
    
    if(fields.length!=values.length){
      throw new Exception("The number of fields and values do not agree, "+
          fields.length+"!="+values.length);
    }
    if(fields.length>jobDefFieldNames.length){
      throw new Exception("The number of fields is too large, "+
          fields.length+">"+jobDefFieldNames.length);
    }
    String [] vals = new String[jobDefFieldNames.length];
    for(int i=0; i<jobDefFieldNames.length; ++i){
      vals[i] = "";
      for(int j=0; i<fields.length; ++j){
        if(fields[j].equalsIgnoreCase(jobDefFieldNames[i])){
          vals[i] = values[j];
          break;
        }
      }
    }
    DBRecord jobDef = new DBRecord(jobDefFieldNames, vals);
    /*try{
        jobDef.setValue("currentState", "DEFINED");
      }
      catch(Exception e){
      Debug.debug("Failed setting currentState. " +e.getMessage(),3);
      return false;
    }*/
    if(createJobDefinition(vals)){
      //shownJobs.add(jobDef);
      return jobDef;
    }
    else{
      throw new Exception("ERROR: createJobDefinition failed");
    }
  }

  public synchronized boolean createTransformation(final String [] values){
    
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.createTransformation(values);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             values.toString(), t);
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

  public synchronized boolean createDataset(final String targetTable,
      final String [] fields, final String [] values){
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.createDataset(targetTable, fields, values);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             values.toString(), t);
        }
      }
      public boolean getBoolRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "createDataset"))
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
        }
        catch(Throwable t){
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

  public synchronized boolean updateJobDefinition(final int jobDefID,
      final String [] fields, final String [] values){
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.updateJobDefinition(jobDefID, fields, values);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
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

  public synchronized boolean updateJobDefinition(final int jobDefID,
      final String [] values){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.updateJobDefinition(jobDefID, values);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
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

  public synchronized boolean updateJobDefStatus(final int jobDefID,
      final String status){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.updateJobDefStatus(jobDefID, status);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
        }
      }
      public boolean getBoolRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "updateJobDefStatus"))
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
          }
          catch(Throwable t){
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

  public synchronized boolean updateDataset(final int taskID,
      final String [] fields, final String [] values){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.updateDataset(taskID, fields, values);
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               taskID, t);
          }
        }
        public boolean getBoolRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "updateDataset"))
        return t.getBoolRes();
      else
        return false;
    }

  public synchronized boolean updateTransformation(final int jobTransID,
      final String [] fields, final String [] values){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.updateTransformation(jobTransID, fields, values);
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               jobTransID, t);
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
          }
          catch(Throwable t){
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

  public synchronized boolean deleteDataset(final int taskID, final boolean cleanup){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.deleteDataset(taskID, cleanup);
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               taskID, t);
          }
        }
        public boolean getBoolRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "deleteDataset"))
        return t.getBoolRes();
      else
        return false;
    }

  public synchronized boolean deleteTransformation(final int transformationID){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.deleteTransformation(transformationID);
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               transformationID, t);
          }
        }
        public boolean getBoolRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "deleteTransformation"))
        return t.getBoolRes();
      else
        return false;
    }

  public synchronized boolean reserveJobDefinition(final int jobDefID, final String userName){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.reserveJobDefinition(jobDefID, userName);
        }
        catch(Throwable t){
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
        }
        catch(Throwable t){
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
        }
        catch(Throwable t){
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

  public synchronized DBResult getTransformations(){
  
    MyThread t = new MyThread(){
      DBResult res = null;
      public void run(){
        try{
          res = db.getTransformations();
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
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

  public synchronized DBRecord getDataset(final int datasetID){
    
      MyThread t = new MyThread(){
        DBRecord res = null;
        public void run(){
          try{
            res = db.getDataset(datasetID);
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               datasetID, t);
          }
        }
        public DBRecord getDBRes(){return res;}
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "getDataset"))
        return t.getDBRes();
      else
        return null;
    }

  public synchronized DBRecord getTransformation(final int jobTransID){
    
      MyThread t = new MyThread(){
        DBRecord res = null;
        public void run(){
          try{
            res = db.getTransformation(jobTransID);
          }
          catch(Throwable t){
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
          }
          catch(Throwable t){
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

  public synchronized DBResult getJobDefinitions(final int datasetID, final String [] fieldNames){
  
    MyThread t = new MyThread(){
      DBResult res = null;
      public void run(){
        try{
          res = db.getJobDefinitions(datasetID, fieldNames);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetID, t);
        }
      }
      public DBResult getDB2Res(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefinitions"))
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
        }
        catch(Throwable t){
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
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
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
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
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
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
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
  
  /**
   * Returns status names for statistics panel.
   * (From AtCom1)
   */
  public static String [] getStatusNames(){
    return new String [] {"Wait", "Run", "Done"};
  }
  
  /**
   * Returns colors corresponding to getStatusNames for statistics panel.
   */
  public static Color [] getStatusColors(){
    return new Color [] {Color.black, Color.blue, Color.green, Color.orange, Color.magenta, Color.red, Color.darkGray};
  }


  /**
   * DB status names for statistics panel. <p>
   * (From AtCom1)
   */
  private static String [] dbStatusNames = new String [] {
      "Defined",
      "Submitted",
      "Validated",
      "Undecided",
      "Failed",
      "Aborted",
      "Unexpected"};

  public static String [] getDBStatusNames(){
    return dbStatusNames;
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
    Debug.debug("ERROR: the status "+status+" does not correspond to any known status ID", 1);
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
      ret = Util.split((String)
          configFile.getValue(dbName, "default "+tableName+" fields"));
      dbDefFields.put(tableName, ret);
      return ret;
    }
    catch(Exception e){
      // hard default
      return new String []  {"*"};
    }
  }
  
  public synchronized String getIdentifier(String dbName, String table){
    String ret = configFile.getValue(dbName,
      table+" identifier");
    if(ret==null || ret.equals("")){
      ret = "identifier";
    }
    Debug.debug("Identifier for "+dbName+" - "+table+" : "+ret, 2);
    return ret;
  }

  public synchronized String getName(String dbName, String table){
    String ret = configFile.getValue(dbName,
      table+" name");
    if(ret==null || ret.equals("")){
      ret = "name";
    }
    Debug.debug("Name for "+dbName+" - "+table+" : "+ret, 2);
    return ret;
  }

  public synchronized String getJobDefDatasetFK(String dbName){
    String ret = configFile.getValue(dbName,
      "jobDefinition dataset FK");
    if(ret==null || ret.equals("")){
      ret = "datasetFK";
    }
    Debug.debug("jobDef dataset FK for "+dbName+" : "+ret, 2);
    return ret;
  }

  public synchronized String [] getDBHiddenFields(String dbName, String tableName){
    HashMap dbDefFields = new HashMap();
    String [] ret;
    try{
      ret =
        configFile.getValues(dbName,
            "hidden "+tableName+" fields");
      dbDefFields.put(tableName, ret);
      return ret;
    }
    catch(Exception e){
      // hard default
      return new String []  {"actualPars"};
    }
  }

  public synchronized String [] getVersions(final String homePackage){
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getVersions(homePackage);
        }
        catch(Throwable t){
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

  public synchronized void clearPanel(final String [] cstAttributesNames,
     final JComponent [] tcCstAttributes,
     final JPanel jobXmlContainer,
     final Vector tcConstant){    
    MyThread t = new MyThread(){
      public void run(){
        try{
           pu.clearPanel(cstAttributesNames, tcCstAttributes, jobXmlContainer,
               tcConstant);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "clearPanel"))
      return;
    else
      return;
  }

  public synchronized void initAttributePanel(
     final String [] cstAttributesNames,
     final String [] cstAttr,
     final JComponent [] tcCstAttributes,
     final JPanel pAttributes,
     final JPanel jobXmlContainer){
    MyThread t = new MyThread(){
      public void run(){
        try{
           pu.initAttributePanel(cstAttributesNames, cstAttr, tcCstAttributes,
               pAttributes, jobXmlContainer);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "initAttributePanel"))
      return;
    else
      return;
  }

  public synchronized void setEnabledAttributes(final boolean enabled,
     final String [] cstAttributesNames,
     final JComponent [] tcCstAttributes){
    MyThread t = new MyThread(){
      public void run(){
        try{
           pu.setEnabledAttributes(enabled, cstAttributesNames, tcCstAttributes);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "setEnabledAttributes"))
      return;
    else
      return;
  }

  public synchronized void setValuesInAttributePanel(final String [] cstAttributesNames,
     final String [] cstAttr,
     final JComponent [] tcCstAttributes){    
    MyThread t = new MyThread(){
      public void run(){
        try{
           pu.setValuesInAttributePanel(cstAttributesNames, cstAttr, tcCstAttributes);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
          t.printStackTrace();
        }
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "setValuesInAttributePanel"))
      return;
    else
      return;
  }
  
  public String getJTextOrEmptyString(final String attr, final JComponent comp,
     final boolean editing){    
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = pu.getJTextOrEmptyString(attr, comp, editing);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
      public String getStringRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJTextOrEmptyString"))
      return t.getStringRes();
    else
      return null;
  }

  public Vector getNonIdTextFields(final String [] cstAttributesNames,
     final JComponent [] tcCstAttributes, final Vector tcConstant){
    MyThread t = new MyThread(){
      Vector res = null;
      public void run(){
        try{
          res = pu.getNonIdTextFields(cstAttributesNames, tcCstAttributes,
              tcConstant);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
      public Vector getVectorRes(){return res;}
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getNonIdTextFields"))
      return t.getVectorRes();
    else
      return null;    
  }
}