package gridpilot;

import java.awt.Color;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

import javax.swing.JOptionPane;

import gridpilot.GridPilot;
import gridpilot.ConfigFile;
import gridpilot.Debug;
import gridpilot.LogFile;
import gridpilot.Database;
import gridpilot.MyThread;

/**
 * This class manages access to databases.
 *
 */
public class DBPluginMgr implements Database{

  private ConfigFile configFile;
  private LogFile logFile;
  private Database db;
  private String dbName;
  
  // TODO: cache here??
  //private HashMap partInfoCacheId = null ;

  // Time out in ms used if neither the specific time out nor "default timeout" is
  // defined in configFile
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
    }

    db = (Database) Util.loadClass(dbClass, dbArgsType, dbArgs);

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
      String transformationName, String transformationVersion){
    Debug.debug("finding target dataset name for "+sourceDatasetName+" in "+targetDB+
        " with tranformation "+transformationName, 3);
        
    String findString = "";
    String replaceString = "";
    Pattern p = null;
    Matcher m = null;
    String s = "";
    String ret = "";
    if(transformationName!=null){
      // TODO: make these patterns configurable
      s = ".*g4sim.*";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(transformationName);
      if(m.matches()){
        replaceString = "SimProd";
      }
      s = ".*g4digit.*";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(transformationName);
      if(m.matches()){
        replaceString = "DigitProd";
      }
      s = ".*reconstruction.*";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(transformationName);
      if(m.matches()){
        replaceString = "ReconProd";
      }
      
      findString = "SimProd";
      Debug.debug("replacing "+findString+" -> "+replaceString, 3);
      ret = sourceDatasetName.replaceFirst(findString, replaceString);
      
      findString = "DigitProd"; 
      Debug.debug("replacing "+findString+" -> "+replaceString, 3);
      ret = ret.replaceFirst(findString, replaceString);
      
      findString = "ReconProd";  
      Debug.debug("replacing "+findString+" -> "+replaceString, 3);
      ret = ret.replaceFirst(findString, replaceString);
      
      Debug.debug("String now "+ret, 3);
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
    if(transformationVersion!=null && !transformationVersion.equals("")){
      // Change the version to match the transformation version
      s = "\\.v\\w*\\.\\w*$";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(ret);
      if(!matched){
        Debug.debug("replacing version", 3);
        ret1 = m.replaceAll("."+transformationVersion);
        if(!ret.equals(ret1)){
          matched = true;
        }
      }
      s = "\\.\\w*\\.v\\w*$";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(ret);
      if(!matched){
        Debug.debug("replacing version", 3);
        ret1 = m.replaceAll("."+transformationVersion);
        if(!ret.equals(ret1)){
          matched = true;
        }
      }
      s = "\\.v\\w*$";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(ret);
      if(!matched){
        Debug.debug("replacing version", 3);
        ret1 = m.replaceAll("."+transformationVersion);
        if(!ret.equals(ret1)){
          matched = true;
        }
      }
    }
    
    return ret1;
  }

  public synchronized String getJobDefCreationPanelClass(){
    return db.getJobDefCreationPanelClass();
  }

  public synchronized String [] getFieldnames(final String table){
    String [] tmpFieldNames = getFieldNames(table);
    for(int i=0; i<tmpFieldNames.length; ++i){
      tmpFieldNames[i] = tmpFieldNames[i].toLowerCase();
    }
    return tmpFieldNames;
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
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getFieldNames")){
      return t.getString2Res();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getPackInitText")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getStdOutFinalDest")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getStdErrFinalDest")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public synchronized String getError(){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getError();
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getError")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getExtractScript")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getValidationScript")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTransformationScript")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public synchronized String [] getTransformationRTEnvironments(final int jobDefinitionID){
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getTransformationRTEnvironments(jobDefinitionID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTransformationRTEnvironments")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public synchronized String [] getTransformationArguments(final int jobDefinitionID){
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getTransformationArguments(jobDefinitionID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTransformationSignature")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public synchronized String getTransformationRuntimeEnvironment(final int transformationID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getTransformationRuntimeEnvironment(transformationID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             transformationID, t);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTransformationRuntimeEnvironment")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefUser")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getDatasetName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getRunNumber")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public synchronized int getTransformationID(final String transName, final String transVersion){
    MyThread t = new MyThread(){
      int res = -1;
      public void run(){
        try{
          res = db.getTransformationID(transName, transVersion);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             transName+":"+transVersion, t);
        }
      }
      public int getIntRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTransformationID")){
      return t.getIntRes();
    }
    else{
      return -1;
    }
  }

  public synchronized int getDatasetID(final String datasetName){
    MyThread t = new MyThread(){
      int res = -1;
      public void run(){
        try{
          res = db.getDatasetID(datasetName);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetName, t);
        }
      }
      public int getIntRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getDatasetID")){
      return t.getIntRes();
    }
    else{
      return -1;
    }
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
      public int getIntRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefDatasetID")){
      return t.getIntRes();
    }
    else{
      return -1;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobStatus")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefValue")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobRunValue")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getUserLabel")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public synchronized String getJobDefTransformationID(final int jobDefID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getJobDefTransformationID(jobDefID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTransformationID")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public synchronized String getDatasetTransformationName(final int datasetID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getDatasetTransformationName(datasetID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetID, t);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getDatasetTransformationName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public synchronized String getDatasetTransformationVersion(final int datasetID){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getDatasetTransformationVersion(datasetID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetID, t);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getDatasetTransformationVersion")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public synchronized String getTransformationValue(final int jobDefID, final String key){
    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = db.getTransformation(
              Integer.parseInt(db.getJobDefTransformationID(jobDefID))
              ).getValue(key).toString();
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTransformationValue")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getOutputs")){
      return t.getString2Res();
    }
    else{
      return null;
    }
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
        public String [] getString2Res(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "getInputs")){
        return t.getString2Res();
      }
      else{
        return null;
      }
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
        public String [] getString2Res(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "getJobDefTransPars")){
        return t.getString2Res();
      }
      else{
        return null;
      }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefOutLocalName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefInLocalName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefOutRemoteName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefInRemoteName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getDefVals")){
      return t.getString2Res();
    }
    else{
      return null;
    }
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
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTransJobParameters")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public synchronized String [] getTransOutputs(final int transformationID){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getTransOutputs(transformationID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             transformationID, t);
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTransOutputs")){
      return t.getString2Res();
    }
    else{
      return null;
    }
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
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "saveDefVals")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
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
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "createJobDefinition")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean createJobDefinition(
      final String datasetName,
      final String [] cstAttrNames,
      final String [] resCstAttr,
      final String [] trpars,
      final String [] [] ofmap,
      final String odest,
      final String edest){
    
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.createJobDefinition(datasetName, cstAttrNames, resCstAttr,
              trpars, ofmap, odest, edest);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetName, t);
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "createJobDefinition")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
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
        public boolean getBoolRes(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "createRunInfo")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  // Here, in contrast to updateJobDef (in DBPluginMgr), because it is not needed by other
  // classes and it needs to update display.
  public DBRecord createJobDef(String [] fields, Object [] values) throws Exception{
    
    String [] jobDefFieldNames = getFieldNames("jobDefinition");
    
    if(fields.length!=values.length){
      throw new DataFormatException("The number of fields and values do not agree, "+
          fields.length+"!="+values.length);
    }
    if(fields.length>jobDefFieldNames.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+jobDefFieldNames.length, 1);
    }
    String [] vals = new String[jobDefFieldNames.length];
    for(int i=0; i<jobDefFieldNames.length; ++i){
      vals[i] = "";
      for(int j=0; i<fields.length; ++j){
        if(fields[j].equalsIgnoreCase(jobDefFieldNames[i]) &&
            !fields[j].equalsIgnoreCase(getIdentifierField("jobDefinition"))){
          vals[i] = values[j].toString();
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
      throw new IOException("ERROR: createJobDefinition failed");
    }
  }

  public synchronized boolean createTransformation(final Object [] values){
    
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
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "createTransformation")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean createRuntimeEnvironment(final Object [] values){
    
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.createRuntimeEnvironment(values);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             values.toString(), t);
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "createRuntimeEnvironment")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean createDataset(final String targetTable,
      final String [] fields, final Object [] values){
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
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "createDataset")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
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
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "setJobDefinitionField")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
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
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "updateJobDefinition")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
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
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "updateJobDefinition")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
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
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "updateJobDefStatus")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
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
        public boolean getBoolRes(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "updateRunInfo")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
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
        public boolean getBoolRes(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "updateDataset")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  public synchronized boolean updateTransformation(final int transformationID,
      final String [] fields, final String [] values){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.updateTransformation(transformationID, fields, values);
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               transformationID, t);
          }
        }
        public boolean getBoolRes(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "updateTransformation")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  public synchronized boolean updateRuntimeEnvironment(final int runtimeEnvironmentID,
    final String [] fields, final String [] values){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.updateRuntimeEnvironment(runtimeEnvironmentID, fields, values);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             runtimeEnvironmentID, t);
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "updateRuntimeEnvironment")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
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
        public boolean getBoolRes(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "deleteJobDefinition")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
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
        public boolean getBoolRes(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "deleteDataset")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
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
        public boolean getBoolRes(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "deleteTransformation")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  public synchronized boolean deleteRuntimeEnvironment(final int runtimeEnvironmentID){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void run(){
          try{
            res = db.deleteRuntimeEnvironment(runtimeEnvironmentID);
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               runtimeEnvironmentID, t);
          }
        }
        public boolean getBoolRes(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "deleteRuntimeEnvironment")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
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
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "reserveJobDefinition")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean cleanRunInfo(final int jobDefID){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = db.cleanRunInfo(jobDefID);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefID, t);
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "jobDefID")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
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
      public DBResult getDB2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "select")){
      return t.getDB2Res();
    }
    else{
      return null;
    }
  }

  public synchronized DBResult getRuntimeEnvironments(){
    
    MyThread t = new MyThread(){
      DBResult res = null;
      public void run(){
        try{
          res = db.getRuntimeEnvironments();
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
      public DBResult getDB2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getRuntimeEnvironments")){
      return t.getDB2Res();
    }
    else{
      return null;
    }
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
      public DBResult getDB2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getTransformations")){
      return t.getDB2Res();
    }
    else{
      return null;
    }
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
        public DBRecord getDBRes(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "getDataset")){
        return t.getDBRes();
      }
      else{
        return null;
      }
    }

  public synchronized DBRecord getRuntimeEnvironment(final int runtimeEnvironmentID){
    
      MyThread t = new MyThread(){
        DBRecord res = null;
        public void run(){
          try{
            res = db.getRuntimeEnvironment(runtimeEnvironmentID);
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               runtimeEnvironmentID, t);
          }
        }
        public DBRecord getDBRes(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "getRuntimeEnvironment")){
        return t.getDBRes();
      }
      else{
        return null;
      }
    }

  public synchronized DBRecord getTransformation(final int transformationID){
    
      MyThread t = new MyThread(){
        DBRecord res = null;
        public void run(){
          try{
            res = db.getTransformation(transformationID);
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               transformationID, t);
          }
        }
        public DBRecord getDBRes(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "getTransformation")){
        return t.getDBRes();
      }
      else{
        return null;
      }
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
        public DBRecord getDBRes(){
          return res;
        }
      };
    
      t.start();
    
      if(waitForThread(t, dbName, dbTimeOut, "getRunInfo")){
        return t.getDBRes();
      }
      else{
        return null;
      }
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
      public DBResult getDB2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefinitions")){
      return t.getDB2Res();
    }
    else{
      return null;
    }
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
      public DBRecord getDBRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "getJobDefinition")){
      return t.getDBRes();
    }
    else{
      return null;
    }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(waitForThread(t, dbName, dbTimeOut, "connect")){
      return t.getStringRes();
    }
    else{
      return null;
    }
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
  
    if(waitForThread(t, dbName, dbTimeOut, "disconnect")){
      return;
    }
    else{
      return;
    }
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
  
    if(waitForThread(t, dbName, dbTimeOut, "clearCaches")){
      return;
    }
    else{
      return;
    }
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
    }
    while(true);
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
    
  public synchronized String [] getDBDefFields(String tableName){
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
  
  public String getIdentifierField(String table){
    String ret = configFile.getValue(dbName,
      table+" identifier");
    if(ret==null || ret.equals("")){
      ret = "identifier";
    }
    Debug.debug("Identifier for "+dbName+" - "+table+" : "+ret, 2);
    return ret;
  }

  /**
   * Get the name of the column holding the name.
   */
  public String getNameField(String table){
    String ret = configFile.getValue(dbName,
      table+" name");
    if(ret==null || ret.equals("")){
      ret = "name";
    }
    Debug.debug("Name for "+dbName+" - "+table+" : "+ret, 2);
    return ret;
  }

  /**
   * Get the name of the column holding the version.
   */
  public synchronized String getVersionField(String table){
    String ret = configFile.getValue(dbName,
      table+" version");
    if(ret==null || ret.equals("")){
      ret = "version";
    }
    Debug.debug("Version for "+dbName+" - "+table+" : "+ret, 2);
    return ret;
  }

  public synchronized String [] getJobDefDatasetReference(){
    String [] ret = configFile.getValues(dbName,
      "jobDefinition dataset reference");
    if(ret==null || ret.length<2){
      ret = new String [] {"name", "datasetName"};
    }
    Debug.debug("jobDef dataset reference for "+dbName
        +" : "+Util.arrayToString(ret), 2);
    return ret;
  }

  public synchronized String [] getDatasetTransformationReference(){
    String [] ret = configFile.getValues(dbName,
      "dataset transformation reference");
    if(ret==null || ret.length<2){
      ret = new String [] {"name", "transformationName"};
    }
    Debug.debug("dataset transformation reference for "+dbName
        +" : "+Util.arrayToString(ret), 2);
    return ret;
  }

  public synchronized String [] getDatasetTransformationVersionReference(){
    String [] ret = configFile.getValues(dbName,
      "dataset transformation version reference");
    if(ret==null || ret.length<2){
      ret = new String [] {"version", "transformationVersion"};
    }
    Debug.debug("dataset transformation version reference for "+dbName
        +" : "+Util.arrayToString(ret), 2);
    return ret;
  }

  public synchronized String [] getTransformationRuntimeReference(){
    String [] ret = configFile.getValues(dbName,
      "transformation runtime environment reference");
    if(ret==null || ret.length<2){
      ret = new String [] {"name", "runtimeEnvironmentName"};
    }
    Debug.debug("transformation runtime environment reference for "+dbName
        +" : "+Util.arrayToString(ret), 2);
    return ret;
  }

  public synchronized String [] getDBHiddenFields(String tableName){
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

  public synchronized String [] getVersions(final String transformationName){
    MyThread t = new MyThread(){
      String [] res = null;
      public void run(){
        try{
          res = db.getVersions(transformationName);
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
  
    if(waitForThread(t, dbName, dbTimeOut, "getVersions")){
      return t.getString2Res();
    }
    else{
      return new String [] {};
    }
  }

  /** 
   * Split events over multiple logicalFiles for
   * a dataset by consulting
   * dataset.totalEvents and dataset.totalFiles.
   * Returns a list of
   * {logicalFileEventMin,logicalFileEventMax} dublets.
   */
  public int [][] getEventSplits(int dataset){
    String arg = "";
    DBResult res = null;
    // totalEvents is the total number of events.
    // nrEvents is events per file.
    int nrEvents = 0;
    int totalEvents = 0;
    int totalFiles = 0;
    int [][] splits = {{0,0}};
    String debug = "";
    
    arg = "select totalEvents, totalFiles from dataset where identifier='"+
    dataset+"'";
    res = select(arg, getIdentifierField("dataset"));
    if(res.values.length>0){
      try{
        totalEvents = Integer.parseInt(res.values[0][0].toString());
        totalFiles = Integer.parseInt(res.values[0][1].toString());
      }
      catch(Exception e){
        Debug.debug("ERROR: could not split. "+e.getMessage(), 2);
        e.printStackTrace();
      }
    }
    if(totalFiles>0 && totalEvents>0){
      Debug.debug("Found totalFiles: "+totalFiles+", totalEvents: "+totalEvents, 2);
      nrEvents = (totalEvents-(totalEvents%totalFiles))/totalFiles;
      if((totalEvents%nrEvents)>0){
        splits = new int [totalFiles+1][2];
      }
      else{
        splits = new int [totalFiles][2];
      }
      for(int i=0; i<totalFiles; ++i){
        splits[i][0] = i*nrEvents+1;
        splits[i][1] = (i+1)*nrEvents;
        debug += "{"+splits[i][0]+","+splits[i][1]+"}";
      }
      if((totalEvents%nrEvents)>0){
        splits[totalFiles][0] = totalFiles*nrEvents+1;
        splits[totalFiles][1] = totalFiles*nrEvents+totalEvents%nrEvents;
        debug += "{"+splits[totalFiles][0]+","+splits[totalFiles][1]+"}";
      }
    }
    Debug.debug("Splitting according to "+debug, 2);
    return splits;
  }

}