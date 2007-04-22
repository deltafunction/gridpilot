package gridpilot;

import java.awt.Color;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

import org.safehaus.uuid.UUIDGenerator;

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
public class DBPluginMgr extends DBCache implements Database{

  private ConfigFile configFile;
  private LogFile logFile;
  private Database db;
  private String dbName;
  private String description;
  
  // TODO: cache here??
  //private HashMap partInfoCacheId = null ;

  // Time out in ms used if neither the specific time out nor "default timeout" is
  // defined in configFile
  private int dbTimeOut = 60*1000;

  public DBPluginMgr(String _dbName){
    dbName = _dbName;
    try{
      description = GridPilot.getClassMgr().getConfigFile().getValue(dbName, "description");
    }
    catch(Exception e){
    }
    if(description==null){
      description = dbName;
    }
  }

  public String getDBName(){
    return dbName;
  }
  
  public String getDBDescription(){
    return description;
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
    if(dbClass==null){
      throw new Exception("Cannot load class for system " + dbName + " : \n"+
                          configFile.getMissingMessage(dbName, "class"));
    }

    String [] parameters = configFile.getValues(dbName, "parameters");
    Class [] dbArgsType = new Class[parameters.length+1];
    Object [] dbArgs = new String[parameters.length+1];
    dbArgsType[0] = String.class;
    dbArgs[0] = dbName;
    for(int i=0; i<parameters.length; ++i){
      dbArgsType[i+1] = String.class;
      dbArgs[i+1] = configFile.getValue(dbName, parameters[i]);
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
        dbTimeOut = 1000*dbTimeOut;
      }
      catch(NumberFormatException nfa){
        logFile.addMessage("value of db timeout (" + tmp +") is not an integer");
      }
    }
  }

  public boolean updateJobStdoutErr(String jobDefID, String result){
    return updateJobDefinition(jobDefID, new String [] {"validationResult"}, new String [] {result});
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

  //NOTICE: lower case: getFieldnames != getFieldNames
  public String [] getFieldnames(final String table){
    String [] tmpFieldNames = getFieldNames(table);
    for(int i=0; i<tmpFieldNames.length; ++i){
      tmpFieldNames[i] = tmpFieldNames[i].toLowerCase();
    }
    return tmpFieldNames;
  }
  
  public String [] getFieldNames(final String table){
    Debug.debug("Getting field names for table "+table, 3);
   
    MyThread t = new MyThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getFieldNames")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public String getRuntimeInitText(final String runtimeEnvName, final String csName){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getRuntimeInitText(runtimeEnvName, csName);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             runtimeEnvName + " " + csName, t);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getPackInitText")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getStdOutFinalDest(final String jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getStdOutFinalDest")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getStdErrFinalDest(final String jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getStdErrFinalDest")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getError(){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getError")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getTransformationScript(final String jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getTransformationScript")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String [] getRuntimeEnvironments(final String jobDefinitionID){
    MyThread t = new MyThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getRuntimeEnvironments(jobDefinitionID);
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getTransformationRTEnvironments")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public String [] getTransformationArguments(final String jobDefinitionID){
    MyThread t = new MyThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getTransformationSignature")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public String getTransformationRuntimeEnvironment(final String transformationID){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getTransformationRuntimeEnvironment")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getJobDefUserInfo(final String jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getJobDefUserInfo(jobDefinitionID);
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getJobDefUser")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getJobDefName(final String jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getJobDefName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getDatasetName(final String datasetID){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getDatasetName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getRunNumber(final String datasetID){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getRunNumber")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getRuntimeEnvironmentID(final String name, final String cs){
    MyThread t = new MyThread(){
      String res = "-1";
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getRuntimeEnvironmentID(name, cs);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             name+":"+cs, t);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getRuntimeEnvironmentID")){
      return t.getStringRes();
    }
    else{
      return "-1";
    }
  }

  public String getTransformationID(final String transName, final String transVersion){
    MyThread t = new MyThread(){
      String res = "-1";
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getTransformationID")){
      return t.getStringRes();
    }
    else{
      return "-1";
    }
  }

  public String getDatasetID(final String datasetName){
    MyThread t = new MyThread(){
      String res = "-1";
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getDatasetID")){
      return t.getStringRes();
    }
    else{
      return "-1";
    }
  }

  public String getFileID(final String datasetName, final String fileName){
    MyThread t = new MyThread(){
      String res = "-1";
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getFileID(datasetName, fileName);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetName, t);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getFileID")){
      return t.getStringRes();
    }
    else{
      return "-1";
    }
  }

  public String getJobDefDatasetID(final String jobDefinitionID){
    MyThread t = new MyThread(){
      String res = "-1";
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getJobDefDatasetID")){
      return t.getStringRes();
    }
    else{
      return "-1";
    }
  }

  public String getJobDefStatus(final String jobDefinitionID){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getJobDefStatus(jobDefinitionID);
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getJobStatus")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getJobDefValue(final String jobDefID, final String key){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          Object ret = db.getJobDefinition(jobDefID).getValue(key);
          if(ret!=null){
            res = (String) ret;
          }
          else{
            res = null;
          }
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +jobDefID+":"+key+":"+
                             jobDefID, t);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getJobDefValue")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getRunInfo(final String jobDefID, final String key){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getRunInfo(jobDefID, key).toString();
        }
        catch(Throwable t){
          Debug.debug((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName , 2);
          res = "";
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getRunInfo")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getJobDefTransformationID(final String jobDefID){
    MyThread t = new MyThread(){
      String res = "-1";
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getTransformationID")){
      return t.getStringRes();
    }
    else{
      return "-1";
    }
  }

  public String getDatasetTransformationName(final String datasetID){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getDatasetTransformationName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getDatasetTransformationVersion(final String datasetID){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getDatasetTransformationVersion")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getTransformationValue(final String jobDefID, final String key){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getTransformation(
              db.getJobDefTransformationID(jobDefID)).getValue(key).toString();
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getTransformationValue")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String [] getOutputFiles(final String jobDefID){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getOutputFiles(jobDefID);
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getOutputs")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public String [] getJobDefInputFiles(final String jobDefID){
    
      MyThread t = new MyThread(){
        String [] res = null;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
        public void run(){
          try{
            res = db.getJobDefInputFiles(jobDefID);
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
    
      if(Util.waitForThread(t, dbName, dbTimeOut, "getInputs")){
        return t.getString2Res();
      }
      else{
        return null;
      }
    }

  public String [] getJobDefTransPars(final String jobDefID){
    
      MyThread t = new MyThread(){
        String [] res = null;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
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
    
      if(Util.waitForThread(t, dbName, dbTimeOut, "getJobDefTransPars")){
        return t.getString2Res();
      }
      else{
        return null;
      }
    }

  public String getJobDefOutLocalName(final String jobDefID, final String outpar){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getJobDefOutLocalName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getJobDefOutRemoteName(final String jobDefinitionID, final String outpar){
    MyThread t = new MyThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getJobDefOutRemoteName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String [] getTransformationJobParameters(final String transformationID){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getTransformationJobParameters(transformationID);
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getTransJobParameters")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public String [] getTransformationOutputs(final String transformationID){
  
    MyThread t = new MyThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getTransformationOutputs(transformationID);
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getTransOutputs")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public String [] getTransformationInputs(final String transformationID){
    
    MyThread t = new MyThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getTransformationInputs(transformationID);
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getTransInputs")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public synchronized boolean isFileCatalog(){
    
    MyThread t = new MyThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.isFileCatalog();
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "isFileCatalog")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean isJobRepository(){
    
    MyThread t = new MyThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.isJobRepository();
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "isJobRepository")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean createJobDefinition(final String [] values){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "createJobDefinition")){
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
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "createJobDefinition")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

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
      for(int j=0; j<fields.length; ++j){
        if(fields[j].equalsIgnoreCase(jobDefFieldNames[i]) &&
            !fields[j].equalsIgnoreCase(Util.getIdentifierField(dbName, "jobDefinition"))){
          if(vals[i]==null){
            vals[i] = "";
         }
          else{
            vals[i] = (String) values[j];
          }
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

  public DBRecord createTrans(String [] fields, Object [] values) throws Exception{
    
    String [] transFieldNames = getFieldNames("transformation");
    
    if(fields.length!=values.length){
      throw new DataFormatException("The number of fields and values do not agree, "+
          fields.length+"!="+values.length);
    }
    if(fields.length>transFieldNames.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+transFieldNames.length, 1);
    }
    String [] vals = new String[transFieldNames.length];
    for(int i=0; i<transFieldNames.length; ++i){
      vals[i] = "";
      for(int j=0; j<fields.length; ++j){
        if(fields[j].equalsIgnoreCase(transFieldNames[i]) &&
            !fields[j].equalsIgnoreCase(Util.getIdentifierField(dbName, "transformation"))){
          if(vals[i]==null){
            vals[i] = "";
          }
          else{
            vals[i] = (String) values[j];
          }
          break;
        }
      }
    }
    DBRecord jobDef = new DBRecord(transFieldNames, vals);
    if(createTransformation(vals)){
       return jobDef;
    }
    else{
      throw new IOException("ERROR: createTransformation failed");
    }
  }

  public DBRecord createRuntimeEnv(String [] fields, Object [] values) throws Exception{
    
    String [] runtimeFieldNames = getFieldNames("runtimeEnvironment");
    
    if(fields.length!=values.length){
      throw new DataFormatException("The number of fields and values do not agree, "+
          fields.length+"!="+values.length);
    }
    if(fields.length>runtimeFieldNames.length){
      Debug.debug("The number of fields is too large, "+
          fields.length+">"+runtimeFieldNames.length, 1);
    }
    String [] vals = new String[runtimeFieldNames.length];
    for(int i=0; i<runtimeFieldNames.length; ++i){
      vals[i] = "";
      for(int j=0; j<fields.length; ++j){
        if(fields[j].equalsIgnoreCase(runtimeFieldNames[i]) &&
            !fields[j].equalsIgnoreCase(Util.getIdentifierField(dbName, "runtimeEnvironment"))){
          if(vals[i]==null){
            vals[i] = "";
          }
          else{
            vals[i] = (String) values[j];
          }
          break;
        }
      }
    }
    DBRecord jobDef = new DBRecord(runtimeFieldNames, vals);
    if(createRuntimeEnvironment(vals)){
       return jobDef;
    }
    else{
      throw new IOException("ERROR: createRuntimeEnvironment failed");
    }
  }

  /**
   * Create a new record in a file table.The idea is to pass on
   * values immediately grabbable from the source table, then look up
   * the file locations. 
   */
  public void createFil(DBPluginMgr sourceMgr, String datasetName, String name,
      String id) throws Exception{
  
    // In case the file was copied from a virtual table from a job repository,
    // strip the extension off when querying for the identifier.
    String fileName = name;
    if(!sourceMgr.isFileCatalog() && sourceMgr.isJobRepository()){
      int lastDot = fileName.lastIndexOf(".");
      if(lastDot>-1){
        fileName = fileName.substring(0, lastDot);
      }  
    }
    
    String datasetID = sourceMgr.getDatasetID(datasetName);
    //String id = sourceMgr.getFileID(datasetName, fileName);
    String [] urls = sourceMgr.getFileURLs(datasetName, id, true);
    
    String uuid = id;
    // In case the file was copied from a virtual table from a job repository,
    // generate new GUID
    if(!sourceMgr.isFileCatalog() && sourceMgr.isJobRepository()){
      uuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
      String message = "Generated new UUID "+uuid.toString()+" for "+fileName;
      GridPilot.getClassMgr().getLogFile().addInfo(message);
    }
    
    boolean ok = true;
    boolean finalOk = true;
    for(int i=0; i<urls.length; ++i){
      try{
        registerFileLocation(datasetID, datasetName, uuid, name, urls[i],
            false);
        finalOk = finalOk || ok;
        ok = true;
      }
      catch(Exception e){
        logFile.addMessage("ERROR: could not register "+urls[i]+" for file "+
            name+" in dataset "+datasetName, e);
        ok = false;
      }
    }
    if(!finalOk){
      throw new IOException("ERROR: could not register any files.");
    }
  }

  public synchronized boolean createTransformation(final Object [] values){
    
    MyThread t = new MyThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "createTransformation")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean createRuntimeEnvironment(final Object [] values){
    
    MyThread t = new MyThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "createRuntimeEnvironment")){
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
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "createDataset")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean setJobDefsField(final String [] identifiers,
      final String field, final String value){  
    MyThread t = new MyThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "setJobDefinitionField")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean updateJobDefinition(final String jobDefID,
      final String [] fields, final String [] values){
    MyThread t = new MyThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "updateJobDefinition")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean updateJobDefinition(final String jobDefID,
      final String [] values){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "updateJobDefinition")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean updateDataset(final String datasetID,
      final String [] fields, final String [] values){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
        public void run(){
          try{
            res = db.updateDataset(datasetID, fields, values);
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
    
      if(Util.waitForThread(t, dbName, dbTimeOut, "updateDataset")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  public synchronized boolean updateTransformation(final String transformationID,
      final String [] fields, final String [] values){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
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
    
      if(Util.waitForThread(t, dbName, dbTimeOut, "updateTransformation")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  public synchronized boolean updateRuntimeEnvironment(final String runtimeEnvironmentID,
    final String [] fields, final String [] values){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "updateRuntimeEnvironment")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean deleteJobDefinition(final String jobDefID, final boolean cleanup){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
        public void run(){
          try{
            res = db.deleteJobDefinition(jobDefID, cleanup);
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
    
      if(Util.waitForThread(t, dbName, dbTimeOut, "deleteJobDefinition")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  public synchronized boolean deleteFiles(final String datasetID,
      final String [] fileIDs, final boolean cleanup){
    
    MyThread t = new MyThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.deleteFiles(datasetID, fileIDs, cleanup);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             Util.arrayToString(fileIDs), t);
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "deleteFile")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean deleteDataset(final String datasetID, final boolean cleanup){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
        public void run(){
          try{
            res = db.deleteDataset(datasetID, cleanup);
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
    
      if(Util.waitForThread(t, dbName, dbTimeOut, "deleteDataset")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  public synchronized boolean deleteTransformation(final String transformationID){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
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
    
      if(Util.waitForThread(t, dbName, dbTimeOut, "deleteTransformation")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  public synchronized boolean deleteRuntimeEnvironment(final String runtimeEnvironmentID){
    
      MyThread t = new MyThread(){
        boolean res = false;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
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
    
      if(Util.waitForThread(t, dbName, dbTimeOut, "deleteRuntimeEnvironment")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  public synchronized boolean reserveJobDefinition(final String jobDefID, final String userName,
      final String cs){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.reserveJobDefinition(jobDefID, userName, cs);
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "reserveJobDefinition")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean cleanRunInfo(final String jobDefID){
  
    MyThread t = new MyThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "jobDefID")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public DBResult select(final String selectQuery, final String identifier,
      final boolean findAll){
  
    MyThread t = new MyThread(){
      DBResult res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.select(selectQuery, identifier, findAll);
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "select")){
      return t.getDB2Res();
    }
    else{
      return null;
    }
  }

  public DBResult getRuntimeEnvironments(){
    
    MyThread t = new MyThread(){
      DBResult res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getRuntimeEnvironments")){
      return t.getDB2Res();
    }
    else{
      return null;
    }
  }

  public DBResult getTransformations(){
  
    MyThread t = new MyThread(){
      DBResult res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getTransformations")){
      return t.getDB2Res();
    }
    else{
      return null;
    }
  }

  public DBRecord getDataset(final String datasetID){
    
      MyThread t = new MyThread(){
        DBRecord res = null;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
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
    
      if(Util.waitForThread(t, dbName, dbTimeOut, "getDataset")){
        return t.getDBRes();
      }
      else{
        return null;
      }
    }

  public DBRecord getRuntimeEnvironment(final String runtimeEnvironmentID){
    
      MyThread t = new MyThread(){
        DBRecord res = null;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
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
    
      if(Util.waitForThread(t, dbName, dbTimeOut, "getRuntimeEnvironment")){
        return t.getDBRes();
      }
      else{
        return null;
      }
    }

  public DBRecord getTransformation(final String transformationID){
    
      MyThread t = new MyThread(){
        DBRecord res = null;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
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
    
      if(Util.waitForThread(t, dbName, dbTimeOut, "getTransformation")){
        return t.getDBRes();
      }
      else{
        return null;
      }
    }

  public DBResult getFiles(final String datasetID){
    
    MyThread t = new MyThread(){
      DBResult res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getFiles(datasetID);
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getFiles")){
      return t.getDB2Res();
    }
    else{
      return null;
    }
  }

  public DBResult getJobDefinitions(final String datasetID, final String [] fieldNames,
      final String [] statusList, final String [] csStatusList){
  
    MyThread t = new MyThread(){
      DBResult res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getJobDefinitions(datasetID, fieldNames, statusList, csStatusList);
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getJobDefinitions")){
      return t.getDB2Res();
    }
    else{
      return null;
    }
  }

  public DBRecord getJobDefinition(final String jobDefinitionID){
  
    MyThread t = new MyThread(){
      DBRecord res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getJobDefinition")){
      return t.getDBRes();
    }
    else{
      return null;
    }
  }

  public void disconnect(){
  
    MyThread t = new MyThread(){
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "disconnect")){
      return;
    }
    else{
      return;
    }
  }

  public synchronized void clearCaches(){
  
    MyThread t = new MyThread(){
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          Debug.debug("Clearing cache of "+dbName, 2); 
          db.clearCaches();
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "clearCaches")){
      return;
    }
    else{
      return;
    }
  }

  public synchronized void registerFileLocation(final String datasetID,
      final String datasetName, final String fileID, final String lfn,
      final String url, final boolean datasetComplete){
    
    MyThread t = new MyThread(){
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
           db.registerFileLocation(datasetID, datasetName, fileID, lfn, url, datasetComplete);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "registerFileLocation")){
      return;
    }
    else{
      return;
    }
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
      case UNEXPECTED : return "Unexpected";
      default : return "status not found";
    }
  }

  public static int getStatusId(String status){
    for(int i=1; i<=7 ; ++i){
      if(status.compareToIgnoreCase(getStatusName(i))==0)
        return i;
    }
    Debug.debug("ERROR: the status "+status+" does not correspond to any known status ID", 1);
    return -1;
  }

  public static String [] getSdtOutput(){
    String [] res = {"stdout", "stderr"};
    return res;
  }
    
  public String [] getDBDefFields(String tableName){
    HashMap dbDefFields = new HashMap();
    String [] ret;
    try{
      ret = configFile.getValues(dbName, "default "+tableName+" fields");
      dbDefFields.put(tableName, ret);
      return ret;
    }
    catch(Exception e){
      // hard default
      return new String []  {"*"};
    }
  }
  
  public String [] getDBHiddenFields(String tableName){
    HashMap dbDefFields = new HashMap();
    String [] ret;
    try{
      ret =
        configFile.getValues(dbName, "hidden "+tableName+" fields");
      dbDefFields.put(tableName, ret);
      return ret;
    }
    catch(Exception e){
      // hard default
      return new String []  {"actualPars"};
    }
  }

  public String [] getVersions(final String transformationName){
    MyThread t = new MyThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
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
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getVersions")){
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
  public int [][] getEventSplits(String datasetID){
    String arg = "";
    DBResult res = null;
    // totalEvents is the total number of events.
    // nrEvents is events per file.
    int nrEvents = 0;
    int totalEvents = 0;
    int totalFiles = 0;
    int [][] splits = {{0, 0}};
    String debug = "";
    
    arg = "select totalEvents, totalFiles from dataset where identifier='"+
    datasetID+"'";
    res = select(arg, Util.getIdentifierField(dbName, "dataset"), true);
    if(res.values.length>0){
      try{
        totalEvents = Integer.parseInt(res.values[0][0].toString());
        totalFiles = Integer.parseInt(res.values[0][1].toString());
      }
      catch(Exception e){
        Debug.debug("ERROR: could not split. "+e.getMessage(), 2);
        e.printStackTrace();
        return null;
      }
    }
    if(totalFiles==0 && totalEvents==0){
      return null;
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

  public DBRecord getFile(final String datasetName, final String fileID,
      final int lookupPFNs){
    
    MyThread t = new MyThread(){
      DBRecord res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getFile(datasetName, fileID, lookupPFNs);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             fileID, t);
        }
      }
      public DBRecord getDBRes(){
        return res;
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getFile")){
      return t.getDBRes();
    }
    else{
      return null;
    }
  }

  public String [] getFileURLs(final String datasetName, final String fileID,
      final boolean findAll){
    Debug.debug("Getting field names for file # "+fileID, 3);
   
    MyThread t = new MyThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          res = db.getFileURLs(datasetName, fileID, findAll);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             fileID, t);
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(Util.waitForThread(t, dbName, dbTimeOut, "getFileURLs")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public void requestStop(){
  }
  
  public void clearRequestStop(){
  }
  
}