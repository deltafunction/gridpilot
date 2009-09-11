package gridpilot;

import java.awt.Color;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

import org.safehaus.uuid.UUIDGenerator;

import com.mysql.jdbc.NotImplemented;

import gridfactory.common.ConfigFile;
import gridfactory.common.DBCache;
import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.ResThread;
import gridpilot.GridPilot;
import gridpilot.Database;

/**
 * This class manages access to databases.
 *
 */
public class DBPluginMgr extends DBCache implements Database{

  private ConfigFile configFile;
  private MyLogFile logFile;
  private Database db;
  private String dbName;
  private String description;
  
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

    db = (Database) MyUtil.loadClass(dbClass, dbArgsType, dbArgs);

  }

  /**
   * Reads time-out values in configuration file.
   */
  public void loadValues(){
    // default timeout  
    String tmp = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "db timeout");
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

  public boolean updateJobValidationResult(String jobDefID, String result){
    return updateJobDefinition(jobDefID, new String [] {"validationResult"}, new String [] {result});
  }

  /** 
   * Construct the name of the target dataset when creating a new dataset
   * from an input dataset.
   */ 
  public String getTargetDatasetName(String targetDB, String sourceDatasetName,
      String executableName, String executableVersion){
    Debug.debug("finding target dataset name for "+sourceDatasetName+" in "+targetDB+
        " with tranformation "+executableName, 3);
        
    String findString = "";
    String replaceString = "";
    Pattern p = null;
    Matcher m = null;
    String s = "";
    String ret = "";
    if(executableName!=null){
      // TODO: make these patterns configurable
      s = ".*g4sim.*";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(executableName);
      if(m.matches()){
        replaceString = "SimProd";
      }
      s = ".*g4digit.*";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(executableName);
      if(m.matches()){
        replaceString = "DigitProd";
      }
      s = ".*reconstruction.*";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(executableName);
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
    if(executableVersion!=null && !executableVersion.equals("")){
      // Change the version to match the executable version
      s = "\\.v\\w*\\.\\w*$";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(ret);
      if(!matched){
        Debug.debug("replacing version", 3);
        ret1 = m.replaceAll("."+executableVersion);
        if(!ret.equals(ret1)){
          matched = true;
        }
      }
      s = "\\.\\w*\\.v\\w*$";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(ret);
      if(!matched){
        Debug.debug("replacing version", 3);
        ret1 = m.replaceAll("."+executableVersion);
        if(!ret.equals(ret1)){
          matched = true;
        }
      }
      s = "\\.v\\w*$";
      p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
      m = p.matcher(ret);
      if(!matched){
        Debug.debug("replacing version", 3);
        ret1 = m.replaceAll("."+executableVersion);
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
   
    ResThread t = new ResThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getFieldNames(table);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getFieldNames")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public String getRuntimeInitText(final String runtimeEnvName, final String csName){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getRuntimeInitText(runtimeEnvName, csName);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getPackInitText")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getStdOutFinalDest(final String jobDefinitionID){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getStdOutFinalDest(jobDefinitionID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getStdOutFinalDest")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getStdErrFinalDest(final String jobDefinitionID){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getStdErrFinalDest(jobDefinitionID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getStdErrFinalDest")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getError(){
    ResThread t = new ResThread(){
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
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getError")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getExecutableFile(final String jobDefinitionID){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getExecutableFile(jobDefinitionID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getExecutableFile")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String [] getRuntimeEnvironments(final String jobDefinitionID){
    ResThread t = new ResThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getRuntimeEnvironments(jobDefinitionID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getRuntimeEnvironments")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public String [] getExecutableArguments(final String jobDefinitionID){
    ResThread t = new ResThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getExecutableArguments(jobDefinitionID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getExecutableArguments")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public String getExecutableRuntimeEnvironment(final String executableID){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getExecutableRuntimeEnvironment(executableID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             executableID, t);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getExecutableRuntimeEnvironment")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getJobDefUserInfo(final String jobDefinitionID){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getJobDefUserInfo(jobDefinitionID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getJobDefUser")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getJobDefName(final String jobDefinitionID){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getJobDefName(jobDefinitionID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getJobDefName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getDatasetName(final String datasetID){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getDatasetName(datasetID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getDatasetName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getRunNumber(final String datasetID){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getRunNumber(datasetID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          /*logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetID, t);*/
          Debug.debug("Could not get runNumber for "+datasetID, 1);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getRunNumber")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String [] getRuntimeEnvironmentIDs(final String name, final String cs){
    ResThread t = new ResThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getRuntimeEnvironmentIDs(name, cs);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             name+":"+cs, t);
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getRuntimeEnvironmentID")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public String getExecutableID(final String exeName, final String exeVersion){
    ResThread t = new ResThread(){
      String res = "-1";
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getExecutableID(exeName, exeVersion);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             exeName+":"+exeVersion, t);
        }
      }
      public String getStringRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getExecutableID")){
      return t.getStringRes();
    }
    else{
      return "-1";
    }
  }

  public String getDatasetID(final String datasetName){
    ResThread t = new ResThread(){
      String res = "-1";
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getDatasetID(datasetName);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getDatasetID")){
      return t.getStringRes();
    }
    else{
      return "-1";
    }
  }

  public String getFileID(final String datasetName, final String fileName){
    ResThread t = new ResThread(){
      String res = "-1";
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getFileID(datasetName, fileName);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getFileID")){
      return t.getStringRes();
    }
    else{
      return "-1";
    }
  }

  public String getJobDefDatasetID(final String jobDefinitionID){
    ResThread t = new ResThread(){
      String res = "-1";
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getJobDefDatasetID(jobDefinitionID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getJobDefDatasetID")){
      return t.getStringRes();
    }
    else{
      return "-1";
    }
  }

  public String getJobDefStatus(final String jobDefinitionID){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getJobDefStatus(jobDefinitionID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getJobStatus")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getJobDefValue(final String jobDefID, final String key){
    ResThread t = new ResThread(){
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
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getJobDefValue")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getRunInfo(final String jobDefID, final String key){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getRunInfo(jobDefID, key).toString();
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getRunInfo")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getJobDefExecutableID(final String jobDefID){
    ResThread t = new ResThread(){
      String res = "-1";
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getJobDefExecutableID(jobDefID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getJobDefExecutableID")){
      return t.getStringRes();
    }
    else{
      return "-1";
    }
  }

  public String getDatasetExecutableName(final String datasetID){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getDatasetExecutableName(datasetID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getDatasetExecutableName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getDatasetExecutableVersion(final String datasetID){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getDatasetExecutableVersion(datasetID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getDatasetExecutableVersion")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getJobDefExecutableValue(final String jobDefID, final String key){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getExecutable(
              db.getJobDefExecutableID(jobDefID)).getValue(key).toString();
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getJobDefExecutableValue")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String [] getOutputFiles(final String jobDefID){
  
    ResThread t = new ResThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getOutputFiles(jobDefID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getOutputs")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public String [] getJobDefInputFiles(final String jobDefID){
    
      ResThread t = new ResThread(){
        String [] res = null;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
        public void run(){
          try{
            db.clearError();
            res = db.getJobDefInputFiles(jobDefID);
          }
          catch(Throwable t){
            db.appendError(t.getMessage());
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
    
      if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getInputs")){
        return t.getString2Res();
      }
      else{
        return null;
      }
    }

  public String [] getJobDefExecutableParameters(final String jobDefID){
    
      ResThread t = new ResThread(){
        String [] res = null;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
        public void run(){
          try{
            db.clearError();
            res = db.getJobDefExecutableParameters(jobDefID);
          }
          catch(Throwable t){
            db.appendError(t.getMessage());
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
    
      if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getJobDefExecutableParameters")){
        return t.getString2Res();
      }
      else{
        return null;
      }
    }

  public String getJobDefOutLocalName(final String jobDefID, final String outpar){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getJobDefOutLocalName(jobDefID, outpar);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getJobDefOutLocalName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String getJobDefOutRemoteName(final String jobDefinitionID, final String outFileName){
    ResThread t = new ResThread(){
      String res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getJobDefOutRemoteName(jobDefinitionID, outFileName);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getJobDefOutRemoteName")){
      return t.getStringRes();
    }
    else{
      return null;
    }
  }

  public String [] getExecutableJobParameters(final String executableID){
  
    ResThread t = new ResThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getExecutableJobParameters(executableID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             executableID, t);
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getTransJobParameters")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public String [] getExecutableOutputs(final String executableID){
  
    ResThread t = new ResThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getExecutableOutputs(executableID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             executableID, t);
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getTransOutputs")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public String [] getExecutableInputs(final String executableID){
    
    ResThread t = new ResThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getExecutableInputs(executableID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             executableID, t);
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getTransInputs")){
      return t.getString2Res();
    }
    else{
      return null;
    }
  }

  public boolean isFileCatalog(){
    
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.isFileCatalog();
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "isFileCatalog")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public boolean isJobRepository(){
    
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.isJobRepository();
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "isJobRepository")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean createJobDefinition(final String [] values){
  
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.createJobDefinition(values);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "createJobDefinition")){
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
    
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.createJobDefinition(datasetName, cstAttrNames, resCstAttr,
              trpars, ofmap, odest, edest);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "createJobDefinition")){
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
            !fields[j].equalsIgnoreCase(MyUtil.getIdentifierField(dbName, "jobDefinition"))){
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

  public DBRecord createExecutable(String [] fields, Object [] values) throws Exception{
    
    String [] transFieldNames = getFieldNames("executable");
    
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
            !fields[j].equalsIgnoreCase(MyUtil.getIdentifierField(dbName, "executable"))){
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
    if(createExecutable(vals)){
       return jobDef;
    }
    else{
      throw new IOException("ERROR: createExecutable failed");
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
            !fields[j].equalsIgnoreCase(MyUtil.getIdentifierField(dbName, "runtimeEnvironment"))){
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
  
  public String getFileBytes(String datasetName, String fileId) throws InterruptedException{
    String size = null;
    // Try and get the size from the source catalog.
    // For a file catalog, just get the size
    if(db.isFileCatalog()){
      try{
        size = (String) db.getFile(datasetName, fileId, LOOKUP_PFNS_NONE).getValue(MyUtil.getFileSizeField(dbName));
      }
      catch(Exception e){
        //e.printStackTrace();
      }
    }
    // For a jobDefinition table, size and md5sum may have been inserted
    // by the CS+validation.
    if((size==null || size.equals("") || Integer.parseInt(size)<0) && db.isJobRepository()){
      try{
        String metaData = null;
        DBRecord jobDef = db.getJobDefinition(fileId);
        size = (String) jobDef.getValue("outputFileBytes");
        if(size==null || size.equals("") || Integer.parseInt(size)<0){
          metaData = (String) jobDef.getValue("metaData");
          size = (String) MyUtil.parseMetaData(metaData).get(MyUtil.getFileSizeField(dbName));
        }
        if(size==null || size.equals("")){
          size = (String) MyUtil.parseMetaData(metaData).get("bytes");
        }
        if(size==null || size.equals("")){
          size = (String) MyUtil.parseMetaData(metaData).get("size");
        }
        if(size==null || size.equals("")){
          size = (String) MyUtil.parseMetaData(metaData).get("fsize");
        }
        if(size!=null){
          size = size.replaceFirst("(\\d+)L", "$1");
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    return size;
  }

  public String getFileChecksum(String datasetName, String fileId) throws InterruptedException{
    String checksum = null;
    // Try and get the checksum from the source catalog
    // For a file catalog, just get the size
    if(db.isFileCatalog()){
      try{
        checksum = (String) db.getFile(datasetName, fileId, LOOKUP_PFNS_NONE).getValue(MyUtil.getChecksumField(dbName));
      }
      catch(Exception e){
        //e.printStackTrace();
      }
    }
    // For a jobDefinition table, size and md5sum may have been inserted
    // by the CS+validation.
    if(checksum==null && db.isJobRepository()){
      try{
        String metaData = null;
        DBRecord jobDef = db.getJobDefinition(fileId);
        checksum = (String) jobDef.getValue("outputFileChecksum");
        if(checksum==null || checksum.equals("")){
          metaData = (String) jobDef.getValue("metaData");
          checksum = (String) MyUtil.parseMetaData(metaData).get(MyUtil.getChecksumField(dbName));
        }
        if(checksum==null || checksum.equals("")){
          checksum = (String) MyUtil.parseMetaData(metaData).get("checksum");
        }
        if(checksum==null || checksum.equals("")){
          checksum = (String) MyUtil.parseMetaData(metaData).get("md5sum");
        }
      }
      catch(Exception e){
      }
    }
    if(checksum!=null && !checksum.equals("") && !checksum.matches("\\w+:.*")){
      checksum = "md5:"+checksum;
    }
    return checksum;
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
    String [] urls = sourceMgr.getFileURLs(datasetName, id, true)[1];
    Debug.debug("Creating new file "+id+":"+name+" in dataset "+
        datasetID+":"+datasetName+" with PFNs: "+MyUtil.arrayToString(urls), 2);
    
    String uuid = id;
    // In case the file was copied from a virtual table from a job repository,
    // generate new GUID
    if(!sourceMgr.isFileCatalog() && sourceMgr.isJobRepository()){
      uuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
      String message = "Generated new UUID "+uuid.toString()+" for "+fileName;
      GridPilot.getClassMgr().getLogFile().addInfo(message);
    }
    
    String size = getFileBytes(datasetName, id);
    String checksum = getFileChecksum(datasetName, id);
    
    boolean ok = true;
    boolean finalOk = true;
    if(urls.length==0){
      logFile.addMessage("WARNING: no URLs for file "+name+". Inserting anyway.");
      Debug.debug("Registering new file: "+datasetID+":"+datasetName+":"+uuid+":"+name+":"+":"+
          size+":"+checksum, 2);
      registerFileLocation(datasetID, datasetName, uuid, name, "",
          size, checksum, false);
      finalOk = true;
    }
    else{
      for(int i=0; i<urls.length; ++i){
        try{
          Debug.debug("Registering new file: "+datasetID+":"+datasetName+":"+uuid+":"+name+":"+urls[i]+":"+
              size+":"+checksum, 2);
          registerFileLocation(datasetID, datasetName, uuid, name, urls[i],
              size, checksum, false);
          finalOk = finalOk || ok;
          ok = true;
        }
        catch(Exception e){
          logFile.addMessage("ERROR: could not register "+urls[i]+" for file "+
              name+" in dataset "+datasetName, e);
          ok = false;
        }
      }
    }
    if(!finalOk){
      throw new IOException("ERROR: could not register any files.");
    }
  }

  public synchronized boolean createExecutable(final Object [] values){
    
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.createExecutable(values);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "createExecutable")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean createRuntimeEnvironment(final Object [] values){
    
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.createRuntimeEnvironment(values);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "createRuntimeEnvironment")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean createDataset(final String targetTable,
      final String [] fields, final Object [] values){
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          String table = null;
          if(targetTable==null){
            table = "dataset";
          }
          else{
            table = targetTable;
          }
          db.clearError();
          res = db.createDataset(table, fields, values);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "createDataset")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean setJobDefsField(final String [] identifiers,
      final String field, final String value){  
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.setJobDefsField(identifiers, field, value);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "setJobDefinitionField")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean updateJobDefinition(final String jobDefID,
      final String [] fields, final String [] values){
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.updateJobDefinition(jobDefID, fields, values);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "updateJobDefinition")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean updateJobDefinition(final String jobDefID,
      final String [] values){
  
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.updateJobDefinition(jobDefID, values);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "updateJobDefinition")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean updateDataset(final String datasetID, final String datasetName,
      final String [] fields, final String [] values){
    
      ResThread t = new ResThread(){
        boolean res = false;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
        public void run(){
          try{
            db.clearError();
            res = db.updateDataset(datasetID, datasetName, fields, values);
          }
          catch(Throwable t){
            db.appendError(t.getMessage());
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
    
      if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "updateDataset")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  public synchronized boolean updateExecutable(final String executableID,
      final String [] fields, final String [] values){
    
      ResThread t = new ResThread(){
        boolean res = false;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
        public void run(){
          try{
            db.clearError();
            res = db.updateExecutable(executableID, fields, values);
          }
          catch(Throwable t){
            db.appendError(t.getMessage());
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               executableID, t);
          }
        }
        public boolean getBoolRes(){
          return res;
        }
      };
    
      t.start();
    
      if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "updateExecutable")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  public synchronized boolean updateRuntimeEnvironment(final String runtimeEnvironmentID,
    final String [] fields, final String [] values){
  
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.updateRuntimeEnvironment(runtimeEnvironmentID, fields, values);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "updateRuntimeEnvironment")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  /**
   * Delete physical file(s) produced by job
   * - including stdout and stderr and, if the DB is not a file catalog,
   * all output file(s). If the DB is a file catalog, the first output file
   * is not deleted.
   * @param datasetID
   */
  protected boolean purgeJobFiles(String jobDefId){
    boolean ret = true;
    DBRecord jobDef = getJobDefinition(jobDefId);
    String [] toDeleteFiles = null;
    if(((String) jobDef.getValue("status")).equalsIgnoreCase(DBPluginMgr.getStatusName(DBPluginMgr.DEFINED))){
      return ret;
    }
    try{
      if(isFileCatalog()){
        // In this case: don't delete the first of the output files, since
        // this is the file registered in the file catalog and will be
        // deleted when deleting the file catalog entry.
        String [] outFiles = getExecutableOutputs(getJobDefExecutableID(jobDefId));
        toDeleteFiles = new String [outFiles.length+2-(outFiles.length>0?1:0)];
        toDeleteFiles[0] = (String) jobDef.getValue("stdoutDest");
        toDeleteFiles[1] = (String) jobDef.getValue("stderrDest");
        for(int i=2; i<toDeleteFiles.length; ++i){
          toDeleteFiles[i] = getJobDefOutRemoteName(jobDefId, outFiles[i-1]);
        }
      }
      else{
        String [] outFiles = getExecutableOutputs(getJobDefExecutableID(jobDefId));
        toDeleteFiles = new String [outFiles.length+2];
        toDeleteFiles[0] = (String) jobDef.getValue("stdoutDest");
        toDeleteFiles[1] = (String) jobDef.getValue("stderrDest");
        for(int i=2; i<toDeleteFiles.length; ++i){
          toDeleteFiles[i] = getJobDefOutRemoteName(jobDefId, outFiles[i-2]);
        }
      }
      Debug.debug("Deleting files "+MyUtil.arrayToString(toDeleteFiles), 2);        
      if(toDeleteFiles!=null){
        GridPilot.getClassMgr().getTransferControl().deleteFiles(toDeleteFiles);
      }
    }
    catch(Exception e){
      ret = false;
      GridPilot.getClassMgr().getLogFile().addMessage("WARNING: Could not delete file(s) "+toDeleteFiles);
    }
    return ret;
  }
  
  // Notice: not in its own thread
  public boolean deleteJobDefinition(String jobDefID) throws InterruptedException{
    return db.deleteJobDefinition(jobDefID);
  }
  
  public synchronized boolean deleteJobDefinition(final String jobDefID, final boolean cleanup){
    ResThread t = new ResThread(){
      boolean res = true;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          if(cleanup){
            res = purgeJobFiles(jobDefID);
          }
          res = res && deleteJobDefinition(jobDefID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "deleteJobDefinition")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  /**
   * Delete physical file(s) registered in a file catalog.
   * @param datasetID
   * @param fileIDs
   * @return
   */
  protected boolean purgeFiles(String datasetID, String[] fileIDs) {
    boolean ok = true;
    for(int i=0; i<fileIDs.length; ++i){
      String fileNames = null;
      try{
        if(isFileCatalog()){
          fileNames = (String) getFile(datasetID, fileIDs[i], LOOKUP_PFNS_ALL).getValue("pfname");
        }
        else{
          fileNames = (String) getFile(datasetID, fileIDs[i], LOOKUP_PFNS_ALL).getValue("url");
        }
        Debug.debug("Deleting files "+fileNames, 2);
        if(fileNames!=null && !fileNames.equals("no such field")){
          String [] fileNameArray = MyUtil.splitUrls(fileNames);
          if(fileNameArray!=null && fileNameArray.length>0){
            GridPilot.getClassMgr().getTransferControl().deleteFiles(fileNameArray);
          }
        }
      }
      catch(Exception e){
        ok = false;
        e.printStackTrace();
        logFile.addMessage("WARNING: Could not delete file(s) "+fileNames);
      }
    }
    return ok;
  }
  
  // Notice: not in its own thread
  public boolean deleteFiles(final String datasetID,
      final String [] fileIDs) throws InterruptedException{
    return db.deleteFiles(datasetID, fileIDs);
  }
  
  public synchronized boolean deleteFiles(final String datasetID,
      final String [] fileIDs, final boolean cleanup){
    
    ResThread t = new ResThread(){
      boolean res = true;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          try{
            if(cleanup){
              // If this is implemented use it
              res = db.deleteFiles(datasetID, fileIDs, cleanup);
              return;
            }
          }
          catch(NotImplemented t){
          }
          // Otherwise, clean up with purgeFiles
          db.clearError();
          if(cleanup){
            res = res && purgeFiles(datasetID, fileIDs);
          }
          res = res && deleteFiles(datasetID, fileIDs);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             MyUtil.arrayToString(fileIDs), t);
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "deleteFile")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  // Notice: not in its own thread
  public boolean deleteDataset(final String datasetID) throws InterruptedException{
    return db.deleteDataset(datasetID);
  }
    
  public synchronized boolean deleteDataset(final String datasetID, final boolean cleanup){
    
      ResThread t = new ResThread(){
        boolean res = false;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
        public void run(){
          try{
            db.clearError();
            try{
              if(cleanup){
                // If this is implemented use it
                res = db.deleteDataset(datasetID, cleanup);
                return;
              }
            }
            catch(NotImplemented t){
            }
            // Otherwise, clean up with deleteJobDefsFromDataset and purgeFilesFromDataset
            boolean ok = true;
            if(isJobRepository() && cleanup){
              purgeJobFilesFromDataset(datasetID);
              purgeFilesFromDataset(datasetID);
              ok = deleteJobDefsFromDataset(datasetID);
              ok = ok && deleteFiles(datasetID, null);
              if(!ok){
                Debug.debug("ERROR: Deleting job definitions of dataset #"+
                    datasetID+" failed."+" Please clean up by hand.", 1);
                String error = "ERROR: Deleting job definitions of dataset #"+
                   datasetID+" failed."+" Please clean up by hand.";
                throw new IOException(error);
              }
            }
            res = deleteDataset(datasetID);
          }
          catch(Throwable t){
            db.appendError(t.getMessage());
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
    
      if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "deleteDataset")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }
  
  /**
   * Delete physical file(s) produced by job(s) of dataset 
   * - including stdout and stderr and, if the DB is not a file catalog,
   * all output file(s). If the DB is a file catalog, the first output file
   * of each job is not deleted.
   * @param datasetID
   */
  protected void purgeJobFilesFromDataset(String datasetID) {
    String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
    String nameField = MyUtil.getNameField(dbName, "jobDefinition");
    DBResult jobDefsRes = getJobDefinitions(datasetID,
        new String [] {idField, nameField, "computingSystem", "jobId"}, null, null);
    String jobDefID;
    for(int i=0; i<jobDefsRes.size(); ++i){
      jobDefID = (String) jobDefsRes.getValue(i, idField);
      purgeJobFiles(jobDefID);
    }
  }

  /**
   * Delete physical file(s) registered as belonging to a dataset.
   * If the DB is not a file catalog, nothing is done.
   * @param datasetID
   */
  protected void purgeFilesFromDataset(String datasetID) {
    String idField = MyUtil.getIdentifierField(dbName, "file");
    DBResult filesRes = getFiles(datasetID);
    String [] fileIDs = new String[filesRes.size()];
    for(int i=0; i<fileIDs.length; ++i){
      fileIDs[i] = (String) filesRes.getValue(i, idField);
    }
    purgeFiles(datasetID, fileIDs);
  }

  public boolean deleteJobDefsFromDataset(final String datasetID){
    
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.deleteJobDefsFromDataset(datasetID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "deleteJobDefsFromDataset")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean deleteExecutable(final String executableID){
    
      ResThread t = new ResThread(){
        boolean res = false;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
        public void run(){
          try{
            db.clearError();
            res = db.deleteExecutable(executableID);
          }
          catch(Throwable t){
            db.appendError(t.getMessage());
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               executableID, t);
          }
        }
        public boolean getBoolRes(){
          return res;
        }
      };
    
      t.start();
    
      if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "deleteExecutable")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  public synchronized boolean deleteRuntimeEnvironment(final String runtimeEnvironmentID){
    
      ResThread t = new ResThread(){
        boolean res = false;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
        public void run(){
          try{
            db.clearError();
            res = db.deleteRuntimeEnvironment(runtimeEnvironmentID);
          }
          catch(Throwable t){
            db.appendError(t.getMessage());
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
    
      if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "deleteRuntimeEnvironment")){
        return t.getBoolRes();
      }
      else{
        return false;
      }
    }

  public synchronized boolean reserveJobDefinition(final String jobDefID, final String userName,
      final String cs){
  
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.reserveJobDefinition(jobDefID, userName, cs);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "reserveJobDefinition")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized boolean cleanRunInfo(final String jobDefID){
  
    ResThread t = new ResThread(){
      boolean res = false;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.cleanRunInfo(jobDefID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
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
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "jobDefID")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }

  public synchronized void executeUpdate(final String sql){
    
    ResThread t = new ResThread(){
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          db.executeUpdate(sql);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          try{
            logFile.addMessage((t instanceof Exception ? "Error: "+db.getError()+"\nException" : "Error") +
                               " from plugin " + dbName, t);
          }
          catch(InterruptedException e){
            e.printStackTrace();
          }
        }
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "executeUpdate")){
      return;
    }
    else{
      return;
    }
  }

  public DBResult select(final String selectQuery, final String identifier,
      final boolean findAll){
  
    ResThread t = new ResThread(){
      DBResult res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.select(selectQuery, identifier, findAll);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                              selectQuery, t);
        }
      }
      public DBResult getDBResultRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "select")){
      return t.getDBResultRes();
    }
    else{
      return null;
    }
  }

  public DBResult getRuntimeEnvironments(){
    
    ResThread t = new ResThread(){
      DBResult res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getRuntimeEnvironments();
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
      public DBResult getDBResultRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getRuntimeEnvironments")){
      return t.getDBResultRes();
    }
    else{
      return null;
    }
  }

  public DBResult getExecutables(){
  
    ResThread t = new ResThread(){
      DBResult res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getExecutables();
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
      public DBResult getDBResultRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getExecutables")){
      return t.getDBResultRes();
    }
    else{
      return null;
    }
  }

  public DBRecord getDataset(final String datasetID){
    
      ResThread t = new ResThread(){
        DBRecord res = null;
        public void requestStop(){
          db.requestStop();
        }
        public void clearRequestStop(){
          db.clearRequestStop();
        }
        public void run(){
          try{
            db.clearError();
            res = db.getDataset(datasetID);
          }
          catch(Throwable t){
            db.appendError(t.getMessage());
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + dbName + " " +
                               datasetID, t);
          }
        }
        public DBRecord getDBRecordRes(){
          return res;
        }
      };
    
      t.start();
    
      if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getDataset")){
        return t.getDBRecordRes();
      }
      else{
        return null;
      }
    }

  public DBRecord getRuntimeEnvironment(final String runtimeEnvironmentID){
    
    ResThread t = new ResThread(){
      DBRecord res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getRuntimeEnvironment(runtimeEnvironmentID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             runtimeEnvironmentID, t);
        }
      }
      public DBRecord getDBRecordRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getRuntimeEnvironment")){
      return t.getDBRecordRes();
    }
    else{
      return null;
    }
    
  }

  public DBRecord getExecutable(final String executableID){
    
    ResThread t = new ResThread(){
      DBRecord res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getExecutable(executableID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             executableID, t);
        }
      }
      public DBRecord getDBRecordRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getExecutable")){
      return t.getDBRecordRes();
    }
    else{
      return null;
    }
    
  }

  public DBResult getFiles(final String datasetID){
    
    ResThread t = new ResThread(){
      DBResult res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getFiles(datasetID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetID, t);
        }
      }
      public DBResult getDBResultRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getFiles")){
      return t.getDBResultRes();
    }
    else{
      return null;
    }
  }

  public DBResult getJobDefinitions(final String datasetID, final String [] fieldNames,
      final String [] statusList, final String [] csStatusList){
  
    ResThread t = new ResThread(){
      DBResult res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getJobDefinitions(datasetID, fieldNames, statusList, csStatusList);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             datasetID, t);
        }
      }
      public DBResult getDBResultRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getJobDefinitions")){
      return t.getDBResultRes();
    }
    else{
      return null;
    }
  }

  public DBRecord getJobDefinition(final String jobDefinitionID){
  
    ResThread t = new ResThread(){
      DBRecord res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getJobDefinition(jobDefinitionID);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             jobDefinitionID, t);
        }
      }
      public DBRecord getDBRecordRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getJobDefinition")){
      return t.getDBRecordRes();
    }
    else{
      return null;
    }
  }

  public void disconnect(){
  
    ResThread t = new ResThread(){
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
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "disconnect")){
      return;
    }
    else{
      return;
    }
  }

  public synchronized void clearCaches(){
  
    ResThread t = new ResThread(){
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          Debug.debug("Clearing cache of "+dbName, 2); 
          db.clearCaches();
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "clearCaches")){
      return;
    }
    else{
      return;
    }
  }

  public synchronized void registerFileLocation(final String datasetID,
      final String datasetName, final String fileID, final String lfn,
      final String url, final String size, final String checksum, final boolean datasetComplete){
    
    ResThread t = new ResThread(){
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          db.registerFileLocation(datasetID, datasetName, fileID, lfn, url, size, checksum, datasetComplete);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName, t);
        }
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "registerFileLocation")){
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
  public final static String [] getStatusNames(){
    return new String [] {"Wait", "Run", "Done"};
  }
  
  public final static int STAT_STATUS_WAIT = 0;
  public final static int STAT_STATUS_RUN = 1;
  public final static int STAT_STATUS_DONE = 2;
  
  /**
   * Returns colors corresponding to getStatusNames for statistics panel.
   */
  public static Color [] getStatusColors(){
    return new Color [] {Color.black, Color.lightGray, Color.blue, Color.green, Color.orange,
        Color.magenta, Color.red, Color.darkGray};
  }


  /**
   * DB status names for statistics panel. <p>
   * (From AtCom1)
   */
  private static String [] dbStatusNames = new String [] {
      "Defined",
      "Prepared",
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
      case PREPARED : return "Prepared";
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

  public String [] getVersions(final String exeName){
    ResThread t = new ResThread(){
      String [] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getVersions(exeName);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName , t);
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getVersions")){
      return t.getString2Res();
    }
    else{
      return new String [] {};
    }
  }

  /** 
   * Split events over multiple jobDefinitions for
   * a dataset by consulting dataset.totalEvents and dataset.totalFiles.
   * Returns a list of
   * {eventMin,eventMax} dublets.
   */
  public int [][] getEventSplits(String datasetID){
    String arg = "";
    DBResult res = null;
    // totalEvents is the total number of events.
    // nrEvents is events per file.
    int nrEvents = 0;
    int totalEvents = 0;
    int totalFiles = 0;
    int [][] splits = null;
    String debug = "";
    
    arg = "select totalEvents, totalFiles from dataset where identifier='"+
    datasetID+"'";
    res = select(arg, MyUtil.getIdentifierField(dbName, "dataset"), true);
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
    Debug.debug("Splitting according to "+splits+" --> "+debug, 2);
    return splits;
  }

  public DBRecord getFile(final String datasetName, final String fileID,
      final int lookupPFNs){
    
    ResThread t = new ResThread(){
      DBRecord res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getFile(datasetName, fileID, lookupPFNs);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             fileID, t);
        }
      }
      public DBRecord getDBRecordRes(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getFile")){
      return t.getDBRecordRes();
    }
    else{
      return null;
    }
  }

  public String [][] getFileURLs(final String datasetName, final String fileID,
      final boolean findAll){
    Debug.debug("Getting field names for file # "+fileID, 3);
   
    ResThread t = new ResThread(){
      String [][] res = null;
      public void requestStop(){
        db.requestStop();
      }
      public void clearRequestStop(){
        db.clearRequestStop();
      }
      public void run(){
        try{
          db.clearError();
          res = db.getFileURLs(datasetName, fileID, findAll);
        }
        catch(Throwable t){
          db.appendError(t.getMessage());
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + dbName + " " +
                             fileID, t);
        }
      }
      public String [][] getString3Res(){
        return res;
      }
    };
  
    t.start();
  
    if(MyUtil.myWaitForThread(t, dbName, dbTimeOut, "getFileURLs")){
      return t.getString3Res();
    }
    else{
      return null;
    }
  }
  
  public void appendError(String error){
    db.appendError(error);
  }

  public void clearError(){
    db.clearError();
  }

  public void requestStopLookup(){
    db.requestStopLookup();
  }
  
  public void clearRequestStopLookup(){
    db.clearRequestStopLookup();
  }
  
  public void requestStop(){
    db.requestStop();
  }
  
  public void clearRequestStop(){
    db.clearRequestStop();
  }
  
}