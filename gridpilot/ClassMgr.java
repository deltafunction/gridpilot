package gridpilot;

import java.util.HashMap;
import java.util.Iterator;

import gridpilot.Debug;
import gridpilot.StatusBar;

/**
 * This class allows access to all global objects in gridpilot.
 */

public class ClassMgr {

  private ConfigFile configFile;
  private GlobalFrame globalFrame;
  private LogFile logFile;
  private StatusBar statusBar;
  private JobValidation jobValidation;
  private GridPilot prodCom;
  private int debugLevel = 3;
  private HashMap dbMgts = new HashMap();

  public void setConfigFile(ConfigFile _configFile){
    configFile = _configFile;
  }

  public boolean setDBPluginMgr(String dbName, DBPluginMgr dbPluginMgr) {
    try{
      dbMgts.put(dbName, dbPluginMgr);
    }catch(NullPointerException e){
      dbMgts.put(dbName, new HashMap());
    }
    try {
      dbPluginMgr.init();
    } catch (Throwable e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public void setLogFile(LogFile _logFile){
    logFile = _logFile;
  }

  public void setStatusBar(StatusBar _statusBar){
    statusBar = _statusBar;
  }

  public void setJobValidation(JobValidation _jobValidation){
    jobValidation = _jobValidation;
  }

  public void setGlobalFrame(GlobalFrame _globalFrame) {
    globalFrame = _globalFrame;
  }

  public void setGridPilot(GridPilot _gridpilot) {
    prodCom = _gridpilot;
  }

  public void setDebugLevel(int _debugLevel){
    debugLevel = _debugLevel;
  }

  public ConfigFile getConfigFile(){
    if(configFile == null){
      Debug.debug("configFile null", 3);
    }
    return configFile;
  }

  public DBPluginMgr getDBPluginMgr(String dbName){
    Debug.debug("Getting DBPluginMgr for db " + dbName, 2);
    if(dbMgts.get(dbName) == null){
      Debug.debug("DBPluginMgr null", 3);
      new Exception().printStackTrace();
    }
    return (DBPluginMgr) dbMgts.get(dbName);
  }

  public void clearDBCaches(){
    for(Iterator i=dbMgts.values().iterator(); i.hasNext();){
      ((DBPluginMgr) i.next()).clearCaches();
    }
  }
  
  public LogFile getLogFile(){
    if(logFile == null){
      Debug.debug("logFile null", 3);
      return new LogFile("");
    }

    return logFile;
  }

  public JobValidation getJobValidation(){
    if(jobValidation == null){
      Debug.debug("jobValidation null", 3);
      new Exception().printStackTrace();
    }

    return jobValidation;
  }

  public StatusBar getStatusBar(){
    if(statusBar == null){
      Debug.debug("statusBar null", 3);
      new Exception().printStackTrace();
    }

    return statusBar;
  }

  public GlobalFrame getGlobalFrame(){
    if(globalFrame == null){
      Debug.debug("globalFrame null", 3);
			new Exception().printStackTrace();
		}

		return globalFrame;
	}

  public GridPilot getGridPilot(){
    if(prodCom == null){
      Debug.debug("prodCom null", 3);
      new Exception().printStackTrace();
    }

    return prodCom;
  }

  public int getDebugLevel(){
    return debugLevel;
  }
}
