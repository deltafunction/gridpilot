package gridpilot;

import java.util.HashMap;
import java.util.Iterator;

/**
 * This class allows access to all global objects in gridpilot.
 */

public class ClassMgr {

  private ConfigFile configFile;
  private GlobalFrame globalFrame;
  private JobControl jobControl;
  private LogFile logFile;
  private int debugLevel = 3;
  private HashMap dbMgts = new HashMap();

  public void setConfigFile(ConfigFile _configFile){
    configFile = _configFile;
  }

  public boolean setDBPluginMgr(String db, String step, DBPluginMgr dbPluginMgr) {
    try{
      if(dbMgts.get(db) == null){
        dbMgts.put(db, new HashMap());
      }       
    }catch(NullPointerException e){
      dbMgts.put(db, new HashMap());
    }
    ((HashMap) dbMgts.get(db)).put(step, dbPluginMgr);
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

  public void setGlobalFrame(GlobalFrame _globalFrame) {
    globalFrame = _globalFrame;
  }

  public void setDebugLevel(int _debugLevel){
    debugLevel = _debugLevel;
  }

  public ConfigFile getConfigFile(){
    if(configFile == null){
      Debug.debug("configFile null", 3);
      //create a fake config file
      return new ConfigFile("");
    }
    return configFile;
  }

  public DBPluginMgr getDBPluginMgr(String db, String step){
    Debug.debug("Getting DBPluginMgr for db " + db + " and step " + step, 2);
    if(((HashMap) dbMgts.get(db)).get(step) == null){
      Debug.debug("DBPluginMgr null", 3);
      new Exception().printStackTrace();
    }
    return (DBPluginMgr) ((HashMap) dbMgts.get(db)).get(step);
  }

  public void clearDBCaches(){
    for (Iterator i = dbMgts.values().iterator(); i.hasNext();) {
        for (Iterator j = ((HashMap) i.next()).values().iterator(); j.hasNext();) {
        ((DBPluginMgr) j.next()).clearCaches();
      }
    }
  }
  
  public JobControl getJobControl(){
    if(jobControl == null){
      Debug.debug("jobControl null", 3);
      new Exception().printStackTrace();
    }

    return jobControl;
  }

  public LogFile getLogFile(){
    if(logFile == null){
      Debug.debug("logFile null", 3);
      return new LogFile("");
    }

    return logFile;
  }

  public GlobalFrame getGlobalFrame(){
    if(globalFrame == null){
      Debug.debug("globalFrame null", 3);
			new Exception().printStackTrace();
		}

		return globalFrame;
	}

  public int getDebugLevel(){
    return debugLevel;
  }
}
