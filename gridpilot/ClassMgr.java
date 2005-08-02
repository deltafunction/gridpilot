package gridpilot;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

/**
 * This class allows access to all global objects in gridpilot.
 */

public class ClassMgr{

  private ConfigFile configFile;
  private GlobalFrame globalFrame;
  private LogFile logFile;
  private StatusBar statusBar;
  private Table statusTable;
  private StatisticsPanel statisticsPanel;
  private JobValidation jobValidation;
  private GridPilot prodCom;
  private int debugLevel = 3;
  private HashMap dbMgts = new HashMap();
  private CSPluginMgr csPluginMgr;
  private Vector taskMgrs = new Vector();
  private DBVector submittedJobs = new DBVector();
  private SubmissionControl submissionControl;

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

  public void setStatusTable(Table _statusTable){
     statusTable = _statusTable;
  }

  public void setStatisticsPanel(StatisticsPanel _statisticsPanel){
      statisticsPanel = _statisticsPanel;
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

  public void setSubmissionControl(SubmissionControl _submissionControl){
     submissionControl = _submissionControl;
  }

  public ConfigFile getConfigFile(){
    if(configFile == null){
      Debug.debug("configFile null", 3);
    }
    return configFile;
  }

  // The HashMap of DB objects, dbMgts, is kept here
  public DBPluginMgr getDBPluginMgr(String dbName){
    Debug.debug("Getting DBPluginMgr for db " + dbName, 2);
    if(dbMgts.get(dbName) == null){
      Debug.debug("DBPluginMgr null for "+dbName, 3);
      new Exception().printStackTrace();
    }
    return (DBPluginMgr) dbMgts.get(dbName);
  }

  // The HashMap of tasks is kept here
  public Vector getTaskMgrs(){
    if(taskMgrs == null){
      Debug.debug("taskMgrs null", 3);
    }
    return taskMgrs;
  }
  
  // The HashMap of tasks is kept here
  public void addTaskMgr(TaskMgr taskMgr){
    if(taskMgrs == null){
      Debug.debug("taskMgrs null", 3);
      new Exception().printStackTrace();
    }
    taskMgrs.add(taskMgr);
  }


  // Different model here: the HashMap of CS objects is kept in csPluginMgr.
  // We don't use a setCsPluginMgr method because we don't want to load
  // the classes and make the connections until it is necessary.
  public CSPluginMgr getCSPluginMgr(){
    if(csPluginMgr==null){
      try{
        csPluginMgr = new CSPluginMgr();
        csPluginMgr.init();
      }
      catch(Throwable e){
        Debug.debug("Could not load plugin. "+e.getMessage(), 3);
        e.printStackTrace();
      }
    }
    return csPluginMgr;
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

  public Table getStatusTable(){
    if(statusTable == null){
      Debug.debug("statusTable null", 3);
      String[] fieldNames = GridPilot.getStatusFields();
      statusTable = new Table(new String [] {}, fieldNames,
          GridPilot.getColorMapping());
       GridPilot.getClassMgr().setStatusTable(statusTable);
      //new Exception().printStackTrace();
    }
    return statusTable;
  }

  public StatisticsPanel getStatisticsPanel(){
    if(statisticsPanel == null){
      Debug.debug("statisticsPanel null", 3);
      statisticsPanel = new StatisticsPanel();
    }
    return statisticsPanel;
  }

  public DBVector getSubmittedJobs(){
    if(submittedJobs == null){
      Debug.debug("statusTable null", 3);
    }
    return submittedJobs;
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
  
  public SubmissionControl getSubmissionControl(){
    if(submissionControl == null){
      Debug.debug("submissionControl null, creating new", 3);
      setSubmissionControl(new SubmissionControl());
    }
    return submissionControl;
  }

}
