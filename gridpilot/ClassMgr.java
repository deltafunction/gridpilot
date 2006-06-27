package gridpilot;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.Timer;

import org.ietf.jgss.GSSCredential;

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
  private HashMap datasetMgrs = new HashMap();
  private Vector submittedJobs = new Vector();
  private SubmissionControl submissionControl;
  private GSSCredential credential = null;
  private Boolean gridProxyInitialized = Boolean.FALSE;
  private Timer timerProxy = new Timer(0, new ActionListener(){
    public void actionPerformed(ActionEvent e){
      Debug.debug("actionPerformed timeProxy", 3);
      gridProxyInitialized = Boolean.FALSE;
    }
  });

  
  public void setConfigFile(ConfigFile _configFile){
    configFile = _configFile;
  }

  public boolean setDBPluginMgr(String dbName, DBPluginMgr dbPluginMgr){
    try{
      dbMgts.put(dbName, dbPluginMgr);
    }
    catch(NullPointerException e){
      dbMgts.put(dbName, new HashMap());
    }
    try {
      dbPluginMgr.init();
    }
    catch(Throwable e){
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
    if(configFile==null){
      Debug.debug("configFile null", 3);
    }
    return configFile;
  }

  // The HashMap of DB objects, dbMgts, is kept here
  public DBPluginMgr getDBPluginMgr(String dbName) throws NullPointerException{
    Debug.debug("Getting DBPluginMgr for db " + dbName, 3);
    if(dbMgts.get(dbName)==null){
      throw new NullPointerException("DBPluginMgr null for "+dbName);
    }
    return (DBPluginMgr) dbMgts.get(dbName);
  }

  // This method creates a new DatasetMgr if there is
  // none in the HashMap with keys dbName, taskID
  public DatasetMgr getDatasetMgr(String dbName, int datasetID){
    if(datasetMgrs==null){
      Debug.debug("datasetMgrs null", 3);
    }
    if(!datasetMgrs.keySet().contains(dbName)){
      datasetMgrs.put(dbName, new HashMap());
    }
    if(!((HashMap) datasetMgrs.get(dbName)).keySet().contains(Integer.toString(datasetID))){
      Debug.debug("Creating new DatasetMgr, "+datasetID+", in "+dbName, 3);
      addTaskMgr(new DatasetMgr(dbName, datasetID));
    }
    return (DatasetMgr) ((HashMap) datasetMgrs.get(dbName)).get(Integer.toString(datasetID));
  }
  
  public Vector getDatasetMgrs(){
    if(datasetMgrs==null){
      Debug.debug("datasetMgrs null", 3);
    }
    Vector allTaskMgrs = new Vector();
    for(Iterator it=datasetMgrs.values().iterator(); it.hasNext();){
      allTaskMgrs.addAll(((HashMap) it.next()).values());
    }
    return allTaskMgrs;
  }
  
  // The HashMap of HashMaps of tasks is kept here
  public void addTaskMgr(DatasetMgr taskMgr){
    if(datasetMgrs==null){
      Debug.debug("datasetMgrs null", 3);
      new Exception().printStackTrace();
    }
    if(!datasetMgrs.keySet().contains(taskMgr.dbName)){
      datasetMgrs.put(taskMgr.dbName, new HashMap());
    }
    ((HashMap) datasetMgrs.get(taskMgr.dbName)
        ).put(Integer.toString(taskMgr.getDatasetID()), taskMgr);
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
    if(logFile==null){
      Debug.debug("logFile null", 3);
      return new LogFile("");
    }
    return logFile;
  }

  public JobValidation getJobValidation(){
    if(jobValidation==null){
      Debug.debug("jobValidation null, creating new", 3);
      setJobValidation(new JobValidation());
    }
    return jobValidation;
  }

  public StatusBar getStatusBar(){
    if(statusBar==null){
      Debug.debug("statusBar null", 3);
      new Exception().printStackTrace();
    }
    return statusBar;
  }

  public Table getStatusTable(){
    if(statusTable==null){
      Debug.debug("statusTable null", 3);
      String[] fieldNames = GridPilot.statusFields;
      Debug.debug("Creating new Table with fields "+Util.arrayToString(fieldNames), 3);
      statusTable = new Table(new String [] {}, fieldNames,
          GridPilot.colorMapping);
       GridPilot.getClassMgr().setStatusTable(statusTable);
      //new Exception().printStackTrace();
    }
    return statusTable;
  }

  public StatisticsPanel getStatisticsPanel(){
    if(statisticsPanel==null){
      Debug.debug("statisticsPanel null", 3);
      statisticsPanel = new StatisticsPanel();
    }
    return statisticsPanel;
  }

  public Vector getSubmittedJobs(){
    if(submittedJobs==null){
      Debug.debug("submittedJobs null", 3);
    }
    return submittedJobs;
  }

  public void clearSubmittedJobs(){
    if(submittedJobs==null){
      Debug.debug("submittedJobs null", 3);
    }
    submittedJobs.removeAllElements();
  }

  public GlobalFrame getGlobalFrame(){
    if(globalFrame==null){
      Debug.debug("globalFrame null", 3);
      new Exception().printStackTrace();
    }

    return globalFrame;
  }

  public GridPilot getGridPilot(){
    if(prodCom==null){
      Debug.debug("Object null", 3);
      new Exception().printStackTrace();
    }
    return prodCom;
  }

  public int getDebugLevel(){
    return debugLevel;
  }
  
  public SubmissionControl getSubmissionControl(){
    if(submissionControl==null){
      Debug.debug("submissionControl null, creating new", 3);
      setSubmissionControl(new SubmissionControl());
    }
    return submissionControl;
  }
  
  public GSSCredential getGridCredential(){
    if(gridProxyInitialized.booleanValue()){
      return credential;
    }
    synchronized(gridProxyInitialized){
      // avoids that dozens of popup open if
      // you submit dozen of jobs and proxy not initialized
      try{
        if(credential==null || credential.getRemainingLifetime()<GridPilot.proxyTimeLeftLimit){
          Debug.debug("Initialzing credential", 3);
          credential = Util.initGridProxy();
          gridProxyInitialized = Boolean.TRUE;
        }
        else{
          timerProxy.setInitialDelay((credential.getRemainingLifetime() - GridPilot.proxyTimeLeftLimit) * 1000);
          timerProxy.start();
          gridProxyInitialized = Boolean.TRUE;
        }
      }
      catch(Exception e){
        Debug.debug("ERROR: could not get grid credential", 1);
        e.printStackTrace();
      }
    }
    return credential;
  }

}
