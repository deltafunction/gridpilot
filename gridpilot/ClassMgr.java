package gridpilot;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.globus.common.CoGProperties;
import org.ietf.jgss.GSSCredential;

import gridpilot.ftplugins.gsiftp.GSIFTPFileTransfer;

/**
 * This class allows access to all global objects in gridpilot.
 */

public class ClassMgr{

  private ConfigFile configFile;
  private GlobalFrame globalFrame;
  private LogFile logFile;
  private StatusBar statusBar;
  private Table jobStatusTable;
  private Table transferStatusTable;
  private StatisticsPanel jobStatisticsPanel;
  private StatisticsPanel transferStatisticsPanel;
  private JobValidation jobValidation;
  private GridPilot prodCom;
  private int debugLevel = 3;
  private HashMap dbMgrs = new HashMap();
  private HashMap ft = new HashMap();
  private HashMap datasetMgrs = new HashMap();
  private Vector submittedJobs = new Vector();
  private Vector submittedTransfers = new Vector();
  private SubmissionControl submissionControl;
  private TransferControl transferControl;
  private GSIFTPFileTransfer gsiftpFileSystem;
  private Vector urlList = new Vector();
  private static String caCertsTmpdir = null;
  // only accessed directly by GridPilot.exit()
  public CSPluginMgr csPluginMgr;
  public GSSCredential credential = null;
  public Boolean gridProxyInitialized = Boolean.FALSE;
  
  public void setConfigFile(ConfigFile _configFile){
    configFile = _configFile;
  }

  public boolean setDBPluginMgr(String dbName, DBPluginMgr dbPluginMgr){
    try{
      dbMgrs.put(dbName, dbPluginMgr);
    }
    catch(NullPointerException e){
      dbMgrs.put(dbName, new HashMap());
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

  public void setJobStatusTable(Table _statusTable){
     jobStatusTable = _statusTable;
  }

  public void setTransferStatusTable(Table _statusTable){
    transferStatusTable = _statusTable;
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

  public void setTransferControl(TransferControl _transferControl){
    transferControl = _transferControl;
 }

  public ConfigFile getConfigFile(){
    if(configFile==null){
      Debug.debug("configFile null", 3);
    }
    return configFile;
  }

  // The HashMap of DB objects, dbMgrs, is kept here
  public DBPluginMgr getDBPluginMgr(String dbName) throws NullPointerException{
    Debug.debug("Getting DBPluginMgr for db " + dbName, 3);
    if(dbMgrs.get(dbName)==null){
      throw new NullPointerException("DBPluginMgr null for "+dbName);
    }
    return (DBPluginMgr) dbMgrs.get(dbName);
  }

  // The HashMap of FT objects, ft, is kept here
  public FileTransfer getFTPlugin(String ftName) throws NullPointerException{
    Debug.debug("Getting FT plugin for " + ftName, 3);
    if(ft.get(ftName)==null){
      throw new NullPointerException("FT plugin null for "+ftName);
    }
    return (FileTransfer) ft.get(ftName);
  }
  
  public void setFTPlugin(String ftName, FileTransfer ftObject){
    ft.put(ftName, ftObject);
  }

  // This method creates a new DatasetMgr if there is
  // none in the HashMap with keys dbName, datasetID
  public DatasetMgr getDatasetMgr(String dbName, String datasetID){
    if(datasetMgrs==null){
      Debug.debug("datasetMgrs null", 3);
    }
    if(!datasetMgrs.keySet().contains(dbName)){
      datasetMgrs.put(dbName, new HashMap());
    }
    if(!((HashMap) datasetMgrs.get(dbName)).keySet().contains(datasetID)){
      Debug.debug("Creating new DatasetMgr, "+datasetID+", in "+dbName, 3);
      addDatasetMgr(new DatasetMgr(dbName, datasetID));
    }
    return (DatasetMgr) ((HashMap) datasetMgrs.get(dbName)).get(datasetID);
  }
  
  public Vector getDatasetMgrs(){
    Vector allDatasetMgrs = new Vector();
    if(datasetMgrs==null || datasetMgrs.size()==0){
      Debug.debug("NO datasetMgrs", 3);
    }
    else{
      //Debug.debug("getting datasetMgrs, "+datasetMgrs.size(), 3);
      for(Iterator it=datasetMgrs.values().iterator(); it.hasNext();){
        allDatasetMgrs.addAll(((HashMap) it.next()).values());
      }
    }
    return allDatasetMgrs;
  }
  
  // The HashMap of HashMaps of datasets is kept here
  public void addDatasetMgr(DatasetMgr datasetMgr){
    if(datasetMgrs==null){
      Debug.debug("datasetMgrs null", 3);
      new Exception().printStackTrace();
    }
    if(!datasetMgrs.keySet().contains(datasetMgr.dbName)){
      datasetMgrs.put(datasetMgr.dbName, new HashMap());
    }
    ((HashMap) datasetMgrs.get(datasetMgr.dbName)
        ).put(datasetMgr.getDatasetID(), datasetMgr);
  }


  // Different model here: the HashMap of CS objects is kept in csPluginMgr.
  // We don't use a setCsPluginMgr method because we don't want to load
  // the classes and make the connections until it is necessary.
  public synchronized CSPluginMgr getCSPluginMgr(){
    if(csPluginMgr==null){
      Debug.debug("csPluginMgr null, creating new", 3);
      try{
        csPluginMgr = new CSPluginMgr();
        Debug.debug("csPluginMgr: "+csPluginMgr, 3);
      }
      catch(Throwable e){
        Debug.debug("Could not load plugins. "+e.getMessage(), 3);
        e.printStackTrace();
      }
    }
    return csPluginMgr;
  }

  public void clearDBCaches(){
    for(Iterator i=dbMgrs.values().iterator(); i.hasNext();){
      ((DBPluginMgr) i.next()).clearCaches();
    }
  }
  
  public LogFile getLogFile(){
    if(logFile==null){
      Debug.debug("logFile null", 3);
      setLogFile(new LogFile(""));
    }
    return logFile;
  }

  public JobValidation getJobValidation(){
    if(jobValidation==null){
      Debug.debug("jobValidation null, creating new", 3);
      setJobValidation(new JobValidation());
      Debug.debug("jobValidation: "+jobValidation, 3);
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

  public Table getJobStatusTable(){
    if(jobStatusTable==null){
      Debug.debug("jobStatusTable null", 3);
      String[] fieldNames = GridPilot.jobStatusFields;
      Debug.debug("Creating new Table with fields "+Util.arrayToString(fieldNames), 3);
      jobStatusTable = new Table(new String [] {}, fieldNames,
          GridPilot.jobColorMapping);
       GridPilot.getClassMgr().setJobStatusTable(jobStatusTable);
      //new Exception().printStackTrace();
    }
    return jobStatusTable;
  }

  public StatisticsPanel getJobStatisticsPanel(){
    if(jobStatisticsPanel==null){
      Debug.debug("jobStatisticsPanel null", 3);
      jobStatisticsPanel = new JobStatisticsPanel("Jobs statistics");
    }
    return jobStatisticsPanel;
  }

  public StatisticsPanel getTransferStatisticsPanel(){
    if(transferStatisticsPanel==null){
      Debug.debug("transferStatisticsPanel null", 3);
      transferStatisticsPanel = new TransferStatisticsPanel("Transfers statistics");
    }
    return transferStatisticsPanel;
  }

  public Vector getSubmittedJobs(){
    if(submittedJobs==null){
      Debug.debug("submittedJobs null", 3);
    }
    return submittedJobs;
  }

  public Table getTransferStatusTable(){
    if(transferStatusTable==null){
      Debug.debug("transferStatusTable null", 3);
      String[] fieldNames = GridPilot.transferStatusFields;
      Debug.debug("Creating new Table with fields "+Util.arrayToString(fieldNames), 3);
      transferStatusTable = new Table(new String [] {}, fieldNames,
          GridPilot.transferColorMapping);
       GridPilot.getClassMgr().setTransferStatusTable(transferStatusTable);
      //new Exception().printStackTrace();
    }
    return transferStatusTable;
  }

  public Vector getSubmittedTransfers(){
    if(submittedTransfers==null){
      Debug.debug("submittedTransfers null", 3);
    }
    return submittedTransfers;
  }

  public Vector getUrlList(){
    if(urlList==null){
      Debug.debug("urlList null", 3);
    }
    return urlList;
  }
  
  public synchronized void addUrl(String url){
    synchronized(urlList){
      if(urlList==null){
        Debug.debug("urlList null", 3);
      }
      urlList.add(url);
    }
  }

  public synchronized void removeUrl(String url){
    synchronized(urlList){
      if(urlList==null){
        Debug.debug("urlList null", 3);
      }
      urlList.remove(url);
    }
  }

  public synchronized void clearUrls(String url){
    synchronized(urlList){
      if(urlList==null){
        Debug.debug("urlList null", 3);
      }
      urlList.removeAllElements();
    }
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
      Debug.debug("submissionControl null, creating new", 1);
      setSubmissionControl(new SubmissionControl());
    }
    return submissionControl;
  }
  
  public TransferControl getTransferControl(){
    if(transferControl==null){
      Debug.debug("transferControl null, creating new", 1);
      setTransferControl(new TransferControl());
    }
    return transferControl;
  }
  
  public String getCaCertsTmpDir(){
    return caCertsTmpdir;
  }
  
  public /*synchronized*/ GSSCredential getGridCredential(){
    if(gridProxyInitialized.booleanValue()){
      return credential;
    }
    synchronized(gridProxyInitialized){
      // avoids that dozens of popups open if
      // you submit dozen of jobs and proxy not initialized
      try{
        if(credential==null || credential.getRemainingLifetime()<GridPilot.proxyTimeLeftLimit){
          Debug.debug("Initializing credential", 3);
          credential = Util.initGridProxy();
          Debug.debug("Initialized credential", 3);
          gridProxyInitialized = Boolean.TRUE;
          if(credential!=null){
            Debug.debug("Initialized credential"+credential.getRemainingLifetime()+
                ":"+GridPilot.proxyTimeLeftLimit, 3);
          }
        }
        else{
          gridProxyInitialized = Boolean.TRUE;
        }
        // set the directory for trusted CA certificates
        CoGProperties prop = null;
        if(CoGProperties.getDefault()==null){
          prop = new CoGProperties();
        }
        else{
          prop = CoGProperties.getDefault();
        }
        if(GridPilot.caCerts==null || GridPilot.caCerts.equals("")){
          if(caCertsTmpdir==null){
            caCertsTmpdir = Util.setupDefaultCACertificates();
            // this adds all certificates in the dir to globus authentication procedures
          }
          caCertsTmpdir = caCertsTmpdir.replaceAll("\\\\", "/");
          prop.setCaCertLocations(caCertsTmpdir);
        }
        else{
          prop.setCaCertLocations(GridPilot.caCerts);
        }
        // set the proxy default location
        prop.setProxyFile(Util.getProxyFile().getAbsolutePath());
        CoGProperties.setDefault(prop);
        Debug.debug("COG defaults now:\n"+CoGProperties.getDefault(), 3);
        Debug.debug("COG defaults file:\n"+CoGProperties.configFile, 3);
      }
      catch(Exception e){
        Debug.debug("ERROR: could not get grid credential", 1);
        e.printStackTrace();
      }
    }
    return credential;
  }

  // Sort of breaks the plugin concept.
  // TODO: improve
  public GSIFTPFileTransfer getGSIFTPFileTransfer(){
    if(gsiftpFileSystem==null){
      Debug.debug("gsiftpFileSystem null", 3);
      gsiftpFileSystem = new GSIFTPFileTransfer();
    }
    return gsiftpFileSystem;
  }

}
