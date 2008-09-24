package gridpilot;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.FileTransfer;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.Shell;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import org.ietf.jgss.GSSCredential;
import org.logicalcobwebs.proxool.ProxoolFacade;

/**
 * This class allows access to all global objects in gridpilot.
 */

public class ClassMgr{

  private ConfigFile configFile;
  private GlobalFrame globalFrame;
  private MyLogFile logFile;
  private StatusBar statusBar;
  private MyTransferStatusUpdateControl transferStatusUpdateControl;
  private MyJTable jobStatusTable;
  private MyJTable transferStatusTable;
  private StatisticsPanel jobStatisticsPanel;
  private StatisticsPanel transferStatisticsPanel;
  private JobValidation jobValidation;
  private GridPilot gridPilot;
  private HashMap dbMgrs = new HashMap();
  private HashMap ft = new HashMap();
  private HashMap jobMgrs = new HashMap();
  private Vector submittedJobs = new Vector();
  private Vector submittedTransfers = new Vector();
  private SubmissionControl submissionControl;
  private MyTransferControl transferControl;
  private Vector urlList = new Vector();
  private HashMap shellMgrs = new HashMap();
  private static String DEFAULT_POOL_SIZE = "10";
  /** List of urls in db pool */
  private HashSet dbURLs = new HashSet();
  private MySSL ssl = null;
  private HashMap<String, RteRdfParser> rdfParsers = new HashMap<String, RteRdfParser>();
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

  public void setLogFile(MyLogFile _logFile){
    logFile = _logFile;
  }

  public void setStatusBar(StatusBar _statusBar){
    statusBar = _statusBar;
  }

  public void setJobStatusTable(MyJTable _statusTable){
     jobStatusTable = _statusTable;
  }

  public void setTransferStatusTable(MyJTable _statusTable){
    transferStatusTable = _statusTable;
 }

  public void setJobValidation(JobValidation _jobValidation){
    jobValidation = _jobValidation;
  }

  public void setGlobalFrame(GlobalFrame _globalFrame) {
    globalFrame = _globalFrame;
  }

  public void setGridPilot(GridPilot _gridpilot) {
    gridPilot = _gridpilot;
  }

  public void setDebugLevel(int _debugLevel){
    Debug.DEBUG_LEVEL = _debugLevel;
  }

  public void setSubmissionControl(SubmissionControl _submissionControl){
     submissionControl = _submissionControl;
  }

  public void setTransferControl(MyTransferControl _transferControl){
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

  // This method creates a new JobMgr if there is
  // none in the HashMap with key dbName
  public JobMgr getJobMgr(String dbName){
    if(jobMgrs==null){
      Debug.debug("jobMgrs null", 3);
    }
    if(!jobMgrs.keySet().contains(dbName)){
      Debug.debug("Creating new JobMgr, "+dbName, 3);
      try{
        jobMgrs.put(dbName, new JobMgr(dbName));
      }
      catch(Exception e){
        e.printStackTrace();
        logFile.addMessage("ERROR: could not create new JobMgr.", e);
      }
    }
    return (JobMgr) jobMgrs.get(dbName);
  }
  
  public Vector getJobMgrs(){
    Vector allJobMgrs = new Vector();
    if(jobMgrs==null || jobMgrs.size()==0){
      Debug.debug("No jobMgrs", 3);
    }
    else{
      //Debug.debug("getting jobMgrs, "+jobMgrs.size(), 3);
      for(Iterator it=jobMgrs.values().iterator(); it.hasNext();){
        allJobMgrs.add(it.next());
      }
    }
    return allJobMgrs;
  }
  
  // The HashMap of HashMaps of datasets is kept here
  public void addJobMgr(JobMgr jobMgr){
    if(jobMgrs==null){
      Debug.debug("jobMgrs null", 3);
      new Exception().printStackTrace();
    }
    if(!jobMgrs.keySet().contains(jobMgr.dbName)){
      jobMgrs.put(jobMgr.dbName, jobMgr);
    }
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

  /**
   * Return the Shell Manager for this job
   */
  public Shell getShellMgr(MyJobInfo job) throws Exception{
    String csName = job.getCSName();
    if(csName==null || csName.equals("")){
      return askWhichShell(job);
    }
    else{
      return getShellMgr(csName);
    }
  }
  
  public void setShellMgr(String csName, Shell shellMgr){
    shellMgrs.put(csName, shellMgr);
  }

  public Shell getShellMgr(String csName) throws Exception{
    Shell smgr = (Shell) shellMgrs.get(csName);
    if(smgr==null){
      Debug.debug("No computing system "+csName, 3);
      throw new Exception("No computing system "+csName);
    }
    else{
      return smgr;
    }
  }

  public void clearDBCaches(){
    for(Iterator i=dbMgrs.values().iterator(); i.hasNext();){
      ((DBPluginMgr) i.next()).clearCaches();
    }
  }
  
  private Shell askWhichShell(MyJobInfo job){

    JComboBox cb = new JComboBox();
    for(int i=0; i<shellMgrs.size() ; ++i){
      String type = "";
      if(shellMgrs.get(GridPilot.csNames[i]) instanceof MySecureShell){
        type = " (remote)";
      }
      if(shellMgrs.get(GridPilot.csNames[i]) instanceof LocalStaticShell){
        type = " (local)";
      }
      cb.addItem(GridPilot.csNames[i] + type);
    }
    cb.setSelectedIndex(0);


    JPanel p = new JPanel(new java.awt.BorderLayout());
    p.add(new JLabel("Which shell do you want to use for this job (" +
                     job.getName() +")"), java.awt.BorderLayout.NORTH );
    p.add(cb, java.awt.BorderLayout.CENTER);

    JOptionPane.showMessageDialog(JOptionPane.getRootFrame(), p,
                                  "This job doesn't have a shell",
                                  JOptionPane.PLAIN_MESSAGE);

    int ind = cb.getSelectedIndex();
    if(ind>=0 && ind<shellMgrs.size()){
      return (Shell) shellMgrs.get(GridPilot.csNames[ind]);
    }
    else{
      return null;
    }
  }

  public MyLogFile getLogFile(){
    if(logFile==null){
      Debug.debug("logFile null", 3);
      setLogFile(new MyLogFile(""));
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

  public MyTransferStatusUpdateControl getTransferStatusUpdateControl(){
    if(transferStatusUpdateControl==null){
      Debug.debug("transferStatusUpdateControl null", 3);
      try{
        transferStatusUpdateControl = new MyTransferStatusUpdateControl();
      }
      catch(Exception e){
        e.printStackTrace();
        logFile.addMessage("ERROR: could not create new TransferStatusUpdateControl.", e);
      }
    }
    return transferStatusUpdateControl;
  }

  public MyJTable getJobStatusTable() throws Exception{
    if(jobStatusTable==null){
      Debug.debug("jobStatusTable null", 3);
      String[] fieldNames = GridPilot.jobStatusFields;
      Debug.debug("Creating new Table with fields "+MyUtil.arrayToString(fieldNames), 3);
      jobStatusTable = new MyJTable(new String [] {}, fieldNames,
          GridPilot.jobColorMapping);
      setJobStatusTable(jobStatusTable);
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

  public MyJTable getTransferStatusTable() throws Exception{
    if(transferStatusTable==null){
      Debug.debug("transferStatusTable null", 3);
      String[] fieldNames = GridPilot.transferStatusFields;
      Debug.debug("Creating new Table with fields "+MyUtil.arrayToString(fieldNames), 3);
      transferStatusTable = new MyJTable(new String [] {}, fieldNames,
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
    return globalFrame;
  }

  public GridPilot getGridPilot(){
    if(gridPilot==null){
      Debug.debug("Object null", 3);
      new Exception().printStackTrace();
    }
    return gridPilot;
  }
  
  public SubmissionControl getSubmissionControl(){
    if(submissionControl==null){
      Debug.debug("submissionControl null, creating new", 1);
      try{
        setSubmissionControl(new SubmissionControl());
      }
      catch(Exception e){
        e.printStackTrace();
        logFile.addMessage("ERROR: could not create new SubmissionControl.", e);
      }
    }
    return submissionControl;
  }
  
  public MyTransferControl getTransferControl(){
    if(transferControl==null){
      Debug.debug("transferControl null, creating new", 1);
      try{
        setTransferControl(new MyTransferControl());
      }
      catch(Exception e){
        e.printStackTrace();
        logFile.addMessage("ERROR: could not create new TransferControl.", e);
      }
    }
    return transferControl;
  }
  
  public MySSL getSSL() throws IOException, GeneralSecurityException{
    if(ssl==null){
      Debug.debug("Constructing SSL with "+GridPilot.certFile+":"+GridPilot.keyFile+":"+
          GridPilot.keyPassword+":"+GridPilot.caCertsDir, 1);
      ssl = new MySSL(GridPilot.certFile, GridPilot.keyFile, GridPilot.keyPassword, GridPilot.caCertsDir);
    }
    return ssl;
  }
  
  /**
   * Establishes a JDBC connection to a database. The connection is
   * relayed through Proxool.
   * 
   * @param dbName the name of the database in the Proxool registry
   * @param driver the name of the class providing the driver to use
   * @param databaseUrl the URL of the database. E.g. jdbc:mysql://server.name
   * @param user user name
   * @param passwd password for authenticating with the database.
   *        Can be null, if X509 authorization is used
   * @param gridAuth whether or not to use X509 authorization
   * @param connectionTimeout timeout in milliseconds of connecting
   * @param milliseconds timeout in milliseconds of establishing a socket
   * @param _poolSize the number of connections in the Proxool pool
   */
  public void sqlConnection(String dbName, String driver, String databaseUrl,
      String user, String passwd, boolean gridAuth, String connectionTimeout,
      String socketTimeout, String _poolSize) throws SQLException{
    
    if(dbURLs.contains(dbName)){
      return;
    }
    
    Debug.debug("connectTimeout: "+connectionTimeout, 3);
    Debug.debug("socketTimeout: "+socketTimeout, 3);
    Debug.debug("database: "+databaseUrl, 3);
    Debug.debug("gridAuth: "+gridAuth, 3);
    Debug.debug("user: "+user, 3);
    Debug.debug("passwd: "+passwd, 3);   

    String poolSize = _poolSize==null?DEFAULT_POOL_SIZE:_poolSize;
    Properties info = new Properties();
        
    try{
      if(gridAuth){
        /*url = database+
            "?user="+user+"&password=&useSSL=true"+
                    "&connectionTimeout="+connectTimeout+
                    "&socketTimeout="+socketTimeout;*/
        info.setProperty("user", user);
        info.setProperty("password", "");
        info.setProperty("useSSL", "true");
        if(connectionTimeout!=null){
          info.setProperty("connectionTimeout", connectionTimeout);
        }
        if(socketTimeout!=null){
          info.setProperty("socketTimeout", socketTimeout);
        }
      }
      else{
        /*url = database+
            "?user="+user+"&password="+passwd+
            "&connectionTimeout="+connectTimeout+
            "&socketTimeout="+socketTimeout;*/
        info.setProperty("user", user);
        info.setProperty("password", passwd);
        if(connectionTimeout!=null){
          info.setProperty("connectionTimeout", connectionTimeout);
        }
        if(socketTimeout!=null){
          info.setProperty("socketTimeout", socketTimeout);
        }
      }
    }
    catch(Exception e){
      String error = "Could not connect to database "+databaseUrl+
          " with "+user+":"+passwd;
      e.printStackTrace();
      throw new SQLException(error);
    }  
    
    try{
      Class.forName("org.logicalcobwebs.proxool.ProxoolDriver");
      info.setProperty("proxool.maximum-connection-count", poolSize);
      //info.setProperty("proxool.house-keeping-test-sql", "select CURRENT_DATE");
      info.setProperty("verbose", "true");
      info.setProperty("trace", "true");
      info.setProperty("test-before-use", "true");
      info.setProperty("test-after-use", "true");
      //info.setProperty("house-keeping-sleep-time", "10");
      //String url = "jdbc:hsqldb:test";
      String proxoolUrl = "proxool." + dbName + ":" + driver + ":" + databaseUrl;
      ProxoolFacade.registerConnectionPool(proxoolUrl, info);

      //Class.forName(driver).newInstance();
    }
    catch(Exception e){
      String error = "Could not load the driver "+driver+". ";
      GridPilot.getClassMgr().getLogFile().addMessage(error, e);
      Debug.debug(error, 1);
      e.printStackTrace();
      throw new SQLException(error);
    }
    dbURLs.add(dbName);
  }

  public RteRdfParser getRteRdfParser(String[] rteCatalogUrls) {
    String [] sortedUrls = rteCatalogUrls.clone();
    Arrays.sort(sortedUrls);
    String key = MyUtil.arrayToString(sortedUrls);
    if(!rdfParsers.containsKey(key)){
      Debug.debug("Creating new RteRdfParser from "+key, 1);
      RteRdfParser rteRdfParser = new RteRdfParser(rteCatalogUrls);
      rdfParsers.put(key, rteRdfParser);
    }
    return rdfParsers.get(key);
  }
  
}
