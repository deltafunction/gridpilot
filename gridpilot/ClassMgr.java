package gridpilot;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import org.globus.common.CoGProperties;
import org.globus.gsi.CertUtil;
import org.ietf.jgss.GSSCredential;
import org.logicalcobwebs.proxool.ProxoolFacade;

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
  private GridPilot gridPilot;
  private int debugLevel = 0;
  private HashMap dbMgrs = new HashMap();
  private HashMap ft = new HashMap();
  private HashMap jobMgrs = new HashMap();
  private Vector submittedJobs = new Vector();
  private Vector submittedTransfers = new Vector();
  private SubmissionControl submissionControl;
  private TransferControl transferControl;
  private Vector urlList = new Vector();
  private HashMap shellMgrs = new HashMap();
  private static String caCertsTmpdir = null;
  private static String DEFAULT_POOL_SIZE = "10";
  /** List of urls in db pool */
  private HashSet dbURLs = new HashSet();
  /**
   * Map of pulled jobs -> computing systems.
   * This map will be cleared on exit - also, all pulled
   * JobDefinitions will be set back to 'ready'.
   */
  private HashMap jobCSMap = new HashMap();
  private X509Certificate x509UserCert = null;
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
    gridPilot = _gridpilot;
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

  // This method creates a new JobMgr if there is
  // none in the HashMap with key dbName
  public JobMgr getJobMgr(String dbName){
    if(jobMgrs==null){
      Debug.debug("jobMgrs null", 3);
    }
    if(!jobMgrs.keySet().contains(dbName)){
      Debug.debug("Creating new JobMgr, "+dbName, 3);
      jobMgrs.put(dbName, new JobMgr(dbName));
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
  public ShellMgr getShellMgr(JobInfo job) throws Exception{
    String csName = job.getCSName();
    if(csName==null || csName.equals("")){
      return askWhichShell(job);
    }
    else{
      return getShellMgr(csName);
    }
  }
  
  public void setShellMgr(String csName, ShellMgr shellMgr){
    shellMgrs.put(csName, shellMgr);
  }

  public ShellMgr getShellMgr(String csName) throws Exception{
    ShellMgr smgr = (ShellMgr) shellMgrs.get(csName);
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
  
  private ShellMgr askWhichShell(JobInfo job){

    JComboBox cb = new JComboBox();
    for(int i=0; i<shellMgrs.size() ; ++i){
      String type = "";
      if(shellMgrs.get(GridPilot.csNames[i]) instanceof SecureShellMgr){
        type = " (remote)";
      }
      if(shellMgrs.get(GridPilot.csNames[i]) instanceof LocalStaticShellMgr){
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
      return (ShellMgr) shellMgrs.get(GridPilot.csNames[ind]);
    }
    else{
      return null;
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
    return globalFrame;
  }

  public GridPilot getGridPilot(){
    if(gridPilot==null){
      Debug.debug("Object null", 3);
      new Exception().printStackTrace();
    }
    return gridPilot;
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
  
  public X509Certificate getX509UserCert() throws IOException, GeneralSecurityException{
    x509UserCert = CertUtil.loadCertificate(Util.clearTildeLocally(GridPilot.certFile));
    return x509UserCert;
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
          caCertsTmpdir = GridPilot.caCerts;
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
      info.setProperty("proxool.house-keeping-test-sql", "select CURRENT_DATE");
      info.setProperty("verbose", "false");
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
  
  /**
   * Obtains a database handle via the Proxool service.
   * 
   * @param dbName the name of the database to use
   * @return a Connection object, allowing database operations
   */
  public Connection getDBConnection(String dbName){
    Connection conn = null;
    try{
      conn = DriverManager.getConnection("proxool."+dbName);
    }
    catch(SQLException e){
      e.printStackTrace();
      GridPilot.getClassMgr().getLogFile().addMessage(
          "ERROR: failed connecting to database "+dbName, e);
    }
    try{
      conn.setAutoCommit(true);
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage(
          "failed setting auto commit to true: "+e.getMessage());
    }
    return conn;
  }
  
  public String getJobCS(String jobDefID){
    return (String) jobCSMap.get(jobDefID);
  }

  public void clearJobCS(String jobDefID){
    jobCSMap.remove(jobDefID);
  }

  public void setJobCS(String jobDefID, String csName){
   jobCSMap.put(jobDefID, csName);
  }

}
