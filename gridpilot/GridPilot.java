package gridpilot;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.FileTransfer;
import gridfactory.common.LocalShell;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.Splash;
import gridpilot.wizards.beginning.BeginningWizard;

import java.awt.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import javax.swing.*;

/**
 * Main class.
 * Instantiates all global objects and calls GlobalFrame.
 */

public class GridPilot extends JApplet{
  
  private static final long serialVersionUID = 1L;
  private boolean packFrame = false;
  private GlobalFrame frame;
  private static String userConfFileName = ".gridpilot";
  private static ClassMgr classMgr = new ClassMgr();
  private static boolean applet = true;  
  private static String debugLevel = "0";
  private static String proxyHost = null;
  private static String proxyPort = null;
  private static JLabel exitPanel = new JLabel();
  private static JPanel topExitPanel = new JPanel();
  
  /**
   * List of main section headers in config file
   */
  public static String [] configSections =
    {"File transfer systems", "Databases", "Computing systems"};
  /**
   * List of items that will not be included in the GUI preferences editor.
   */
  public static String [] myExcludeItems = {"Systems", "*field*", "class", "driver",
      "parameters", "randomized", "* name", "* identifier", "* reference", "default user"};
  public static String topConfigSection = "GridPilot";
  public static String defaultConfFileName = "gridpilot.conf";
  public static String [] preferredFileServers = null;
  public static int fileRows = 300;
  public static HashMap tmpConfFile = new HashMap();
  public static String logFileName = "gridpilot.log";
  public static String [] jobColorMapping;
  public static String [] transferColorMapping;
  public static String[] jobStatusFields;
  public static String[] transferStatusFields;
  public static String resourcesPath = "";
  public static String [] tabs = null;
  public static Splash splash;
  // Allow plugins to add monitoring panels. Any Component in
  // to extraMonitorTabs will be added by MonitoringPanel (called by initGUI).
  public static Vector extraMonitorTabs = new Vector();
  public static int proxyTimeLeftLimit = 43200;
  public static int proxyTimeValid = 129600;
  public static String keyFile = "~/.globus/userkey.pem";
  public static String certFile = "~/.globus/usercert.pem";
  public static String proxyDir = "~/.globus/usercert.pem";
  public static String keyPassword = null;
  public static String caCertsDir = null;
  public static String dateFormatString = "yyyy-MM-dd HH:mm:ss";
  public static String [] fixedJobAttributes = {"number", "name"};
  public static String browserHistoryFile = null;
  public static String globusTcpPortRange = null;
  public static String [] dbNames;
  public static String [] ftNames;
  public static String [] csNames = null;
  public static String gridHomeURL = null;
  public static boolean isExiting = false;
  // Default when interrupting threads. Can be overridden by argument to waitForThread.
  public static boolean askBeforeInterrupt = true;
  // This is set to true only if "remember this answer" is checked in a thread interruption
  // dialog. It overrides the various thread timeouts and can be cleared only by
  // "reload values from config file"
  public static boolean waitForever = false;
  public static boolean pullEnabled = false;
  public static int maxPullRerun = 0;
  public static boolean firstRun = false;
  public static File userConfFile = null;
  public static int PROXY_STRENGTH = 512;

  /**
   * Constructor
   */
  public GridPilot(){
    
    try{
      getClassMgr().setLogFile(new MyLogFile(logFileName));
      // First try and get ~/.gridpilot or Documents and Settings/<user name>/gridpilot.conf
      if(System.getProperty("os.name").toLowerCase().startsWith("windows")){
        userConfFileName = defaultConfFileName;
      }
      ConfigFile confFile = null;
      userConfFile = new File(System.getProperty("user.home") + File.separator +
          userConfFileName);
      try{
        confFile = new ConfigFile(userConfFile, topConfigSection, configSections);
        confFile.excludeItems = myExcludeItems;
        if(!userConfFile.exists()){
          throw new FileNotFoundException("WARNING: Configuration file "+
              userConfFile.getAbsolutePath()+" not found.");
        }
        System.out.println("Trying to load configuration file "+userConfFile);
        getClassMgr().setConfigFile(confFile);
      }
      catch(Exception ee){
        System.out.println("WARNING: could not load user configuration file, " +
                "using defaults.");
        //ee.printStackTrace();
        //confFile = new ConfigFile(defaultConfFileName);
        firstRun = true;
        new BeginningWizard(firstRun);
        firstRun = false;
      }      
      loadConfigValues();
      initDebug();
      loadDBs();
      loadFTs();
      initGUI();
      try{
        (new TestDatasets()).createAll();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      splash.stopSplash();
      splash = null;
      getClassMgr().getLogFile().addInfo("GridPilot loaded");
    }
    catch(Throwable e){
      if(e instanceof Error){
        getClassMgr().getLogFile().addMessage("Error during gridpilot loading", e);
      }
      else{
        getClassMgr().getLogFile().addMessage("Exception during gridpilot loading", e);
        exit(-1);
      }
    }
  }
  
  // TODO: enclose each in try/catch and set sensible default if it fails
  public static void loadConfigValues(){
    try{
      fileRows = Integer.parseInt(
          getClassMgr().getConfigFile().getValue("GridPilot", "file rows"));
      preferredFileServers = getClassMgr().getConfigFile().getValues("GridPilot", "preferred file servers");
      proxyHost = getClassMgr().getConfigFile().getValue("GridPilot", "proxy host");
      proxyPort = getClassMgr().getConfigFile().getValue("GridPilot", "proxy port");
      if(proxyHost!=null && proxyHost.length()>0){
        if(proxyPort==null || proxyPort.length()==0){
          proxyPort = "80";
        }
        Properties systemProperties = System.getProperties();
        systemProperties.put("http.proxySet", "true");
        systemProperties.put("http.proxyHost", proxyHost);
        systemProperties.put("http.proxyPort", proxyPort);
        systemProperties.put("https.proxyHost",proxyHost);
        systemProperties.put("https.proxyPort",proxyPort); 
        //systemProperties.put("http.proxyUser", "");
        //systemProperties.put("http.proxyPassword", "");
        System.setProperties(systemProperties);
      }
      
      debugLevel = getClassMgr().getConfigFile().getValue("GridPilot", "debug");
      resourcesPath =  getClassMgr().getConfigFile().getValue("GridPilot", "resources");
      if(resourcesPath==null){
        getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage("GridPilot", "resources"));
        resourcesPath = "./";
      }
      else{
        if(!resourcesPath.endsWith("/"))
          resourcesPath = resourcesPath + "/";
      }
      splash = new Splash(resourcesPath+"splash.png", GridPilot.class);
      jobColorMapping = getClassMgr().getConfigFile().getValues("GridPilot", "job color mapping");  
      /** Job status table header*/
      jobStatusFields = new String [] {
          " ", "Job Name", "Job ID", "Job status", "CS", "Host", "DB", "DB status", "user"};
      transferColorMapping = getClassMgr().getConfigFile().getValues("GridPilot", "transfer color mapping");  
      /** Job status table header*/
      /** Transfer status table header*/
      transferStatusFields = new String [] {
          " ", "Transfer ID", "Source", "Destination", "User", "Status", "Transferred"};

      csNames = getClassMgr().getConfigFile().getValues("Computing systems", "systems");
      if(csNames==null || csNames.length==0){
        getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage("Computing systems", "systems"));
      }
      else{
        String enabled = "no";
        for(int i=0; i<csNames.length; ++i){
          enabled = "no";
          try{
            enabled = GridPilot.getClassMgr().getConfigFile().getValue(csNames[i], "Enabled");
          }
          catch(Exception e){
            continue;
          }
          if(enabled==null || !enabled.equalsIgnoreCase("yes") &&
              !enabled.equalsIgnoreCase("true")){
            continue;
          }
          String host = getClassMgr().getConfigFile().getValue(csNames[i], "host");
          if(host!=null && !host.startsWith("localhost") && !host.equals("127.0.0.1")){
            String user = getClassMgr().getConfigFile().getValue(csNames[i], "user");
            String password = getClassMgr().getConfigFile().getValue(csNames[i], "password");
            String sshKeyFile = GridPilot.getClassMgr().getConfigFile().getValue(csNames[i], "Ssh key file");
            String sshKeyPassword = GridPilot.getClassMgr().getConfigFile().getValue(csNames[i], "Ssh key passphrase");
            getClassMgr().setShellMgr(csNames[i],
                new MySecureShell(host, user, password,
                    sshKeyFile==null?null:new File(MyUtil.clearTildeLocally(MyUtil.clearFile(sshKeyFile))),
                    sshKeyPassword));
           }
          else if(host!=null && (host.startsWith("localhost") || host.equals("127.0.0.1"))){
            getClassMgr().setShellMgr(csNames[i], new LocalShell());
          }
          else{
            // no shell used by this plugin
          }
        }
      }
      tabs = getClassMgr().getConfigFile().getValues("GridPilot", "initial panels");
      proxyTimeLeftLimit = Integer.parseInt(
        getClassMgr().getConfigFile().getValue("GridPilot", "proxy time left limit"));
      proxyTimeValid = Integer.parseInt(
          getClassMgr().getConfigFile().getValue("GridPilot", "proxy time valid"));
      keyFile = getClassMgr().getConfigFile().getValue("GridPilot",
          "key file");
      certFile = getClassMgr().getConfigFile().getValue("GridPilot",
          "certificate file");
      proxyDir = getClassMgr().getConfigFile().getValue("GridPilot",
      "grid proxy directory");
      keyPassword = getClassMgr().getConfigFile().getValue("GridPilot",
          "key password");
      caCertsDir = getClassMgr().getConfigFile().getValue("GridPilot",
          "ca certificates");
      if(caCertsDir==null){
        getClassMgr().getConfigFile().missingMessage(
            "GridPilot", "ca certificates");
        getClassMgr().getLogFile().addMessage(
            "WARNING: you have not specified any CA certificates. " +
            "A default set will be used.");
      }
      String [] _fixedJobAttributes = getClassMgr().getConfigFile().getValues("GridPilot",
      "job attributes");
      if(_fixedJobAttributes==null || _fixedJobAttributes.length==0){
        getClassMgr().getConfigFile().missingMessage(
            "GridPilot", "job attributes");
      }
      else{
        fixedJobAttributes = _fixedJobAttributes;
        Debug.debug("Job attributes: "+MyUtil.arrayToString(fixedJobAttributes)+" "+
            fixedJobAttributes.length, 2);
      }
      Debug.debug("Job attributes: "+MyUtil.arrayToString(fixedJobAttributes)+" "+
          fixedJobAttributes.length, 2);
      browserHistoryFile = getClassMgr().getConfigFile().getValue("GridPilot",
         "browser history file");
      globusTcpPortRange = getClassMgr().getConfigFile().getValue("File transfer systems",
         "globus tcp port range");
      gridHomeURL = getClassMgr().getConfigFile().getValue("GridPilot",
         "Grid home url");
      String maxReRunString = getClassMgr().getConfigFile().getValue("GridPilot", "Max pull rerun");
      if(maxReRunString!=null){
        try{
          maxPullRerun = Integer.parseInt(maxReRunString);
        }
        catch(Exception ee){
          ee.printStackTrace();
        }
     }
      String ask = null;
      try{
        ask = getClassMgr().getConfigFile().getValue("GridPilot",
        "Ask before thread interrupt");
        askBeforeInterrupt = !(ask!=null && (
            ask.equalsIgnoreCase("no") ||
            ask.equalsIgnoreCase("false")));
      }
      catch(Exception e){
        askBeforeInterrupt = true;
      }
      //getClassMgr().getConfigFile().printConfig();
    }
    catch(Throwable e){
      e.printStackTrace();
      if(e instanceof Error){
        getClassMgr().getLogFile().addMessage("Error during loading of config values", e);
      }
      else{
        getClassMgr().getLogFile().addMessage("Exception during loading of config values", e);
        System.exit(-1);
      }
    }
    waitForever = false;
  }
  
  public static void loadDBs() throws Throwable{
    String tmpDb = null;
    dbNames = getClassMgr().getConfigFile().getValues("Databases", "Systems");
    Vector dbVector = new Vector();
    String enabled = "no";
    for(int i=0; i<dbNames.length; ++i){
      enabled = "no";
      try{
        enabled = getClassMgr().getConfigFile().getValue(dbNames[i], "Enabled");
      }
      catch(Exception e){
        e.printStackTrace();
        continue;
      }
      if(enabled==null || !enabled.equalsIgnoreCase("yes") &&
          !enabled.equalsIgnoreCase("true")){
        continue;
      }
      dbVector.add(dbNames[i]);
    }
    int j = 0;
    dbNames = new String [dbVector.size()];
    for(Iterator it=dbVector.iterator(); it.hasNext();){
      dbNames[j] = (String) it.next();
      ++j;
    }
    for(int i=0; i<dbNames.length; ++i){
      try{
        splashShow("Connecting to "+dbNames[i]+"...");
      }
      catch(Exception e){
        // if we cannot show text on splash, just silently ignore
      }
      try{
        tmpDb = getClassMgr().getDBPluginMgr(dbNames[i]).getDBName();
      }
      catch(NullPointerException e){
      }
      if(tmpDb==null){
        Debug.debug("Initializing db "+i+": "+dbNames[i],3);
        getClassMgr().setDBPluginMgr(dbNames[i], new DBPluginMgr(dbNames[i]));
      }
    }          
  }
    
  public static void loadFTs() throws Throwable{
    // in case we are reloading, destroy any existing FT objects
    if(ftNames!=null){
      for(int i=0; i<ftNames.length; ++i){
        try{
          getClassMgr().setFTPlugin(ftNames[i], null);
        }
        catch(Exception e){
          // if we cannot show text on splash, just silently ignore
        }
      }
    }
    ftNames = getClassMgr().getConfigFile().getValues("File transfer systems", "Systems");
    String enabled = null;
    for(int i=0; i<ftNames.length; ++i){
      enabled = null;
      try{
        enabled = getClassMgr().getConfigFile().getValue(ftNames[i], "Enabled");
      }
      catch(Exception e){
        e.printStackTrace();
      }
      if(enabled==null || !enabled.equalsIgnoreCase("yes") && !enabled.equalsIgnoreCase("true")){
        continue;
      }
      try{
        if(!GridPilot.firstRun){
          splashShow("Loading file transfer system: "+ftNames[i]);
        }
      }
      catch(Exception e){
        // if we cannot show text on splash, just silently ignore
      }
      String ftClass = getClassMgr().getConfigFile().getValue(ftNames[i], "Class");
      getClassMgr().setFTPlugin(ftNames[i],
          (FileTransfer) MyUtil.loadClass(ftClass, new Class []{}, new Object []{}));
    }          
  }
    
  /**
   * "Class distributor"
   */
  public static ClassMgr getClassMgr(){
    if(classMgr==null){
      Debug.debug("classMgr == null", 3);
    }
    return classMgr;
  }

  /**
   * Return file transfer systems specified in the configuration file
   */
   public static String [] getFTs(){
     if(ftNames == null || ftNames[0] == null){
       Debug.debug("ftNames null", 3);
     }
     return ftNames;
   }
  

 /**
 + Are we running as an applet?
 */
  public static boolean isApplet(){
    return applet;
  } 

  /**
   * GUI
   */
  private void initGUI() throws Exception{

    if(applet){
      getClassMgr().setGlobalFrame(frame = new GlobalFrame());
      getClassMgr().getGlobalFrame().initGUI(this.getContentPane());
      setJMenuBar(
          getClassMgr().getGlobalFrame().makeMenu());
    }
    else{
      getClassMgr().setGlobalFrame(frame = new GlobalFrame());
      getClassMgr().getGlobalFrame().initGUI(((JFrame)  
          getClassMgr().getGlobalFrame()).getContentPane());
      frame.setJMenuBar(
          getClassMgr().getGlobalFrame().makeMenu());

      //Validate frames that have preset sizes.
      //Pack frames that have useful preferred size info, e.g. from their layout.
      if(packFrame){
        frame.pack();
      }
      else{
        frame.validate();
      }

      frame.setSize(new Dimension(950, 700));

      //Center the window
      Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
      Dimension frameSize = frame.getSize();
      if(frameSize.height>screenSize.height){
        frameSize.height = screenSize.height;
      }
      if(frameSize.width>screenSize.width){
        frameSize.width = screenSize.width;
      }
      requestFocusInWindow();
      frame.setLocation((screenSize.width - frameSize.width) / 2, (screenSize.height - frameSize.height) / 2);
      frame.setVisible(true);
    }
  }
  
  public static void exit(final int exitCode){
    if(isExiting){
      return;
    }
    Thread t1 = new Thread(){
      public void run(){
        isExiting = true;
        exitPanel.setPreferredSize(new Dimension(400, 40));
        exitPanel.setIgnoreRepaint(true);
        exitPanel.setText("Exiting... Please wait or click OK to force quit.");
        JProgressBar jp = new JProgressBar();
        jp.setIndeterminate(true);
        topExitPanel.setLayout(new GridBagLayout());
        topExitPanel.add(exitPanel, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER, GridBagConstraints.BOTH,
            new Insets(5, 5, 5, 5), 0, 0));
        topExitPanel.add(jp, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER, GridBagConstraints.BOTH,
            new Insets(5, 5, 5, 5), 0, 0));
        int ret = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(),
            topExitPanel,
            "Exiting", JOptionPane.PLAIN_MESSAGE);
        Debug.debug("return value: ", ret);
        if(ret==JOptionPane.OK_OPTION){
          System.exit(-1);
        }
      }
    };
    Thread t2 = new Thread(){
      public void run(){
        //  Cancel all transfers
        String message = "Cancelling all running transfers...";
        Debug.debug(message, 2);
        exitPanel.setText(message+" Click OK to force quit.");
        GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.jobMonitor.exit();
        TransferControl.exit();
        //Delete temporary files
        File delFile = null;
        try{
          for(Iterator it=tmpConfFile.keySet().iterator(); it.hasNext(); ){
            delFile = ((File) tmpConfFile.get(it.next()));
            Debug.debug("Cleaning up: deleting "+delFile.getAbsolutePath(), 2);
            if(delFile.isDirectory()){
              LocalStaticShell.deleteDir(delFile.getAbsolutePath());
            }
            else{
              delFile.delete();
            }
          }
        }
        catch(Exception e){
          e.printStackTrace();
        }
        // Disconnect DBs and CSs
        message = "Disconnecting computing systems...";
        Debug.debug(message, 2);
        exitPanel.setText(message+" Click OK to force quit.");
        if(getClassMgr().csPluginMgr!=null){
          getClassMgr().getCSPluginMgr().disconnect();
          getClassMgr().getCSPluginMgr().exit();
        }
        message = "Disconnecting databases...";
        Debug.debug(message, 2);
        exitPanel.setText(message+" Click OK to force quit.");
        for(int i=0; i<dbNames.length; ++i){
          getClassMgr().getDBPluginMgr(dbNames[i]).disconnect();
          Debug.debug("Disconnecting "+dbNames[i], 2);
        }
        message = "All systems disconnected.";
        Debug.debug(message, 2);
        exitPanel.setText(message);
        if(!applet){
          System.exit(exitCode);
        }
        else{
          try{
            Debug.debug("NAME: "+getClassMgr().getGridPilot(), 2);
            getClassMgr().getGlobalFrame().dispose();
          }
          catch(Exception e){
            Debug.debug(e.getMessage(), 1);
          }
        }
      }
    };
    t1.start();
    t2.start();
  }
  
  public static String [] userPwd(String message, String [] fields, String [] initialValues){    
    if(splash!=null){
      splash.hide();
    }   
    JPanel pUserPwd = new JPanel(new GridBagLayout());
    pUserPwd.add(new JLabel(message), new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
        GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 15, 0), 0, 0));
    JTextField [] tf = new JTextField [fields.length];
    for(int i=0; i<fields.length; ++i){
      if(fields[i].equalsIgnoreCase("password")){
        tf[i] = new JPasswordField(initialValues[i], 24);
      }
      else{
        tf[i] = new JTextField(initialValues[i], 24);
      }
      pUserPwd.add(new JLabel(fields[i]+": "), new GridBagConstraints(0, i+1, 1, 1, 0.0, 0.0,
          GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
      pUserPwd.add(tf[i], new GridBagConstraints(1, i+1, 1, 1, 1.0, 0.0,
          GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 5, 0), 0, 0));
    }
    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), pUserPwd,
        "Login", JOptionPane.OK_CANCEL_OPTION);
    String [] results = new String [fields.length];
    if(choice == JOptionPane.OK_OPTION){
      results = new String [fields.length];
      for(int i=0; i<fields.length; ++i){
        if(fields[i].equalsIgnoreCase("password")){
          results[i] = new String(((JPasswordField) tf[i]).getPassword());
        }
        else{
          results[i] = tf[i].getText();
        }
      }
    }
    else{
      results = null;
    }
    if(splash!=null){
      splash.show();
    }
    return results;
  }
  
  public void init(){
    getClassMgr().setGridPilot(this);
    Debug.debug("NAME: "+getClassMgr().getGridPilot().getAppletInfo(), 2);
  }

  /**
   * Main method
   */
  public static void main(String[] args) {
    applet = false;
    if(args!=null){
      for(int i=0; i<args.length; ++i){
        if(args[i]!=null && (args[i].equals("-c") || args[i].equals("-conf"))){
          if(i+1>=args.length){
            badUsage("Configuration file missing after " + args[i]);
            break;
          }
          else{
            defaultConfFileName = args[i+1];
            ++i;
          }
        }
        else{
          if(args[i]!=null && (args[i].equals("-l") || args[i].equals("-log"))){
            if(i+1>=args.length){
              badUsage("log file missing after " + args[i]);
              break;
            }
            else{
              logFileName = args[i+1];
              ++i;
            }
          }
          else{
            badUsage("unknown option "+ args[i]);
            break;
          }
        }
      }
    }
    getClassMgr().setGridPilot(new GridPilot());
    Debug.debug("NAME: "+getClassMgr().getGridPilot().getName(), 2);
  }

  private static void badUsage(String s){
    System.err.println(
        "Bad usage of parameters : " + s + "\nCorrect usage :\n" +
        GridPilot.class.getName() + " [-log log_file][-conf conf_file]\n");
  }
  
  /**
   * Reloads values from configuration file.
   * Called when user chooses "Reload values" in gridpilot menu
   */
  public static void reloadConfigValues(){
    
    // First try and get ~/.gridpilot or Documents and Settings/<user name>/gridpilot.conf
    if(System.getProperty("os.name").toLowerCase().startsWith("windows")){
      userConfFileName = defaultConfFileName;
    }
    ConfigFile confFile = null;
    try{
      File exConfFile = new File(System.getProperty("user.home") + File.separator +
          userConfFileName);
      if(!exConfFile.exists()){
        throw new FileNotFoundException("WARNING: Configuration file "+
            exConfFile.getAbsolutePath()+" not found.");
      }
      System.out.println("Trying to load configuration file "+exConfFile);
      confFile = new ConfigFile(exConfFile, topConfigSection, configSections);
      confFile.excludeItems = myExcludeItems;
    }
    catch(Exception ee){
      System.out.println("WARNING: could not load external configuration file, " +
              "using default config file.");
      ee.printStackTrace();
      confFile = new ConfigFile(defaultConfFileName, topConfigSection, configSections);
      confFile.excludeItems = myExcludeItems;
    }
    getClassMgr().setConfigFile(confFile);
    
    try{
      for(Iterator it=tmpConfFile.keySet().iterator(); it.hasNext();){
        ((File) tmpConfFile.get(it.next())).delete();
      }
    }
    catch(Exception e){
    }
    //getClassMgr().getConfigFile().makeTmpConfigFile();
    loadConfigValues();
    getClassMgr().getJobValidation().loadValues();
    getClassMgr().getSubmissionControl().loadValues();
    getClassMgr().getGlobalFrame().monitoringPanel.jobMonitor.statusUpdateControl.loadValues();
    getClassMgr().getTransferStatusUpdateControl().loadValues();
    getClassMgr().getCSPluginMgr().loadValues();
    for(int i=0; i<dbNames.length; ++i){
      getClassMgr().getDBPluginMgr(dbNames[i]).loadValues();
    }
    try{
      loadFTs();
      splash.stopSplash();
      splash = null;
    }
    catch(Throwable e){
       e.printStackTrace();
    }
    initDebug();
    splash = null;
  }

  /**
   * Reads in configuration file the debug level.
   */
  private static void initDebug(){
        if(debugLevel==null){
      getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage("GridPilot", "debug"));
      getClassMgr().setDebugLevel(0);
    }
    else{
      try{
        getClassMgr().setDebugLevel(new Integer(debugLevel).intValue());
      }
      catch(NumberFormatException nfe){
        getClassMgr().getLogFile().addMessage("Debug is not an integer in configFile, section [gridpilot]");
        getClassMgr().setDebugLevel(0);
      }
    }
  }

  public static void splashShow(String message){
    if(splash!=null){
      splash.show(message);
    }
    else{
      GridPilot.getClassMgr().getStatusBar().setLabel(message);
    }

  }

  public static void dbReconnect(){
    GridPilot.getClassMgr().getStatusBar().setLabel(
        "Reconnecting "+dbNames.length+" databases. Please wait...");
    GridPilot.getClassMgr().getStatusBar().animateProgressBar();
    /*
     Reconnect DBs
     */
    for(int i=0; i<dbNames.length; ++i){
      try{
        Debug.debug("Disconnecting "+dbNames[i], 2);
        GridPilot.getClassMgr().getStatusBar().setLabel(
            "Disconnecting "+dbNames[i]);
        GridPilot.getClassMgr().getStatusBar().animateProgressBar();
        getClassMgr().getDBPluginMgr(dbNames[i]).disconnect();
        Debug.debug("Connecting to "+dbNames[i], 2);
        GridPilot.getClassMgr().getStatusBar().setLabel(
            "Connecting to "+dbNames[i]);
        GridPilot.getClassMgr().getStatusBar().animateProgressBar();
        getClassMgr().getDBPluginMgr(dbNames[i]).init();
        Debug.debug("Connection ok.", 2);
        GridPilot.getClassMgr().getStatusBar().setLabel(
            "Connection ok.");
        GridPilot.getClassMgr().getStatusBar().animateProgressBar();
        // TODO: reload panels?
      }
      catch (Throwable e){
        Debug.debug("ERROR: Could not load DB " + dbNames[i] + ". " + 
            e.getMessage(), 3);
        GridPilot.getClassMgr().getStatusBar().setLabel("ERROR: Could not load DB " + dbNames[i] + ". " + 
            e.getMessage());
        //exit(-1);
      }
    }
    GridPilot.getClassMgr().getStatusBar().stopAnimation();
  }

  public static void csReconnect(){
    GridPilot.getClassMgr().getStatusBar().setLabel(
        "Reconnecting computing systems. Please wait...");
    GridPilot.getClassMgr().getStatusBar().animateProgressBar();
    pullEnabled = false;
    /*
     Reconnect CSs
     */
    try{
      getClassMgr().getCSPluginMgr().reconnect();
      // TODO: reload panels?
    }
    catch(Throwable e){
      Debug.debug("ERROR: Could not reload CSs. " + e.getMessage(), 3);
      GridPilot.getClassMgr().getStatusBar().setLabel("ERROR: Could not reload computing systems. " + 
          e.getMessage());
      //exit(-1);
    }
    GridPilot.getClassMgr().getStatusBar().stopAnimation();
    GridPilot.getClassMgr().getStatusBar().setLabel("Connection ok.");
  }

}
