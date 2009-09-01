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
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import javax.swing.*;
import javax.swing.plaf.metal.DefaultMetalTheme;
import javax.swing.plaf.metal.MetalLookAndFeel;
import javax.swing.plaf.metal.OceanTheme;

/**
 * Main class.
 * Instantiates all global objects and calls GlobalFrame.
 */

public class GridPilot extends JApplet{
  
  private static final long serialVersionUID = 1L;
  
  private boolean packFrame = false;
  
  private GlobalFrame frame;

  private static ClassMgr CLASS_MGR = new ClassMgr();
  private static boolean IS_APPLET = true;  
  private static String DEBUG_LEVEL = "0";
  private static String PROXY_HOST = null;
  private static String PROXY_PORT = null;
  private static JLabel EXIT_PANEL = new JLabel();
  private static JPanel TOP_EXIT_PANEL = new JPanel();
  /** List of files that will be deleted on exit. */
  private static HashMap<String, File> TMP_FILES = new HashMap<String, File>();
  
  private final static String DEFAULT_APP_STORE_URL = "https://www.gridpilot.dk/apps/";
  public static String APP_STORE_URL;
  protected static String userConfFileName;
  /**
   * List of main section headers in config file
   */
  public static String [] CONFIG_SECTIONS =
    {"File transfer systems", "Databases", "Computing systems"};
  /**
   * List of items that will not be included in the GUI preferences editor.
   */
  public static String [] MY_EXCLUDE_ITEMS = {"Systems", "*field*", "class", "driver",
      "parameters", "randomized", "* name", "* identifier", "* reference", "default user"};
  public static String TOP_CONFIG_SECTION = "GridPilot";
  public static String DEFAULT_CONF_FILE_NAME_UNIX = ".gridpilot";
  public static String DEFAULT_FILE_NAME_WINDOWS = "gridpilot.conf";
  public static String [] PREFERRED_FILE_SERVERS = null;
  public static int FILE_ROWS = 300;
  public static String LOG_FILE_NAME = "gridpilot.log";
  public static String [] JOB_COLOR_MAPPING;
  public static String [] TRANSFER_COLOR_MAPPING;
  public static String[] JOB_STATUS_FIELDS;
  public static String[] TRANSFER_STATUS_FIELDS;
  public static String RESOURCES_PATH = null;
  private static String SKIN_NAME = null;
  public static String ICONS_PATH = null;
  public static String [] TABS = null;
  public static Splash SPLASH;
  // Allow plugins to add monitoring panels. Any Component in
  // to extraMonitorTabs will be added by MonitoringPanel (called by initGUI).
  public static Vector EXTRA_MONITOR_TABS = new Vector();
  public static String PROXY_TYPE = "RFC";
  public static int PROXY_TIME_LEFT_LIMIT = 43200;
  public static int PROXY_TIME_VALID = 129600;
  public static String KEY_FILE = "~/.globus/userkey.pem";
  public static String CERT_FILE = "~/.globus/usercert.pem";
  public static String PROXY_DIR = "~/.globus/usercert.pem";
  public static String KEY_PASSWORD = null;
  public static String CA_CERTS_DIR = null;
  public static String DATE_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss";
  public static String [] FIXED_JOB_ATTRIBUTES = {"number", "name"};
  public static String BROWSER_HISTORY_FILE = null;
  public static String GLOBUS_TCP_PORT_RANGE = null;
  public static String [] DB_NAMES;
  public static String [] FT_NAMES;
  public static String [] CS_NAMES = null;
  public static String GRID_HOME_URL = null;
  public static boolean IS_EXITING = false;
  // Default when interrupting threads. Can be overridden by argument to waitForThread.
  public static boolean ASK_BEFORE_INTERRUPT = true;
  // This is set to true only if "remember this answer" is checked in a thread interruption
  // dialog. It overrides the various thread timeouts and can be cleared only by
  // "reload values from config file"
  public static boolean WAIT_FOREVER = false;
  public static boolean IS_FIRST_RUN = false;
  public static File USER_CONF_FILE = null;
  public static String RUNTIME_DIR = null;
  public static int PROXY_STRENGTH = 512;
  public static String VO;
  public static String VOMS_SERVER_URL;
  public static String FQAN;
  public static String VOMS_DIR;

  /**
   * Constructor
   */
  public GridPilot(){
    
    /** This will test for GUI events launched outside of the event dispatching thread. */
    //CheckThreadViolationRepaintManager.initMonitoring();
    
    try{
      getClassMgr().setLogFile(new MyLogFile(LOG_FILE_NAME));
      // First try and get ~/.gridpilot or Documents and Settings/<user name>/gridpilot.conf
      if(MyUtil.onWindows()){
        userConfFileName = DEFAULT_FILE_NAME_WINDOWS;
      }
      else{
        userConfFileName = DEFAULT_CONF_FILE_NAME_UNIX;
      }
      USER_CONF_FILE = new File(System.getProperty("user.home") + File.separator +
          userConfFileName);
      try{
        setConfigFile();
      }
      catch(Exception ee){
        System.out.println("WARNING: could not load user configuration file, " +
                "using defaults.");
        //ee.printStackTrace();
        //confFile = new ConfigFile(defaultConfFileNameWindows);
        try{
          loadConfigValues();
        }
        catch(Exception eee){
        }
        IS_FIRST_RUN = true;
        new BeginningWizard(IS_FIRST_RUN);
        IS_FIRST_RUN = false;
        setConfigFile();
      }      
      loadConfigValues();
      initDebug();
      loadDBs();
      loadFTs();
      Debug.debug("Grid home URL: "+GridPilot.GRID_HOME_URL, 2);
      mkGridHomeDirIfNotThere();
      setLookAndFeel();
      initGUI();
      try{
        (new TestDatasets()).createAll();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      SPLASH.stopSplash();
      SPLASH = null;
      getClassMgr().getLogFile().addInfo("GridPilot loaded");
      /** This will test for GUI hanging threads and report on stderr. */
      //EventDispatchThreadHangMonitor.initMonitoring();
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
  
  private void setConfigFile() throws FileNotFoundException{
    ConfigFile confFile = null;
    confFile = new ConfigFile(USER_CONF_FILE, TOP_CONFIG_SECTION, CONFIG_SECTIONS);
    confFile.excludeItems = MY_EXCLUDE_ITEMS;
    if(!USER_CONF_FILE.exists()){
      throw new FileNotFoundException("WARNING: Configuration file "+
          USER_CONF_FILE.getAbsolutePath()+" not found.");
    }
    System.out.println("Trying to load configuration file "+USER_CONF_FILE);
    getClassMgr().setConfigFile(confFile);
  }
  
  private void mkGridHomeDirIfNotThere(){
    if(!MyUtil.urlIsRemote(GridPilot.GRID_HOME_URL) &&
        !LocalStaticShell.existsFile(GridPilot.GRID_HOME_URL)){
      LocalStaticShell.mkdirs(GridPilot.GRID_HOME_URL);
    }
  }

  public static File getTmpFile(String key){
    return TMP_FILES.get(key);
  }

  public static void addTmpFile(String key, File file){
    TMP_FILES.put(key, file);
  }
  
  public static void forgetTmpFile(String key){
    TMP_FILES.remove(key);
  }

  // TODO: enclose each in try/catch and set sensible default if it fails
  public static void loadConfigValues(){
    try{
      doLoadConfigValues();
    }
    catch(Throwable e){
      e.printStackTrace();
      if(e instanceof Error){
        getClassMgr().getLogFile().addMessage("Error during loading of config values", e);
        System.exit(-1);
      }
      else{
        getClassMgr().getLogFile().addMessage("Exception during loading of config values", e);
      }
    }
    WAIT_FOREVER = false;
  }

  public static void doLoadConfigValues() throws Exception{   
    try{
      RESOURCES_PATH = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION, "Resources");
    }
    catch(Throwable e){
    }
    if(RESOURCES_PATH==null){
      RESOURCES_PATH = "/resources/";
    }
    else{
      if(!RESOURCES_PATH.endsWith("/"))
        RESOURCES_PATH = RESOURCES_PATH + "/";
    }
    try{
      SKIN_NAME = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION, "Skin");
    }
    catch(Throwable e){
    }
    if(SKIN_NAME==null){
      SKIN_NAME = "clear";
    }
    ICONS_PATH = RESOURCES_PATH + "skins/" + SKIN_NAME + "/";

    FILE_ROWS = Integer.parseInt(
        getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION, "File rows"));
    PREFERRED_FILE_SERVERS = getClassMgr().getConfigFile().getValues(TOP_CONFIG_SECTION, "Preferred file servers");
    PROXY_HOST = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION, "Proxy host");
    PROXY_PORT = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION, "Proxy port");
    if(PROXY_HOST!=null && PROXY_HOST.length()>0){
      if(PROXY_PORT==null || PROXY_PORT.length()==0){
        PROXY_PORT = "80";
      }
      Properties systemProperties = System.getProperties();
      systemProperties.put("http.proxySet", "true");
      systemProperties.put("http.proxyHost", PROXY_HOST);
      systemProperties.put("http.proxyPort", PROXY_PORT);
      systemProperties.put("https.proxyHost",PROXY_HOST);
      systemProperties.put("https.proxyPort",PROXY_PORT); 
      //systemProperties.put("http.proxyUser", "");
      //systemProperties.put("http.proxyPassword", "");
      System.setProperties(systemProperties);
    }
    
    DEBUG_LEVEL = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION, "Debug");
    setButtonDefaults();
    RUNTIME_DIR = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION, "Runtime directory");
    SPLASH = new Splash(RESOURCES_PATH+"splash.png", GridPilot.class);
    JOB_COLOR_MAPPING = getClassMgr().getConfigFile().getValues(TOP_CONFIG_SECTION, "Job color mapping");  
    /** Job status table header*/
    JOB_STATUS_FIELDS = new String [] {
        " ", "Job Name", "Job ID", "Job status", "CS", "Host", "DB", "DB status", "User"};
    TRANSFER_COLOR_MAPPING = getClassMgr().getConfigFile().getValues(TOP_CONFIG_SECTION, "Transfer color mapping");  
    /** Job status table header*/
    /** Transfer status table header*/
    TRANSFER_STATUS_FIELDS = new String [] {
        " ", "Transfer ID", "Source", "Destination", "User", "Status", "Transferred"};

    CS_NAMES = getClassMgr().getConfigFile().getValues("Computing systems", "systems");
    if(CS_NAMES==null || CS_NAMES.length==0){
      getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage("Computing systems", "systems"));
    }
    else{
      for(int i=0; i<CS_NAMES.length; ++i){
        if(!MyUtil.checkCSEnabled(CS_NAMES[i])){
          continue;
        }
        String host = getClassMgr().getConfigFile().getValue(CS_NAMES[i], "Host");
        if(host!=null && !host.startsWith("localhost") && !host.equals("127.0.0.1")){
          String user = getClassMgr().getConfigFile().getValue(CS_NAMES[i], "User");
          String password = getClassMgr().getConfigFile().getValue(CS_NAMES[i], "Password");
          String sshKeyFile = GridPilot.getClassMgr().getConfigFile().getValue(CS_NAMES[i], "Ssh key file");
          String sshKeyPassword = GridPilot.getClassMgr().getConfigFile().getValue(CS_NAMES[i], "Ssh key passphrase");
          getClassMgr().setShell(CS_NAMES[i],
              new MySecureShell(host, user, password,
                  sshKeyFile==null?null:new File(MyUtil.clearTildeLocally(MyUtil.clearFile(sshKeyFile))),
                  sshKeyPassword));
         }
        else if(host!=null && (host.startsWith("localhost") || host.equals("127.0.0.1"))){
          getClassMgr().setShell(CS_NAMES[i], new LocalShell());
        }
        else{
          // no shell used by this plugin
        }
      }
    }
    TABS = getClassMgr().getConfigFile().getValues(TOP_CONFIG_SECTION, "Initial panels");
    PROXY_TYPE = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION,
       "Proxy type", "RFC");
    PROXY_TIME_LEFT_LIMIT = Integer.parseInt(
      getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION, "Proxy time left limit"));
    PROXY_TIME_VALID = Integer.parseInt(
        getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION, "Proxy time valid"));
    KEY_FILE = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION,
        "Key file");
    CERT_FILE = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION,
        "Certificate file");
    PROXY_DIR = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION,
       "Proxy directory", "~/.globus");
    KEY_PASSWORD = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION,
        "Key password");
    CA_CERTS_DIR = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION,
        "CA certificates");
    if(CA_CERTS_DIR==null){
      getClassMgr().getConfigFile().missingMessage(
          TOP_CONFIG_SECTION, "CA certificates");
      getClassMgr().getLogFile().addMessage(
          "WARNING: you have not specified any CA certificates. " +
          "A default set will be used.");
    }
    VOMS_DIR = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION,
       "Voms directory");
    if(VOMS_DIR==null){
      getClassMgr().getConfigFile().missingMessage(
          TOP_CONFIG_SECTION, "voms directory");
      getClassMgr().getLogFile().addMessage(
          "WARNING: you have not specified any VOMS directory. " +
          "A default set of VOMS definitions will be used.");
    }
    VO = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION, "Virtual organization");
    VOMS_SERVER_URL = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION, "Voms server");
    FQAN = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION, "Voms fqan");
    String [] _fixedJobAttributes = getClassMgr().getConfigFile().getValues(TOP_CONFIG_SECTION,
       "Job attributes");
    if(_fixedJobAttributes==null || _fixedJobAttributes.length==0){
      getClassMgr().getConfigFile().missingMessage(
          TOP_CONFIG_SECTION, "Job attributes");
    }
    else{
      FIXED_JOB_ATTRIBUTES = _fixedJobAttributes;
      Debug.debug("Job attributes: "+MyUtil.arrayToString(FIXED_JOB_ATTRIBUTES)+" "+
          FIXED_JOB_ATTRIBUTES.length, 2);
    }
    Debug.debug("Job attributes: "+MyUtil.arrayToString(FIXED_JOB_ATTRIBUTES)+" "+
        FIXED_JOB_ATTRIBUTES.length, 2);
    BROWSER_HISTORY_FILE = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION,
       "Browser history file");
    GLOBUS_TCP_PORT_RANGE = getClassMgr().getConfigFile().getValue("File transfer systems",
       "Globus tcp port range");
    GRID_HOME_URL = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION,
       "Grid home url");
    APP_STORE_URL = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION,
       "Application store URL");
    if(APP_STORE_URL==null || APP_STORE_URL.equals("")){
      getClassMgr().getConfigFile().missingMessage(
          TOP_CONFIG_SECTION, "application store URL");
      getClassMgr().getLogFile().addMessage(
          "WARNING: you have not specified any application store URL. " +
          "The default will be used:"+DEFAULT_APP_STORE_URL);
      APP_STORE_URL = DEFAULT_APP_STORE_URL;
    }
    String ask = null;
    try{
      ask = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION,
      "Ask before thread interrupt");
      ASK_BEFORE_INTERRUPT = !(ask!=null && (
          ask.equalsIgnoreCase("no") ||
          ask.equalsIgnoreCase("false")));
    }
    catch(Exception e){
      ASK_BEFORE_INTERRUPT = true;
    }
    //getClassMgr().getConfigFile().printConfig();
  }
  
  private static void setLookAndFeel() {
    String lookAndFeel;
    String theme = null;
    String[] lines;
    try{
      String url = ICONS_PATH+"look_and_feel.txt";
      Debug.debug("Reading URL "+url, 3);
      URL lfUrl = GridPilot.class.getResource(url);
      lines = MyUtil.readURL(lfUrl, null, null, "#");
      lookAndFeel = lines[0];
    }
    catch(Exception e){
      lookAndFeel = UIManager.getCrossPlatformLookAndFeelClassName();
      GridPilot.getClassMgr().getLogFile().addInfo("No look and feel set, using default: "+lookAndFeel);
      e.printStackTrace();
      return;
    }
    try{
      theme = lines[1];
    }
    catch(Exception e){
    }
    if(theme!=null){
      Debug.debug("Setting Metal theme "+theme, 2);
      try{
        if(theme.equals("DefaultMetal")){
          MetalLookAndFeel.setCurrentTheme(new DefaultMetalTheme());
        }
        else if(theme.equals("Ocean")){
          MetalLookAndFeel.setCurrentTheme(new OceanTheme());
        }
        else{
          throw new Exception("Theme "+theme+" not known.");
        }
      }
      catch(Exception e){
        GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not set theme "+theme, e);
      }
    }
    try{
      if(lookAndFeel.equalsIgnoreCase("CrossPlatform") || 
          lookAndFeel.equalsIgnoreCase("Metal") || lookAndFeel.equalsIgnoreCase("MetalLookAndFeel") ){
        lookAndFeel = UIManager.getCrossPlatformLookAndFeelClassName();
      }
      else if(lookAndFeel.equalsIgnoreCase("System") || lookAndFeel.equalsIgnoreCase("SystemLookAndFeel") ){
        lookAndFeel = UIManager.getSystemLookAndFeelClassName();
      }
      Debug.debug("Setting Swing look and feel "+lookAndFeel, 2);
      UIManager.setLookAndFeel(lookAndFeel);
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not set look and feel "+lookAndFeel, e);
    }
  }

  public static void loadDBs() throws Throwable{
    String tmpDb = null;
    DB_NAMES = getClassMgr().getConfigFile().getValues("Databases", "Systems");
    Vector dbVector = new Vector();
    String enabled = "no";
    for(int i=0; i<DB_NAMES.length; ++i){
      enabled = "no";
      try{
        enabled = getClassMgr().getConfigFile().getValue(DB_NAMES[i], "Enabled");
      }
      catch(Exception e){
        e.printStackTrace();
        continue;
      }
      if(enabled==null || !enabled.equalsIgnoreCase("yes") &&
          !enabled.equalsIgnoreCase("true")){
        continue;
      }
      dbVector.add(DB_NAMES[i]);
    }
    int j = 0;
    DB_NAMES = new String [dbVector.size()];
    for(Iterator it=dbVector.iterator(); it.hasNext();){
      DB_NAMES[j] = (String) it.next();
      ++j;
    }
    for(int i=0; i<DB_NAMES.length; ++i){
      try{
        splashShow("Connecting to "+DB_NAMES[i]+"...");
      }
      catch(Exception e){
        // if we cannot show text on splash, just silently ignore
      }
      try{
        tmpDb = getClassMgr().getDBPluginMgr(DB_NAMES[i]).getDBName();
      }
      catch(NullPointerException e){
      }
      if(tmpDb==null){
        Debug.debug("Initializing db "+i+": "+DB_NAMES[i],3);
        getClassMgr().setDBPluginMgr(DB_NAMES[i], new DBPluginMgr(DB_NAMES[i]));
      }
    }          
  }
    
  public static void loadFTs() throws Throwable{
    // in case we are reloading, destroy any existing FT objects
    if(FT_NAMES!=null){
      for(int i=0; i<FT_NAMES.length; ++i){
        try{
          getClassMgr().setFTPlugin(FT_NAMES[i], null);
        }
        catch(Exception e){
          // if we cannot show text on splash, just silently ignore
        }
      }
    }
    FT_NAMES = getClassMgr().getConfigFile().getValues("File transfer systems", "Systems");
    String enabled = null;
    for(int i=0; i<FT_NAMES.length; ++i){
      enabled = null;
      try{
        enabled = getClassMgr().getConfigFile().getValue(FT_NAMES[i], "Enabled");
      }
      catch(Exception e){
        e.printStackTrace();
      }
      if(enabled==null || !enabled.equalsIgnoreCase("yes") && !enabled.equalsIgnoreCase("true")){
        continue;
      }
      try{
        if(!GridPilot.IS_FIRST_RUN){
          splashShow("Loading file transfer system: "+FT_NAMES[i]);
        }
      }
      catch(Exception e){
        // if we cannot show text on splash, just silently ignore
      }
      try{
        String ftClass = getClassMgr().getConfigFile().getValue(FT_NAMES[i], "Class");
        getClassMgr().setFTPlugin(FT_NAMES[i],
            (FileTransfer) MyUtil.loadClass(ftClass, new Class []{}, new Object []{}));
      }
      catch(Exception e){
        // load as many FTS as possible
        if(!GridPilot.IS_FIRST_RUN){
          GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not load file transfer system "+
              FT_NAMES[i], e);
        }
      }
    }          
  }
    
  /**
   * "Class distributor"
   */
  public static ClassMgr getClassMgr(){
    if(CLASS_MGR==null){
      Debug.debug("classMgr == null", 3);
    }
    return CLASS_MGR;
  }

  /**
   * Return file transfer systems specified in the configuration file
   */
   public static String [] getFTs(){
     if(FT_NAMES == null || FT_NAMES[0] == null){
       Debug.debug("ftNames null", 3);
     }
     return FT_NAMES;
   }
  

 /**
 + Are we running as an applet?
 */
  public static boolean isApplet(){
    return IS_APPLET;
  }

  /*
   * GUI
   */

  private void initGUI() throws Exception{
    splashShow("Initializing GUI");
    SwingUtilities.invokeAndWait(
      new Runnable(){
        public void run(){
          try{
            getClassMgr().setGlobalFrame(frame = new GlobalFrame());
            if(IS_APPLET){
              getClassMgr().getGlobalFrame().initGUI(getContentPane());
              splashShow("Initializing menus");
              setJMenuBar(
                  getClassMgr().getGlobalFrame().makeMenuBar());
            }
            else{
              getClassMgr().getGlobalFrame().initGUI(((JFrame)  
                  getClassMgr().getGlobalFrame()).getContentPane());
              splashShow("Initializing menus");
              frame.setJMenuBar(
                  getClassMgr().getGlobalFrame().makeMenuBar());
            }

          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
    );
    GridPilot.splashShow("Initializing monitoring panel...");
    getClassMgr().getGlobalFrame().initMonitoringPanel();
    GridPilot.splashShow("Setting up DB panels...");
    SwingUtilities.invokeAndWait(
      new Runnable(){
        public void run(){
          getClassMgr().getGlobalFrame().addDBPanels();
        }
      }
    );
    splashShow("Validating GUI");
    validateGUI();
  }
  
  private void validateGUI() throws InterruptedException, InvocationTargetException{
    SwingUtilities.invokeAndWait(
      new Runnable(){
        public void run(){
          try{
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
            //frame.setLocation((screenSize.width - frameSize.width) / 2, (screenSize.height - frameSize.height) / 2);
            Splash.centerWindow(frame);
            frame.setVisible(true);
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
    );
  }
  
  public static synchronized void exit(final int exitCode){
    if(IS_EXITING){
      return;
    }
    Thread t1 = new Thread(){
      public void run(){
        IS_EXITING = true;
        EXIT_PANEL.setPreferredSize(new Dimension(400, 40));
        EXIT_PANEL.setIgnoreRepaint(true);
        EXIT_PANEL.setText("Exiting... Please wait or click OK to force quit.");
        JProgressBar jp = new JProgressBar();
        jp.setIndeterminate(true);
        TOP_EXIT_PANEL.setLayout(new GridBagLayout());
        TOP_EXIT_PANEL.add(EXIT_PANEL, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER, GridBagConstraints.BOTH,
            new Insets(5, 5, 5, 5), 0, 0));
        TOP_EXIT_PANEL.add(jp, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
            GridBagConstraints.CENTER, GridBagConstraints.BOTH,
            new Insets(5, 5, 5, 5), 0, 0));
        TOP_EXIT_PANEL.setBackground(SystemColor.getColor("window"));
        int ret = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(),
            TOP_EXIT_PANEL,
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
        String message = "Cancelling running transfer(s)...";
        Debug.debug(message, 2);
        setExitPanelText(message+" Click OK to force quit.");
        jobManagerPanelExit();
        GridPilot.getClassMgr().getTransferControl().exit();
        //Delete temporary files
        File delFile = null;
        try{
          for(Iterator<String> it=TMP_FILES.keySet().iterator(); it.hasNext();){
            delFile = TMP_FILES.get(it.next());
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
        setExitPanelText(message+" Click OK to force quit.");
        if(getClassMgr().csPluginMgr!=null){
          getClassMgr().getCSPluginMgr().disconnect();
          getClassMgr().getCSPluginMgr().exit();
        }
        message = "Disconnecting databases...";
        Debug.debug(message, 2);
        setExitPanelText(message+" Click OK to force quit.");
        for(int i=0; i<DB_NAMES.length; ++i){
          getClassMgr().getDBPluginMgr(DB_NAMES[i]).disconnect();
          Debug.debug("Disconnecting "+DB_NAMES[i], 2);
        }
        message = "All systems disconnected.";
        Debug.debug(message, 2);
        setExitPanelText(message);
        if(!IS_APPLET){
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
    
    SwingUtilities.invokeLater(t1);
    t2.start();
  }
  
  private static void setExitPanelText(final String text){
    try{
      SwingUtilities.invokeAndWait(
        new Runnable(){
          public void run(){
            EXIT_PANEL.setText(text);
          }
        }
      );
    }
    catch(Exception e){
      e.printStackTrace();
    }

  }
  
  private static void jobManagerPanelExit() {
    MonitoringPanel mPanel = getClassMgr().getGlobalFrame().getMonitoringPanel();
    if(mPanel!=null){
      JobMonitoringPanel jmPanel = mPanel.getJobMonitoringPanel();
      if(jmPanel!=null){
        jmPanel.exit();
      }
    }
  }

  public static String [] userPwd(String message, String [] fields, String [] initialValues){    
    if(SPLASH!=null){
      SPLASH.hide();
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
    if(SPLASH!=null){
      SPLASH.show();
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
    IS_APPLET = false;
    if(args!=null){
      for(int i=0; i<args.length; ++i){
        if(args[i]!=null && (args[i].equals("-c") || args[i].equals("-conf"))){
          if(i+1>=args.length){
            badUsage("Configuration file missing after " + args[i]);
            break;
          }
          else{
            userConfFileName = args[i+1];
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
              LOG_FILE_NAME = args[i+1];
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
    if(MyUtil.onWindows()){
      userConfFileName = DEFAULT_FILE_NAME_WINDOWS;
    }
    else{
      userConfFileName = DEFAULT_CONF_FILE_NAME_UNIX;
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
      confFile = new ConfigFile(exConfFile, TOP_CONFIG_SECTION, CONFIG_SECTIONS);
      confFile.excludeItems = MY_EXCLUDE_ITEMS;
    }
    catch(Exception ee){
      String error = "WARNING: could not load external configuration file, " +
      "using default config file.";
      System.out.println(error);
      CLASS_MGR.getLogFile().addMessage(error, ee);
      ee.printStackTrace();
    }
    getClassMgr().setConfigFile(confFile);
    
    try{
      for(Iterator it=TMP_FILES.keySet().iterator(); it.hasNext();){
        ((File) TMP_FILES.get(it.next())).delete();
      }
    }
    catch(Exception e){
    }
    //getClassMgr().getConfigFile().makeTmpConfigFile();
    loadConfigValues();
    getClassMgr().getJobValidation().loadValues();
    getClassMgr().getSubmissionControl().loadValues();
    getClassMgr().getGlobalFrame().getMonitoringPanel().getJobMonitoringPanel().getJobStatusUpdateControl().loadValues();
    getClassMgr().getTransferStatusUpdateControl().loadValues();
    getClassMgr().getCSPluginMgr().loadValues();
    for(int i=0; i<DB_NAMES.length; ++i){
      getClassMgr().getDBPluginMgr(DB_NAMES[i]).loadValues();
    }
    try{
      loadFTs();
      SPLASH.stopSplash();
      SPLASH = null;
    }
    catch(Throwable e){
       e.printStackTrace();
    }
    initDebug();
    SPLASH = null;
  }
  
  private static void setButtonDefaults(){
    MyUtil.BUTTON_IMAGE_DIR = GridPilot.ICONS_PATH;
    MyUtil.BUTTON_IMAGE_LOAD_CLASS = GridPilot.class;
    String buttonsDisplay = getClassMgr().getConfigFile().getValue(TOP_CONFIG_SECTION, "Buttons display");
    if(buttonsDisplay!=null && !buttonsDisplay.trim().equals("")){
      MyUtil.BUTTON_DISPLAY = Integer.parseInt(buttonsDisplay);
    }
    else{
      MyUtil.BUTTON_DISPLAY = MyUtil.ICON_AND_TEXT;
    }
  }

  /**
   * Reads in configuration file the debug level.
   */
  private static void initDebug(){
    if(DEBUG_LEVEL==null){
      getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage(TOP_CONFIG_SECTION, "debug"));
      getClassMgr().setDebugLevel(0);
    }
    else{
      try{
        getClassMgr().setDebugLevel(new Integer(DEBUG_LEVEL).intValue());
      }
      catch(NumberFormatException nfe){
        getClassMgr().getLogFile().addMessage("Debug is not an integer in configFile, section [gridpilot]");
        getClassMgr().setDebugLevel(0);
      }
    }
  }

  public static void splashShow(String message){
    if(SPLASH!=null){
      SPLASH.show(message);
    }
    else{
      try{
        GridPilot.getClassMgr().getStatusBar().setLabel(message);
      }
      catch(Exception e){
      }
    }
  }

  public static void dbReconnect(){
    GridPilot.getClassMgr().getStatusBar().setLabel(
        "Reconnecting "+DB_NAMES.length+" databases. Please wait...");
    GridPilot.getClassMgr().getStatusBar().animateProgressBar();
    /*
     Reconnect DBs
     */
    for(int i=0; i<DB_NAMES.length; ++i){
      try{
        Debug.debug("Disconnecting "+DB_NAMES[i], 2);
        GridPilot.getClassMgr().getStatusBar().setLabel(
            "Disconnecting "+DB_NAMES[i]);
        GridPilot.getClassMgr().getStatusBar().animateProgressBar();
        getClassMgr().getDBPluginMgr(DB_NAMES[i]).disconnect();
        Debug.debug("Connecting to "+DB_NAMES[i], 2);
        GridPilot.getClassMgr().getStatusBar().setLabel(
            "Connecting to "+DB_NAMES[i]);
        GridPilot.getClassMgr().getStatusBar().animateProgressBar();
        getClassMgr().getDBPluginMgr(DB_NAMES[i]).init();
        Debug.debug("Connection ok.", 2);
        GridPilot.getClassMgr().getStatusBar().setLabel(
            "Connection ok.");
        GridPilot.getClassMgr().getStatusBar().animateProgressBar();
        // TODO: reload panels?
      }
      catch (Throwable e){
        Debug.debug("ERROR: Could not load DB " + DB_NAMES[i] + ". " + 
            e.getMessage(), 3);
        GridPilot.getClassMgr().getStatusBar().setLabel("ERROR: Could not load DB " + DB_NAMES[i] + ". " + 
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

  public static String getTabDisplayName(String tableName) {
    if(tableName.equalsIgnoreCase("executable")){
      return "executables";
    }
    else if(tableName.equalsIgnoreCase("runtimeEnvironment")){
      return "runtime environments";
    }
    else if(tableName.equalsIgnoreCase("dataset")){
      return "applications/datasets";
    }
    else if(tableName.equalsIgnoreCase("jobDefinition")){
      return "job definitions";
    }
    else if(tableName.equalsIgnoreCase("file")){
      return "files";
    }
    return tableName;
  }

  public static String getRecordDisplayName(String tableName) {
    if(tableName.equalsIgnoreCase("executable")){
      return "executable";
    }
    else if(tableName.equalsIgnoreCase("runtimeEnvironment")){
      return "runtime environment";
    }
    else if(tableName.equalsIgnoreCase("dataset")){
      return "application/dataset";
    }
    else if(tableName.equalsIgnoreCase("jobDefinition")){
      return "job definition";
    }
    return "Unknown:"+tableName;
  }

}
