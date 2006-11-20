package gridpilot;

import java.awt.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import javax.swing.*;

/**
 * Main class.
 * Instantiates all global objects and calls GlobalFrame.
 */

public class GridPilot extends JApplet{
  
  private static final long serialVersionUID = 1L;
  private boolean packFrame = false;
  private GlobalFrame frame;
  private static String confFileName = "gridpilot.conf";
  private static String userConfFileName = ".gridpilot";
  private static ClassMgr classMgr = new ClassMgr();
  private static boolean applet = true;  
  private static String debugLevel = "0";
  private static String proxyHost = null;
  private static String proxyPort = null;
  private static JLabel exitPanel = new JLabel();
  private static JPanel topExitPanel = new JPanel();
  
  public static HashMap tmpConfFile = new HashMap();
  public static String logFileName = "gridpilot.log";
  public static String [] jobColorMapping;
  public static String [] transferColorMapping;
  public static String[] jobStatusFields;
  public static String[] transferStatusFields;
  public static String resourcesPath = "";
  public static String [] tabs = null;
  public static Splash splash;
  public static int proxyTimeLeftLimit = 43200;
  public static int proxyTimeValid = 129600;
  public static String keyFile = "~/.globus/userkey.pem";
  public static String certFile = "~/.globus/usercert.pem";
  public static String keyPassword = null;
  public static String caCerts = null;
  public static String dateFormatString = "yyyy-MM-dd HH:mm:ss";
  public static String [] fixedJobAttributes = {"number", "name"};
  public static String browserHistoryFile = null;
  public static String globusTcpPortRange = null;
  public static String [] dbNames;
  public static String [] ftNames;
  public static String [] csNames = null;
  public static String gridftpHomeURL = null;

  /**
   * Constructor
   */
  public GridPilot(){
    
    try{
      getClassMgr().setLogFile(new LogFile(logFileName));
      // First try and get ~/.gridpilot or Documents and Settings/<user name>/gridpilot.conf
      if(System.getProperty("os.name").toLowerCase().startsWith("windows")){
        userConfFileName = confFileName;
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
        confFile = new ConfigFile(exConfFile);
      }
      catch(Exception ee){
        System.out.println("WARNING: could not load external configuration file, " +
                "using default config file.");
        ee.printStackTrace();
        confFile = new ConfigFile(confFileName);
      }
      getClassMgr().setConfigFile(confFile);
      loadConfigValues();
      loadDBs();
      loadFTs();
      initDebug();
      initGUI();
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

  public static void loadConfigValues(){
    try{
      
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
      splash = new Splash(resourcesPath, "splash.png");
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
        for(int i=0; i<csNames.length; ++i){
          String host = getClassMgr().getConfigFile().getValue(csNames[i], "host");
          if(host!=null && !host.endsWith("localhost")){
            String user = getClassMgr().getConfigFile().getValue(csNames[i], "user");
            String password = getClassMgr().getConfigFile().getValue(csNames[i], "password");
            getClassMgr().setShellMgr(csNames[i],
               new SecureShellMgr(host, user, password));
           }
          else if(host!=null && host.endsWith("localhost")){
            getClassMgr().setShellMgr(csNames[i], new LocalShellMgr());
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
      keyPassword = getClassMgr().getConfigFile().getValue("GridPilot",
          "key password");
      caCerts = getClassMgr().getConfigFile().getValue("GridPilot",
          "ca certificates");
      if(caCerts==null){
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
        Debug.debug("Job attributes: "+Util.arrayToString(fixedJobAttributes)+" "+
            fixedJobAttributes.length, 2);
      }
      Debug.debug("Job attributes: "+Util.arrayToString(fixedJobAttributes)+" "+
          fixedJobAttributes.length, 2);
      browserHistoryFile = getClassMgr().getConfigFile().getValue("GridPilot",
         "browser history file");
      globusTcpPortRange = getClassMgr().getConfigFile().getValue("File transfer systems",
         "globus tcp port range");
      gridftpHomeURL = getClassMgr().getConfigFile().getValue("GridPilot",
         "Gridftp home url");
    }
    catch(Throwable e){
      if(e instanceof Error)
        getClassMgr().getLogFile().addMessage("Error during loading of config values", e);
      else{
        getClassMgr().getLogFile().addMessage("Exception during loading of config values", e);
        System.exit(-1);
      }
    }    
  }
  
  public static void loadDBs() throws Throwable{
    String tmpDb = null;
    dbNames = getClassMgr().getConfigFile().getValues("Databases", "Systems");
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
    for(int i=0; i<ftNames.length; ++i){
      try{
        splashShow("Loading file transfer system: "+ftNames[i]);
      }
      catch(Exception e){
        // if we cannot show text on splash, just silently ignore
      }
      String fsClass = getClassMgr().getConfigFile().getValue(ftNames[i], "class");
      getClassMgr().setFTPlugin(ftNames[i],
          (FileTransfer) Util.loadClass(fsClass, new Class []{}, new Object []{}));
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
    Thread t1 = new Thread(){
      public void run(){
        exitPanel.setText("<html>Exiting...<br>Click OK to force quit.</html>");
        JProgressBar jp = new JProgressBar();
        jp.setIndeterminate(true);
        topExitPanel.setLayout(new GridBagLayout());
        topExitPanel.add(exitPanel);
        topExitPanel.add(jp);
        int ret = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(),
            topExitPanel,
            "Exiting", JOptionPane.PLAIN_MESSAGE);
        topExitPanel.validate();
        Debug.debug("return value: ", ret);
        if(ret==JOptionPane.OK_OPTION){
          System.exit(-1);
        }
      }
    };
    Thread t2 = new Thread(){
      public void run(){
        //  Cancel all transfers
        String message = "Cancelling all transfers...";
        Debug.debug(message, 2);
        exitPanel.setText("<html>"+message+"<br>Click OK to force quit.</html>");
        topExitPanel.validate();
        TransferControl.exit();
        //Delete temporary files
        File delFile = null;
        try{
          for(Iterator it=tmpConfFile.keySet().iterator(); it.hasNext(); ){
            delFile = ((File) tmpConfFile.get(it.next()));
            Debug.debug("Cleaning up: deleting "+delFile.getAbsolutePath(), 2);
            if(delFile.isDirectory()){
              LocalStaticShellMgr.deleteDir(delFile);
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
        exitPanel.setText("<html>"+message+"<br>Click OK to force quit.</html>");
        topExitPanel.validate();
        if(getClassMgr().csPluginMgr!=null){
          getClassMgr().getCSPluginMgr().disconnect();
          getClassMgr().getCSPluginMgr().exit();
        }
        message = "Disconnecting databases...";
        Debug.debug(message, 2);
        exitPanel.setText("<html>"+message+"<br>Click OK to force quit.</html>");
        for(int i=0; i<dbNames.length; ++i){
          getClassMgr().getDBPluginMgr(dbNames[i]).disconnect();
          Debug.debug("Disconnecting "+dbNames[i], 2);
        }
        message = "All systems disconnected.";
        Debug.debug(message, 2);
        exitPanel.setText(message);
        topExitPanel.validate();
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
            confFileName = args[i+1];
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
    try{
      for(Iterator it=tmpConfFile.keySet().iterator(); it.hasNext();){
        ((File) tmpConfFile.get(it.next())).delete();
      }
    }
    catch(Exception e){
    }
    getClassMgr().getConfigFile().makeTmpConfigFile();
    loadConfigValues();
    getClassMgr().getJobValidation().loadValues();
    getClassMgr().getSubmissionControl().loadValues();
    getClassMgr().getGlobalFrame().monitoringPanel.jobMonitor.statusUpdateControl.loadValues();
    getClassMgr().getGlobalFrame().monitoringPanel.transferMonitor.statusUpdateControl.loadValues();
    getClassMgr().getCSPluginMgr().loadValues();
    for(int i=0; i<dbNames.length; ++i){
      getClassMgr().getDBPluginMgr(dbNames[i]).loadValues();
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
        getClassMgr().setDebugLevel(3);
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
