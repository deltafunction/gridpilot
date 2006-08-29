package gridpilot;

import java.awt.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;

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
  public static HashMap tmpConfFile = new HashMap();
  public static String logFileName = "gridpilot.log";
  public static String [] dbs;
  public static String [] colorMapping;
  public static String[] statusFields;
  public static String resourcesPath = "";
  public static String [] csNames = null;
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
      initDebug();
      initGUI();
      splash.stopSplash();
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
      debugLevel = getClassMgr().getConfigFile().getValue("GridPilot", "debug");
      resourcesPath =  getClassMgr().getConfigFile().getValue("GridPilot", "resources");
      if(resourcesPath==null){
        getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage("GridPilot", "resources"));
        resourcesPath = "./";
      }
      else{
        if (!resourcesPath.endsWith("/"))
          resourcesPath = resourcesPath + "/";
      }
      splash = new Splash(resourcesPath, "splash.png");
      String tmpDb = null;
      dbs = getClassMgr().getConfigFile().getValues("Databases", "Systems");
      for(int i=0; i<dbs.length; ++i){
        try{
          splash.show("Connecting to "+dbs[i]+"...");
        }
        catch(Exception e){
          // if we cannot show text on splash, just silently ignore
        }
        try{
          tmpDb = getClassMgr().getDBPluginMgr(dbs[i]).getDBName();
        }
        catch(NullPointerException e){
        }
        if(tmpDb==null){
          Debug.debug("Initializing db "+i+": "+dbs[i],3);
          getClassMgr().setDBPluginMgr(dbs[i], new DBPluginMgr(dbs[i]));
        }
      }          
      colorMapping = getClassMgr().getConfigFile().getValues("GridPilot", "color mapping");  
      /** Status table header*/
      statusFields = new String [] {
          " ", "Job Name", "Job ID", "Job status", "CS", "Host", "DB", "DB status", "user"};

      csNames = getClassMgr().getConfigFile().getValues("Computing systems", "systems");
      if(csNames==null || csNames.length==0){
        getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage("Computing systems", "systems"));
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
      globusTcpPortRange = getClassMgr().getConfigFile().getValue("Data management",
      "globus tcp port range");
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
  * Return databases specified in the configuration file
  */
  public static String [] getDBs(){
    if(dbs == null || dbs[0] == null){
      Debug.debug("dbs null", 3);
    }
    //Debug.debug("dbs: "+Util.arrayToString(dbs), 3);
    return dbs;
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

      frame.setSize(new Dimension(800, 600));

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

  public static void exit(int exitCode){
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
    /*
    Disconnect DBs and CSs
    */
    Debug.debug("Disconnecting computing systems...", 2);
    if(getClassMgr().csPluginMgr!=null){
      getClassMgr().getCSPluginMgr().disconnect();
      getClassMgr().getCSPluginMgr().exit();
    }
    for(int i=0; i<dbs.length; ++i){
      getClassMgr().getDBPluginMgr(dbs[i]).disconnect();
      Debug.debug("Disconnecting "+dbs[i], 2);
    }
    Debug.debug("All systems disconnected.", 2);
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
  
  public static String [] userPwd(String _user, String _passwd, String _database){
    // asking for user and password for DBPluginMgr
    
    JPanel pUserPwd = new JPanel(new GridBagLayout());
    JTextField tfUser = new JTextField(_user);
    JPasswordField pfPwd = new JPasswordField(_passwd);
    JTextField tfDatabase = new JTextField(_database);
    
    pUserPwd.add(new JLabel("User : "), new GridBagConstraints(0,0, 1, 1, 0.0, 0.0,
        GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));

    pUserPwd.add(tfUser, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0,
        GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));

    pUserPwd.add(new JLabel("Password : "), new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0,
        GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));

    pUserPwd.add(pfPwd, new GridBagConstraints(1, 2, 1, 1, 1.0, 0.0,
        GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));

    pUserPwd.add(new JLabel("Database : "), new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0,
        GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));

    pUserPwd.add(tfDatabase, new GridBagConstraints(1, 3, 1, 1, 1.0, 0.0,
        GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));


    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(),pUserPwd, "DB login", JOptionPane.OK_CANCEL_OPTION);

    String [] results;
    if(choice == JOptionPane.OK_OPTION){
      results = new String [3];
      results[0] = tfUser.getText();
      results[1] = new String(pfPwd.getPassword());
      results[2] = tfDatabase.getText();
    }
    else{
      results = null;
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
      for(Iterator it=tmpConfFile.keySet().iterator(); it.hasNext(); ){
        ((File) tmpConfFile.get(it.next())).delete();
      }
    }
    catch(Exception e){
    }
    getClassMgr().getConfigFile().makeTmpConfigFile();
    loadConfigValues();
    getClassMgr().getJobValidation().loadValues();
    getClassMgr().getSubmissionControl().loadValues();
    getClassMgr().getGlobalFrame().jobMonitoringPanel.statusUpdateControl.loadValues();
    getClassMgr().getCSPluginMgr().loadValues();
    for(int i=0; i<dbs.length; ++i){
      getClassMgr().getDBPluginMgr(dbs[i]).loadValues();
    }
    initDebug();
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


  public static void dbReconnect(){
    GridPilot.getClassMgr().getStatusBar().setLabel(
        "Reconnecting "+dbs.length+" databases. Please wait...");
    GridPilot.getClassMgr().getStatusBar().animateProgressBar();
    /*
     Reconnect DBs
     */
    for(int i=0; i<dbs.length; ++i){
      try{
        Debug.debug("Disconnecting "+dbs[i], 2);
        GridPilot.getClassMgr().getStatusBar().setLabel(
            "Disconnecting "+dbs[i]);
        GridPilot.getClassMgr().getStatusBar().animateProgressBar();
        getClassMgr().getDBPluginMgr(dbs[i]).disconnect();
        Debug.debug("Connecting "+dbs[i], 2);
        GridPilot.getClassMgr().getStatusBar().setLabel(
            "Connecting "+dbs[i]);
        GridPilot.getClassMgr().getStatusBar().animateProgressBar();
        getClassMgr().getDBPluginMgr(dbs[i]).init();
        Debug.debug("Connection ok.", 2);
        GridPilot.getClassMgr().getStatusBar().setLabel(
            "Connection ok.");
        GridPilot.getClassMgr().getStatusBar().animateProgressBar();
        // TODO: reload panels?
      }
      catch (Throwable e){
        Debug.debug("ERROR: Could not load DB " + dbs[i] + ". " + 
            e.getMessage(), 3);
        GridPilot.getClassMgr().getStatusBar().setLabel("ERROR: Could not load DB " + dbs[i] + ". " + 
            e.getMessage());
        exit(-1);
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
    catch (Throwable e){
      Debug.debug("ERROR: Could not reload CSs. " + e.getMessage(), 3);
      GridPilot.getClassMgr().getStatusBar().setLabel("ERROR: Could not reload computing systems. " + 
          e.getMessage());
      exit(-1);
    }
    GridPilot.getClassMgr().getStatusBar().stopAnimation();
  }

}
