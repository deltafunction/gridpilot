package gridpilot;

import java.awt.*;

import javax.swing.*;

/**
 * Main class.
 * Instantiates all global objects and calls GlobalFrame.
 */

public class GridPilot extends JApplet{
  private boolean packFrame = false;
  private GlobalFrame frame;

  private static String logsFileName = "gridpilot.logs";
  private static String confFileName = "gridpilot.conf";

  private static ClassMgr classMgr = new ClassMgr();
  
  private static String [] dbs;
  private static boolean applet = true;
  private static String dbNames;
  
  public static String [] colorMapping;
  public static String[] statusFields;
  public static String resourcesPath = "";
  public static String prefix = "";
  public static String url = "";
  public static String [] csNames = null;
  public static int sshChannels = 0;

  /**
   * Constructor
   */
  public GridPilot(){
    
    try{
      classMgr.setLogFile(new LogFile(logsFileName));
      classMgr.setConfigFile(new ConfigFile(confFileName));
      initDebug();
      loadConfigValues();
      initGUI();
      classMgr.getLogFile().addInfo("gridpilot loaded");
    }
    catch(Throwable e){
      if(e instanceof Error)
        getClassMgr().getLogFile().addMessage("Error during gridpilot loading", e);
      else{
        getClassMgr().getLogFile().addMessage("Exception during gridpilot loading", e);
        exit(-1);
      }
    }
  }

  public static void loadConfigValues(){
    try{
      String database;
      String [] up = null;
      String tmpDb = null;
      dbs = getClassMgr().getConfigFile().getValues("Databases", "Systems");
      for(int i = 0; i < dbs.length; ++i){
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

      resourcesPath =  getClassMgr().getConfigFile().getValue("GridPilot", "resources");
      if(resourcesPath == null){
        getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage("GridPilot", "resources"));
        resourcesPath = ".";
      }
      else{
        if (!resourcesPath.endsWith("/"))
          resourcesPath = resourcesPath + "/";
      }    
      prefix = getClassMgr().getConfigFile().getValue("GridPilot","prefix");
      url = getClassMgr().getConfigFile().getValue("GridPilot","url");
      csNames = getClassMgr().getConfigFile().getValues("Computing systems", "systems");
      if(csNames == null || csNames.length == 0){
        getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage("Computing systems", "systems"));
      }
      String channelsString = getClassMgr().getConfigFile().getValue("GridPilot", "ssh channels");
      if(channelsString == null){
        Debug.debug("ssh channels not found in config file", 1);
        sshChannels = 4;
      }
      else{
        sshChannels = Integer.parseInt(channelsString);
      }
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
    if(classMgr == null)
      Debug.debug("classMgr == null", 3);

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
      if(packFrame)
        frame.pack();
      else
        frame.validate();

      frame.setSize(new Dimension(800, 600));

      //Center the window
      Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
      Dimension frameSize = frame.getSize();
      if (frameSize.height > screenSize.height) {
        frameSize.height = screenSize.height;
      }
      if (frameSize.width > screenSize.width) {
        frameSize.width = screenSize.width;
      }
      requestFocusInWindow();
      frame.setLocation((screenSize.width - frameSize.width) / 2, (screenSize.height - frameSize.height) / 2);
      frame.setVisible(true);
    }
          
   }

  public static void exit(int exitCode){
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
    if(args != null){
      for(int i=0; i<args.length; ++i){
        if(args[i] != null && (args[i].equals("-c") || args[i].equals("-conf"))){
          if(i+1 >= args.length){
            badUsage("Configuration file missing after " + args[i]);
            break;
          }else{
            confFileName = args[i+1];
            ++i;
          }
        }
        else{
          if(args[i] != null && (args[i].equals("-l") || args[i].equals("-log"))){
            if(i+1 >= args.length){
              badUsage("log file missing after " + args[i]);
              break;
            }else{
              logsFileName = args[i+1];
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
  public static void reloadValues(){
    loadConfigValues();
    GridPilot.getClassMgr().getJobValidation().loadValues();
    GridPilot.getClassMgr().getSubmissionControl().loadValues();
    GridPilot.getClassMgr().getGlobalFrame().jobMonitoringPanel.statusUpdateControl.loadValues();
    GridPilot.getClassMgr().getCSPluginMgr().loadValues();
    dbs = getClassMgr().getConfigFile().getValues("GridPilot", "Databases");
    for(int i = 0; i < dbs.length; ++i){
      getClassMgr().getDBPluginMgr(dbs[i]).loadValues();
    }
    initDebug();
  }

  /**
   * Reads in configuration file the debug level.
   */
  private static void initDebug(){
    
    String debugLevel = getClassMgr().getConfigFile().getValue("GridPilot", "debug");
    if(debugLevel == null){
      getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage("GridPilot", "debug"));
      getClassMgr().setDebugLevel(3);
    }

    try{
      getClassMgr().setDebugLevel(new Integer(debugLevel).intValue());
    }
    catch(NumberFormatException nfe){
      getClassMgr().getLogFile().addMessage("Debug is not an integer in configFile, section [gridpilot]");
      getClassMgr().setDebugLevel(3);
    }
    
  }


  // TODO: need to re-think this a bit

  public static void dbReconnect(){
    /*
     Show small window with label
     */
    JWindow w = new JWindow(JOptionPane.getRootFrame());
    JLabel message = new JLabel("Reconnecting... please wait...");
    JPanel panel = new JPanel(new FlowLayout());
    panel.add(message);
    panel.updateUI();
    w.getContentPane().add(panel);
    w.pack();
    w.setVisible(true);
    Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
    w.setLocation(screenSize.width/2 - w.getSize().width/2,
                  screenSize.height/2 - w.getSize().height/2);
    /*
     Reconnect DB
     */
    dbs = getClassMgr().getConfigFile().getValues("GridPilot", "Databases");
    for(int i = 0; i < dbs.length; ++i){
      getClassMgr().getDBPluginMgr(dbs[i]).disconnect();
      try{
        getClassMgr().getDBPluginMgr(dbs[i]).init();
        // TODO: reload panels?
      }
      catch (Throwable e){
        Debug.debug("Could not load db  " + e.getMessage(), 3);
        exit(-1);
      }
    }

    /*
     Close small progress window
     */
    w.setVisible(false);
    w.dispose();
  }

}
