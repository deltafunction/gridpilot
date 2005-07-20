package gridpilot;

import java.awt.*;
import java.util.StringTokenizer;

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
  
  // TODO: reread on reloading values
  private static String [] dbs;
  private static String [] colorMapping;
  private static boolean applet = true;
  private static String replicaPrefix = "";

  private static String dbNames;
  private static String userName;
  private static String passwd;

  /**
   * Constructor
   */
  public GridPilot() {
    
    try{
      classMgr.setLogFile(new LogFile(logsFileName));
      classMgr.setConfigFile(new ConfigFile(confFileName));
      initDebug();
      gridpilotCommon();
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

  public static void gridpilotCommon () {
    String user;
    String passwd;
    String database;

    String [] up = null;
  	 dbs = getClassMgr().getConfigFile().getValues("Databases", "Systems");
     for(int i = 0; i < dbs.length; ++i){
       user = getClassMgr().getConfigFile().getValue(dbs[i], "user");
       passwd = getClassMgr().getConfigFile().getValue(dbs[i], "passwd");
       Debug.debug("Initializing db "+i+": "+dbs[i],3);
       getClassMgr().setDBPluginMgr(dbs[i], new DBPluginMgr(dbs[i], user, passwd));
     }
          
     colorMapping = getClassMgr().getConfigFile().getValues("gridpilot", "color mapping");
     
     String resourcesPath =  getClassMgr().getConfigFile().getValue("gridpilot", "resources");
     if(resourcesPath == null){
       getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage("gridpilot", "resources"));
       resourcesPath = ".";
     }
     else{
       if (!resourcesPath.endsWith("/"))
         resourcesPath = resourcesPath + "/";
     }
     
     replicaPrefix = getClassMgr().getConfigFile().getValue("Replica", "prefix");
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
  + Return databases specified in the configuration file
  */
  public static String [] getDBs(){
    if(dbs == null)
      Debug.debug("dbs == null", 3);
    return dbs;
  }
 
 /**
 + Are we running as an applet?
 */
public static boolean isApplet(){
  return applet;
} 

   /**
   * Return color mapping for job definition table, specified in the configuration file
   */
  public static String [] getColorMapping(){
    return colorMapping;
  }

  /**
   * Return replica prefix specified in the configuration file
   */
  public static String getReplicaPrefix(){
    return replicaPrefix;
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
  
  public static String [] split(String s) {
    StringTokenizer tok = new StringTokenizer(s);
    int len = tok.countTokens();
    String [] res = new String[len];
    for (int i = 0 ; i < len ; i++) {
      res[i] = tok.nextToken();
    }
    return res ;
  }

  /**
   * Reloads values from configuration file.
   * Called when user chooses "Reload values" in gridpilot menu
   */
  private void reloadValues(){
   
    dbs = getClassMgr().getConfigFile().getValues("gridpilot", "Databases");
    for(int i = 0; i < dbs.length; ++i){
      userName = getClassMgr().getConfigFile().getValue("gridpilot", "Databases");
      passwd = getClassMgr().getConfigFile().getValue("gridpilot", "Databases");
      getClassMgr().getDBPluginMgr(dbs[i]).loadValues();
    }
    initDebug();
  }

  /**
   * Reads in configuration file the debug level.
   */
  private static void initDebug(){
    if ((getClassMgr().getConfigFile() == null ) || getClassMgr().getConfigFile().isFake()) return;
    String debugList = getClassMgr().getConfigFile().getValue("gridpilot", "debugList");
    if(debugList == null){
      getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage("gridpilot", "debugList"));
      Debug.toTrace="";
    } else {
      Debug.toTrace=debugList;
      Debug.debug("debug list set to : "+debugList, 2);
    }

    String debugLevel = getClassMgr().getConfigFile().getValue("gridpilot", "debug");
    if(debugLevel == null){
      getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage("gridpilot", "debug"));
      getClassMgr().setDebugLevel(3);
    }

    try{ getClassMgr().setDebugLevel(new Integer(debugLevel).intValue());}
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
    w.show();
    Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
    w.setLocation(screenSize.width/2 - w.getSize().width/2,
                  screenSize.height/2 - w.getSize().height/2);
    /*
     Reconnect DB
     */
    dbs = getClassMgr().getConfigFile().getValues("gridpilot", "Databases");
    for(int i = 0; i < dbs.length; ++i){
      userName = getClassMgr().getConfigFile().getValue("gridpilot", "Databases");
      passwd = getClassMgr().getConfigFile().getValue("gridpilot", "Databases");
      getClassMgr().getDBPluginMgr(dbs[i]).disconnect();
      try {
        getClassMgr().getDBPluginMgr(dbs[i]).init();
        // TODO: reload panels?
      } catch (Throwable e) {
        Debug.debug("Could not load db  " + e.getMessage(), 3);
        exit(-1);
      }
    }

    /*
     Close small progress window
     */
    w.hide();
    w.dispose();
  }

}
