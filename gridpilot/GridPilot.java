package gridpilot;

import java.awt.*;
import java.util.HashMap;
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

  static ClassMgr classMgr = new ClassMgr();
  
  // TODO: reread on reloading values
  public static String [] dbs;
  public static HashMap steps = new HashMap();
  private static String step;
  private static String userName;
  private static String passwd;
  public static boolean applet = true;

  /**
   * Constructor
   */
  public GridPilot() {
    
    try{
      classMgr.setLogFile(new LogFile(logsFileName));
      classMgr.setConfigFile(new ConfigFile(confFileName));

      String resourcesPath =  getClassMgr().getConfigFile().getValue("gridpilot", "resources");
      if(resourcesPath == null){
        getClassMgr().getLogFile().addMessage(getClassMgr().getConfigFile().getMissingMessage("gridpilot", "resources"));
        resourcesPath = ".";
      }
      else{
        if (!resourcesPath.endsWith("/"))
          resourcesPath = resourcesPath + "/";
      }

     gridpilotCommon();

     initGUI();

     classMgr.getLogFile().addInfo("gridpilot loaded");

    }catch(Throwable e){
      if(e instanceof Error)
        getClassMgr().getLogFile().addMessage("Error during gridpilot loading", e);
      else{
        getClassMgr().getLogFile().addMessage("Exception during gridpilot loading", e);
        exit(-1);
      }
    }
  }

  public static void gridpilotCommon () {
     String user = null;
     String [] up = null;
  	 dbs = getClassMgr().getConfigFile().getValues("Databases", "Systems");
     for(int i = 0; i < dbs.length; ++i){
       GridPilot.userName = getClassMgr().getConfigFile().getValue(dbs[i], "user");
       passwd = getClassMgr().getConfigFile().getValue(dbs[i], "passwd");
       steps.put(dbs[i], getClassMgr().getConfigFile().getValues(dbs[i], "steps"));

       for(int j = 0; j < ((String []) steps.get(dbs[i])).length; ++j){
         step = ((String []) steps.get(dbs[i]))[j];
         Debug.debug("Initializing step "+step+". For db "+dbs[i],3);
         GridPilot.getClassMgr().setDBPluginMgr(dbs[i], step, new DBPluginMgr(dbs[i], step, userName, passwd));
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
  + Return databases specified in the configuration file
  */
 public static String [] getDBs(){
   if(dbs == null)
     Debug.debug("dbs == null", 3);
   return dbs;
 }

  /**
  + Return database steps specified in the configuration file
  */
 public static String [] getSteps(String dbName){
   if(steps == null) {
	Debug.debug("steps == null", 3);
	return new String[] { "test" };
	}
     
   Debug.debug("db -> "+dbName, 3);
   //   Debug.debug("step 1 -> "+((String []) steps.get(dbName))[0], 3);
   try {
   		String[] st = (String []) steps.get(dbName);
   } catch (Exception e) {
   		return new String[] { "test" };
   }
   return ((String []) steps.get(dbName));
 }

  /**
   * GUI
   */
  private void initGUI() throws Exception{

    if(applet){
      GridPilot.getClassMgr().setGlobalFrame(frame = new GlobalFrame());
      GridPilot.getClassMgr().getGlobalFrame().initGUI(this.getContentPane());
      setJMenuBar(
          GridPilot.getClassMgr().getGlobalFrame().makeMenu());
    }
    else{
      GridPilot.getClassMgr().setGlobalFrame(frame = new GlobalFrame());
      GridPilot.getClassMgr().getGlobalFrame().initGUI(((JFrame)  
          GridPilot.getClassMgr().getGlobalFrame()).getContentPane());
      frame.setJMenuBar(
          GridPilot.getClassMgr().getGlobalFrame().makeMenu());
      //frame = new GlobalFrame();
      //classMgr.getJobControl().setGlobalFrame(); // job control was created before GlobalFrame so now we must set GlobalFrame pointer in JobControl class

      //Validate frames that have preset sizes
      //Pack frames that have useful preferred size info, e.g. from their layout

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
        Debug.debug("NAME: "+GridPilot.getClassMgr().getGridPilot(), 2);
        GridPilot.getClassMgr().getGlobalFrame().dispose();
      }
      catch(Exception e){
        Debug.debug(e.getMessage(), 1);
      }
    }
  }
  
  public static String [] userPwd(String user, String passwd, String database){
    // asking for user and password for DBPluginMgr

    JPanel pUserPwd = new JPanel(new GridBagLayout());
    JTextField tfUser = new JTextField(user);
    JPasswordField pfPwd = new JPasswordField(passwd);
    JTextField tfDatabase = new JTextField(database);
    
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
    GridPilot.getClassMgr().setGridPilot(this);
    Debug.debug("NAME: "+GridPilot.getClassMgr().getGridPilot().getAppletInfo(), 2);
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
    GridPilot.getClassMgr().setGridPilot(new GridPilot());
    Debug.debug("NAME: "+GridPilot.getClassMgr().getGridPilot().getName(), 2);
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


}
