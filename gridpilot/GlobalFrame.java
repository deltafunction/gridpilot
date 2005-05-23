package gridpilot;

import java.awt.*;
import java.awt.event.*;
import java.net.URL;

import javax.swing.*;
import javax.swing.event.*;

import java.util.*;

import gridpilot.ConfigFile;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.JobPanel;
import gridpilot.IconProxy;

/**
 * Main frame of GridPilot application.
 * This frame contains tab, more can be added dynamically.
 */

public class GlobalFrame extends JFrame {


  public JTabbedPane tabbedPane = new JTabbedPane();
  private Vector allPanels;
  private Vector taskMgrs = new Vector() ;
  private Vector taskTransMgrs = new Vector() ;
  private int selectedPanel;
  private ConfigFile configFile;
  private ImageIcon closeIcon = new ImageIcon("resources/close.png");
  
  private String dbNames;
  private String userName;
  private String passwd;
  private String step;


  /**
   * Constructor
   */

  public GlobalFrame() throws Exception{
    /**
     * Called by : gridpilot.gridpilot
     */

    enableEvents(AWTEvent.WINDOW_EVENT_MASK);

    configFile = GridPilot.getClassMgr().getConfigFile();

    initDebug();
    String resourcesPath = configFile.getValue("gridpilot", "resources");
    if(resourcesPath == null){
      GridPilot.getClassMgr().getLogFile().addMessage(configFile.getMissingMessage("gridpilot", "resources"));
      resourcesPath = ".";
    }
    else{
      if (!resourcesPath.endsWith("/"))
        resourcesPath = resourcesPath + "/";
    }

    allPanels = new Vector();
  }

  /**
   * GUI initialisation
   */

  public void initGUI(Container container) throws Exception  {
    /**
     * Called by : this.GlobalFrame();
     */

    container.setLayout(new BorderLayout());

    setTitle("GridPilot welcomes you");

    container.add(tabbedPane,  BorderLayout.CENTER);

    addPanel(new DBPanel("task", "TASKID"));
    selectedPanel = tabbedPane.getSelectedIndex();

    /*
    Detect click over X in tab
    */
   tabbedPane.addMouseListener(new MouseAdapter() {
     public void mouseReleased(MouseEvent evt) {
       if (tabbedPane.getTabCount() == 0 || tabbedPane.getSelectedIndex() < 0) {
         return;
       }

       if (!evt.isPopupTrigger()) {
         IconProxy iconProxy = (IconProxy) tabbedPane.getIconAt(tabbedPane.getSelectedIndex());

         if (iconProxy.contains(evt.getX(), evt.getY())) {
           removePanel();
         }
       }
     }
   });

    tabbedPane.addChangeListener(new ChangeListener(){
      public void stateChanged(ChangeEvent e){
        tabbedSelected(e);
      }
    });


    tabbedPane.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        switch(e.getKeyCode()){
          case KeyEvent.VK_F1 :
            menuHelpAbout_actionPerformed();
          default :
            tabbedPane.getSelectedComponent().dispatchEvent(e);
        }
      }
    });

  }

  /**
   * ActionEvent
   */


  /*
  Add a new panel.
  */

  public void addPanel(JobPanel newPanel, String title) {
    Debug.debug("Adding panel "+newPanel.getTitle(), 3);
    addPanel(newPanel);
  }

  public void addPanel(JobPanel newPanel) {
  
  // Trim title name before adding new tab
  String title = newPanel.getTitle();
  String smallTitle = null;
  if (title.length() > 20) {
    smallTitle = title.substring(0,20) + "...";
  } else {
    smallTitle = title;
  }
  Debug.debug("Adding tab "+allPanels.size(), 3);
  allPanels.addElement(newPanel);
  tabbedPane.addTab(smallTitle, new IconProxy(closeIcon), (JPanel) newPanel);
  Debug.debug("Added tab "+allPanels.size(), 3);
  // focus on new panel
  ((JobPanel) tabbedPane.getComponentAt(tabbedPane.getSelectedIndex())).panelHidden();
  int newSelIndex = tabbedPane.getTabCount()-1;
  ((JobPanel) tabbedPane.getComponentAt(newSelIndex)).panelShown();
  Debug.debug("Setting selected index "+newSelIndex, 3);
  tabbedPane.setSelectedIndex(newSelIndex);
  setTitle("GridPilot - "+title);
}

 /*
  Remove panel.
  */
  public void removePanel() {
    JobPanel panel = (JobPanel)allPanels.elementAt(tabbedPane.getSelectedIndex());
    removePanel(panel);
  }
  
  public void removePanel(JobPanel panel) {
    // remove from vector and from tab
    Debug.debug("Removing panel#"+tabbedPane.getSelectedIndex(), 3);
    try{
      allPanels.removeElement(panel);
    }catch(Exception e){
      Debug.debug("ERROR: could not remove panel.", 1);
      return;
    }
    tabbedPane.removeTabAt(tabbedPane.getSelectedIndex());
  }

  //File | Exit action performed
  public void exit(){
    Debug.debug("Exiting ...", 2);
    //gridpilot.getClassMgr().getJobControl().exit();
    Debug.debug("Exit", 2);
    GridPilot.exit(0);
  }
  //Help | About action performed
  public void menuHelpAbout_actionPerformed() {
    String path = configFile.getValue("gridpilot", "resources");
    URL aboutURL = null;
    try{
      //aboutURL = AtCom.class.getResource(AtCom.resources + "about.htm");
      aboutURL = GridPilot.class.getResource(path + "about.htm");
    }catch(Exception e){
      Debug.debug("Could not find file "+ path + "about.htm", 3);
      return;
    } 
    WebBox dlg = new WebBox(this, "About", aboutURL);
  }

  //Overridden so we can exit when window is closed
  protected void processWindowEvent(WindowEvent e) {
    super.processWindowEvent(e);
    if (e.getID() == WindowEvent.WINDOW_CLOSING) {
      exit();
    }
  }

  /**
   * Called when selected tab changes
   */
  private void tabbedSelected(ChangeEvent e){
    selectedPanel = tabbedPane.getSelectedIndex();
    if(selectedPanel>=0){
      ((JobPanel) tabbedPane.getComponentAt(selectedPanel)).panelHidden();
      ((JobPanel) tabbedPane.getComponentAt(selectedPanel)).panelShown();
      String title = ((JobPanel)allPanels.elementAt(selectedPanel)).getTitle();
      setTitle("GridPilot - "+title);
    }
  }

  /**
   * Creates the menu in the main menu bar.
   */
  public JMenuBar makeMenu(){

    JMenuBar menuBar = new JMenuBar();

    // gridpilot

    JMenu menuGridPilot = null;
    
    menuGridPilot = new JMenu("GridPilot");
    JMenuItem miNewTab = new JMenuItem("New tab");
    miNewTab.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e) {
        try{
          addPanel(new DBPanel("task", "TASKID"), "task");          
        }catch(Exception ex){
          Debug.debug("Could not add panel ", 1);
          ex.printStackTrace();
        }
        selectedPanel = tabbedPane.getSelectedIndex();
      }
    });
    menuGridPilot.add(miNewTab);

    if(!GridPilot.applet){
      menuGridPilot.addSeparator();
      JMenuItem miExit = new JMenuItem("Exit");
      miExit.addActionListener(new ActionListener()  {
        public void actionPerformed(ActionEvent e) {
          exit();
        }
      });
      menuGridPilot.add(miExit);
    }


    //Help
    JMenu menuHelp = new JMenu("Help");
    JMenuItem menuHelpAbout = new JMenuItem("About");
    menuHelpAbout.addActionListener(new ActionListener()  {
      public void actionPerformed(ActionEvent e) {
        menuHelpAbout_actionPerformed();
      }
    });

    menuHelp.add(menuHelpAbout);


    //DB

    JMenu menuDB = new JMenu("DB");
    JMenuItem miDbClearCaches = new JMenuItem("Clear DB caches");
    miDbClearCaches.addActionListener(new ActionListener()  {
      public void actionPerformed(ActionEvent e) {
        /*
         Clear caches in all of the DB connections
         */
        GridPilot.getClassMgr().clearDBCaches();
      }
    });

    JMenuItem miDbReconnect = new JMenuItem("Reconnect");
    miDbReconnect.addActionListener(new ActionListener()  {
      public void actionPerformed(ActionEvent e) {
        dbReconnect();
      }
    });


    menuDB.add(miDbClearCaches);
    menuDB.add(miDbReconnect);

    if(!GridPilot.applet){
      menuBar.add(menuGridPilot);
    }
    menuBar.add(menuDB);
    menuBar.add(menuHelp);
    
    return menuBar;

  }

  /**
   * Reloads values from configuration file.
   * Called when user chooses "Reload values" in gridpilot menu
   */
  private void reloadValues(){
   
    GridPilot.dbs = GridPilot.getClassMgr().getConfigFile().getValues("gridpilot", "Databases");
    GridPilot.steps.clear();
    for(int i = 0; i < GridPilot.dbs.length; ++i){
      userName = GridPilot.getClassMgr().getConfigFile().getValue("gridpilot", "Databases");
      passwd = GridPilot.getClassMgr().getConfigFile().getValue("gridpilot", "Databases");
      GridPilot.steps.put(GridPilot.dbs[i], GridPilot.getClassMgr().getConfigFile().getValues(GridPilot.dbs[i], "steps"));
      for(int j = 0; j < ((String []) GridPilot.steps.get(GridPilot.dbs[i])).length; ++j){
        step = ((String []) GridPilot.steps.get(GridPilot.dbs[i]))[j];
        GridPilot.getClassMgr().getDBPluginMgr(GridPilot.dbs[i], step).loadValues();
        // TODO: reload panels?
      }
    }
    initDebug();
  }

  /**
   * Reads in configuration file the debug level.
   */
  private void initDebug(){
  	if ((configFile == null ) || configFile.isFake()) return;
    String debugList = configFile.getValue("gridpilot", "debugList");
    if(debugList == null){
      GridPilot.getClassMgr().getLogFile().addMessage(configFile.getMissingMessage("gridpilot", "debugList"));
      Debug.toTrace="";
    } else {
      Debug.toTrace=debugList;
      Debug.debug("debug list set to : "+debugList, 2);
    }

    String debugLevel = configFile.getValue("gridpilot", "debug");
    if(debugLevel == null){
      GridPilot.getClassMgr().getLogFile().addMessage(configFile.getMissingMessage("gridpilot", "debug"));
      GridPilot.getClassMgr().setDebugLevel(3);
    }

    try{ GridPilot.getClassMgr().setDebugLevel(new Integer(debugLevel).intValue());}
    catch(NumberFormatException nfe){
      GridPilot.getClassMgr().getLogFile().addMessage("Debug is not an integer in configFile, section [gridpilot]");
      GridPilot.getClassMgr().setDebugLevel(3);
    }
  }


  //need to re-think this a bit

  private void dbReconnect(){
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
    GridPilot.dbs = GridPilot.getClassMgr().getConfigFile().getValues("gridpilot", "Databases");
    GridPilot.steps.clear();
    for(int i = 0; i < GridPilot.dbs.length; ++i){
      userName = GridPilot.getClassMgr().getConfigFile().getValue("gridpilot", "Databases");
      passwd = GridPilot.getClassMgr().getConfigFile().getValue("gridpilot", "Databases");
      GridPilot.steps.put(GridPilot.dbs[i], GridPilot.getClassMgr().getConfigFile().getValues(GridPilot.dbs[i], "steps"));
      for(int j = 0; j < ((String []) GridPilot.steps.get(GridPilot.dbs[i])).length; ++j){
        step = ((String []) GridPilot.steps.get(GridPilot.dbs[i]))[j];
        GridPilot.getClassMgr().getDBPluginMgr(GridPilot.dbs[i], step).disconnect();
        try {
          GridPilot.getClassMgr().getDBPluginMgr(GridPilot.dbs[i], step).init();
          // TODO: reload panels?
        } catch (Throwable e) {
          Debug.debug("Could not load step/project " + step + " " + e.getMessage(), 3);
          GridPilot.exit(-1);
        }
      }
    }

    /*
     Close small progress window
     */
    w.hide();
    w.dispose();
  }
}
