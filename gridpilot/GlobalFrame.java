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
import gridpilot.DBPluginMgr;
import gridpilot.TaskMgr;
import gridpilot.JobPanel;
import gridpilot.IconProxy;

/**
 * Main frame of GridPilot application.
 * This frame contains 1 fixed tabbed panel: JobDefinitionPanel.
 * More tabs can be added dynamically.
 *
 */

public class GlobalFrame extends JFrame {


  private JMenuBar menuBar = new JMenuBar();
  private JPanel contentPane;
  private JTabbedPane tabbedPane = new JTabbedPane();
  private AllTasksPanel allTasksPanel;
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
    // save global frame pointer in classMgr
    GridPilot.getClassMgr().setGlobalFrame(this);

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


    ImageIcon iconAtCom = new ImageIcon(resourcesPath + "gridpilot.gif");
    setIconImage(iconAtCom.getImage());

    allPanels = new Vector();
    
    initGUI();
  }

  /**
   * GUI initialisation
   */

  private void initGUI() throws Exception  {
    /**
     * Called by : this.GlobalFrame();
     */

    contentPane = (JPanel) this.getContentPane();
    contentPane.setLayout(new BorderLayout());

    setTitle("GridPilot welcomes you");

    //// Menu

    makeMenu();

    contentPane.add(tabbedPane,  BorderLayout.CENTER);

    allTasksPanel = new AllTasksPanel(this);

    tabbedPane.add(allTasksPanel, "Tasks");

    selectedPanel = tabbedPane.getSelectedIndex();

    /*
    Detect click over X in tab
    */
   tabbedPane.addMouseListener(new MouseAdapter() {
     public void mouseReleased(MouseEvent evt) {
       if (tabbedPane.getTabCount() == 1 || tabbedPane.getSelectedIndex() == 0) {
         return;
       }

       if (!evt.isPopupTrigger()) {
         IconProxy iconProxy = (IconProxy) tabbedPane.getIconAt(tabbedPane.getSelectedIndex());

         if (iconProxy.contains(evt.getX(), evt.getY())) {
           removeMonitoringPanel();
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
  Add a new monitoring panel.
  */
public void addMonitoringPanel(JobPanel newPanel) {
  //      Trim title name before adding new tab
  String title = newPanel.getTitle();
  String smallTitle = null;
  if (title.length() > 20) {
    smallTitle = title.substring(0,20) + "...";
  } else {
    smallTitle = title;
  }

  tabbedPane.addTab(smallTitle, new IconProxy(closeIcon), (JPanel) newPanel);
  allPanels.addElement(newPanel);
  // focus on new panel
  ((JobPanel) tabbedPane.getComponentAt(tabbedPane.getSelectedIndex())).panelHidden();
  int newSelIndex = tabbedPane.getTabCount()-1;
  ((JobPanel) tabbedPane.getComponentAt(newSelIndex)).panelShown();
  tabbedPane.setSelectedIndex(newSelIndex);
  setTitle("GridPilot - "+title);
}

 /*
  Remove monitoring panel.
  () version is called from mouselistener
  (panel) version is called from TaskPanel::close() called from menu therein
  */
public void removeMonitoringPanel() {
  JobPanel panel = (JobPanel)allPanels.elementAt(tabbedPane.getSelectedIndex()-1);
  removeMonitoringPanel(panel);
}

public void removeMonitoringPanel(JobPanel panel) {
  // remove from vector and from tab
  /***/Debug.debug2("entering removeMonitoringPanel");
  allPanels.removeElement(panel);
  if(panel.getClass() == TaskPanel.class){
    taskMgrs.remove(((TaskPanel) panel).taskMgr);
  }
  else if(panel.getClass() == TaskTransPanel.class){
    taskTransMgrs.remove(((TaskTransPanel) panel).taskTransMgr);
  }
  else{
    Debug.debug("WARNING: unkown class " + panel.getClass(), 1);
  }
  tabbedPane.removeTabAt(tabbedPane.getSelectedIndex());
  /***/Debug.debug2("leaving removeMonitoringPanel");
}

  public void addTaskPanel(DBPluginMgr dbPluginMgr, int selectedTask, String selectedName) {
  /*
   Check if selected task is already opened. If it is, don't let user create another one.
   Otherwise, create a TaskMgr for the selected task
   */
    /**/Debug.debug2("entering");
    boolean exists = false;
    for (int i=0; i<taskMgrs.size(); i++) {
      if (((TaskMgr)taskMgrs.elementAt(i)).getTaskIdentifier() == selectedTask) {
        exists = true;
        break;
      }
    }
    if (!exists) {
      TaskMgr newTask = new TaskMgr(dbPluginMgr, selectedTask, selectedName);
      try{
        /**/Debug.debug2("creating panel");
        TaskPanel panel = new TaskPanel(newTask,this) ;
        /**/Debug.debug2("adding panel");
        addMonitoringPanel(panel);
        taskMgrs.addElement(newTask);
        /**/Debug.debug2("added panel");
      } catch (Exception e) {
        Debug.debug2("Couldn't create monitoring panel for task " + selectedName + "\n" +
                           "\tException\t : " + e.getMessage());
        e.printStackTrace();
      }
    } else {
      JOptionPane.showMessageDialog(JOptionPane.getRootFrame(), "Task is already open! Cannot open another copy.", "Error", JOptionPane.INFORMATION_MESSAGE);
    }
  }

  public void addTaskTransPanel(DBPluginMgr dbPluginMgr, int taskTransID, String taskTransName) {
  /*
   Check if selected taskTrans is already opened. If it is, don't let user create another one.
   Otherwise, create a TaskTransMgr for the selected task
   */
    /**/Debug.debug2("entering");
    boolean exists = false;
    for (int i=0; i<taskTransMgrs.size(); i++) {
      if (((TaskTransMgr) taskTransMgrs.elementAt(i)).getTaskTransID() == taskTransID) {
        exists = true;
        break;
      }
    }
    if (!exists) {
      TaskTransMgr newTT = new TaskTransMgr(dbPluginMgr, taskTransID, taskTransName);
      try{
        /**/Debug.debug2("creating panel");
        TaskTransPanel taskTransPanel = new TaskTransPanel(newTT,this) ;
        /**/Debug.debug2("adding panel");
        addMonitoringPanel(taskTransPanel);
        taskTransMgrs.addElement(newTT);
        /**/Debug.debug2("added panel");
      } catch (Exception e) {
        Debug.debug2("Couldn't create monitoring panel for task " + taskTransName + "\n" +
                           "\tException\t : " + e.getMessage());
        e.printStackTrace();
      }
    } else {
      JOptionPane.showMessageDialog(JOptionPane.getRootFrame(), "taskTrans is already open! Cannot open another copy.", "Error", JOptionPane.INFORMATION_MESSAGE);
    }
  }

  //File | Exit action performed
  public void exit(){
    Debug.debug("Exiting ...", 2);
    //gridpilot.getClassMgr().getJobControl().exit();
    Debug.debug("Exit", 2);
    System.exit(0);

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
    ((JobPanel) tabbedPane.getComponentAt(selectedPanel)).panelHidden();
    selectedPanel = tabbedPane.getSelectedIndex();
    ((JobPanel) tabbedPane.getComponentAt(selectedPanel)).panelShown();
    if (selectedPanel > 0){
      /*
      Trim title name if it's too long
      */
      String title = ((JobPanel)allPanels.elementAt(selectedPanel-1)).getTitle();
      setTitle("gridpilot - "+title);
    } else {
      setTitle("gridpilot - an Atlas Commander");
    }
  }

  /**
   * Creates the menu in the main menu bar.
   */
  private void makeMenu(){

    // gridpilot

    JMenu menuAtCom = new JMenu("GridPilot");
    JMenuItem miExit = new JMenuItem("Exit");
    miExit.addActionListener(new ActionListener()  {
      public void actionPerformed(ActionEvent e) {
        exit();
      }
    });
    menuAtCom.add(miExit);
    //menuAtCom.addSeparator();
    //menuAtCom.addSeparator();


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

    menuBar.add(menuAtCom);
    menuBar.add(menuDB);
    menuBar.add(menuHelp);

    this.setJMenuBar(menuBar);

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
      Debug.debug2("debug list set to : "+debugList);
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
          System.exit(-1);
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
