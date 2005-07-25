package gridpilot;

import java.awt.*;
import java.awt.event.*;
import java.net.URL;

import javax.swing.*;
import javax.swing.event.*;

import gridpilot.StatusBar;

import java.util.*;

import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.JobPanel;
import gridpilot.IconProxy;

/**
 * Main frame of GridPilot application.
 * This frame contains tab, more can be added dynamically.
 */

public class GlobalFrame extends JFrame{


  public JTabbedPane tabbedPane = new JTabbedPane();
  private Vector allPanels;
  private Vector taskMgrs = new Vector() ;
  private Vector taskTransMgrs = new Vector() ;
  private int selectedPanel;
  private StatusBar statusBar;
  private static int i;

  /**
   * Constructor
   */

  public GlobalFrame() throws Exception{
    /**
     * Called by : gridpilot.gridpilot
     */

    enableEvents(AWTEvent.WINDOW_EVENT_MASK);

    String resourcesPath = GridPilot.getClassMgr().getConfigFile().getValue("gridpilot", "resources");
    if(resourcesPath == null){
      GridPilot.getClassMgr().getLogFile().addMessage(GridPilot.getClassMgr().getConfigFile().getMissingMessage("gridpilot", "resources"));
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

  public void initGUI(Container container) throws Exception {
    /**
     * Called by : this.GlobalFrame();
     */
    
    container.setLayout(new BorderLayout());
    
    GridPilot.getClassMgr().setStatusBar(new StatusBar());
    statusBar = GridPilot.getClassMgr().getStatusBar();
    container.add(statusBar, BorderLayout.SOUTH);
    container.add(tabbedPane,  BorderLayout.CENTER);

    statusBar.setLabel("GridPilot welcomes you", 20);
    
    container.add(tabbedPane,  BorderLayout.CENTER);

    addPanel(new DBPanel(GridPilot.getDBs()[0], "task"));
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
    
    String resourcesPath = GridPilot.getClassMgr().getConfigFile().getValue("gridpilot", "resources");
    if(resourcesPath == null){
      GridPilot.getClassMgr().getLogFile().addMessage(GridPilot.getClassMgr().getConfigFile().getMissingMessage("gridpilot", "resources"));
      resourcesPath = ".";
    }
    else{
      if (!resourcesPath.endsWith("/"))
        resourcesPath = resourcesPath + "/";
    }

    URL imgURL=null;
    ImageIcon closeIcon = null;
    try{
      imgURL = GridPilot.class.getResource(resourcesPath + "close.png");
      closeIcon = new ImageIcon(imgURL);
    }catch(Exception e){
      Debug.debug("Could not find image "+ resourcesPath + "close.png", 3);
      closeIcon = new ImageIcon();
    }
  
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
    String path = GridPilot.getClassMgr().getConfigFile().getValue("gridpilot", "resources");
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
    
    JMenu menuGridPilot = new JMenu("GridPilot");
    JMenu menuNewTab = new JMenu("New tab");
    JMenu menuNewMonitor = new JMenu("New job monitor");
   
    JMenu miNewTaskTab = new JMenu("task");
    JMenuItem [] miNewTaskTabs = new JMenuItem[GridPilot.getDBs().length];
    menuNewTab.add(miNewTaskTab);
    for(i=0; i<GridPilot.getDBs().length; ++i){
      miNewTaskTabs[i] = new JMenuItem(GridPilot.getDBs()[i]);
      miNewTaskTabs[i].addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          try{
            addPanel(new DBPanel(
                ((JMenuItem)e.getSource()).getText(), "task"), "task");          
          }catch(Exception ex){
            Debug.debug("Could not add panel ", 1);
            ex.printStackTrace();
          }
          selectedPanel = tabbedPane.getSelectedIndex();
        }
      });
      miNewTaskTab.add(miNewTaskTabs[i]);
    }
    
    JMenu miNewJobDefTab = new JMenu("jobDefinition");
    JMenuItem [] miNewJobDefTabs = new JMenuItem[GridPilot.getDBs().length];
    menuNewTab.add(miNewJobDefTab);
    for(i=0; i<GridPilot.getDBs().length; ++i){
      miNewJobDefTabs[i] = new JMenuItem(GridPilot.getDBs()[i]);
      miNewJobDefTabs[i].addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          try{
            addPanel(new DBPanel(
                ((JMenuItem)e.getSource()).getText(), "job definition"), "job definition");          
          }catch(Exception ex){
            Debug.debug("Could not add panel ", 1);
            ex.printStackTrace();
          }
          selectedPanel = tabbedPane.getSelectedIndex();
        }
      });
      miNewJobDefTab.add(miNewJobDefTabs[i]);
    }
    
    JMenu miNewJobTransTab = new JMenu("jobTrans");
    JMenuItem [] miNewJobTransTabs = new JMenuItem[GridPilot.getDBs().length];
    menuNewTab.add(miNewJobTransTab);
    for(i=0; i<GridPilot.getDBs().length; ++i){
      miNewJobTransTabs[i] = new JMenuItem(GridPilot.getDBs()[i]);
      miNewJobTransTabs[i].addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          try{
            addPanel(new DBPanel(
                ((JMenuItem)e.getSource()).getText(), "transformation"), "transformation");          
          }catch(Exception ex){
            Debug.debug("Could not add panel ", 1);
            ex.printStackTrace();
          }
          selectedPanel = tabbedPane.getSelectedIndex();
        }
      });
      miNewJobTransTab.add(miNewJobTransTabs[i]);
    }

    menuGridPilot.add(menuNewTab);
    
    menuNewMonitor.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          addPanel(new JobMonitoringPanel(), "Job Monitor");          
        }catch(Exception ex){
          Debug.debug("Could not add panel ", 1);
          ex.printStackTrace();
        }
        selectedPanel = tabbedPane.getSelectedIndex();
      }
    });    
    
    menuGridPilot.add(menuNewMonitor);

    if(!GridPilot.isApplet()){
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
        GridPilot.dbReconnect();
      }
    });


    menuDB.add(miDbClearCaches);
    menuDB.add(miDbReconnect);

    //if(!GridPilot.applet){
      menuBar.add(menuGridPilot);
    //}
    menuBar.add(menuDB);
    menuBar.add(menuHelp);
    
    return menuBar;

  }

}
