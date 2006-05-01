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
import gridpilot.ListPanel;
import gridpilot.IconProxy;

/**
 * Main frame of GridPilot application.
 * This frame contains tab, more can be added dynamically.
 */

public class GlobalFrame extends JFrame{

  private static final long serialVersionUID = 1L;
  private Vector allPanels;
  private int selectedPanel;
  private StatusBar statusBar;
  private static int i;
  private CreateEditDialog pDialog;
  
  public JTabbedPane tabbedPane = new JTabbedPane();
  public JobMonitoringPanel jobMonitoringPanel;
  public JMenu menuEdit = new JMenu("Edit");
  public JMenuItem menuEditCopy = new JMenuItem("Copy (ctrl c)");
  public JMenuItem menuEditCut = new JMenuItem("Cut (ctrl x)");
  public JMenuItem menuEditPaste = new JMenuItem("Paste (ctrl v)");

  
  /**
   * Constructor
   */
  public GlobalFrame() throws Exception{
    enableEvents(AWTEvent.WINDOW_EVENT_MASK);
    allPanels = new Vector();
    ImageIcon icon = null;
    URL imgURL = null;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "Aviateur.png");
      icon = new ImageIcon(imgURL);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.resourcesPath + "Aviateur.png", 3);
      icon = new ImageIcon();
    }
    setIconImage(icon.getImage());
  }

  /**
   * GUI initialisation
   */

  public void initGUI(Container container) throws Exception{
    /**
     * Called by : this.GlobalFrame();
     */
    
    container.setLayout(new BorderLayout());
    
    GridPilot.getClassMgr().setStatusBar(new StatusBar());
    statusBar = GridPilot.getClassMgr().getStatusBar();
    container.add(statusBar, BorderLayout.SOUTH);
    container.add(tabbedPane,  BorderLayout.CENTER);

    statusBar.setLabel("GridPilot welcomes you!", 20);
    
    container.add(tabbedPane,  BorderLayout.CENTER);

    if(GridPilot.getDBs().length>0){
      try{
        addPanel(new DBPanel(GridPilot.getDBs()[0], "runtimeEnvironment"));
        addPanel(new DBPanel(GridPilot.getDBs()[0], "transformation"));
        addPanel(new DBPanel(GridPilot.getDBs()[0], "dataset"));
        addPanel(new DBPanel(GridPilot.getDBs()[0], "jobDefinition"));
      }
      catch(Exception e){
        Debug.debug("ERROR: could not load database panel for "+
            GridPilot.getDBs()[0], 1);
        e.printStackTrace();
      }
    }
    selectedPanel = tabbedPane.getSelectedIndex();

    /*
    Detect click over X in tab
    */
   tabbedPane.addMouseListener(new MouseAdapter(){
     public void mouseReleased(MouseEvent evt){
       if (tabbedPane.getTabCount() == 0 || tabbedPane.getSelectedIndex() < 0){
         return;
       }

       if (!evt.isPopupTrigger()){
         IconProxy iconProxy = (IconProxy) tabbedPane.getIconAt(tabbedPane.getSelectedIndex());

         if (iconProxy.contains(evt.getX(), evt.getY())){
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

    jobMonitoringPanel = new JobMonitoringPanel();

    menuEditCopy.setEnabled(false);
    menuEditCut.setEnabled(false);
    menuEditPaste.setEnabled(false);

  }

  /**
   * ActionEvent
   */


  /*
  Add a new panel.
  */

  public void addPanel(ListPanel newPanel, String title){
    Debug.debug("Adding panel "+newPanel.getTitle(), 3);
    addPanel(newPanel);
  }

  public void addPanel(ListPanel newPanel){
    
    URL imgURL=null;
    ImageIcon closeIcon = null;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "close.png");
      closeIcon = new ImageIcon(imgURL);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.resourcesPath + "close.png", 3);
      closeIcon = new ImageIcon();
    }
  
    // Trim title name before adding new tab
    String title = newPanel.getTitle();
    String smallTitle = null;
    if(title.length()>20){
      smallTitle = title.substring(0,20) + "...";
    }
    else{
      smallTitle = title;
    }
    Debug.debug("Adding tab "+allPanels.size(), 3);
    allPanels.addElement(newPanel);
    tabbedPane.addTab(smallTitle, new IconProxy(closeIcon), (JPanel) newPanel);
    Debug.debug("Added tab "+allPanels.size(), 3);
    // focus on new panel
    ((ListPanel) tabbedPane.getComponentAt(tabbedPane.getSelectedIndex())).panelHidden();
    int newSelIndex = tabbedPane.getTabCount()-1;
    ((ListPanel) tabbedPane.getComponentAt(newSelIndex)).panelShown();
    Debug.debug("Setting selected index "+newSelIndex, 3);
    tabbedPane.setSelectedIndex(newSelIndex);
    setTitle("GridPilot - "+title);
  }

 /*
  Remove panel.
  */
  public void removePanel(){
    ListPanel panel = (ListPanel)allPanels.elementAt(tabbedPane.getSelectedIndex());
    removePanel(panel);
  }
  
  public void removePanel(ListPanel panel){
    // remove from vector and from tab
    Debug.debug("Removing panel#"+tabbedPane.getSelectedIndex(), 3);
    try{
      allPanels.removeElement(panel);
    }
    catch(Exception e){
      Debug.debug("ERROR: could not remove panel.", 1);
      return;
    }
    tabbedPane.removeTabAt(tabbedPane.getSelectedIndex());
  }

  //Edit-cut | About action performed
  public void menuEditCut_actionPerformed(){
    Debug.debug("Cutting", 3);
    ListPanel panel = (ListPanel)allPanels.elementAt(tabbedPane.getSelectedIndex());
    panel.cut();
  }
  //Edit-copy | About action performed
  public void menuEditCopy_actionPerformed(){
    Debug.debug("Copying", 3);
    ListPanel panel = (ListPanel)allPanels.elementAt(tabbedPane.getSelectedIndex());
    panel.copy();
  }
  //Edit-paste | About action performed
  public void menuEditPaste_actionPerformed(){
    Debug.debug("Pasting", 3);
    ListPanel panel = (ListPanel)allPanels.elementAt(tabbedPane.getSelectedIndex());
    panel.paste();
  }

  //Help | About action performed
  public void menuHelpAbout_actionPerformed(){
    URL aboutURL = null;
    try{
      aboutURL = GridPilot.class.getResource(GridPilot.resourcesPath + "about.htm");
    }
    catch(Exception e){
      Debug.debug("Could not find file "+ GridPilot.resourcesPath + "about.htm", 3);
      return;
    } 
    try{
      new WebBox(this, "About", aboutURL.toExternalForm(), "", false);
    }
    catch(Exception e){
      Debug.debug("WARNING: could not create WebBox", 1);
      e.printStackTrace();
    }
  }

  //Overridden so we can exit when window is closed
  protected void processWindowEvent(WindowEvent e){
    super.processWindowEvent(e);
    if (e.getID() == WindowEvent.WINDOW_CLOSING){
      GridPilot.exit(0);
    }
  }

  /**
   * Called when selected tab changes
   */
  private void tabbedSelected(ChangeEvent e){
    selectedPanel = tabbedPane.getSelectedIndex();
    if(selectedPanel>=0){
      ((ListPanel) tabbedPane.getComponentAt(selectedPanel)).panelHidden();
      ((ListPanel) tabbedPane.getComponentAt(selectedPanel)).panelShown();
      String title = ((ListPanel)allPanels.elementAt(selectedPanel)).getTitle();
      setTitle("GridPilot - "+title);
    }
  }

  public JCheckBoxMenuItem cbMonitor = new JCheckBoxMenuItem("Show job monitor");
  /**
   * Creates the menu in the main menu bar.
   */
  public JMenuBar makeMenu(){

    JMenuBar menuBar = new JMenuBar();

    // gridpilot
    
    JMenu menuGridPilot = new JMenu("File");
    JMenu menuNewTab = new JMenu("New tab");
    
    JMenuItem miReloadValues = new JMenuItem("Reload values from config file");
    miReloadValues.addActionListener(new ActionListener()  {
      public void actionPerformed(ActionEvent e){
        GridPilot.reloadValues();
      }
    });
    menuGridPilot.add(miReloadValues);
   
    //DB
    JMenu menuDB = new JMenu("Databases");
    JMenuItem miDbClearCaches = new JMenuItem("Clear DB caches");
    miDbClearCaches.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        /*
         Clear caches in all of the DB connections
         */
        GridPilot.getClassMgr().clearDBCaches();
      }
    });

    JMenuItem miDbReconnect = new JMenuItem("Reconnect");
    miDbReconnect.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        Debug.debug("Reconnecting DBs", 2);
        GridPilot.dbReconnect();
      }
    });

    menuDB.add(miDbClearCaches);
    menuDB.add(miDbReconnect);
    menuGridPilot.add(menuDB);

    //CS
    JMenu menuCS = new JMenu("Computing systems");
    JMenuItem miCsReconnect = new JMenuItem("Reconnect");
    miCsReconnect.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        Debug.debug("Reconnecting CSs", 2);
        GridPilot.csReconnect();
      }
    });

    menuCS.add(miCsReconnect);
    menuGridPilot.add(menuCS);

    JMenu miNewRuntimeEnvironmentTab = new JMenu("runtimeEnvironment");
    JMenuItem [] miNewRuntimeEnvironmentTabs = new JMenuItem[GridPilot.getDBs().length];
    menuNewTab.add(miNewRuntimeEnvironmentTab);
    for(i=0; i<GridPilot.getDBs().length; ++i){
      miNewRuntimeEnvironmentTabs[i] = new JMenuItem(GridPilot.getDBs()[i]);
      miNewRuntimeEnvironmentTabs[i].addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          try{
            addPanel(new DBPanel(
                ((JMenuItem)e.getSource()).getText(), "runtimeEnvironment"), "transformation");          
          }
          catch(Exception ex){
            Debug.debug("Could not add panel ", 1);
            ex.printStackTrace();
          }
          selectedPanel = tabbedPane.getSelectedIndex();
        }
      });
      miNewRuntimeEnvironmentTab.add(miNewRuntimeEnvironmentTabs[i]);
    }

    JMenu miNewTransformationTab = new JMenu("transformation");
    JMenuItem [] miNewTransformationTabTabs = new JMenuItem[GridPilot.getDBs().length];
    menuNewTab.add(miNewTransformationTab);
    for(i=0; i<GridPilot.getDBs().length; ++i){
      miNewTransformationTabTabs[i] = new JMenuItem(GridPilot.getDBs()[i]);
      miNewTransformationTabTabs[i].addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          try{
            addPanel(new DBPanel(
                ((JMenuItem)e.getSource()).getText(), "transformation"), "transformation");          
          }
          catch(Exception ex){
            Debug.debug("Could not add panel ", 1);
            ex.printStackTrace();
          }
          selectedPanel = tabbedPane.getSelectedIndex();
        }
      });
      miNewTransformationTab.add(miNewTransformationTabTabs[i]);
    }

    JMenu miNewTaskTab = new JMenu("dataset");
    JMenuItem [] miNewTaskTabs = new JMenuItem[GridPilot.getDBs().length];
    menuNewTab.add(miNewTaskTab);
    for(i=0; i<GridPilot.getDBs().length; ++i){
      miNewTaskTabs[i] = new JMenuItem(GridPilot.getDBs()[i]);
      miNewTaskTabs[i].addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          try{
            addPanel(new DBPanel(
                ((JMenuItem)e.getSource()).getText(), "dataset"), "dataset");          
          }
          catch(Exception ex){
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
                ((JMenuItem)e.getSource()).getText(), "jobDefinition"), "job definition");          
          }
          catch(Exception ex){
            Debug.debug("Could not add panel ", 1);
            ex.printStackTrace();
          }
          selectedPanel = tabbedPane.getSelectedIndex();
        }
      });
      miNewJobDefTab.add(miNewJobDefTabs[i]);
    }
    
    menuGridPilot.add(menuNewTab);
    
    cbMonitor.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          if(pDialog==null){
            pDialog = new CreateEditDialog(jobMonitoringPanel, false);
             pDialog.setTitle("Job Monitoring");
             pDialog.remove(pDialog.buttonPanel);
          }
          if(pDialog.isShowing()){
            pDialog.setVisible(false);
          }
          else{
            pDialog.setVisible(true);
          }
        }
        catch(Exception ex){
          Debug.debug("Could not create panel ", 1);
          ex.printStackTrace();
        }
      }
    });    
    
    menuGridPilot.add(cbMonitor);

    if(!GridPilot.isApplet()){
      menuGridPilot.addSeparator();
      JMenuItem miExit = new JMenuItem("Exit");
      miExit.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          GridPilot.exit(0);
        }
      });
      menuGridPilot.add(miExit);
    }

    menuEditCopy.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        menuEditCopy_actionPerformed();
      }
    });
    menuEdit.add(menuEditCopy);
    menuEditCut.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        menuEditCut_actionPerformed();
      }
    });
    menuEdit.add(menuEditCut);
    menuEditPaste.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        menuEditPaste_actionPerformed();
      }
    });
    menuEdit.add(menuEditPaste);

    //Help
    JMenu menuHelp = new JMenu("Help");
    JMenuItem menuHelpAbout = new JMenuItem("About");
    menuHelpAbout.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        menuHelpAbout_actionPerformed();
      }
    });

    menuHelp.add(menuHelpAbout);
    
    menuBar.add(menuGridPilot);
    menuBar.add(menuEdit);
    menuBar.add(menuHelp);
    
    return menuBar;

  }

}
