package gridpilot;

import java.awt.*;
import java.awt.event.*;
import java.net.URL;

import javax.swing.*;
import javax.swing.event.*;

import java.util.*;

import gridpilot.StatusBar;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.ListPanel;
import gridpilot.IconProxy;

/**
 * Main frame of GridPilot application.
 * This frame contains tab, more can be added dynamically.
 */

public class GlobalFrame extends GPFrame{

  private static final long serialVersionUID = 1L;
  private Vector allPanels;
  private int selectedPanel;
  private static int i;
  private CreateEditDialog pDialog;
  
  public JTabbedPane tabbedPane = new JTabbedPane();
  public MonitoringPanel monitoringPanel;
  public JMenu menuEdit = new JMenu("Edit");
  public JMenuItem menuEditCopy = new JMenuItem("Copy (ctrl c)");
  public JMenuItem menuEditCut = new JMenuItem("Cut (ctrl x)");
  public JMenuItem menuEditPaste = new JMenuItem("Paste (ctrl v)");
  // keep track of whether or not we are cutting on the sub-panels
  public boolean cutting = false;
  public ListPanel cutPanel = null;

  
  /**
   * Constructor
   */
  public GlobalFrame() throws Exception{
    enableEvents(AWTEvent.WINDOW_EVENT_MASK);
    allPanels = new Vector();
  }

/**
   * GUI initialisation
   */

  public void initGUI(Container container) throws Exception{
    
    container.setLayout(new BorderLayout());
    container.setPreferredSize(new Dimension(800, 600));
        
    GridPilot.getClassMgr().setStatusBar(new StatusBar());
    statusBar = GridPilot.getClassMgr().getStatusBar();
    container.add(statusBar, BorderLayout.SOUTH);
    statusBar.setLabel("GridPilot welcomes you!", 20);
    
    monitoringPanel = new MonitoringPanel();
    
    container.add(tabbedPane, BorderLayout.CENTER);
    
    container.validate();

    if(GridPilot.dbNames.length>0){
      for(int i=0; i<GridPilot.tabs.length; ++i){
        try{
          addPanel(new DBPanel(GridPilot.dbNames[0], GridPilot.tabs[i]));
        }
        catch(Exception e){
          Debug.debug("ERROR: could not load database panel for "+
              GridPilot.dbNames[0] + " : " + GridPilot.tabs[i], 1);
          e.printStackTrace();
        }
      }
    }
    selectedPanel = tabbedPane.getSelectedIndex();

    /*
    Detect click over X in tab
    */
   tabbedPane.addMouseListener(new MouseAdapter(){
     public void mouseReleased(MouseEvent evt){
       if (tabbedPane.getTabCount()==0 || tabbedPane.getSelectedIndex()<0){
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
   
   // Keyboard shortcuts
   KeyboardFocusManager.getCurrentKeyboardFocusManager(
       ).addKeyEventDispatcher(
           new KeyEventDispatcher(){
      public boolean dispatchKeyEvent(KeyEvent e){
        if(!e.isControlDown()){
          return false;
        }
        if(e.getID()==KeyEvent.KEY_PRESSED){
          if(e.getKeyCode()==KeyEvent.VK_O){
            MyThread t = (new MyThread(){
              public void run(){
                try{
                  BrowserPanel bp = new BrowserPanel(GridPilot.getClassMgr().getGlobalFrame(),
                      "GridPilot File Browser", "", "", false, true, true, null);
                  bp.okSetEnabled(false);
                }
                catch(Exception ex){
                  Debug.debug("WARNING: could not create BrowserPanel.", 1);
                  ex.printStackTrace();
                }
              }
            });     
            SwingUtilities.invokeLater(t);
          }
        }
        else if(e.getKeyCode()==KeyEvent.VK_Q){
          try{
            GridPilot.exit(0);
          }
          catch(Exception ex){
            Debug.debug("WARNING: could not exit!!", 1);
            ex.printStackTrace();
          }
        }
        else if(e.getKeyCode()==KeyEvent.VK_M){
          try{
            toggleMonitoringPanel();
          }
          catch(Exception ex){
            Debug.debug("WARNING: could not open monitor panel.", 1);
            ex.printStackTrace();
          }
        }
        return false;
      }
    });

    tabbedPane.addChangeListener(new ChangeListener(){
      public void stateChanged(ChangeEvent e){
        tabbedSelected(e);
      }
    });

    menuEditCopy.setEnabled(false);
    menuEditCut.setEnabled(false);
    menuEditPaste.setEnabled(false);
    
  }

  /**
   * ActionEvents
   */

  /*
  Add a new panel.
  */
  public void addPanel(ListPanel newPanel, String title){
    Debug.debug("Adding panel "+newPanel.getTitle(), 3);
    addPanel(newPanel);
    this.setTitle("GridPilot - "+title);
    //this.pack();
    //this.setVisible(true);
  }

  public void addPanel(ListPanel newPanel){
    
    URL imgURL = null;
    Dimension size = this.getSize();
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
      smallTitle = title.substring(0, 20) + "...";
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
    setSize(size);
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
      BrowserPanel wb = new BrowserPanel(this, "About",
          aboutURL.toExternalForm(), "", false, false, false, null);
      wb.bCancel.setEnabled(false);
    }
    catch(Exception e){
      Debug.debug("WARNING: could not create BrowserPanel", 1);
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

  public JCheckBoxMenuItem cbMonitor = new JCheckBoxMenuItem("Show monitor (ctrl m)");
  /**
   * Creates the menu in the main menu bar.
   */
  public JMenuBar makeMenu(){

    JMenuBar menuBar = new JMenuBar();

    // gridpilot
    
    JMenu menuFile = new JMenu("File");
    JMenu menuView = new JMenu("View");
    
    JMenuItem miReloadValues = new JMenuItem("Reload values from config file");
    miReloadValues.addActionListener(new ActionListener()  {
      public void actionPerformed(ActionEvent e){
        GridPilot.reloadConfigValues();
      }
    });
    menuFile.add(miReloadValues);
   
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
    menuFile.add(menuDB);

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
    menuFile.add(menuCS);
    
    if(!GridPilot.isApplet()){
      menuFile.addSeparator();
      JMenuItem miExit = new JMenuItem("Quit (ctrl q)");
      miExit.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          GridPilot.exit(0);
        }
      });
      menuFile.add(miExit);
    }

    menuView.add(cbMonitor);
    
    menuView.addSeparator();

    JMenuItem miBrowser = new JMenuItem("New browser (ctrl o)");
    miBrowser.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        MyThread t = (new MyThread(){
          public void run(){
            try{
              BrowserPanel bp = new BrowserPanel(GridPilot.getClassMgr().getGlobalFrame(),
                  "GridPilot File Browser", "", "", false, true, true, null);
              bp.okSetEnabled(false);
            }
            catch(Exception ex){
              Debug.debug("WARNING: could not create BrowserPanel.", 1);
              ex.printStackTrace();
            }
          }
        });     
        SwingUtilities.invokeLater(t);
      }
    });
    menuView.add(miBrowser);
    
    menuView.addSeparator();
    
    JMenu miNewRuntimeEnvironmentTab = new JMenu("runtimeEnvironment");
    JMenuItem [] miNewRuntimeEnvironmentTabs = new JMenuItem[GridPilot.dbNames.length];
    menuView.add(miNewRuntimeEnvironmentTab);
    for(i=0; i<GridPilot.dbNames.length; ++i){
      // Check if there is a runtimeEnvironment table in this database
      try{
        if((GridPilot.getClassMgr().getDBPluginMgr(
            GridPilot.dbNames[i]).getFieldNames("runtimeEnvironment")==null)){
          continue;
        }
      }
      catch(Exception e){
        continue;
      }
      miNewRuntimeEnvironmentTabs[i] = new JMenuItem(GridPilot.dbNames[i]);
      miNewRuntimeEnvironmentTabs[i].addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          try{
            addPanel(new DBPanel(
                ((JMenuItem)e.getSource()).getText(), "runtimeEnvironment"), "runtime environments");          
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
    JMenuItem [] miNewTransformationTabTabs = new JMenuItem[GridPilot.dbNames.length];
    menuView.add(miNewTransformationTab);
    for(i=0; i<GridPilot.dbNames.length; ++i){
      // Check if there is a transformation table in this database
      try{
        if((GridPilot.getClassMgr().getDBPluginMgr(
            GridPilot.dbNames[i]).getFieldNames("transformation")==null)){
          continue;
        }
      }
      catch(Exception e){
        continue;
      }
      miNewTransformationTabTabs[i] = new JMenuItem(GridPilot.dbNames[i]);
      miNewTransformationTabTabs[i].addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          try{
            addPanel(new DBPanel(
                ((JMenuItem)e.getSource()).getText(), "transformation"), "transformations");          
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
    JMenuItem [] miNewTaskTabs = new JMenuItem[GridPilot.dbNames.length];
    menuView.add(miNewTaskTab);
    for(i=0; i<GridPilot.dbNames.length; ++i){
      miNewTaskTabs[i] = new JMenuItem(GridPilot.dbNames[i]);
      miNewTaskTabs[i].addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          try{
            addPanel(new DBPanel(
                ((JMenuItem)e.getSource()).getText(), "dataset"), "datasets");          
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
    JMenuItem [] miNewJobDefTabs = new JMenuItem[GridPilot.dbNames.length];
    menuView.add(miNewJobDefTab);
    
    for(i=0; i<GridPilot.dbNames.length; ++i){
      // Check if there is a jobDefinition table in this database
      try{
        if((GridPilot.getClassMgr().getDBPluginMgr(
            GridPilot.dbNames[i]).getFieldNames("jobDefinition")==null)){
          continue;
        }
      }
      catch(Exception e){
        continue;
      }
      miNewJobDefTabs[i] = new JMenuItem(GridPilot.dbNames[i]);
      miNewJobDefTabs[i].addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          try{
            addPanel(new DBPanel(
                ((JMenuItem)e.getSource()).getText(), "jobDefinition"), "job definitions");          
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
    
    cbMonitor.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        toggleMonitoringPanel();
      }
    });    
    
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
    
    menuBar.add(menuFile);
    menuBar.add(menuEdit);
    menuBar.add(menuView);
    menuBar.add(menuHelp);
    
    return menuBar;

  }

  public void toggleMonitoringPanel(){
    try{
      if(pDialog==null){
        Debug.debug("Creating new job monitoring dialog", 2);
        pDialog = new CreateEditDialog(monitoringPanel, false, false, false);
        pDialog.setTitle("Job Monitor");
        pDialog.pack();
        pDialog.setVisible(true);
        return;
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

  public void showMonitoringPanel(){
    try{
      if(pDialog==null){
        Debug.debug("Creating new job monitoring dialog", 2);
        pDialog = new CreateEditDialog(monitoringPanel, false, false, false);
        pDialog.setTitle("Job Monitor");
        pDialog.pack();
      }
      pDialog.setVisible(true);
      cbMonitor.setSelected(true);
    }
    catch(Exception ex){
      Debug.debug("Could not create panel ", 1);
      ex.printStackTrace();
    }
  }

}
