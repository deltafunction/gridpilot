package gridpilot;

import java.awt.*;
import java.awt.event.*;
import java.io.IOException;
import java.net.URL;

import javax.swing.*;
import javax.swing.event.*;

import java.util.*;

import gridfactory.common.ConfigFile;
import gridfactory.common.ConfigNode;
import gridfactory.common.ConfirmBox;
import gridfactory.common.Debug;
import gridfactory.common.LogFile;
import gridfactory.common.ResThread;

import gridpilot.GridPilot;
import gridpilot.ListPanel;
import gridpilot.IconProxy;
import gridpilot.wizards.beginning.BeginningWizard;
import gridpilot.wizards.manage_software.CreateSoftwarePackageWizard;
import gridpilot.wizards.run_jobs.RunCommandWizard;

/**
 * Main frame of GridPilot application.
 * This frame contains tab, more can be added dynamically.
 */
public class GlobalFrame extends GPFrame{

  private static final long serialVersionUID = 1L;
  
  private Vector allPanels;
  private int selectedPanel;
  private CreateEditDialog pDialog;
  private MyPreferencesPanel prefsPanel = null;
  private static int i;

  private JTabbedPane tabbedPane = new DnDTabbedPane();
  private MonitoringPanel monitoringPanel;
  private JMenu menuEdit = new JMenu("Edit");
  private JMenuItem menuEditCopy = new JMenuItem("Copy (ctrl c)");
  private JMenuItem menuEditCut = new JMenuItem("Cut (ctrl x)");
  private JMenuItem menuEditPaste = new JMenuItem("Paste (ctrl v)");
  private JMenuItem menuEditPrefs = new JMenuItem("Preferences");
  private ListPanel cutPanel = null;
  private JCheckBoxMenuItem cbMonitor = new JCheckBoxMenuItem("Show monitor (ctrl m)");
  private JMenuItem miDbEditRecord = new JMenuItem("Edit record");
  private JMenuItem miDbDeleteRecords = new JMenuItem("Delete record(s)");
  private JMenu mDbDefineRecords = new JMenu("Define new record(s)");
  private JMenuItem miDbDefineRecords = new JMenuItem("Define new record(s)");
  private JMenuItem miWithoutInput = new JMenuItem("from scratch");
  private JMenuItem miWithInputDataset = new JMenuItem("with selected input dataset(s)");
  // keep track of whether or not we are cutting on the sub-panels
  public boolean cutting = false;

  
  /**
   * Constructor
   */
  public GlobalFrame() throws Exception{
    enableEvents(AWTEvent.WINDOW_EVENT_MASK);
    allPanels = new Vector();
  }
  
  protected void initMonitoringPanel() throws Exception{
    SwingUtilities.invokeAndWait(
      new Runnable(){
        public void run(){
          try{
            monitoringPanel = new MonitoringPanel();
            Debug.debug("Creating new monitoring dialog", 2);
            pDialog = new CreateEditDialog(monitoringPanel,
                false, false, false, false, false);
            pDialog.setVisible(false);
            pDialog.setTitle("Monitor");
            pDialog.pack();
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
    );
    pDialog.activate();
  }

  /**
   * GUI initialisation
   */
  public void initGUI(Container container) throws Exception{

    container.setLayout(new BorderLayout());
    //container.setPreferredSize(new Dimension(800, 600));
    GridPilot.getClassMgr().setNewStatusBar();
    statusBar = GridPilot.getClassMgr().getStatusBar();
    container.add(statusBar, BorderLayout.SOUTH);
    statusBar.setLabel("GridPilot welcomes you!");
    container.add(tabbedPane, BorderLayout.CENTER);
    container.validate();
    selectedPanel = tabbedPane.getSelectedIndex();

   /**
    * Detect click over X in tab
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
            ResThread t = (new ResThread(){
              public void run(){
                try{
                  BrowserPanel bp = new BrowserPanel(GridPilot.getClassMgr().getGlobalFrame(),
                      "GridPilot File Browser", "", "", false, true, true, null, null, false);
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

  protected void addDBPanels() {
    if(GridPilot.DB_NAMES.length<=0){
      return;
    }
    for(int i=0; i<GridPilot.TABS.length; ++i){
      try{
        DBPanel panel = new DBPanel();
        panel.initDB(GridPilot.DB_NAMES[0], GridPilot.TABS[i]);
        panel.initGUI();
        addPanel(panel);
      }
      catch(Exception e){
        Debug.debug("ERROR: could not load database panel for "+
            GridPilot.DB_NAMES[0] + " : " + GridPilot.TABS[i], 1);
        e.printStackTrace();
      }
    }
  }
  
  /**
   * Add a new panel.
   */
  public void addPanel(ListPanel newPanel, String title){
    Debug.debug("Adding panel "+newPanel.getTitle(), 3);
    addPanel(newPanel);
    this.setTitle("GridPilot - "+title);
    //this.pack();
    //this.setVisible(true);
  }

  /**
   * Add a new panel.
   */
  public void initPanel(DBPanel panel, String title){
    Debug.debug("Initializing panel "+panel.getTitle(), 3);
    panel.initDB(panel.getDBName(), panel.getTableName());
  }

  public void addPanel(ListPanel newPanel){
    
    URL imgURL = null;
    Dimension size = this.getSize();
    ImageIcon closeIcon = null;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.RESOURCES_PATH + "close.png");
      closeIcon = new ImageIcon(imgURL);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.RESOURCES_PATH + "close.png", 3);
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
  
  /**
   * Get active panel
   */
  public DBPanel getActiveDBPanel(){
    int selectedIndex = tabbedPane.getTabCount()-1;
    if(selectedPanel>=0){
      selectedIndex = selectedPanel;
      Debug.debug("Selected index: "+selectedIndex, 3);
    }
    DBPanel dbPanel = (DBPanel)allPanels.elementAt(selectedIndex);
    Debug.debug("Selected panel : "+dbPanel.getTableName(), 3);
    return dbPanel;
  }

 /**
  * Remove panel.
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

  // Edit-cut | About action performed
  public void menuEditCut_actionPerformed(){
    Debug.debug("Cutting", 3);
    ListPanel panel = (ListPanel)allPanels.elementAt(tabbedPane.getSelectedIndex());
    panel.cut();
  }
  // Edit-copy | About action performed
  public void menuEditCopy_actionPerformed(){
    Debug.debug("Copying", 3);
    ListPanel panel = (ListPanel)allPanels.elementAt(tabbedPane.getSelectedIndex());
    panel.copy();
  }
  // Edit-paste | About action performed
  public void menuEditPaste_actionPerformed(){
    Debug.debug("Pasting", 3);
    ListPanel panel = (ListPanel)allPanels.elementAt(tabbedPane.getSelectedIndex());
    panel.paste();
  }
  
  public void menuEditPrefs_actionPerformed(){
    if(prefsPanel!=null && prefsPanel.isEditing()){
      return;
    }
    // Schedule a job for the event-dispatching thread:
    // creating and showing this application's GUI.
    javax.swing.SwingUtilities.invokeLater(new Runnable() {
        public void run(){
          createAndShowPrefsGUI();
          prefsPanel.setEditing(true);
        }
    });
  }

  // Help -> About action performed
  private void menuHelpAbout_actionPerformed(){
    URL aboutURL = null;
    try{
      aboutURL = GridPilot.class.getResource(GridPilot.RESOURCES_PATH + "about.htm");
    }
    catch(Exception e){
      Debug.debug("Could not find file "+ GridPilot.RESOURCES_PATH + "about.htm", 3);
      return;
    } 
    try{
      BrowserPanel wb = new BrowserPanel(this, "About",
          aboutURL.toExternalForm(), "", false, false, false, null, null, true);
      wb.setCancelButtonEnabled(false);
    }
    catch(Exception e){
      Debug.debug("WARNING: could not create BrowserPanel", 1);
      e.printStackTrace();
    }
  }

  // Help -> Show my distinguished name
  private void menuHelpShowDN_actionPerformed(){
    try{
      String dn = GridPilot.getClassMgr().getSSL().getDN();
      String label;
      JPanel jp = new JPanel();
      if(dn==null || dn.equals("")){
        label = "You don't have any active X.509 certificate";
        jp.add(new JLabel(label));
      }
      else{
        label = "Distinguished name (DN) of your active X.509 certificate: ";
        JTextArea jt = new JTextArea(dn);
        jt.setEditable(false);
        jp.add(new JLabel(label));
        jp.add(jt);
      }
      ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
      confirmBox.getConfirm("My DN", jp, new Object[] {"OK"});
    }
    catch(Exception e){
      e.printStackTrace();
      return;
    } 
  }

  // Overridden so we can exit when window is closed
  protected void processWindowEvent(WindowEvent e){
    super.processWindowEvent(e);
    if (e.getID()==WindowEvent.WINDOW_CLOSING){
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
  
  /**
   * Creates the main menu bar.
   */
  public JMenuBar makeMenuBar(){

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
    //menuFile.add(miReloadValues);
    
    // Import/export
    JMenuItem miImport = new JMenuItem("Import");
    JMenuItem miExport = new JMenuItem("Export");
    menuFile.add(miImport);
    menuFile.add(miExport);
    miImport.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        importToDB();
      }
    });
    miExport.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        exportDB();
      }
    });
   
    // DB
    JMenu menuDB = new JMenu("DB");
    menuDB.addMenuListener(new MenuListener(){
      public void menuCanceled(MenuEvent e) {
      }

      public void menuDeselected(MenuEvent e) {
      }

      public void menuSelected(MenuEvent e) {
        // Refresh active elements of the menu
        ((DBPanel) tabbedPane.getComponentAt(selectedPanel)).dbMenuSelected();
      }
    });

    miDbDefineRecords.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        // Define new record(s) in the DB of the active pane
        createRecords(false);
      }
    });
    
    miWithoutInput.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        // Define new record(s) in the DB of the active pane
        createRecords(false);
      }
    });
    
    miWithInputDataset.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        // Define new record(s) in the DB of the active pane
        createRecords(true);
      }
    });
    
    mDbDefineRecords.add(miWithoutInput);
    mDbDefineRecords.add(miWithInputDataset);
    menuDB.add(miDbDefineRecords);
    menuDB.add(mDbDefineRecords);

    miDbEditRecord.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        // Edit record in the DB of the active pane
        editRecord();
      }
    });
    menuDB.add(miDbEditRecord);

    miDbDeleteRecords.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        // Delete record(s) in the DB of the active pane
        deleteRecords();
      }
    });
    menuDB.add(miDbDeleteRecords);

    JMenuItem miDbClearCaches = new JMenuItem("Clear database cache");
    miDbClearCaches.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        // Clear caches of all DB connections
        GridPilot.getClassMgr().clearDBCaches();
      }
    });

    JMenuItem miDbReconnect = new JMenuItem("Reconnect databases");
    miDbReconnect.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        Debug.debug("Reconnecting DBs", 2);
        GridPilot.dbReconnect();
      }
    });

    // CS
    JMenuItem miCsReconnect = new JMenuItem("Reconnect computing systems");
    miCsReconnect.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        Debug.debug("Reconnecting CSs", 2);
        GridPilot.csReconnect();
        Debug.debug("Reconnecting CSs done", 2);
      }
    });

    JMenuItem miDbRefreshRTEs = new JMenuItem("Refresh runtime environments");
    miDbRefreshRTEs.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        /*
         Call setupRuntimeEnvironments() on all CSs
         */
        ResThread t = new ResThread(){
          public void run(){
            try{
              for(int i=0; i<GridPilot.CS_NAMES.length; ++i){
                GridPilot.getClassMgr().getCSPluginMgr().cleanupRuntimeEnvironments(
                    GridPilot.CS_NAMES[i]);
                GridPilot.getClassMgr().getCSPluginMgr().setupRuntimeEnvironments(
                    GridPilot.CS_NAMES[i]);
              }
            }
            catch(Exception ex){
              Debug.debug("WARNING: could not create refresh runtimeEnvironments.", 1);
              ex.printStackTrace();
            }
          }
        };     
        SwingUtilities.invokeLater(t);        
      }
    });

    menuFile.addSeparator();
    menuFile.add(miCsReconnect);
    menuFile.add(miDbRefreshRTEs);
    menuFile.addSeparator();
    menuFile.add(miDbClearCaches);
    menuFile.add(miDbReconnect);
   
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
        ResThread t = (new ResThread(){
          public void run(){
            try{
              BrowserPanel bp = new BrowserPanel(GridPilot.getClassMgr().getGlobalFrame(),
                  "GridPilot File Browser", "", "", false, true, true, null, null, false);
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
    
    for(i=0; i<GridPilot.DB_NAMES.length; ++i){
      
      final JMenu mDB = new JMenu("New tab with "+GridPilot.DB_NAMES[i]);
      mDB.setName(GridPilot.DB_NAMES[i]);
      
      // Check if there is a runtimeEnvironment table in this database
      try{
        if((GridPilot.getClassMgr().getDBPluginMgr(
            GridPilot.DB_NAMES[i]).getFieldNames("runtimeEnvironment")!=null)){
          JMenuItem miNewTab = new JMenuItem("runtimeEnvironments");
          miNewTab.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
              try{
                DBPanel panel = new DBPanel();
                panel.initDB(mDB.getName(), "runtimeEnvironment");
                panel.initGUI();
                addPanel(panel, "runtime environments");          
              }
              catch(Exception ex){
                Debug.debug("Could not add panel ", 1);
                ex.printStackTrace();
              }
              selectedPanel = tabbedPane.getSelectedIndex();
            }
          });
          mDB.add(miNewTab);
        }
      }
      catch(Exception e){
      }

      // Check if there is a transformation table in this database
      try{
        if((GridPilot.getClassMgr().getDBPluginMgr(
            GridPilot.DB_NAMES[i]).getFieldNames("transformation")!=null)){
          JMenuItem miNewTab = new JMenuItem("transformations");
          miNewTab.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
              try{
                DBPanel panel = new DBPanel();
                panel.initDB(mDB.getName(), "transformation");
                panel.initGUI();
                addPanel(panel, "transformations");          
              }
              catch(Exception ex){
                Debug.debug("Could not add panel ", 1);
                ex.printStackTrace();
              }
              selectedPanel = tabbedPane.getSelectedIndex();
            }
          });
          mDB.add(miNewTab);
        }
      }
      catch(Exception e){
      }
      
      // Check if there is a dataset table in this database
      try{
        Debug.debug("Checking for dataset in "+GridPilot.DB_NAMES[i], 2);
        if((GridPilot.getClassMgr().getDBPluginMgr(
            GridPilot.DB_NAMES[i]).getFieldNames("dataset")!=null)){
          Debug.debug("---> ok, adding", 2);
          JMenuItem miNewTab = new JMenuItem("datasets");
          miNewTab.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
              try{
                DBPanel panel = new DBPanel();
                panel.initDB(mDB.getName(), "dataset");
                panel.initGUI();
                addPanel(panel, "datasets");          
              }
              catch(Exception ex){
                Debug.debug("Could not add panel ", 1);
                ex.printStackTrace();
              }
              selectedPanel = tabbedPane.getSelectedIndex();
            }
          });
          mDB.add(miNewTab);
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
      
      // Check if there is a jobDefinition table in this database
      try{
        if((GridPilot.getClassMgr().getDBPluginMgr(
            GridPilot.DB_NAMES[i]).getFieldNames("jobDefinition")!=null)){
          JMenuItem miNewTab = new JMenuItem("jobDefinitions");
          miNewTab.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
              try{
                DBPanel panel = new DBPanel();
                panel.initDB(mDB.getName(), "jobDefinition");
                panel.initGUI();
                addPanel(panel, "jobDefinitions");          
              }
              catch(Exception ex){
                Debug.debug("Could not add panel ", 1);
                ex.printStackTrace();
              }
              selectedPanel = tabbedPane.getSelectedIndex();
            }
          });
          mDB.add(miNewTab);
        }
      }
      catch(Exception e){
      }
      
      if(mDB.getMenuComponentCount()>0){
        menuView.add(mDB);
      }

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
    menuEditPrefs.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        menuEditPrefs_actionPerformed();
      }
    });
    menuEdit.add(menuEditPrefs);

    // Help
    JMenu menuHelp = new JMenu("Help");
    JMenuItem menuHelpAbout = new JMenuItem("About GridPilot");
    menuHelpAbout.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        menuHelpAbout_actionPerformed();
      }
    });
    menuHelp.add(menuHelpAbout);
    menuHelp.addSeparator();
    JMenuItem menuHelpShowDN = new JMenuItem("Show my distinguished name");
    menuHelpShowDN.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        menuHelpShowDN_actionPerformed();
      }
    });
    menuHelp.add(menuHelpShowDN);
    menuHelp.addSeparator();
    JMenuItem menuHelpBeginning = new JMenuItem("Wizard: Configure GridPilot");
    menuHelpBeginning.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        (new ResThread(){
          public void run(){
            new BeginningWizard(false);
          }
        }).start();
      }
    });
    menuHelp.add(menuHelpBeginning);
    menuHelp.addSeparator();
    JMenuItem menuHelpCreateSoftwarePackage = new JMenuItem("Wizard: Create software package (runtime environment)");
    menuHelpCreateSoftwarePackage.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        JFrame frame = new CreateSoftwarePackageWizard();
        frame.pack();
        frame.setVisible(true);

      }
    });
    menuHelp.add(menuHelpCreateSoftwarePackage);
    JMenuItem menuHelpRunOneJob = new JMenuItem("Wizard: Run a command");
    menuHelpRunOneJob.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        new RunCommandWizard();
      }
    });
    menuHelp.addSeparator();
    menuHelp.add(menuHelpRunOneJob);
    
    menuBar.add(menuFile);
    menuBar.add(menuEdit);
    menuBar.add(menuView);
    menuBar.add(menuDB);
    menuBar.add(menuHelp);
    
    return menuBar;

  }
  
  protected void setDefineMenu(boolean ds, int selectedRows, boolean notFilePanel){
    mDbDefineRecords.setVisible(ds);
    miWithoutInput.setVisible(ds);
    miWithInputDataset.setVisible(ds);  
    miDbDefineRecords.setVisible(!ds && notFilePanel);
    miWithInputDataset.setEnabled(ds && selectedRows>0);
    miWithoutInput.setEnabled(notFilePanel);
    miDbEditRecord.setEnabled(notFilePanel && selectedRows==1);
    miDbDeleteRecords.setEnabled(selectedRows>0);

  }

  private void createRecords(boolean withInputDataset) {
    DBPanel panel = getActiveDBPanel();
    if(panel.getTableName().equalsIgnoreCase("runtimeEnvironment")){
      panel.createRuntimeEnvironment();
    }
    else if(panel.getTableName().equalsIgnoreCase("transformation")){
      panel.createTransformation();
    }
    else if(panel.getTableName().equalsIgnoreCase("dataset")){
      panel.createDatasets(withInputDataset);
    }
    else if(panel.getTableName().equalsIgnoreCase("jobDefinition")){
      panel.createJobDefinitions();
    }
  }

  private void editRecord() {
    DBPanel panel = getActiveDBPanel();
    if(panel.getTableName().equalsIgnoreCase("runtimeEnvironment")){
      panel.editRuntimeEnvironment();
    }
    else if(panel.getTableName().equalsIgnoreCase("transformation")){
      panel.editTransformation();
    }
    else if(panel.getTableName().equalsIgnoreCase("dataset")){
      panel.editDataset();
    }
    else if(panel.getTableName().equalsIgnoreCase("jobDefinition")){
      panel.editJobDef();
    }
  }

  private void deleteRecords() {
    DBPanel panel = getActiveDBPanel();
    if(panel.getTableName().equalsIgnoreCase("runtimeEnvironment")){
      panel.deleteRuntimeEnvironments();
    }
    else if(panel.getTableName().equalsIgnoreCase("transformation")){
      panel.deleteTransformations();
    }
    else if(panel.getTableName().equalsIgnoreCase("dataset")){
      panel.deleteDatasets();
    }
    else if(panel.getTableName().equalsIgnoreCase("jobDefinition")){
      panel.deleteJobDefs();
    }
  }

  protected void exportDB() {
    String url = null;
    try{
      url = MyUtil.getURL("file:~/", null, true, "Choose destination directory");
    }
    catch(IOException e){
      e.printStackTrace();
    }
    try{
      if(url!=null && !url.equals("")){
        Debug.debug("Exporting to "+url, 2);
        ExportImport.exportDB(MyUtil.clearTildeLocally(MyUtil.clearFile(url)), null, null);
      }
      else{
        Debug.debug("Not exporting. "+url, 2);
      }
    }
    catch(Exception ex){
      String error = "ERROR: could not export DB(s). "+ex.getMessage();
      MyUtil.showError(error);
      GridPilot.getClassMgr().getLogFile().addMessage(error, ex);
      ex.printStackTrace();
    }
  }
    
  protected void importToDB() {
    String url = null;
    try{
      url = MyUtil.getURL("file:~/", null, false, "Choose tar.gz file to import from.");
    }
    catch(IOException e){
      e.printStackTrace();
    }
    try{
      if(url!=null && !url.equals("")){
        if(!url.endsWith(".tar.gz")){
          throw new IOException("Only gzipped tar archives (with extension tar.gz) can be imported.");
        }
        Debug.debug("Importing from "+url, 2);
        String importUrl = url;
        if(MyUtil.isLocalFileName(importUrl)){
          importUrl = MyUtil.clearTildeLocally(MyUtil.clearFile(importUrl));
        }
        ExportImport.importToDB(importUrl);
      }
      else{
        Debug.debug("Not importing. "+url, 2);
      }
    }
    catch(Exception ex){
      String error = "ERROR: could not import. "+ex.getMessage();
      MyUtil.showError(error);
      GridPilot.getClassMgr().getLogFile().addMessage(error, ex);
      ex.printStackTrace();
    }
  }
  
  public void toggleMonitoringPanel(){
    try{
      if(pDialog.isShowing()){
        pDialog.setVisible(false);
      }
      else{
        pDialog.setVisible(true);
      }
    }
    catch(Exception ex){
      Debug.debug("Could not toggle monitoring panel.", 1);
      ex.printStackTrace();
    }
  }

  public void showMonitoringPanel(final int index){
    if(SwingUtilities.isEventDispatchThread()){
      pDialog.setVisible(true);
      cbMonitor.setSelected(true);
      getMonitoringPanel().getTPStatLog().setSelectedIndex(index);
    }
    else{
      SwingUtilities.invokeLater(
        new Runnable(){
          public void run(){
            try{
              pDialog.setVisible(true);
              cbMonitor.setSelected(true);
              getMonitoringPanel().getTPStatLog().setSelectedIndex(index);
            }
            catch(Exception ex){
              Debug.debug("Could not create panel ", 1);
              ex.printStackTrace();
            }
          }
        }
      );
    }
  }
  
  /**
   * Create GUI for preferences and show it. For thread safety,
   * this method should be invoked from the event-dispatching thread.
   */
  private void createAndShowPrefsGUI() {
    // Create the window.
    JFrame frame = new JFrame("Preferences");
    
    // Create the nodes.
    GridPilot.getClassMgr().getConfigFile().resetConfiguration();
    GridPilot.getClassMgr().getConfigFile().parseSections();
    ConfigNode topNode = GridPilot.getClassMgr().getConfigFile().getHeadNode();
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    LogFile logFile = GridPilot.getClassMgr().getLogFile();

    // Create and set up the content pane.
    prefsPanel = new MyPreferencesPanel(frame, topNode, configFile, logFile);
    prefsPanel.setOpaque(true); // content panes must be opaque
    frame.setContentPane(prefsPanel);

    // Display the window.
    frame.pack();
    frame.setVisible(true);
    /*frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
    frame.addWindowListener(new WindowAdapter(){
      public void windowClosing(WindowEvent we){
        prefsPanel.savePrefs();
        //we.getWindow().setVisible(false);
        we.getWindow().dispose();
      }
    });*/
  
  }

  public MonitoringPanel getMonitoringPanel() {
    return monitoringPanel;
  }

  public JCheckBoxMenuItem getCBMonitor() {
    return cbMonitor;
  }

  public JMenuItem getMenuEditCopy() {
    return menuEditCopy;
  }

  public JMenuItem getMenuEditCut() {
    return menuEditCut;
  }

  public JMenuItem getMenuEditPaste() {
    return menuEditPaste;
  }

  public JMenu getMenuEdit() {
    return menuEdit;
  }

  public void setCutPanel(DBPanel panel) {
    cutPanel = panel;
  }

  public void refreshCutPanel() {
    try{
      if(cutPanel!=null){
        ((DBPanel) cutPanel).refresh();
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }

  }

}
