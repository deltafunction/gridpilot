package gridpilot;

import java.awt.*;
import java.awt.event.*;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;

import javax.swing.*;
import javax.swing.event.*;
import javax.swing.text.StyleConstants;

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

  private static final int MAX_TAB_TITLE_LENGTH = 24;

  private static final String GridPilotURL = "http://www.gridpilot.dk/";

  public static final String GPA_FILTER = "*"+GridPilot.APP_EXTENSION+"|"+GridPilot.APP_INDEX_FILE;
  
  private Vector<ListPanel> allPanels;
  private DBPanel selectedPanel;
  private CreateEditDialog pDialog;
  private MyPreferencesPanel prefsPanel = null;
  private static int i;

  private JTabbedPane tabbedPane = new DnDTabbedPane();
  private MonitoringPanel monitoringPanel;
  private JMenu menuEdit = new JMenu("Edit");
  private JMenuItem menuEditCopy = new JMenuItem("Copy (ctrl c)");
  private JMenuItem menuEditCut = new JMenuItem("Cut (ctrl x)");
  private JMenuItem menuEditPaste = new JMenuItem("Paste (ctrl v)");
  private JCheckBoxMenuItem cbMonitor = new JCheckBoxMenuItem("Show monitor (ctrl m)");
  private ListPanel cutPanel = null;
  private JMenuItem miDbEditRecord = new JMenuItem("Edit record");
  private JMenuItem miDbDeleteRecords = new JMenuItem("Delete record(s)");
  private JMenu mDbDefineRecords = new JMenu("Define new record(s)");
  private JMenuItem miDbDefineRecords = new JMenuItem("Define new record(s)");
  private JMenuItem miWithoutInput = new JMenuItem("from scratch");
  private JMenuItem miWithInputDataset = new JMenuItem("with selected input dataset(s)");
  private JMenuItem miExport = new JMenuItem();

  private Object app;
  private Class<?> appc;
  private Class<?> lc;


  // keep track of whether or not we are cutting on the sub-panels
  public boolean cutting = false;

  public GlobalFrame() throws Exception{
    enableEvents(AWTEvent.WINDOW_EVENT_MASK);
    allPanels = new Vector<ListPanel>();
    if(MyUtil.onMacOSX()){
      setMacOSMenus();
    }
  }
  
  private void setMacOSMenus(){

    menuEditCopy.setAlignmentX(StyleConstants.ALIGN_JUSTIFIED);
    menuEditCut.setAlignmentX(StyleConstants.ALIGN_JUSTIFIED);
    menuEditPaste.setAlignmentX(StyleConstants.ALIGN_JUSTIFIED);
    cbMonitor.setAlignmentX(StyleConstants.ALIGN_JUSTIFIED);
    menuEditCopy.setText("Copy \t\t\t\t \u2318 c");
    menuEditCut.setText("Cut \t\t\t\t \u2318 x");
    menuEditPaste.setText("Paste \t\t\t\t \u2318 v");
    cbMonitor.setText("Show monitor \t\t\t\t \u2318 m");
    
    try{
      app = MyUtil.loadClass("com.apple.eawt.Application", new Class[] {}, new String [] {});
      appc = Class.forName("com.apple.eawt.Application");
      lc = Class.forName("com.apple.eawt.ApplicationListener");
    }
    catch(Throwable e2){
      e2.printStackTrace();
    }
    
    // Handle quit, about and preferences
    try{
      Object listener = Proxy.newProxyInstance(new MyClassLoader(), new Class[] {lc},
          new InvocationHandler() {
         public Object invoke(Object proxy, Method method, Object[] args){
           if(method.getName().equals("handleQuit")){
             GridPilot.exit(0);
           }
           else if(method.getName().equals("handleAbout")){
             menuHelpAbout_actionPerformed();
             Object event = args[0];
             Method eventSetter;
             try{
               eventSetter = Class.forName("com.apple.eawt.ApplicationEvent").getDeclaredMethod("setHandled", Boolean.TYPE);
               eventSetter.invoke(event, true);
             }
             catch(Exception e){
               e.printStackTrace();
             }
           }
           else if(method.getName().equals("handlePreferences")){
             menuEditPrefs_actionPerformed();
           }
           return null;
         }
       });
      appc.getMethod("addApplicationListener", lc).invoke(app, listener);
      appc.getDeclaredMethod("setEnabledPreferencesMenu", Boolean.TYPE).invoke(app, new Object[] {true});
    }
    catch(Throwable e1){
      e1.printStackTrace();
    }
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
    selectedPanel = getActiveDBPanel();

   /**
    * Detect click over X in tab
    */
   tabbedPane.addMouseListener(new MouseAdapter(){
     public void mouseReleased(MouseEvent evt){
       if(tabbedPane.getTabCount()==0 || tabbedPane.getSelectedIndex()<0){
         return;
       }
       if(!evt.isPopupTrigger()){
         IconProxy iconProxy = (IconProxy) tabbedPane.getIconAt(tabbedPane.getSelectedIndex());
         DBPanel activePanel = getActiveDBPanel();
         if((GridPilot.ADVANCED_MODE || !activePanel.getTableName().startsWith("application") &&
             !activePanel.getTableName().toLowerCase().startsWith("dataset")) &&
             iconProxy.contains(evt.getX(), evt.getY())){
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
        if(!MyUtil.isModifierDown(e)){
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
    for(int i=0; i<GridPilot.TAB_DBS.length; ++i){
      try{
        DBPanel panel = new DBPanel();
        panel.initDB(GridPilot.TAB_DBS[i], GridPilot.TAB_TABLES[i]);
        panel.initGUI();
        addPanel(panel);
      }
      catch(Exception e){
        Debug.debug("ERROR: could not load database panel for "+
            GridPilot.TAB_DBS[i] + " : " + GridPilot.TAB_TABLES[i], 1);
        e.printStackTrace();
      }
    }
    if(tabbedPane.getTabCount()>0){
      tabbedPane.setSelectedIndex(0);
    }
  }
  
  /**
   * Add a new panel.
   */
  public void addPanel(DBPanel newPanel, String title){
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

  public void addPanel(DBPanel newPanel){
    
    URL imgURL = null;
    Dimension size = this.getSize();
    ImageIcon closeIcon = null;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + "close.png");
      closeIcon = new ImageIcon(imgURL);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.ICONS_PATH + "close.png", 3);
      closeIcon = new ImageIcon();
    }
  
    // Trim title name before adding new tab
    String title = newPanel.getTitle();
    String smallTitle = null;
    if(title.length()>MAX_TAB_TITLE_LENGTH){
      smallTitle = title.substring(0, MAX_TAB_TITLE_LENGTH) + "...";
    }
    else{
      smallTitle = title;
    }
    Debug.debug("Adding tab "+allPanels.size(), 3);
    allPanels.addElement(newPanel);
    tabbedPane.addTab(smallTitle,
        !GridPilot.ADVANCED_MODE && (title.toLowerCase().startsWith("application") ||
         title.toLowerCase().startsWith("dataset"))?null:new IconProxy(closeIcon),
        (JPanel) newPanel);
    Debug.debug("Added tab "+allPanels.size(), 3);
    // focus on new panel
    int newSelIndex = tabbedPane.getTabCount()-1;
    setSelectedPanel(newSelIndex);
    setTitle("GridPilot - "+title);
    setSize(size);
    newPanel.refresh();
  }
  
  private void setSelectedPanel(int newSelIndex) {
    ((ListPanel) tabbedPane.getComponentAt(tabbedPane.getSelectedIndex())).panelHidden();
    ((ListPanel) tabbedPane.getComponentAt(newSelIndex)).panelShown();
    Debug.debug("Setting selected index "+newSelIndex, 3);
    tabbedPane.setSelectedIndex(newSelIndex);
  }

  /**
   * Get active panel
   */
  public DBPanel getActiveDBPanel(){
    selectedPanel = (DBPanel) tabbedPane.getSelectedComponent();
    return selectedPanel;
  }

 /**
  * Remove panel.
  */
  public void removePanel(){
    ListPanel panel = getActiveDBPanel();
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

  // Edit-cut action performed
  public void menuEditCut_actionPerformed(){
    Debug.debug("Cutting", 3);
    ListPanel panel = getActiveDBPanel();
    panel.cut();
  }
  // Edit-copy action performed
  public void menuEditCopy_actionPerformed(){
    Debug.debug("Copying", 3);
    ListPanel panel = getActiveDBPanel();
    panel.copy();
  }
  // Edit-paste action performed
  public void menuEditPaste_actionPerformed(){
    Debug.debug("Pasting", 3);
    ListPanel panel = getActiveDBPanel();
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

  // Help -> GridPilot website action performed
  private void menuWebsite_actionPerformed(){
    try{
      BrowserPanel wb = new BrowserPanel(this, "About",
          GridPilotURL, "", false, false, false, null, null, true);
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
      ConfirmBox confirmBox = new ConfirmBox(this);
      confirmBox.getConfirm("My DN", jp, new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane())});
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
    if(selectedPanel!=null){
      ((ListPanel)selectedPanel).panelHidden();
      ((ListPanel)selectedPanel).panelShown();
      String title = ((ListPanel)selectedPanel).getTitle();
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
    JMenuItem miImport = new JMenuItem("Import application(s)/dataset(s)");
    menuFile.add(miImport);
    if(GridPilot.ADVANCED_MODE){
      menuFile.add(miExport);
    }
    miImport.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        DBPanel activePanel = getActiveDBPanel();
        importToDB(activePanel);
      }
    });
    miExport.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        exportDB();
      }
    });
    menuFile.addMenuListener(new MenuListener(){
      public void menuCanceled(MenuEvent e) {
      }

      public void menuDeselected(MenuEvent e) {
      }

      public void menuSelected(MenuEvent e) {
        // Refresh active elements of the menu
        selectedPanel = getActiveDBPanel();
        selectedPanel.fileMenuSelected();
      }
    });
   
    // Edit
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
    menuEdit.addMenuListener(new MenuListener(){
      public void menuCanceled(MenuEvent e) {
      }

      public void menuDeselected(MenuEvent e) {
      }

      public void menuSelected(MenuEvent e) {
        // Refresh active elements of the menu
        getActiveDBPanel().dbMenuSelected();
      }
    });
    
    menuEdit.validate();

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
    menuEdit.addSeparator();
    menuEdit.add(miDbDefineRecords);
    menuEdit.add(mDbDefineRecords);

    miDbEditRecord.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        // Edit record in the DB of the active pane
        editRecord();
      }
    });
    menuEdit.add(miDbEditRecord);

    miDbDeleteRecords.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        // Delete record(s) in the DB of the active pane
        deleteRecords();
      }
    });
    menuEdit.add(miDbDeleteRecords);
    if(!MyUtil.onMacOSX()){
      menuEdit.addSeparator();
      JMenuItem menuEditPrefs = new JMenuItem("Preferences");
      menuEditPrefs.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          menuEditPrefs_actionPerformed();
        }
      });
      menuEdit.add(menuEditPrefs);
    }

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

    if(GridPilot.ADVANCED_MODE){
      menuFile.addSeparator();
      menuFile.add(miCsReconnect);
      menuFile.add(miDbRefreshRTEs);
      menuFile.addSeparator();
      menuFile.add(miDbClearCaches);
      menuFile.add(miDbReconnect);
    }
   
    if(!GridPilot.isApplet()){
      if(!MyUtil.onMacOSX()){
        menuFile.addSeparator();
        JMenuItem miExit;
        miExit = new JMenuItem("Quit (ctrl q)");
        miExit.addActionListener(new ActionListener(){
          public void actionPerformed(ActionEvent e){
            GridPilot.exit(0);
          }
        });
        menuFile.add(miExit);
      }
      else{
        try{
          
          /*com.apple.eawt.Application app = new
          com.apple.eawt.Application();

          app.addApplicationListener(new
          com.apple.eawt.ApplicationAdapter() {
          public void handleQuit(com.apple.eawt.ApplicationEvent
          e)
          {
            GridPilot.exit(0);
          }
          });*/
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
    }

    menuView.add(cbMonitor);
    
    menuView.addSeparator();

    JMenuItem miBrowser;
    if(MyUtil.onMacOSX()){
      miBrowser = new JMenuItem("New file browser \t\t\t \u2318 o");
      miBrowser.setAlignmentX(StyleConstants.ALIGN_JUSTIFIED);
    }
    else{
      miBrowser = new JMenuItem("New file browser (ctrl o)");
    }
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
    menuView.validate();
    
    if(GridPilot.ADVANCED_MODE){
      menuView.addSeparator();
      for(i=0; i<GridPilot.DB_NAMES.length; ++i){
        addDBMenuItem(GridPilot.DB_NAMES[i], menuView);
      }
    }

    cbMonitor.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        toggleMonitoringPanel();
      }
    });    

    // Help
    JMenu menuHelp = new JMenu("Help");
    if(!MyUtil.onMacOSX()){
      JMenuItem menuHelpAbout = new JMenuItem("About GridPilot");
      menuHelpAbout.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          menuHelpAbout_actionPerformed();
        }
      });
      menuHelp.add(menuHelpAbout);
      menuHelp.addSeparator();
    }
    JMenuItem menuHelpWebsite = new JMenuItem("GridPilot website");
    menuHelpWebsite.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        menuWebsite_actionPerformed();
      }
    });
    menuHelp.add(menuHelpWebsite);
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
    menuBar.add(menuHelp);
    
    return menuBar;

  }
  
  private void addDBMenuItem(final String dbName, JMenu menuView) {
    final JMenu mDB = new JMenu("New tab with "+dbName);
    mDB.setName(dbName);
    
    // Check if there is a runtimeEnvironment table in this database
    try{
      if((GridPilot.getClassMgr().getDBPluginMgr(
          dbName).getFieldNames("runtimeEnvironment")!=null)){
        JMenuItem miNewTab = new JMenuItem(GridPilot.getTabDisplayName(dbName, "runtimeEnvironment"));
        miNewTab.addActionListener(new ActionListener(){
          public void actionPerformed(ActionEvent e){
            try{
              DBPanel panel = new DBPanel();
              panel.initDB(mDB.getName(), "runtimeEnvironment");
              panel.initGUI();
              addPanel(panel, GridPilot.getTabDisplayName(dbName, "runtimeEnvironment"));          
            }
            catch(Exception ex){
              Debug.debug("Could not add panel ", 1);
              ex.printStackTrace();
            }
            selectedPanel = getActiveDBPanel();
          }
        });
        mDB.add(miNewTab);
      }
    }
    catch(Exception e){
    }

    // Check if there is a executable table in this database
    try{
      if((GridPilot.getClassMgr().getDBPluginMgr(
          dbName).getFieldNames("executable")!=null)){
        JMenuItem miNewTab = new JMenuItem(GridPilot.getTabDisplayName(dbName, "executable"));
        miNewTab.addActionListener(new ActionListener(){
          public void actionPerformed(ActionEvent e){
            try{
              DBPanel panel = new DBPanel();
              panel.initDB(mDB.getName(), "executable");
              panel.initGUI();
              addPanel(panel, GridPilot.getTabDisplayName(dbName, "executable"));          
            }
            catch(Exception ex){
              Debug.debug("Could not add panel ", 1);
              ex.printStackTrace();
            }
            selectedPanel = getActiveDBPanel();
          }
        });
        mDB.add(miNewTab);
      }
    }
    catch(Exception e){
    }
    
    // Check if there is a dataset table in this database
    try{
      Debug.debug("Checking for dataset in "+dbName, 2);
      if((GridPilot.getClassMgr().getDBPluginMgr(
          dbName).getFieldNames("dataset")!=null)){
        Debug.debug("---> ok, adding", 2);
        JMenuItem miNewTab = new JMenuItem(GridPilot.getTabDisplayName(dbName, "dataset"));
        miNewTab.addActionListener(new ActionListener(){
          public void actionPerformed(ActionEvent e){
            try{
              DBPanel panel = new DBPanel();
              panel.initDB(mDB.getName(), "dataset");
              panel.initGUI();
              addPanel(panel, GridPilot.getTabDisplayName(dbName, "dataset"));          
            }
            catch(Exception ex){
              Debug.debug("Could not add panel ", 1);
              ex.printStackTrace();
            }
            selectedPanel = getActiveDBPanel();
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
          dbName).getFieldNames("jobDefinition")!=null)){
        JMenuItem miNewTab = new JMenuItem(GridPilot.getTabDisplayName(dbName, "jobDefinition"));
        miNewTab.addActionListener(new ActionListener(){
          public void actionPerformed(ActionEvent e){
            try{
              DBPanel panel = new DBPanel();
              panel.initDB(mDB.getName(), "jobDefinition");
              panel.initGUI();
              addPanel(panel, GridPilot.getTabDisplayName(dbName, "jobDefinition"));          
            }
            catch(Exception ex){
              Debug.debug("Could not add panel ", 1);
              ex.printStackTrace();
            }
            selectedPanel = getActiveDBPanel();
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

  protected void setDefineMenu(boolean ds, int selectedRows, boolean notFilePanel){
    mDbDefineRecords.setVisible(ds);
    miWithoutInput.setVisible(ds);
    miWithInputDataset.setVisible(ds);
    miDbDefineRecords.setEnabled(!ds && notFilePanel);
    miDbDefineRecords.setVisible(!ds && notFilePanel);
    miWithInputDataset.setEnabled(ds && selectedRows>0);
    miWithoutInput.setEnabled(notFilePanel);
    miDbEditRecord.setEnabled(notFilePanel && selectedRows==1);
    miDbDeleteRecords.setEnabled(selectedRows>0);

  }

  protected void setImportExportMenu(boolean ds, String [] selectedIds){
    miExport.setVisible(ds);
    miExport.setEnabled(true);
    if(!ds){
      return;
    }
    if(selectedIds.length>0){
      miExport.setText("Export selected application(s)");
    }
    else if(selectedIds.length==0){
      miExport.setText("Export all application(s)");
    }
    else{
      miExport.setEnabled(false);
    }
  }

  private void createRecords(boolean withInputDataset) {
    DBPanel panel = getActiveDBPanel();
    if(panel.getTableName().equalsIgnoreCase("runtimeEnvironment")){
      panel.createRuntimeEnvironment();
    }
    else if(panel.getTableName().equalsIgnoreCase("executable")){
      panel.createExecutable();
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
    else if(panel.getTableName().equalsIgnoreCase("executable")){
      panel.editExecutable();
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
    else if(panel.getTableName().equalsIgnoreCase("executable")){
      panel.deleteExecutables();
    }
    else if(panel.getTableName().equalsIgnoreCase("dataset")){
      panel.deleteDatasets();
    }
    else if(panel.getTableName().equalsIgnoreCase("jobDefinition")){
      panel.deleteJobDefs();
    }
  }

  protected void exportDB() {
    final DBPanel activePanel = getActiveDBPanel();
    activePanel.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    new Thread(){
      public void run(){
        doExportDB(activePanel);
      }
    }.start();
  }
  
  protected void doExportDB(DBPanel activePanel) {
    String url = null;
    try{
      //url = MyUtil.getURL("file:~/", null, true, "Choose destination directory");
      url = MyUtil.getURL(GridPilot.APP_STORE_URL, null, true, "Choose destination directory");
    }
    catch(IOException e){
      e.printStackTrace();
    }
    try{
      if(url!=null && !url.equals("")){
        Debug.debug("Exporting to "+url, 2);
        String [] datasetIDs = activePanel.getSelectedIdentifiers();
        if(datasetIDs.length>1){
          if(ExportImport.exportDB(MyUtil.clearTildeLocally(MyUtil.clearFile(url)),
              activePanel.getDBName(), datasetIDs)){
            MyUtil.showMessage(this, "Export successful",
                "Thanks and congratulations! You've successfully exported "+
                datasetIDs.length+" application"+(datasetIDs.length>1?"s":"")+
                "/dataset"+(datasetIDs.length>1?"s":"")+".\n");
          }
        }
        else{
          if(ExportImport.exportDB(MyUtil.clearTildeLocally(MyUtil.clearFile(url)),
              null, null)){
            MyUtil.showMessage("Export successful",
            "Thanks and congratulations! You've successfully exported your application(s)/dataset(s).");
          }
        }
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
    activePanel.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
  }
    
  public void importToDB(final DBPanel activePanel) {
    activePanel.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    ResThread t = new ResThread(){
      public void run(){
        try{
          doImportToDB(activePanel);
          activePanel.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        }
        catch(Exception ex){
          activePanel.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          Debug.debug("WARNING: could not import.", 1);
          ex.printStackTrace();
        }
      }
    };     
    t.start(); 
  }
  
  private void doImportToDB(DBPanel activePanel) {
    String url = null;
    try{
      //url = MyUtil.getURL("file:~/", null, false, "Choose *.gpa file to import from.");
      url = MyUtil.getURL(GridPilot.APP_STORE_URL, null, false, "Choose application to import.", GPA_FILTER);
    }
    catch(IOException e){
      activePanel.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      e.printStackTrace();
    }
    try{
      String[] importUrls = new String[]{url};
      boolean readmeAvailable = false;
      if(url!=null && !url.equals("")){
        if(!url.endsWith(GridPilot.APP_EXTENSION)){
          if(url.replaceFirst(":443/", "/").startsWith(GridPilot.APP_STORE_URL.replaceFirst(":443/", "/")) &&
              url.endsWith(GridPilot.APP_INDEX_FILE)){
            importUrls = findImportUrls(url.replaceFirst(GridPilot.APP_INDEX_FILE+"$", ""));
            readmeAvailable = true;
          }
          else{
            throw new IOException("Only gzipped tar archives (with extension gpa) can be imported.");
          }
        }
        Debug.debug("Importing from "+MyUtil.arrayToString(importUrls), 2);
        StringBuffer msgs = new StringBuffer();
        boolean atLeastOneImported = false;
        for(int i=0; i<importUrls.length; ++i){
          if(MyUtil.isLocalFileName(importUrls[i])){
            importUrls[i] = MyUtil.clearTildeLocally(MyUtil.clearFile(importUrls[i]));
          }
          String [] res = ExportImport.importToDB(importUrls[i]);
          if(res==null){
            continue;
          }
          refreshTab(res[0]);
          msgs.append(res[1]);
          msgs.append("\n\n");
          atLeastOneImported = true;
        }
        if(atLeastOneImported){
          String message = msgs.toString()+
          "Right-click on your new application/dataset to add files or create and run jobs."+
          (readmeAvailable?"\n\nSee <a href=\""+url+"\">"+url+"</a> for more information.":"");
          MyUtil.showHtmlMessage(this, "Import successful", message);
        }
      }
      else{
        Debug.debug("Not importing. "+url, 2);
      }
    }
    catch(Exception ex){
      activePanel.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      String error = "ERROR: could not import. "+ex.getMessage();
      MyUtil.showError(error);
      GridPilot.getClassMgr().getLogFile().addMessage(error, ex);
      ex.printStackTrace();
    }
  }
  
  /**
   * Find .gpa files in the directory 'url'
   * @param url
   * @return list of URLs
   * @throws Exception 
   */
  private String[] findImportUrls(String url) throws Exception {
    GridPilot.getClassMgr().getTransferControl();
    String[][] urlsAndSizes = MyTransferControl.findAllFilesAndDirs(url, "*"+GridPilot.APP_EXTENSION);
    HashSet<String> urlsSet = new HashSet<String>();
    for(int i=0; i<urlsAndSizes[0].length; ++i){
      if(urlsAndSizes[0][i].endsWith(GridPilot.APP_EXTENSION) &&
          urlsAndSizes[0][i].replace(url, "").indexOf("/")<0){
        urlsSet.add(urlsAndSizes[0][i]);
      }
    }
    String [] ret = urlsSet.toArray(new String[urlsSet.size()]);
    Arrays.sort(ret);
    return ret;
  }

  private void refreshTab(String dbName) {
    DBPanel panel;
    int i = 0;
    boolean ok = false;
    for(Iterator<ListPanel> it=allPanels.iterator(); it.hasNext();){
      panel = (DBPanel) it.next();
      if(panel.getDBName().equalsIgnoreCase(dbName) && panel.getTableName().equalsIgnoreCase("dataset")){
        panel.refresh();
        setSelectedPanel(i);
        ok = true;
      }
      ++i;
    }
    if(!ok){
      try{
        DBPanel newPanel = new DBPanel();
        newPanel.initDB(dbName, "dataset");
        newPanel.initGUI();
        addPanel(newPanel);
        newPanel.refresh();
      }
      catch(Exception e){
        Debug.debug("ERROR: could not load database panel for "+
            dbName + " : " + "dataset", 1);
        e.printStackTrace();
      }
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
