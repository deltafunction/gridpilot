package gridpilot;

import gridfactory.common.ConfirmBox;
import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.DBVectorTableModel;
import gridfactory.common.Debug;
import gridfactory.common.ResThread;
import gridfactory.common.StatusBar;
import gridfactory.common.TransferInfo;
import gridpilot.MyJTable;
import gridpilot.SelectPanel.SPanel;
import gridpilot.GridPilot;

import javax.swing.*;
import javax.swing.event.*;

import org.globus.util.GlobusURL;

import java.awt.*;
import java.awt.event.*;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.ClipboardOwner;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.awt.Toolkit;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;

/**
 * This panel contains one SelectPanel.
 * TODO: split off 4 subclasses: RuntimeEnvironmentDBPanel, ...
 */
public class DBPanel extends JPanel implements ListPanel, ClipboardOwner{

  private static final long serialVersionUID = 1L; 
  /** Number of seconds between each refresh when processing dataset(s). */
  private static final int AUTO_REFRESH_SECONDS = 20;
  private static final String SEARCH_TEXT = "Search";
  private static final String SEARCH_MOUSEOVER_TEXT = "Search with the chosen constraints";
  private static final String REFRESH_TEXT = "Refresh";
  private static final String REFRESH_MOUSEOVER_TEXT = "Refresh search results";
  /** Show define, edit and delete buttons on all DB panes. */
  private boolean SHOW_DB_BUTTONS = false;
  
  private JScrollPane spSelectPanel = new JScrollPane();
  private JPanel pButtonSelectPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  private JScrollPane spTableResults = new JScrollPane();
  private MyJTable tableResults = null;
  private JPanel pButtonTableResults = new JPanel(new FlowLayout(FlowLayout.CENTER));
  private JCheckBox cbFindAllFiles = new JCheckBox();
  private JButton bReplicate;
  private JPopupMenu pmSubmitMenu = new JPopupMenu();
  private JPopupMenu pmProcessMenu = new JPopupMenu();
  private JPopupMenu pmCreateDSMenu = new JPopupMenu();
  private JMenuItem miWithInput = new JMenuItem("with selected input dataset(s)");
  private JMenuItem miWithoutInput = new JMenuItem("from scratch");
  private JButton bSubmit;
  private JButton bMonitor;
  private JButton bProcessDatasets;
  private JButton bMonitorDatasets;
  private JButton bCleanupDatasets;
  private JButton bSearch;
  private JButton bNext;
  private JButton bPrevious;
  private JButton bClear;
  private JButton bDeleteRecords;
  private JButton bCreateRecords;
  private JButton bEditRecord;
  private JButton bViewJobDefinitions;
  private JButton bViewFiles ;
  private JButton bDefineJobDefinitions;
  private JButton bShowFilter;
  private JButton bHideFilter;
  private JMenuItem miEdit = null;
  private JMenuItem miImportFiles = new JMenuItem("Import file(s)");
  private JMenuItem miExportDatasets = new JMenuItem("Export application(s)");
  private String [] identifiers;
  // lists of field names with table name as key
  private String [] fieldNames = null;
  private String tableName;
  private String identifier = null;
  private String jobDefIdentifier = null;
  private String [] defaultFields = null;
  private String [] hiddenFields = null;
  private String [] shownFields = null;
  private String [] selectFields = null;
  private JMenu jmSetFieldValue = null;
  private StatusBar statusBar = null;
  private DBPluginMgr dbPluginMgr = null;
  private String parentId = "-1";
  private boolean clipboardOwned = false;
  private JMenuItem menuEditCopy = null;
  private JMenuItem menuEditCut = null;
  private JMenuItem menuEditPaste = null;
  private ResThread workThread;
  // WORKING THREAD SEMAPHORE
  // The idea is to ignore new requests when working on a request
  private boolean working = false;
  private boolean exportingDataset = false;
  private int cursor = -1;
  private DBResult res = null;

  private static String defaultURL;
  
  private JPanel panelSelectPanel = new JPanel(new GridBagLayout());
  private SelectPanel selectPanel;
  private JPanel showHidePanel = new JPanel();
  private String dbName = null;
  private boolean menuSet = false;
  private Boolean deleteJobDefs = null;
  private Boolean cleanupJobDefs = null;
  private Boolean deleteFiles = null;
  private Boolean cleanupFiles = null;
  // Replicating semaphore
  private boolean replicating = false;
  private int fileCatalogTimeout;
  private Window window = SwingUtilities.getWindowAncestor(this);

  /**
   * Create a new DBPanel from scratch.
   */
   public DBPanel() throws Exception{
   }
   
  /**
   * Create a new DBPanel from a parent panel.
   */
  private DBPanel(String _parentId) throws Exception{
    parentId = _parentId;
  }
  
  // try grabbing the semaphore
  private /*synchronized*/ boolean getWorking(){
    if(!working){
      working = true;
      return true;
    }
    return false;
  }
  // release the semaphore
  private /*synchronized*/ void stopWorking(){
    working = false;
  }
  
  // see if the find all checkbox is checked
  private boolean findAll(){
    return cbFindAllFiles.isSelected();
  }
  
  protected SelectPanel getSelectPanel(){
    return selectPanel;
  }
  
  protected String getDBName(){
    return dbName;
  }
  
  protected DBPluginMgr getDBPluginMgr(){
    return dbPluginMgr;
  }
  
  protected String getTableName(){
    return tableName;
  }
  
   protected void initDB(String _dbName, String _tableName){

     dbName = _dbName;     
     tableName = _tableName;
     dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
     statusBar = GridPilot.getClassMgr().getStatusBar();
     
     // Get download dir or URL.
     // Default to either home dir on "home gridftp server"
     // as defined in config file, or user.home
     if((defaultURL==null || defaultURL.equals("")) && GridPilot.GRID_HOME_URL!=null){
       defaultURL = GridPilot.GRID_HOME_URL;
     }
     else if(defaultURL==null || defaultURL.equals("")){
       defaultURL = "~";
     }
     
     if(defaultURL.startsWith("~")){
       try{
         String userHome = System.getProperty("user.home");
         if(userHome.endsWith(File.separator) || userHome.endsWith("/")){
           userHome = userHome.substring(0, userHome.length()-1);
         }
         defaultURL = userHome + "/" +
         (defaultURL.length()>1 ? defaultURL.substring(2) : "");
       }
       catch(Exception e){
         e.printStackTrace();
       }
     }
     
     //GridPilot.gridHomeURL = defaultURL;
          
     identifier = MyUtil.getIdentifierField(dbPluginMgr.getDBName(), tableName);
     
     if(tableName.equalsIgnoreCase("jobDefinition")){
       jobDefIdentifier = MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "jobDefinition");
     }

     defaultFields = DBPluginMgr.getDBDefFields(dbName, tableName);
     Debug.debug("Default fields "+defaultFields.length, 3);

     fieldNames = dbPluginMgr.getFieldNames(tableName);
     
     hiddenFields = dbPluginMgr.getDBHiddenFields(tableName);
     Debug.debug("Hidden fields "+MyUtil.arrayToString(hiddenFields), 3);
     
     // Pass on only non-hidden fields to
     // Table. Perhaps rethink: - Table hides fields...
     Vector<String> fieldSet = new Vector<String>();
     boolean ok;
     for(int i=0; i<fieldNames.length; ++i){
       ok = true;
       for(int j=0; j<hiddenFields.length; ++j){
         //Debug.debug("Checking fields for hiding: "+fieldNames[i]+"<->"+hiddenFields[j], 3);
         if(fieldNames[i].equalsIgnoreCase(hiddenFields[j])){
           ok = false;
           break;
         }
       }
       if(ok){
         fieldSet.add(fieldNames[i]);
       }
     }
     
     fieldNames = new String[fieldSet.size()];
     for(int i=0; i<fieldSet.size(); ++i){
       fieldNames[i] = fieldSet.get(i).toString();
     }
     
     // Set the timeout for querying file catalogs
     String timeout = GridPilot.getClassMgr().getConfigFile().getValue(dbName, "File catalog timeout");
     if(timeout!=null){
       try{
         fileCatalogTimeout = Integer.parseInt(timeout);
       }
       catch(Exception e){
         GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not parse file catalog timeout", e);
       }
     }

   }
   
   public MyJTable getTable(){
     return tableResults;
   }

   private String getDatasetName(String datasetID) {
     // Get the dataset name and id
     // Well, again: DQ2 cannot lookup dataset name from id...
     //String datasetName = dbPluginMgr.getDatasetName(datasetID);
     String [] datasetNameFields = MyUtil.getFileDatasetReference(dbPluginMgr.getDBName());
     String datasetNameField = datasetNameFields[0];
     int datasetNameIndex = -1;
     String [] columnNames = tableResults.getColumnNames();
     for(int i=0; i<tableResults.getColumnNames().length; ++i){
       if(columnNames[i].equalsIgnoreCase(datasetNameField)){
         datasetNameIndex = i;
         break;
       }
     }
     Debug.debug("dataset fields "+datasetNameField+"-->"+datasetNameIndex+":"+
         MyUtil.arrayToString(columnNames), 3);
     String datasetName = (String) tableResults.getUnsortedValueAt(
         tableResults.getSelectedRow(), datasetNameIndex);
     return datasetName;
   }

  private void setFieldArrays(){
    Vector<String> shownSet = new Vector<String>();  
    boolean ok = true;
    boolean hiddenOk = true;
    Debug.debug("Finding fields to show from: "+MyUtil.arrayToString(defaultFields), 3);
    for(int i=0; i<defaultFields.length; ++i){
      ok = false;
      for(int j=0; j<fieldNames.length; ++j){
        Debug.debug("Checking fields for showing: "+defaultFields[i]+"<->"+fieldNames[j], 3);
        if(defaultFields[i].equalsIgnoreCase(fieldNames[j]) ||
            defaultFields[i].equalsIgnoreCase("*")){
          ok = true;
          break;
        }
      }
      hiddenOk = true;
      for(int j=0; j<hiddenFields.length; ++j){
        //Debug.debug("Checking fields for hiding "+defaultFields[i]+"<->"+hiddenFields[j], 3);
        if(defaultFields[i].equalsIgnoreCase(hiddenFields[j]) &&
            !hiddenFields[j].equalsIgnoreCase("*")){
          hiddenOk = false;
          break;
        }
      }
      if(ok && hiddenOk){
        Debug.debug("Showing "+defaultFields[i], 3);
        shownSet.add(defaultFields[i]);
      }
    }
          
    if(selectPanel!=null){
      Debug.debug("selectPanel fields: "+selectPanel.getDisplayFieldsCount(), 3);
      for(int i=0; i<selectPanel.getDisplayFieldsCount(); ++i){
        SPanel.DisplayPanel cb =
          ((SPanel.DisplayPanel) selectPanel.getDisplayPanel(i));
        if(!shownSet.contains(cb.getSelected())){
          shownSet.add(cb.getSelected());
        }
      }    
    }

    shownFields = new String[shownSet.size()];
    for(int i=0; i<shownSet.size(); ++i){
      shownFields[i] = shownSet.get(i);
    }
     
    for(int k=0; k<shownFields.length; ++k){
      shownFields[k] = tableName+"."+shownFields[k];
    }
    Debug.debug("shownFields: "+shownFields.length, 3);
     
    // Set the default values of the selection drop downs.
    // selectFields only used by clear().
    Vector<String> selectSet = new Vector<String>();  
    for(int i=0; i<defaultFields.length; ++i){
      boolean fieldOk = false;
      for(int j=0; j<fieldNames.length; ++j){
        if(fieldNames[j].equalsIgnoreCase(defaultFields[i])){
          fieldOk = true;
          break;
        }
      }
      if(fieldOk){
        Debug.debug("Selecting "+defaultFields[i], 3);
        selectSet.add(defaultFields[i]);
      }
    }
     
    selectFields = new String[selectSet.size()];
    for(int i=0; i<selectSet.size(); ++i){
      selectFields[i] = selectSet.get(i).toString();
    }
     
    for(int k=0; k<selectFields.length; ++k){
      selectFields[k] = tableName+"."+selectFields[k];
    }
  }
   
  private void initButtons(){
    
    bDeleteRecords = MyUtil.mkButton("clear.png", "Delete", "Delete record(s)");
    bCreateRecords = MyUtil.mkButton("file_new.png", "New", "Define new record(s)");
    bEditRecord = MyUtil.mkButton("edit.png", "Edit", "Edit record");
    bViewJobDefinitions = MyUtil.mkButton("find.png", "Show jobDefinition(s)", "Show jobDefinition(s)");
    bViewFiles = MyUtil.mkButton("find.png", "Show file(s)", "Show file(s)");
    bDefineJobDefinitions = MyUtil.mkButton("file_new.png", "Create jobDefinition(s)", "Create jobDefinition(s)");

    bSearch =  MyUtil.mkButton("find.png", SEARCH_TEXT, SEARCH_MOUSEOVER_TEXT);
    bClear =  MyUtil.mkButton("clear.png", "Clear", "Clear");
    bNext =  MyUtil.mkButton1("next.png", "Next search results", ">>");
    bPrevious =  MyUtil.mkButton1("previous.png", "Previous search results", "<<");
    bReplicate =  MyUtil.mkButton("replicate.png", "Replicate file(s)", "Replicate file(s) from or to a remote server");
    bSubmit =  MyUtil.mkButton("run.png", "Submit job(s)", "Submit job(s) to a computing backend");
    bMonitor =  MyUtil.mkButton("monitor.png", "Monitor", "Monitor job(s)");
    bProcessDatasets =  MyUtil.mkButton("run.png", "Run", "Run application(s)/dataset(s)");
    bMonitorDatasets =  MyUtil.mkButton("monitor.png", "Monitor", "Monitor job(s) of application(s)/dataset(s)");
    bCleanupDatasets =  MyUtil.mkButton("clean.png", "Cleanup", "Cleanup job(s) and file(s) of application(s)/dataset(s)");

  }
   
  /**
  * GUI initialization
  */
  protected void initGUI() throws Exception{
    
    GridPilot.getClassMgr().getGlobalFrame().getContentPane().setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    
    initShowHideFilter();
    
    try{
      initButtons();
    }
    catch(Exception e){
    }
    
    tableResults = new MyJTable(hiddenFields, fieldNames, GridPilot.JOB_COLOR_MAPPING);
    
    menuEditCopy = GridPilot.getClassMgr().getGlobalFrame().getMenuEditCopy();
    menuEditCut = GridPilot.getClassMgr().getGlobalFrame().getMenuEditCut();
    menuEditPaste = GridPilot.getClassMgr().getGlobalFrame().getMenuEditPaste();

    this.setLayout(new BorderLayout());

    // SelectPanel
    selectPanel = new SelectPanel(tableName, fieldNames);
    selectPanel.initGUI();
    setFieldArrays();
    clear();
    
    spSelectPanel.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_NEVER);
    spSelectPanel.getViewport().add(selectPanel);

    panelSelectPanel.add(spSelectPanel,
        new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    panelSelectPanel.add(showHidePanel,
        new GridBagConstraints(0, 1, 1, 1, 1.0, 0.0,
            GridBagConstraints.WEST,
            GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));
    panelSelectPanel.add(pButtonSelectPanel,
        new GridBagConstraints(0, 1, 1, 1, 1.0, 0.0,
            GridBagConstraints.EAST,
            GridBagConstraints.NONE,new Insets(10, 10, 10, 10), 0, 0));
    panelSelectPanel.validate();

    selectPanel.setConstraint(MyUtil.getNameField(dbPluginMgr.getDBName(), tableName), "", 1);
    
    setEnterKeyListener();
    
    //tableResults.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
    tableResults.setAutoResizeMode(JTable.AUTO_RESIZE_SUBSEQUENT_COLUMNS);
    spTableResults.getViewport().add(tableResults);

    panelSelectPanel.setPreferredSize(new Dimension(0, 180));
    
    this.add(panelSelectPanel, BorderLayout.PAGE_START);
    this.add(spTableResults, BorderLayout.CENTER);
    this.add(pButtonTableResults, BorderLayout.PAGE_END);

    // Disable clipboard handling inherited from JPanel
    TransferHandler th = new TransferHandler(null);
    tableResults.setTransferHandler(th);
    
    setCopyPasteKeyListeners();
    
    initSearchButtons();
    
    addDBDescription();
    
    // This is to pad with empty space and keep the table results
    // full width
    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.HORIZONTAL;
    pButtonTableResults.add(new JLabel(), ct);
    
    if(tableName.equalsIgnoreCase("dataset")){
      
      miWithInput.addActionListener(new ActionListener(){
        public void actionPerformed(final ActionEvent e){
          createDatasets(true);
        }});
      miWithoutInput.addActionListener(new ActionListener(){
        public void actionPerformed(final ActionEvent e){
          createDatasets(false);
        }});
      pmCreateDSMenu.add(miWithInput);
      pmCreateDSMenu.add(miWithoutInput);
      
      tableResults.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          if(e.getClickCount()==2){
            editDataset();
          }
        }
      });
      
      for(int i=0; i<GridPilot.CS_NAMES.length; ++i){
        Debug.debug("Checking CS "+GridPilot.CS_NAMES[i], 2);
        if(!MyUtil.checkCSEnabled(GridPilot.CS_NAMES[i])){
          continue;
        }
        JMenuItem miP = new JMenuItem(GridPilot.CS_NAMES[i]);
        miP.addActionListener(new ActionListener(){
          public void actionPerformed(final ActionEvent e){
            processDatasets(e);
          }});
        Debug.debug("Adding CS "+GridPilot.CS_NAMES[i], 2);
        pmProcessMenu.add(miP);
      }

      bProcessDatasets.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          bProcess_mousePressed();
        }
      });
            
      bMonitorDatasets.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          monitorDatasets();
        }
      });
            
      bCleanupDatasets.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          cleanupDatasets();
        }
      });
            
      pButtonTableResults.add(bProcessDatasets);
      pButtonTableResults.add(bMonitorDatasets);
      pButtonTableResults.add(bCleanupDatasets);
      
      if(SHOW_DB_BUTTONS){
        initDSEditButtons();
        pButtonTableResults.add(new JLabel("|"));
        pButtonTableResults.add(bViewFiles);
        pButtonTableResults.add(bViewJobDefinitions);
        pButtonTableResults.add(bDefineJobDefinitions);
        pButtonTableResults.add(new JLabel("|"));
        pButtonTableResults.add(bCreateRecords);
        pButtonTableResults.add(bEditRecord);
        pButtonTableResults.add(bDeleteRecords);
      }
      
      pButtonSelectPanel.add(bSearch);
      pButtonSelectPanel.add(bClear);
      
      bProcessDatasets.setEnabled(false);
      bMonitorDatasets.setEnabled(false);
      bCleanupDatasets.setEnabled(false);
      bViewFiles.setEnabled(false);
      bViewJobDefinitions.setEnabled(false);
      bDefineJobDefinitions.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecords.setEnabled(false);
    }
    else if(tableName.equalsIgnoreCase("file")){
      
      tableResults.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          if(e.getClickCount()==2){
            editFile();
          }
        }
      });
      
      bReplicate.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          download(null, null);
        }
      });
            
      pButtonTableResults.add(bReplicate);
      if(SHOW_DB_BUTTONS){
        initFileEditButtons();
        pButtonTableResults.add(new JLabel("|"));
        pButtonTableResults.add(bEditRecord);
        //pButtonTableResults.add(bCreateRecords);
        pButtonTableResults.add(bDeleteRecords);
      }
      // For files, add next/previous buttons
      bPrevious.setEnabled(false);
      bNext.setEnabled(false);
      pButtonSelectPanel.add(bPrevious);
      pButtonSelectPanel.add(bNext);
      pButtonSelectPanel.add(new JLabel("  "));
      // Add tickbox to search panel
      JPanel pFindAll = new JPanel();
      pFindAll.add(new JLabel("Find all PFN(s)"));
      cbFindAllFiles.setToolTipText("Find all physical file names (URLs)");
      pFindAll.add(cbFindAllFiles);
      pButtonSelectPanel.add(pFindAll);
      pButtonSelectPanel.add(new JLabel(" "));
      pButtonSelectPanel.add(bSearch);
      pButtonSelectPanel.add(bClear);
      bEditRecord.setEnabled(false);
      bDeleteRecords.setEnabled(false);
      bReplicate.setEnabled(false);
    }
    else if(tableName.equalsIgnoreCase("jobDefinition")){
      
      tableResults.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          if(e.getClickCount()==2){
            editJobDef();
          }
        }
      });

      bSubmit.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          bSubmit_mousePressed();
        }
      });
      
      bMonitor.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          monitorJobs();
        }
      });
      
      for(int i=0; i<GridPilot.CS_NAMES.length; ++i){
        Debug.debug("Checking CS "+GridPilot.CS_NAMES[i], 2);
        if(!MyUtil.checkCSEnabled(GridPilot.CS_NAMES[i])){
          continue;
        }
        JMenuItem mi = new JMenuItem(GridPilot.CS_NAMES[i]);
        mi.addActionListener(new ActionListener(){
          public void actionPerformed(final ActionEvent e){
            submit(e);
          }});
        Debug.debug("Adding CS "+GridPilot.CS_NAMES[i], 2);
        pmSubmitMenu.add(mi);
      }

      pButtonTableResults.add(bSubmit);
      pButtonTableResults.add(bMonitor);
      if(SHOW_DB_BUTTONS){
        initJobDefEditButtons();
        pButtonTableResults.add(new JLabel("|"));
        pButtonTableResults.add(bCreateRecords);
        pButtonTableResults.add(bEditRecord);
        pButtonTableResults.add(bDeleteRecords);
      }
      pButtonSelectPanel.add(bSearch);
      pButtonSelectPanel.add(bClear);
      bSubmit.setEnabled(false);
      bMonitor.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecords.setEnabled(false);
    }
    else if(tableName.equalsIgnoreCase("executable")){
      
      tableResults.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          if(e.getClickCount()==2){
            editExecutable();
          }
        }
      });
      
      if(SHOW_DB_BUTTONS){
        initExecutableEditbuttons();
        pButtonTableResults.add(bCreateRecords);
        pButtonTableResults.add(bEditRecord);
        pButtonTableResults.add(bDeleteRecords);
      }
      pButtonSelectPanel.add(bSearch);
      pButtonSelectPanel.add(bClear);
      bEditRecord.setEnabled(false);
      bDeleteRecords.setEnabled(false);
    }    
    else if(tableName.equalsIgnoreCase("runtimeEnvironment")){
      
      tableResults.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          if(e.getClickCount()==2){
            editRuntimeEnvironment();
          }
        }
      });
      
      if(SHOW_DB_BUTTONS){
        initRuntimeEditButtons();
        pButtonTableResults.add(bCreateRecords);
        pButtonTableResults.add(bEditRecord);
        pButtonTableResults.add(bDeleteRecords);
      }
      pButtonSelectPanel.add(bSearch);
      pButtonSelectPanel.add(bClear);
      bEditRecord.setEnabled(false);
      bDeleteRecords.setEnabled(false);
      menuEditCopy.setEnabled(false);
      menuEditCut.setEnabled(false);
      menuEditPaste.setEnabled(false);
    }
    showFilter(dbPluginMgr.isFileCatalog() && !dbPluginMgr.isJobRepository());
    updateUI();
    
    GridPilot.getClassMgr().getGlobalFrame().getContentPane().setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
    
  }

  private void addDBDescription() {
    //pButtonSelectPanel.add(dbLabel);
    //pButtonSelectPanel.add(new JLabel("   "));
    JLabel dbLabel = new JLabel();
    showHidePanel.add(new JLabel("  "));
    showHidePanel.add(dbLabel);
    switch(MyUtil.BUTTON_DISPLAY){
      case MyUtil.ICON_AND_TEXT:
        try{
          URL imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH +
             (dbPluginMgr.getDBDescription().matches("(?i).*local.*")?"db_local.png":"db_remote.png"));
          ImageIcon imgIcon = new ImageIcon(imgURL);
          dbLabel.setText(dbPluginMgr.getDBDescription());
          dbLabel.setToolTipText(dbName);
          dbLabel.setIcon(imgIcon);
        }
        catch(Exception e){
          e.printStackTrace();
        }
        break;
      case MyUtil.ICON_ONLY:
        try{
          URL imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH +
             (dbPluginMgr.getDBDescription().matches("(?i).*local.*")?"db_local.png":"db_remote.png"));
          ImageIcon imgIcon = new ImageIcon(imgURL);
          dbLabel.setIcon(imgIcon);
          dbLabel.setToolTipText(dbName+": "+dbPluginMgr.getDBDescription());
        }
        catch(Exception e){
          e.printStackTrace();
        }
        break;
      case MyUtil.TEXT_ONLY:
        dbLabel = new JLabel(dbName+" : "+dbPluginMgr.getDBDescription());
        break;
    }
  }

  private void initShowHideFilter() {
    bShowFilter = MyUtil.mkButton1("down.png", "Show search filter", "+");
    bHideFilter = MyUtil.mkButton1("up.png", "Hide search filter", "-");
    bShowFilter.setPreferredSize(new Dimension(22, 26));
    bHideFilter.setPreferredSize(new Dimension(22, 26));
    bShowFilter.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        showFilter(true);
      }
    });
    bHideFilter.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        showFilter(false);
      }
    });
    showHidePanel.add(bShowFilter);
    showHidePanel.add(bHideFilter);
  }
  
  private void showFilter(final boolean ok){
    //SwingUtilities.invokeLater(
      //new Runnable(){
        //public void run(){
          Debug.debug("Showing filter: "+ok, 2);
          spSelectPanel.setVisible(ok);
          bHideFilter.setVisible(ok);
          bShowFilter.setVisible(!ok);
          bClear.setVisible(ok);
          panelSelectPanel.setPreferredSize(new Dimension(0, ok?180:40));
        //}
      //}
    //);
  }

  private void initRuntimeEditButtons() {
    bCreateRecords.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        createRuntimeEnvironment();
      }
    });

    bEditRecord.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editRuntimeEnvironment();
      }
    });

    bDeleteRecords.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteRuntimeEnvironments();
      }
    });
  }

  private void initExecutableEditbuttons() {
    bCreateRecords.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        createExecutable();
      }
    });

    bEditRecord.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editExecutable();
      }
    });

    bDeleteRecords.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteExecutables();
      }
    });
  }

  private void initJobDefEditButtons() {
    bEditRecord.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editJobDef();
      }
    });

    bDeleteRecords.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteJobDefs();
      }
    });

    bCreateRecords.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        createJobDefinitions();
      }
    });
  }

  private void initFileEditButtons() {
    bEditRecord.setText("View record");
    bEditRecord.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editFile();
      }
    });

    bDeleteRecords.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteFiles();
      }
    });
  }

  private void initDSEditButtons() {
    
    bViewJobDefinitions.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        String [] ids = getSelectedIdentifiers();
        if(ids==null || ids.length==0){
          return;
        }
        viewJobDefinitions(ids);
      }
    });
    
    bViewFiles.addActionListener(
      new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){
          new Thread(){
            public void run(){
              viewFiles(false, getSelectedIdentifiers(), tableResults.getSelectedRows());
            }
          }.start();
        }
      }
    );

    bDefineJobDefinitions.addActionListener(
      new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){
          defineJobDefinitions();
        }
      }
    );
    
    bCreateRecords.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        bCreateDatasets_mousePressed(bCreateRecords);
      }
    });

    bEditRecord.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editDataset();
      }
    });

    bDeleteRecords.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteDatasets();
      }
    });
  }

  private void initSearchButtons() {
    //// buttons
    bSearch.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        cursor = -1;
        ResThread t = (new ResThread(){
          public void run(){
            try{
              search();
            }
            catch(Exception ee){
              statusBar.setLabel("ERROR: could not search "+ee.getMessage());
              Debug.debug("ERROR: could not search. "+ee.getMessage(), 1);
              ee.printStackTrace();
            }
          }
        });     
        t.start();
      }
    });
    bSearch.setText(SEARCH_TEXT);
    bSearch.setToolTipText(SEARCH_MOUSEOVER_TEXT);
    bClear.setToolTipText("Clear text field and reset filter");
    bNext.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        cursor = cursor+GridPilot.FILE_ROWS;
        ResThread t = (new ResThread(){
          public void run(){
            try{
              search();
            }
            catch(Exception ee){
              statusBar.setLabel("ERROR: could not search "+ee.getMessage());
              Debug.debug("ERROR: could not search. "+ee.getMessage(), 1);
              ee.printStackTrace();
            }
          }
        });     
        t.start();
      }
    });
    bPrevious.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        cursor = cursor-GridPilot.FILE_ROWS;
        ResThread t = (new ResThread(){
          public void run(){
            try{
              search();
            }
            catch(Exception ee){
              statusBar.setLabel("ERROR: could not search "+ee.getMessage());
              Debug.debug("ERROR: could not search. "+ee.getMessage(), 1);
              ee.printStackTrace();
            }
          }
        });     
        t.start();
      }
    });
    bClear.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        clear();
      }
    });
  }

  private void setCopyPasteKeyListeners() {
    tableResults.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        //Debug.debug("key code: "+KeyEvent.getKeyText(e.getKeyCode()), 3);
        if(e.getKeyCode()==KeyEvent.VK_F1){
          //menuHelpAbout_actionPerformed();
        }
        else if(KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("x")){
          if(MyUtil.isModifierDown(e)){
            cut();
          }
        }
        else if(KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("c")){
          if(MyUtil.isModifierDown(e)){
            copy();
          }
        }
        else if(KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("v")){
          if(MyUtil.isModifierDown(e)){
            paste();
          }
        }
      }
    });
  }

  /**
   * Listen for enter key in text field
   */
  private void setEnterKeyListener() {
    selectPanel.addListenerForEnter(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        // Change the text on the search button
        bSearch.setText(SEARCH_TEXT);
        bSearch.setToolTipText(SEARCH_MOUSEOVER_TEXT);
        switch(e.getKeyCode()){
          case KeyEvent.VK_ENTER:
            cursor = -1;
            ResThread t = (new ResThread(){
              public void run(){
                try{
                  search();
                }
                catch(Exception ee){
                  statusBar.setLabel("ERROR: could not search "+ee.getMessage());
                  Debug.debug("ERROR: could not search. "+ee.getMessage(), 1);
                  ee.printStackTrace();
                }
              }
            });     
            t.start();
        }
        if(KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("c") ||
            KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("x")){
          if(MyUtil.isModifierDown(e)){
            clipboardOwned = false;
            menuEditPaste.setEnabled(clipboardOwned);
          }
        }

      }
    });
  }

  public String getTitle(){
    return GridPilot.getTabDisplayName(dbName, tableName);
  }

  public void panelShown(){
    boolean rowsAreSelected = tableResults.getSelectedRows().length>0;
    Debug.debug("panelShown - selected "+rowsAreSelected, 1);
    menuEditCopy.setEnabled(rowsAreSelected);
    menuEditCut.setEnabled(rowsAreSelected);
    // Check if clipboard is of the form "db table id1 id2 id3 ..."
    String clip = getClipboardContents();
    String clips [] = null;
    if(clip!=null){
      clips = MyUtil.split(clip);
    }
    if(clips!=null && clips.length>2 && clips[1].equalsIgnoreCase(tableName)){
      clipboardOwned = true;
    }
    else{
      clipboardOwned = false;
    }
    menuEditPaste.setEnabled(clipboardOwned);
  }

  protected void dbMenuSelected(){
    int selectedRows = tableResults.getSelectedRows().length;
    boolean notFilePanel = !getTableName().equalsIgnoreCase("file");
    boolean datasetPanel = getTableName().equalsIgnoreCase("dataset");
    Debug.debug(getTableName()+" is "+(notFilePanel?"not":"")+" a file panel", 3);
    GridPilot.getClassMgr().getGlobalFrame().setDefineMenu(datasetPanel, selectedRows, notFilePanel);
  }
  
  protected void fileMenuSelected(){
    String [] selectedIds = getSelectedIdentifiers();
    boolean datasetPanel = getTableName().equalsIgnoreCase("dataset");
    GridPilot.getClassMgr().getGlobalFrame().setImportExportMenu(datasetPanel, selectedIds);
  }

  public void panelHidden(){
    Debug.debug("", 1);
  }

  /**
   * Returns identifiers of the selected jobDefinitions, corresponding to
   * jobDefinition.identifier in DB.
   */
  public String [] getSelectedIdentifiers(){

    int [] selectedRows = tableResults.getSelectedRows();
    String [] selectedIdentifiers = new String[selectedRows.length];
    for(int i=0; i<selectedIdentifiers.length; ++i){
      selectedIdentifiers[i] = identifiers[selectedRows[i]];
    }
    return selectedIdentifiers;
  }

  /**
   * Returns the Id of the first selected row, -1 if no row is selected.
   *
   * @return Id of the first selected row, -1 if no row is selected
   *
   */
  public String getSelectedIdentifier(){
    int selRow = tableResults.getSelectedRow();
    return (selRow==-1) ? "-1" : identifiers[selRow];
  }

  /**
   * Carries out search according to selection
   */
  private void search(){
    if(tableResults==null){
      searchRequest(true, false);
    }
    else{
      DBVectorTableModel tableModel = (DBVectorTableModel) tableResults.getModel();
      int sortColumn = tableModel.getColumnSort();
      boolean isAscending = tableModel.isSortAscending();
      String [] columnNames = tableModel.getColumnNames();
      int [] columnWidths = new int [columnNames.length];
      for(int i=0; i<columnNames.length; ++i){
        columnWidths[i] = tableResults.getColumn(columnNames[i]).getPreferredWidth();
      }
      searchRequest(sortColumn, isAscending, columnWidths);
    }
    /*remove(panelTableResults);
    add(panelTableResults, ct);*/
    //updateUI();
    //tableResults.updateUI()
  }

  /**
   * Resets fields to default fields and clear values
   */
  public void clear(){
    String [][] values = new String[selectFields.length][2];
    Debug.debug("Clearing "+selectFields.length, 3);
    for(int i=0; i<selectFields.length; ++i){
      String [] split = selectFields[i].split("\\.");
      if(split.length != 2){
          Debug.debug(selectFields[i] + " " + " : wrong format in config file ; " +
                    "should be : \ndefault dataset fields = table.field1 table.field2", 3);
      }
      else{
        Debug.debug("Setting default value "+split[0]+" "+split[1],3);
        values[i][0] = split[0];
        values[i][1] = split[1];
      }
    }
    selectPanel.setDisplayFieldValues(values);
    selectPanel.resetConstraintList(tableName);
    selectPanel.setConstraint(MyUtil.getNameField(dbPluginMgr.getDBName(), tableName), "", 1);
    selectPanel.updateUI();
  }
  
  private boolean waitForWorking(){
    setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    for(int i=0; i<1; ++i){
      if(!getWorking()){
        // retry 3 times with 3 seconds in between
        statusBar.setLabel("Busy, please wait...");
        try{
          Thread.sleep(3000);
        }
        catch(Exception e){
        }
        if(i==2){
          return false;
        }
        else{
          continue;
        }
      }
      else{
        break;
      }
    }
    setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
    return true;
  }

  /**
   * Request DBPluginMgr for select request from SelectPanel, and fill tableResults
   * with results. The request is performed in a separeted thread, avoiding to block
   * all the GUI during this action.
   * Called when button "Search" is clicked
   */
  private void searchRequest(final int sortColumn, final boolean isAscending,
      final int [] columnWidths){
    searchRequest(sortColumn, isAscending, columnWidths,
        /*true*/false/* hmm, did I have any good reason for putting this in the event thread?...*/,
        false);
  }

  private void searchRequest(final int sortColumn, final boolean isAscending,
      final int [] columnWidths, boolean runInEventThread, boolean waitForThread){
        
    workThread = new ResThread(){
      public void run(){
        if(!waitForWorking()){
          GridPilot.getClassMgr().getLogFile().addMessage("WARNING: table busy, search not performed");
          return;
        }
        statusBar.setLabel("Searching, please wait...");
        setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        statusBar.animateProgressBar();
        statusBar.setIndeterminateProgressBarToolTip("click here to cancel");
        statusBar.addIndeterminateProgressBarMouseListener(new MouseAdapter(){
          public void mouseClicked(MouseEvent me){
            setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
            statusBar.stopAnimation();
            workThread.interrupt();
          }
        });
        try{
          doSearchRequest();
          fixSort(sortColumn, isAscending, columnWidths);
        }
        catch(Exception e){
          e.printStackTrace();
          setException(e);
        }
        stopWorking();
        setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      }
    };
    
    if(waitForThread){
      if(runInEventThread){
        try{
          SwingUtilities.invokeAndWait(workThread);
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
      else{
        workThread.start();
        MyUtil.myWaitForThread(workThread, dbName, 0, "searchRequest");
      }
    }
    else{
      if(runInEventThread){
        SwingUtilities.invokeLater(workThread);
      }
      else{
        workThread.start();
      }
    }
  }
  
  private void doSearchRequest() throws InterruptedException, InvocationTargetException {
    
    String selectRequest;
    
    if(SwingUtilities.isEventDispatchThread()){
      selectRequest = selectPanel.getRequest(shownFields);
    }
    else{
      MyResThread rt = new MyResThread(){
        String selectRequest;
         public void run(){
           setFieldArrays();
           selectRequest = selectPanel.getRequest(shownFields);
         }
         public String getStringRes(){
           return selectRequest;
         }
       };
      SwingUtilities.invokeAndWait(rt);
      selectRequest = rt.getStringRes();
    }

    if(selectRequest==null){
      return;
    }
    Debug.debug("Select request "+selectRequest, 2);
    res = dbPluginMgr.select(selectRequest, identifier, findAll());
    
    if(SwingUtilities.isEventDispatchThread()){
      setSearchTableResults();
      setSearchTable();
      bSearch.setText(REFRESH_TEXT);
      bSearch.setToolTipText(REFRESH_MOUSEOVER_TEXT);
    }
    else{
      SwingUtilities.invokeAndWait(
        new Runnable(){
          public void run(){
            setSearchTableResults();
            setSearchTable();
            bSearch.setText(REFRESH_TEXT);
            bSearch.setToolTipText(REFRESH_MOUSEOVER_TEXT);
          }
        }
      );
    }
  }
  
  private void setSearchTable() {
    bViewFiles.setEnabled(false);
    bViewJobDefinitions.setEnabled(false);
    bDefineJobDefinitions.setEnabled(false);
    bProcessDatasets.setEnabled(false);
    bMonitorDatasets.setEnabled(false);
    bCleanupDatasets.setEnabled(false);
    bEditRecord.setEnabled(false);
    bDeleteRecords.setEnabled(false);
    bSubmit.setEnabled(false);
    bMonitor.setEnabled(false);
    menuEditCopy.setEnabled(false);
    menuEditCut.setEnabled(false);
    menuEditPaste.setEnabled(clipboardOwned);
            
    identifiers = new String[tableResults.getRowCount()];
    // 'col' is the column with the jobDefinition identifier
    int col = tableResults.getColumnCount()-1;
    for(int i=0; i<tableResults.getColumnCount(); ++i){
      Debug.debug("Column: "+tableResults.getColumnName(i)+"<->"+identifier, 3);
      if(tableResults.getColumnName(i).equalsIgnoreCase(identifier)){
        col = i;
        Debug.debug("OK: "+i, 3);
        break;
      }
    }
    for(int i=0; i<identifiers.length; ++i){
      if(tableResults.getUnsortedValueAt(i, col)!=null){
        identifiers[i] = tableResults.getUnsortedValueAt(i, col).toString();
      }
      else{
        identifiers[i] = "-1";
      }
    }

    if(tableName.equalsIgnoreCase("dataset")){
      setDatasetTable();
    }
    else if(tableName.equalsIgnoreCase("file")){
      setFileTable();
    }
    else if(tableName.equalsIgnoreCase("jobDefinition")){
      setJobDefTable();
    }
    else if(tableName.equalsIgnoreCase("executable")){
      setExecutableTable();
    }
    else if(tableName.equalsIgnoreCase("runtimeEnvironment")){
      setRuntimeEnvironmentTable();
    }
    
    statusBar.stopAnimation();
    if(tableName.equalsIgnoreCase("file")){
      if(res!=null && res.values!=null && res.values.length>0){
        statusBar.setLabel("Records found: "+res.values.length+
            ". Displaying "+(cursor==-1?"1":""+(cursor+1))+" to "+
            ((cursor==-1?0:cursor)+tableResults.getRowCount()));
      }
      else{
        statusBar.setLabel("No records found");
      }
    }
    else{
      statusBar.setLabel("Records found: "+tableResults.getRowCount());
    }
  }

  private void setDatasetTable() {
    tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
    tableResults.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        if (e.getValueIsAdjusting()) return;
        ListSelectionModel lsm = (ListSelectionModel)e.getSource();
        //Debug.debug("lsm indices: "+
        //    lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
        bViewFiles.setEnabled(!lsm.isSelectionEmpty() &&
            lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
        // We assume that there are only two kinds of databases:
        // runtime/executable/dataset/job catalogs and dataset/file catalogs.
        bViewJobDefinitions.setEnabled(dbPluginMgr.isJobRepository() && !lsm.isSelectionEmpty() &&
            lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
        boolean ok = dbPluginMgr.isJobRepository() && !lsm.isSelectionEmpty();
        bDefineJobDefinitions.setEnabled(ok);
        bProcessDatasets.setEnabled(ok);
        bMonitorDatasets.setEnabled(ok);
        bCleanupDatasets.setEnabled(ok);
        bDeleteRecords.setEnabled(!lsm.isSelectionEmpty());
        bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
            lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
        miEdit.setEnabled(!lsm.isSelectionEmpty() &&
            lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
        miImportFiles.setEnabled(dbPluginMgr.isFileCatalog() && !lsm.isSelectionEmpty() &&
            lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
        miExportDatasets.setEnabled(dbPluginMgr.isJobRepository() && !lsm.isSelectionEmpty());
        menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
        menuEditCut.setEnabled(!lsm.isSelectionEmpty());
        menuEditPaste.setEnabled(clipboardOwned);
      }
    });
    if(!menuSet){
      Debug.debug("Making dataset menu", 3);
      makeDatasetMenu();
      Debug.debug("Done making dataset menu", 3);
      menuSet = true;
    }
  }
  
  private void setFileTable() {
    tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
    tableResults.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        if (e.getValueIsAdjusting()) return;
        ListSelectionModel lsm = (ListSelectionModel)e.getSource();
        //Debug.debug("lsm indices: "+
        //    lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
        bReplicate.setEnabled(!lsm.isSelectionEmpty());
        bDeleteRecords.setEnabled(dbPluginMgr.isFileCatalog() && !lsm.isSelectionEmpty());
        bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
            lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
        miEdit.setEnabled(!lsm.isSelectionEmpty() &&
            lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
        menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
        menuEditCut.setEnabled(!lsm.isSelectionEmpty());
        menuEditPaste.setEnabled(clipboardOwned);
      }
    });
    if(!menuSet){
      makeFileMenu();
      menuSet = true;
    }
  }

  private void setJobDefTable() {
    tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
    tableResults.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        if (e.getValueIsAdjusting()) return;
        ListSelectionModel lsm = (ListSelectionModel)e.getSource();
        //Debug.debug("lsm indices: "+
        //    lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
        bSubmit.setEnabled(!lsm.isSelectionEmpty());
        bMonitor.setEnabled(!lsm.isSelectionEmpty());
        bDeleteRecords.setEnabled(!lsm.isSelectionEmpty());
        bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
            lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
        miEdit.setEnabled(!lsm.isSelectionEmpty() &&
            lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
        menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
        menuEditCut.setEnabled(!lsm.isSelectionEmpty());
        menuEditPaste.setEnabled(clipboardOwned);
      }
    });
    if(!menuSet){
      makeJobDefMenu();
      menuSet = true;
    }
  }

  private void setExecutableTable() {
    tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
    tableResults.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        if (e.getValueIsAdjusting()) return;
        ListSelectionModel lsm = (ListSelectionModel)e.getSource();
        //Debug.debug("lsm indices: "+
        //    lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
        bDeleteRecords.setEnabled(!lsm.isSelectionEmpty());
        bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
            lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
        miEdit.setEnabled(!lsm.isSelectionEmpty() &&
            lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
        menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
        menuEditCut.setEnabled(!lsm.isSelectionEmpty());
        menuEditPaste.setEnabled(clipboardOwned);
      }
    });
    if(!menuSet){
      makeExecutableMenu();
      menuSet = true;
    }
  }

  private void setRuntimeEnvironmentTable() {
    tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
    tableResults.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        if (e.getValueIsAdjusting()) return;
        ListSelectionModel lsm = (ListSelectionModel)e.getSource();
        //Debug.debug("lsm indices: "+
        //    lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
        bDeleteRecords.setEnabled(!lsm.isSelectionEmpty());
        bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
            lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
        miEdit.setEnabled(!lsm.isSelectionEmpty() &&
            lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
        menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
        menuEditCut.setEnabled(!lsm.isSelectionEmpty());
        menuEditPaste.setEnabled(clipboardOwned);
      }
    });
    if(!menuSet){
      makeRuntimeEnvironmentMenu();
      menuSet = true;
    }
  }

  private void setSearchTableResults() {
    
    if(res==null){
      return;
    }
    
    Object[][] vals = null;
    if(tableName.equalsIgnoreCase("file")){
      Debug.debug("Searching from cursor "+cursor, 2);
      if(cursor==-1){
        if(GridPilot.FILE_ROWS>0 && res.values.length>GridPilot.FILE_ROWS){
          vals = new String [GridPilot.FILE_ROWS][];
          System.arraycopy(res.values, 0, vals, 0, GridPilot.FILE_ROWS);
          bNext.setEnabled(true);
          bPrevious.setEnabled(false);
          cursor = 0;
        }
        else{
          vals = res.values;
          bNext.setEnabled(false);
          bPrevious.setEnabled(false);
          cursor = -1;
        }
      }
      else{
        // we've reached the end
        if(cursor+GridPilot.FILE_ROWS>res.values.length){
          vals = new String [res.values.length-cursor][];
          System.arraycopy(res.values, cursor, vals, 0, res.values.length-cursor);
          bNext.setEnabled(false);
          bPrevious.setEnabled(true);
        }
        else{
          vals = new String [GridPilot.FILE_ROWS][];
          System.arraycopy(res.values, cursor, vals, 0, GridPilot.FILE_ROWS);
          if(res.values.length-cursor>GridPilot.FILE_ROWS){
            bNext.setEnabled(true);
          }
          else{
            bNext.setEnabled(false);
          }
          bPrevious.setEnabled(cursor>0);
        }
      }
    }
    else{
      vals = res.values;
    }
    
    Debug.debug("Setting table", 3);
    try{
      tableResults.setTable(vals, res.fields);
    }
    catch(Exception e1){
      e1.printStackTrace();
      GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not set values in table.", e1);
    }
    Debug.debug("Done setting table", 3);

  }
  
  private void fixSort(final int sortColumn, final boolean isAscending, final int [] columnWidths) throws InterruptedException, InvocationTargetException{
    if(SwingUtilities.isEventDispatchThread()){
      doFixSort(sortColumn, isAscending, columnWidths);
    }
    else{
      SwingUtilities.invokeAndWait(
        new Runnable(){
          public void run(){
            doFixSort(sortColumn, isAscending, columnWidths);
          }
        }
      );
    }
  }
  
  private void doFixSort(int sortColumn, boolean isAscending, int [] columnWidths){
    Debug.debug("Sorting", 3);
    if(sortColumn>-1){
      Debug.debug("Sorting: "+sortColumn+":"+isAscending, 3);
      ((DBVectorTableModel) tableResults.getModel()).sort(sortColumn, isAscending);
    }
    Debug.debug("Setting column widths", 3);
    if(columnWidths!=null){
      String [] columnNames = ((DBVectorTableModel) tableResults.getModel()).getColumnNames();
      // If we have changed the displayed columns, there's no point...
      if(columnWidths.length==columnNames.length){
        for(int i=0; i<columnNames.length; ++i){
          tableResults.getColumn(columnNames[i]).setPreferredWidth(
              columnWidths[i]);
        }
      }
    }
    Debug.debug("Updating edit menu", 3);
    GridPilot.getClassMgr().getGlobalFrame().getMenuEdit().updateUI();
    Debug.debug("Done Updating edit menu", 3);
  }
  
  protected void searchRequest(boolean runInEventThread, boolean waitForThread){
    DBVectorTableModel tableModel = (DBVectorTableModel) tableResults.getModel();
    tableModel.ascending = true;
    searchRequest(-1, tableModel.ascending, null, runInEventThread, waitForThread);
  }
  
  /**
   * Add menu items to the table with search results. This function is called from within DBPanel
   * after the results table is filled
   */
  private void makeDatasetMenu(){
    Debug.debug("Making dataset menu", 3);
    JMenuItem miCreateJobDefinitions = new JMenuItem("Create job definition(s)");
    miCreateJobDefinitions.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        defineJobDefinitions();
      }
    });
    JMenuItem miViewJobDefinitions = new JMenuItem("Show job definition(s)");
    miViewJobDefinitions.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        String [] ids = getSelectedIdentifiers();
        if(ids==null || ids.length==0){
          return;
        }
        viewJobDefinitions(ids);
      }
    });
    miImportFiles.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        ResThread rt = new ResThread(){
          public void run(){
            try{
              String datasetID = getSelectedIdentifier();
              String datasetName = getDatasetName(datasetID);
              ExportImport.importFiles(datasetID, datasetName, getDBPluginMgr());
            }
            catch(Exception e){
              setException(e);
              e.printStackTrace();
            }
            setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          }
        };
        rt.start();
        MyUtil.waitForThread(rt, "import", fileCatalogTimeout, "importFiles", GridPilot.getClassMgr().getLogFile());
      }
    });
    JMenuItem miViewFiles = new JMenuItem("Show file(s)");
    miViewFiles.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        new Thread(){
          public void run(){
            viewFiles(false, getSelectedIdentifiers(), tableResults.getSelectedRows());
          }
        }.start();
      }
    });
    JMenuItem miReplicateDataset = new JMenuItem("Replicate file(s)");
    miReplicateDataset.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        if(replicating){
          GridPilot.getClassMgr().getStatusBar().setLabel("Already replicating.");
          return;
        }
        replicate();
      }
    });
    JMenuItem miGetInfoOnDataset = new JMenuItem("Get info on file(s)");
    miGetInfoOnDataset.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        getInfo();
      }
    });
    miExportDatasets.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        exportDataset();
      }
    });
    JMenuItem miDelete = new JMenuItem("Delete");
    miDelete.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteDatasets();
      }
    });
    miEdit = new JMenuItem("Edit");
    miEdit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editDataset();
      }
    });
    miImportFiles.setEnabled(dbPluginMgr.isFileCatalog());
    miViewFiles.setEnabled(true);
    miReplicateDataset.setEnabled(true);
    miGetInfoOnDataset.setEnabled(true);
    miExportDatasets.setEnabled(true);
    miViewJobDefinitions.setEnabled(dbPluginMgr.isJobRepository());
    miCreateJobDefinitions.setEnabled(dbPluginMgr.isJobRepository());
    miDelete.setEnabled(true);
    miEdit.setEnabled(true);
    
    if(GridPilot.ADVANCED_MODE){
      tableResults.addMenuSeparator();
    }
    tableResults.addMenuItem(miImportFiles);
    if(GridPilot.ADVANCED_MODE){
      tableResults.addMenuSeparator();
      tableResults.addMenuItem(miExportDatasets);
      tableResults.addMenuSeparator();
    }

    tableResults.addMenuItem(miGetInfoOnDataset);
    tableResults.addMenuItem(miViewFiles);
    tableResults.addMenuItem(miViewJobDefinitions);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miReplicateDataset);
    tableResults.addMenuItem(miCreateJobDefinitions);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miEdit);
    tableResults.addMenuItem(miDelete);
  }

  private void makeExecutableMenu(){
    Debug.debug("Making executable menu", 3);
    JMenuItem miDelete = new JMenuItem("Delete");
    miEdit = new JMenuItem("Edit");
    miDelete.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteExecutables();
      }
    });
    miEdit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editExecutable();
      }
    });
    miDelete.setEnabled(true);
    miEdit.setEnabled(true);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miEdit);
    tableResults.addMenuItem(miDelete);
  }
  
  private void makeRuntimeEnvironmentMenu(){
    Debug.debug("Making runtime environment menu", 3);
    JMenuItem miDelete = new JMenuItem("Delete");
    miEdit = new JMenuItem("Edit");
    miDelete.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteRuntimeEnvironments();
      }
    });
    miEdit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editRuntimeEnvironment();
      }
    });
    miDelete.setEnabled(true);
    miEdit.setEnabled(true);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miEdit);
    tableResults.addMenuItem(miDelete);
  }

  private void makeFileMenu(){
    Debug.debug("Making file menu", 3);
    JMenuItem miDelete = new JMenuItem("Delete");
    miEdit = new JMenuItem("View");
    JMenuItem miDownload = new JMenuItem("Replicate file(s)");
    JMenuItem miLookupPFNs = new JMenuItem("Lookup PFN(s)");
    JMenuItem miCopyPFNs = new JMenuItem("Copy PFN(s) to clipboard");
    miDelete.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteFiles();
      }
    });
    miEdit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editFile();
      }
    });
    miDownload.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        download(null, null);
      }
    });
    miLookupPFNs.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        new Thread(){
          public void run(){               
            lookupPFNs(null, null);
          }
        }.start();
      }
    });
    miCopyPFNs.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        copyPFNs();
      }
    });
    miDelete.setEnabled(dbPluginMgr.isFileCatalog());
    miEdit.setEnabled(true);
    miDownload.setEnabled(true);
    boolean hasPFNs = false;
    if(dbPluginMgr.isFileCatalog()){
      String [] fileFields = dbPluginMgr.getFieldNames("file");
      for(int i=0; i<fileFields.length; ++i){
        if(fileFields[i].equalsIgnoreCase("pfns")){
          hasPFNs = true;
          break;
        }
      }
    }
    miLookupPFNs.setEnabled(hasPFNs);
    miCopyPFNs.setEnabled(hasPFNs);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miDownload);
    tableResults.addMenuItem(miLookupPFNs);
    tableResults.addMenuItem(miCopyPFNs);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miEdit);
    tableResults.addMenuItem(miDelete);
  }

  private void makeJobDefMenu(){
    JMenuItem miDelete = new JMenuItem("Delete");
    miEdit = new JMenuItem("Edit");
    jmSetFieldValue = new JMenu("Set field value");
    JMenu jmSubmit = new JMenu("Submit job(s)");
    JMenuItem miMonitor = new JMenuItem("Monitor job(s)");
    String [] fieldNames = tableResults.getColumnNames();
    JMenuItem [] miSetFields = new JMenuItem[fieldNames.length];
    for(int i=0; i<fieldNames.length; ++i){
      if(fieldNames[i]!=null && !fieldNames[i].equalsIgnoreCase("") &&
          !fieldNames[i].equalsIgnoreCase(jobDefIdentifier)){
           miSetFields[i] = new JMenuItem(fieldNames[i]);
           miSetFields[i].setName(fieldNames[i]);
           miSetFields[i].addActionListener(new ActionListener(){
             public void actionPerformed(ActionEvent e){
               String choiceStr = JOptionPane.showInputDialog(GridPilot.getClassMgr().getGlobalFrame(),
                   "New value of "+((JMenuItem) e.getSource()).getName()+":", "",
                   JOptionPane.QUESTION_MESSAGE);
               Debug.debug("choiceStr: "+choiceStr, 3);
               if(choiceStr==null || choiceStr.equals("")){
                 return;
               }
               else{
                 setFieldValues(((JMenuItem) e.getSource()).getName(), choiceStr);
               }
             }
           });
           jmSetFieldValue.add(miSetFields[i]);
      }
    }
    for(int i=0; i<GridPilot.CS_NAMES.length; ++i){
      if(!MyUtil.checkCSEnabled(GridPilot.CS_NAMES[i])){
        continue;
      }
      JMenuItem mi = new JMenuItem(GridPilot.CS_NAMES[i]);
      mi.addActionListener(new ActionListener(){
        public void actionPerformed(final ActionEvent e){
              submit(e);
        }});
      jmSubmit.add(mi);
    }
    miMonitor.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        monitorJobs();
      }
    });
    miDelete.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteJobDefs();
      }
    });
    miEdit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editJobDef();
      }
    });
    miDelete.setEnabled(true);
    miEdit.setEnabled(true);
    miMonitor.setEnabled(true);
    if(GridPilot.ADVANCED_MODE){
      tableResults.addMenuItem(jmSetFieldValue);
      tableResults.addMenuSeparator();
    }
    tableResults.addMenuItem(miMonitor);
    tableResults.addMenuItem(jmSubmit);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miEdit);
    tableResults.addMenuItem(miDelete);
  }
  
  private void setFieldValues(String field, String value){
    dbPluginMgr.setJobDefsField(getSelectedIdentifiers(), field, value);
    refresh();
  }
    
   /**
   * Open dialog with jobDefinition creation panel (from datasets)
   */ 
  private void createJobDefs(){
    SwingUtilities.invokeLater(
      new Runnable(){
        public void run(){
          Debug.debug("Creating job definition(s), "+getSelectedIdentifiers().length, 3);
          JobCreationPanel panel = new JobCreationPanel(dbPluginMgr, getTable().getColumnNames(),
              getSelectedIdentifiers(), false);
          CreateEditDialog pDialog = new CreateEditDialog(panel, false, true, true, true, true);
          pDialog.focusCreateUpdateButton();
          pDialog.setTitle("Create "+GridPilot.getRecordDisplayName("jobDefinition")+"(s)");
        }
      }
    );
  }

  protected void createJobDefinitions(){
    Debug.debug("Creating job definition(s), "+getSelectedIdentifiers().length, 3);
    JobDefCreationPanel panel = new JobDefCreationPanel(dbName, null, this, new Boolean(false));
    CreateEditDialog pDialog = new CreateEditDialog(panel, false, false, true, false, true);
    pDialog.setTitle(GridPilot.getRecordDisplayName("jobDefinition"));
  }

  protected void editJobDef(){
    editJobDef(null);
  }
  
  private void editJobDef(String displayName){
    String selectedDatasetID = dbPluginMgr.getJobDefDatasetID(getSelectedIdentifier());
    if(parentId.equals("-1")){
      parentId = selectedDatasetID;
    }
    Debug.debug("Got parent dataset id:"+parentId, 1);
    JobDefCreationPanel panel = null;
    panel = new JobDefCreationPanel(dbName, selectedDatasetID, this,
        new Boolean(true));
    CreateEditDialog pDialog = new CreateEditDialog(panel, true, false, true, false, true);
    pDialog.setTitle(displayName!=null?displayName:GridPilot.getRecordDisplayName(tableName));
    //pDialog.setVisible(true);
  }

  private void editFile(){
    // This should be safe. Only mysql and hsqldb contain jobDefinitions and are directly editable.
    // TODO: support editing other file catalogs
    /*if(dbPluginMgr.isJobRepository() && !dbPluginMgr.isFileCatalog()){
      editJobDef("jobDefinition");
    }
    else{*/
      // Just display the content
      int i = tableResults.getSelectedRow();
      String [] values = new String [fieldNames.length];
      for(int j=0; j<fieldNames.length; ++j){
        // Not displayed colums
        values[j] = "--- not displayed ---";
        for(int k=0; k<tableResults.getColumnCount(); ++k){
          if(tableResults.getColumnName(k).equalsIgnoreCase(fieldNames[j])){
            values[j] = (String) tableResults.getUnsortedValueAt(i, k);
            break;
          }
        }
        // This would work if all file catalogs would support searches on individual files.
        // DQ2 doesn't
        /*values[j] = dbPluginMgr.getFile(tableResults.getUnsortedValueAt(i,
            // the identifier is the last column
            tableResults.getColumnCount()-1).toString()).getValue(fieldNames[j]).toString();*/
      }
      MyUtil.showResult(tableResults,
          fieldNames, values, "file", MyUtil.OK_OPTION, "Skip");
    //}
  }
  
  private void deleteFiles(){
    if(!dbPluginMgr.isFileCatalog()){
      MyUtil.showMessage(window, "This is a virtual file table. " +
            "Entries cannot be modified directly.", "Cannot delete");
    }
    // Should be safe, only mysql and hsqldb contain jobDefinitions
    else{
      String msg = "<html>Are you sure you want to delete "+getSelectedIdentifiers().length+
      " file";
      if(getSelectedIdentifiers().length>1){
        msg += "s";
      }
      String [] ids = getSelectedIdentifiers();
      for(int i=0; i<getSelectedIdentifiers().length; ++i){
        if(i>0){
          msg += ",";
        }
        if(i>2){
          msg += "...";
          break;
        }
        msg += " " + ids[i];
      }
      msg += "?</html>";
      final JCheckBox cbCleanup = new JCheckBox("Delete physical file(s)", true);
      ConfirmBox confirmBox = new ConfirmBox(GridPilot.getClassMgr().getGlobalFrame());
      try{
        int choice = confirmBox.getConfirm("Confirm delete",
            msg, new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane()),
                               MyUtil.mkCancelObject(confirmBox.getOptionPane()),
                               cbCleanup});
        if(choice==1){
          return;
        }
      }
      catch(Exception e){
        e.printStackTrace();
        return;
      }
      deleteFiles(ids, cbCleanup.isSelected());
    }
  }
  
  private void deleteFiles(final String[] ids, final boolean cleanup) {
    
    workThread = new ResThread(){
      public void run(){
        if(!getWorking()){
          return;
        }
        boolean anyDeleted = doDeleteFiles(ids, cleanup);
        stopWorking();
        if(anyDeleted){
          refresh();
        }
      }
    };
    workThread.start();
  }

  private boolean doDeleteFiles(String[] _ids, boolean cleanup) {
    boolean anyDeleted = false;
    
    HashMap<String, Vector<String>> datasetNameAndIds = new HashMap<String, Vector<String>>();
    
    String dsn = null;
    if(getTableName().equalsIgnoreCase("dataset")){
      dsn = dbPluginMgr.getDatasetName(getSelectedIdentifier());
      datasetNameAndIds.put(dsn, new Vector<String>());
      Collections.addAll(datasetNameAndIds.get(dsn), _ids);
    }
    else if(getTableName().equalsIgnoreCase("file")){
      String [] fileDatasetReference = MyUtil.getFileDatasetReference(dbPluginMgr.getDBName());
      int fileDatasetNameColumn = -1;
      // Find the dataset name column on the files tab
      for(int i=0; i<tableResults.getColumnCount(); ++i){
        if(tableResults.getColumnName(i).equalsIgnoreCase(fileDatasetReference[1])){
          fileDatasetNameColumn = i;
          break;
        }
      }
      int [] rows = tableResults.getSelectedRows();
      // Group the file IDs by dataset name
      for(int i=0; i<_ids.length; ++i){
        dsn = (String) tableResults.getUnsortedValueAt(rows[i], fileDatasetNameColumn);
        if(!datasetNameAndIds.containsKey(dsn)){
          datasetNameAndIds.put(dsn, new Vector<String>());
        }
        (datasetNameAndIds.get(dsn)).add(_ids[i]);
      }          
    }
    
    Debug.debug("Deleting "+_ids.length+" rows. "+MyUtil.arrayToString(_ids), 2);
    if(_ids.length!=0){
      GridPilot.getClassMgr().getStatusBar().setLabel("Deleting file(s). Please wait...");
      JProgressBar pb = statusBar.setProgressBar();
      statusBar.setProgressBarMax(pb, _ids.length);
      
      String [] ids = null;
      Vector<String> idVec = null;
      String datasetId = null;
      Debug.debug("Deleting from "+datasetNameAndIds.keySet().size()+" datasets. "+
          MyUtil.arrayToString(datasetNameAndIds.keySet().toArray()), 2);
      for(Iterator<String> it=datasetNameAndIds.keySet().iterator(); it.hasNext();){
        dsn = it.next();
        datasetId = dbPluginMgr.getDatasetID(dsn);
        idVec = datasetNameAndIds.get(dsn);
        ids = new String [idVec.size()];
        for(int i=0; i<idVec.size(); ++i){
          ids[i] = idVec.get(i).toString();
        }
        Debug.debug("Now deleting "+ids.length+" files: "+MyUtil.arrayToString(ids), 3);
        boolean success = true;
        try{
          success = dbPluginMgr.deleteFiles(datasetId, ids, cleanup);
        }
        catch(Exception e){
          e.printStackTrace();
          success = false;
        }
        if(!success){
          String msg = "Deleting files "+MyUtil.arrayToString(ids)+" failed.";
          Debug.debug(msg, 1);
          GridPilot.getClassMgr().getStatusBar().setLabel("Deleting file(s) failed");
          GridPilot.getClassMgr().getLogFile().addMessage(msg);
        }
        else{
          anyDeleted = true;
        }
      }
                  
      for(int i=_ids.length-1; i>=0; i--){
        statusBar.incrementProgressBarValue(pb, 1);
        // Not necessary, we refresh below
        //tableResults.removeRow(rows[i]);
      }
      //tableResults.tableModel.fireTableDataChanged();
      GridPilot.getClassMgr().getStatusBar().setLabel("Deleting file(s) done.");
      statusBar.removeProgressBar(pb);
    }
    return anyDeleted;
  }

  protected void deleteJobDefs(){
    String msg = "Are you sure you want to delete jobDefinition";
    if(getSelectedIdentifiers().length>1){
      msg += "s";
    }
    String [] ids = getSelectedIdentifiers();
    for(int i=0; i<getSelectedIdentifiers().length; ++i){
      if(i>0){
        msg += ",";
      }
      if(i>7){
        msg += "...";
        break;
      }
      msg += " " + ids[i];
    }
    msg += "?";

    final JCheckBox cbCleanup = new JCheckBox("Delete output file(s)", true);
    ConfirmBox confirmBox = new ConfirmBox(GridPilot.getClassMgr().getGlobalFrame());
    try{
      int choice = confirmBox.getConfirm("Confirm delete",
          msg, new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane()),
                             MyUtil.mkCancelObject(confirmBox.getOptionPane()),
                             cbCleanup});
      if(choice==1){
        return;
      }
    }
    catch(Exception e){
      e.printStackTrace();
      return;
    }

    workThread = new ResThread(){
      public void run(){
        if(!getWorking()){
          return;
        }
        String [] ids = getSelectedIdentifiers();
        boolean anyDeleted = doDeleteJobDefs(ids, cbCleanup.isSelected());
        stopWorking();
        if(anyDeleted){
          refresh();
        }
      }
    };
    workThread.start();
  }

  private boolean doDeleteJobDefs(String [] ids, boolean cleanup) {
    boolean anyDeleted = false;

    // Update job monitoring display
    for(int i=ids.length-1; i>=0; --i){
      Debug.debug("Got dbPluginMgr:"+dbPluginMgr+":"+parentId, 1);
      JobMgr jobMgr = null;
      try{
        jobMgr = GridPilot.getClassMgr().getJobMgr(dbName);
        jobMgr.removeRow(ids[i]);
      }
      catch(Throwable e){
        Debug.debug("ERROR: could not get JobMgr. "+e.getMessage(), 1);
        e.printStackTrace();
        return false;
      }
    }
    
    Debug.debug("Deleting "+ids.length+" rows", 2);
    if(ids.length != 0){
      GridPilot.getClassMgr().getStatusBar().setLabel("Deleting job definition(s). Please wait...");
      JProgressBar pb = new JProgressBar();
      statusBar.setProgressBar(pb);
      statusBar.setProgressBarMax(pb, ids.length);
      for(int i=ids.length-1; i>=0; i--){
        boolean success = dbPluginMgr.deleteJobDefinition(ids[i], cleanup);
        if(!success){
          String msg = "Deleting job definition "+ids[i]+" failed";
          Debug.debug(msg, 1);
          GridPilot.getClassMgr().getStatusBar().setLabel(msg);
          GridPilot.getClassMgr().getLogFile().addMessage(msg);
          continue;
        }
        else{
          anyDeleted = true;
        }
        pb.setValue(pb.getValue()+1);
      }
      GridPilot.getClassMgr().getStatusBar().setLabel("Deleting job definition(s) done.");
      statusBar.removeProgressBar(pb);
    }
    return anyDeleted;
  }

  /**
   * Open dialog with dataset creation panel in creation mode
   */ 
  protected void createDatasets(boolean withInput){
    if(!withInput && tableResults.getSelectedRowCount()>0){
      tableResults.clearSelection();
    }
    CreateEditDialog pDialog = new CreateEditDialog(
        new DatasetCreationPanel(dbPluginMgr, this, false), false, true, true, false, true);
    pDialog.setTitle(GridPilot.getRecordDisplayName(tableName));
 }
  
  /**
   * Open dialog with dataset creation panel in editing mode
   */ 
 protected void editDataset(){
   DatasetCreationPanel dscp = new DatasetCreationPanel(dbPluginMgr, this, true);
   CreateEditDialog pDialog = new CreateEditDialog(dscp, true, true, true, false, true);
   if(!dscp.editable){
     pDialog.setBCreateUpdateEnabled(false);
   }
   pDialog.setTitle(GridPilot.getRecordDisplayName(tableName));
 }

  /**
   *  Delete datasets. Returns HashSet of identifier strings.
   *  From AtCom1.
   */
  protected void deleteDatasets(){
    statusBar.setLabel("Deleting dataset(s).");
    workThread = new ResThread(){
      public void run(){
        if(!getWorking()){
          return;
        }
        String [] datasetIdentifiers = getSelectedIdentifiers();
        HashSet<String> deleted = doDeleteDatasets(datasetIdentifiers);
        stopWorking();
        if(deleted!=null && deleted.size()>0){
          refresh();
          statusBar.setLabel(deleted.size()+" of "+
              datasetIdentifiers.length+" datasets deleted.");
        }
      }
    };
    workThread.start();
  }
  
  private HashSet<String> doDeleteDatasets(String [] datasetIdentifiers) {
     HashSet<String> deleted = new HashSet<String>();
    boolean skip = false;
    boolean okAll = false;
    int choice = 3;
    JCheckBox cbCleanup = null;
    for(int i=datasetIdentifiers.length-1; i>=0; --i){
      if(!datasetIdentifiers[i].equals("-1")){
        if(!okAll){
          ConfirmBox confirmBox = new ConfirmBox(GridPilot.getClassMgr().getGlobalFrame()/*,"",""*/); 
          cbCleanup = new JCheckBox("Delete job definitions", true);    
          if(i<1){
            try{
              choice = confirmBox.getConfirm("Confirm delete",
                                   "Really delete application/dataset "+datasetIdentifiers[i]+"?",
                                   dbPluginMgr.isJobRepository() ?
                                   new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane()),
                                                 MyUtil.mkSkipObject(confirmBox.getOptionPane()),
                                                 cbCleanup} :
                                     new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane()),
                                                   MyUtil.mkSkipObject(confirmBox.getOptionPane())});
            }
            catch(java.lang.Exception e){
              Debug.debug("Could not get confirmation, "+e.getMessage(),1);
            }
          }
          else{
            try{
              choice = confirmBox.getConfirm("Confirm delete",
                                   "Really delete dataset/dataset "+datasetIdentifiers[i]+"?",
                                   dbPluginMgr.isJobRepository() ?
                                   new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane()),
                                                 MyUtil.mkSkipObject(confirmBox.getOptionPane()),
                                                 MyUtil.mkOkAllObject(confirmBox.getOptionPane()),
                                                 MyUtil.mkSkipAllObject(confirmBox.getOptionPane()),
                                                 cbCleanup} :
                                     new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane()),
                                                   MyUtil.mkSkipObject(confirmBox.getOptionPane()),
                                                   MyUtil.mkOkAllObject(confirmBox.getOptionPane()),
                                                   MyUtil.mkSkipAllObject(confirmBox.getOptionPane())});
              }
            catch(java.lang.Exception e){
              Debug.debug("Could not get confirmation, "+e.getMessage(),1);
            }
          }
    
          switch(choice){
          case 0  : skip = false;break;  // OK
          case 1  : skip = true ; break;  // Skip
          case 2  : skip = false; okAll = true ;break;  // OK for all
          case 3  : skip = true ; return null; // Skip all
          default : skip = true;    // other (closing the dialog). Same action as "Skip"
          }
        }
        if(!skip || okAll){
          Debug.debug("deleting dataset # " + datasetIdentifiers[i], 2);
          boolean isJobRep = dbPluginMgr.isJobRepository();
          if(dbPluginMgr.deleteDataset(datasetIdentifiers[i],
              isJobRep && cbCleanup.isSelected())){
            deleted.add(datasetIdentifiers[i]);
            statusBar.setLabel("Dataset # " + datasetIdentifiers[i] + " deleted.");
          }
          else{
            Debug.debug("WARNING: dataset "+datasetIdentifiers[i]+" could not be deleted", 1);
            statusBar.setLabel("Dataset # " + datasetIdentifiers[i] + " NOT deleted.");
          }
        }
      }
      else{
        Debug.debug("WARNING: dataset undefined and could not be deleted",1);
      }
    }
    return deleted;
  }
  /**
   * Refresh search results.
   */ 
  protected void refresh(){
    if(!waitForWorking()){
      GridPilot.getClassMgr().getLogFile().addMessage("WARNING: table busy, search not performed");
      return;
    }
    Debug.debug("Refreshing search results", 3);
    if(tableResults==null /*|| tableResults.getRowCount()==0*/){
      searchRequest(true, false);
      //return;
    }
    else{
      DBVectorTableModel tableModel = (DBVectorTableModel) tableResults.getModel();
      int sortColumn = tableModel.getColumnSort();
      boolean isAscending = tableModel.isSortAscending();
      String [] columnNames = tableModel.getColumnNames();
      int [] columnWidths = new int [columnNames.length];
      for(int i=0; i<columnNames.length; ++i){
        columnWidths[i] = tableResults.getColumn(columnNames[i]).getPreferredWidth();
      }
      searchRequest(sortColumn, isAscending, columnWidths);
    }
  }
  
  /**
   * Open dialog with executable creation panel.
   */ 
  protected void createExecutable(){
    CreateEditDialog pDialog = new CreateEditDialog(
       new ExecutableCreationPanel(dbPluginMgr, this, false, null),
       false, true, true, false, true);
    pDialog.setTitle(GridPilot.getRecordDisplayName(tableName));
  }
  /**
   * Open dialog with runtime environment creation panel
   */ 
  protected void createRuntimeEnvironment(){
    CreateEditDialog pDialog = new CreateEditDialog(
       new RuntimeCreationPanel(dbPluginMgr, this, false),
       false, false, true, false, true);
    pDialog.setTitle(GridPilot.getRecordDisplayName(tableName));
  }

  protected void editExecutable(){
    CreateEditDialog pDialog = new CreateEditDialog(
       new ExecutableCreationPanel(dbPluginMgr, this, true, null),
       true, true, true, false, true);
    pDialog.setTitle(GridPilot.getRecordDisplayName(tableName));
  }
  
  protected void editRuntimeEnvironment(){
    CreateEditDialog pDialog = new CreateEditDialog(
       new RuntimeCreationPanel(dbPluginMgr, this, true),
       true, false, true, false, true);
    pDialog.setTitle(GridPilot.getRecordDisplayName(tableName));
  }

  protected void deleteExecutables(){
    String msg = "Are you sure you want to delete executable record";
    if(getSelectedIdentifiers().length>1){
      msg += "s";
    }
    String [] ids = getSelectedIdentifiers();
    for(int i=0; i<getSelectedIdentifiers().length; ++i){
      if(i>0){
        msg += ",";
      }
      if(i>7){
        msg += "...";
        break;
      }
      msg += " " + ids[i];
    }
    msg += "?";
    int choice = JOptionPane.showConfirmDialog(this,
        msg, "Delete?",
        JOptionPane.YES_NO_OPTION);
    if(choice==JOptionPane.NO_OPTION){
      return;
    }
    workThread = new ResThread(){
      public void run(){
        if(!getWorking()){
          return;
        }
        stopWorking();
        boolean anyDeleted = doDeleteExecutables();
        if(anyDeleted){
          refresh();
        }
      }
    };
    workThread.start();
  }

  private boolean doDeleteExecutables() {
    boolean anyDeleted = false;
    String [] ids = getSelectedIdentifiers();
    //int [] rows = tableResults.getSelectedRows();
    Debug.debug("Deleting "+ids.length+" rows", 2);
    if(ids.length!=0){
      GridPilot.getClassMgr().getStatusBar().setLabel(
         "Deleting executable(s). Please wait...");
      JProgressBar pb = new JProgressBar();
      statusBar.setProgressBar(pb);
      statusBar.setProgressBarMax(pb, ids.length);
      for(int i = ids.length-1; i>=0; i--){
        boolean success = dbPluginMgr.deleteExecutable(ids[i]);
        if(!success){
          String msg = "Deleting executable "+ids[i]+" failed";
          Debug.debug(msg, 1);
          GridPilot.getClassMgr().getStatusBar().setLabel(msg);
          GridPilot.getClassMgr().getLogFile().addMessage(msg);
          continue;
        }
        anyDeleted = true;
        pb.setValue(pb.getValue()+1);
        // We anyway refresh below, so this is not necessary.
        //tableResults.removeRow(rows[i]);
        //tableResults.tableModel.fireTableDataChanged();
      }
      GridPilot.getClassMgr().getStatusBar().setLabel(
         "Deleting executable(s) done.");
      statusBar.removeProgressBar(pb);
    }
    return anyDeleted;
  }
  
  protected void deleteRuntimeEnvironments(){
    String msg = "Are you sure you want to delete runtime environment record";
    if(getSelectedIdentifiers().length>1){
      msg += "s";
    }
    String [] ids = getSelectedIdentifiers();
    for(int i=0; i<getSelectedIdentifiers().length; ++i){
      if(i>0){
        msg += ",";
      }
      if(i>7){
        msg += "...";
        break;
      }
      msg += " " + ids[i];
    }
    msg += "?";
    int choice = JOptionPane.showConfirmDialog(this,
        msg, "Delete?",
        JOptionPane.YES_NO_OPTION);
    if(choice==JOptionPane.NO_OPTION){
      return;
    }
    workThread = new ResThread(){
      public void run(){
        if(!getWorking()){
          return;
        }
        boolean anyDeleted = doDeleteRuntimeEnvironments();
        stopWorking();
        if(anyDeleted){
          refresh();
        }
      }
    };
    workThread.start();
  }

  private boolean doDeleteRuntimeEnvironments() {
    boolean anyDeleted = false;
    String [] ids = getSelectedIdentifiers();
    Debug.debug("Deleting "+ids.length+" rows", 2);
    if(ids.length != 0){
      GridPilot.getClassMgr().getStatusBar().setLabel(
         "Deleting runtime environment(s). Please wait...");
      JProgressBar pb = new JProgressBar();
      statusBar.setProgressBar(pb);
      statusBar.setProgressBarMax(pb, ids.length);
      for(int i = ids.length-1; i>=0; i--){
        boolean success = dbPluginMgr.deleteRuntimeEnvironment(ids[i]);
        if(!success){
          String msg = "Deleting runtime environment "+ids[i]+" failed";
          Debug.debug(msg, 1);
          GridPilot.getClassMgr().getStatusBar().setLabel(msg);
          GridPilot.getClassMgr().getLogFile().addMessage(msg);
          continue;
        }
        else{
          anyDeleted = true;
        }
        pb.setValue(pb.getValue()+1);
        //tableResults.removeRow(rows[i]);
        //tableResults.tableModel.fireTableDataChanged();
      }
      statusBar.removeProgressBar(pb);
      GridPilot.getClassMgr().getStatusBar().setLabel(
         "Deleting runtime environment(s) done.");
    }
    return anyDeleted;
  }
  
  private void viewFiles(boolean waitForThread, String [] ids, int [] rows){
    for(int i=0; i<rows.length; ++i){
      viewFiles(waitForThread, ids[i], rows[i]);
      if(ids.length>0){
        try{
          Thread.sleep(3000);
        }
        catch(InterruptedException e){
          break;
        }
      }
    }
  }

  /**
   * Open new pane with list of files.
   */
  private DBPanel viewFiles(boolean waitForThread, String id, int row){
    if(id.equals("-1")){
      return null;
    }
    try{
      // We assume that the dataset name is used as reference...
       // TODO: improve this
       String datasetColumn = "dsn";
       String [] fileDatasetReference = MyUtil.getFileDatasetReference(dbPluginMgr.getDBName());
       if(fileDatasetReference!=null){
         datasetColumn = fileDatasetReference[1];
       }
       // Try to save a database lookup (which DQ2 cannot handle anyway):
       // use the dataset name if it is displayed. Otherwise look it up from the id.
       String datasetName = null;
       int datasetNameIndex = -1;
       try{
         for(int i=0; i<tableResults.getColumnNames().length; ++i){
           if(tableResults.getColumnNames()[i].equalsIgnoreCase(datasetColumn)){
             datasetNameIndex = i;
             break;
           }
         }
         datasetName = (String) tableResults.getUnsortedValueAt(row, datasetNameIndex);
         Debug.debug("Found dataset name "+datasetName+" from row "+row, 2);
       }
       catch(Exception e){
       }
       if(datasetName==null || datasetName.equals("")){
         datasetName = dbPluginMgr.getDataset(id).getValue(
             fileDatasetReference[0]).toString();
       }
       // Create and return new panel with files.
       DBPanel ret = createViewFilesPanel(id, datasetColumn, datasetName, waitForThread);
       Debug.debug("Created new panel "+ret, 2);
       return ret;
     }
     catch(Exception e){
       Debug.debug("Couldn't create panel for dataset " + "\n" +
                          "\tException\t : " + e.getMessage(), 2);
       e.printStackTrace();
       return null;
     }
  }
  
  private DBPanel createViewFilesPanel(final String id, final String datasetColumn,
      final String datasetName, final boolean waitForThread) throws Exception{
    MyResThread rt = new MyResThread(){
      private DBPanel dbPanel = null;
      public void run(){
        try{
          dbPanel = new DBPanel(id);
          dbPanel.initDB(dbName, "file");
          dbPanel.initGUI();
          dbPanel.selectPanel.setConstraint(datasetColumn, datasetName, 0);
          dbPanel.searchRequest(false, waitForThread);
          dbPanel.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          GridPilot.getClassMgr().getGlobalFrame().addPanel(dbPanel);
          Debug.debug("Created new files panel "+id+":"+datasetName+":"+dbPanel, 2);
          return;
        }
        catch(Exception e){
          setException(e);
          e.printStackTrace();
        }
      }
      public DBPanel getDBPanelRes(){
        return dbPanel;
      }
    };
    if(waitForThread){
      //SwingUtilities.invokeAndWait(rt);
      //SwingUtilities.invokeLater(rt);
      rt.start();
      if(!MyUtil.waitForThread(rt, "rt", fileCatalogTimeout, "createViewFilesPanel", GridPilot.getClassMgr().getLogFile())){
        throw new Exception("Timed out creating files panel.");
      }
      long startMillis = MyUtil.getDateInMilliSeconds();
      while(rt.getDBPanelRes()==null){
        Thread.sleep(10000);
        if(rt.getDBPanelRes()==null && MyUtil.getDateInMilliSeconds()-startMillis>fileCatalogTimeout){
          throw new Exception("Timed out creating files panel. "+rt.getDBPanelRes());
        }
      }
      return rt.getDBPanelRes();
    }
    else{
      SwingUtilities.invokeLater(rt);
      return null;
    }
  }
 
  /**
   * Open new pane(s) with list(s) of jobDefinitions.
   */
  private void viewJobDefinitions(String [] ids){
    for(int i=0; i<ids.length; ++i){
      viewJobDefinitions(ids[i]);
    }
  }

  private void viewJobDefinitions(final String id){
    new Thread(){
      public void run(){
        doViewJobDefinitions(id);
      }
    }.start();
  }
  
  private void doViewJobDefinitions(final String id){
    //SwingUtilities.invokeLater(
      //new Runnable(){
        //public void run(){
          try{
            // Create new panel with jobDefinitions.         
            DBPanel dbPanel = new DBPanel(id);
            dbPanel.initDB(dbName, "jobDefinition");
            dbPanel.initGUI();
            String [] jobDefDatasetReference =
               MyUtil.getJobDefDatasetReference(dbPluginMgr.getDBName());
            dbPanel.selectPanel.setConstraint(jobDefDatasetReference[1],
                dbPluginMgr.getDataset(id).getValue(
                    jobDefDatasetReference[0]).toString(),
                0);
            dbPanel.searchRequest(true, false);
            GridPilot.getClassMgr().getGlobalFrame().addPanel(dbPanel);    
          }
          catch(Exception e){
            Debug.debug("Couldn't create panel for dataset " + "\n" +
                               "\tException\t : " + e.getMessage(), 2);
            e.printStackTrace();
          }
        //}
      //}
    //);
  }
 
  private void processDatasets(final ActionEvent e){
    final String [] datasetIds = getSelectedIdentifiers();
    if(datasetIds==null || datasetIds.length==0){
      return;
    }
    Debug.debug("processing dataset(s): "+MyUtil.arrayToString(datasetIds), 3);
    new Thread(){
      public void run(){
        String csName = ((JMenuItem)e.getSource()).getText();
        boolean ok = true;
        String error = "";
        try{
          // Grab semaphore
          if(!waitForWorking()){
            GridPilot.getClassMgr().getLogFile().addMessage("WARNING: table busy, cannot process dataset.");
            return;
          }
          setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
          doProcessDatasets(csName, datasetIds);
          stopWorking();
        }
        catch(Exception e){
          Debug.debug("Couldn't process dataset(s) " + "\n" +
                             "\tException\t : " + e.getMessage(), 2);
          e.printStackTrace();
          error = e.getMessage();
          ok = false;
        }
        if(!ok){
          MyUtil.showError(window,
              "Problem processing dataset(s). "+
              (error==null||error.equals("")?"See the log for details.":error));
        }
        setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      }
    }.start();
  }
  
  /**
   * Check if dataset has jobDefinitions.
   * If not:
   *   - create jobDefinitions
   *   - close create window
   * Then:
   *   - submit one job and wait till it's submitted
   *   - if it does not submit, return an error
   *   - if it does submit, submit the rest
   *
   */
  private boolean doProcessDatasets(String csName, String [] ids) {
    boolean ok = true;
    int jobCount = 0;
    Debug.debug("Processing dataset(s): "+MyUtil.arrayToString(ids), 3);
    DBResult jobDefs;
    String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
    // Create jobs if none present
    String datasetName;
    Vector<String> datasetNames = new Vector<String>();
    String exeName;
    for(int i=ids.length-1; i>=0; --i){
      datasetName = dbPluginMgr.getDatasetName(ids[i]);
      datasetNames.add(datasetName);
      exeName = dbPluginMgr.getDatasetExecutableName(ids[i]);
      // Pop up a warning if this is not an application dataset
      if(exeName==null || exeName.equals("")){
        MyUtil.showError(window, "You cannot run the dataset "+datasetName+" - it has no executable defined.");
        continue;
      }
      jobDefs = dbPluginMgr.getJobDefinitions(ids[i],
          new String [] {idField}, null, null);
      if(jobDefs.values.length==0){
        Debug.debug("Creating job(s) for dataset: "+i+":"+ids[i], 3);
        createJobDefsForDataset(ids[i]);
      }
    }
    // Submit submitable jobs
    Vector<String> toSubmitJobDefIds = new Vector<String>();
    for(int i=ids.length-1; i>=0; --i){
      jobDefs = dbPluginMgr.getJobDefinitions(ids[i],
          new String [] {idField}, new String [] {DBPluginMgr.getStatusName(DBPluginMgr.DEFINED)}, null);
      if(jobDefs.values.length==0){
        continue;
      }
      for(int ii=0; ii<jobDefs.values.length; ++ii){
        toSubmitJobDefIds.add((String) jobDefs.get(ii).getValue(idField));
        ++jobCount;
      }
    }
    Debug.debug("Submitting "+jobCount+" job(s)", 3);
    if(jobCount==0){
      MyUtil.showMessage(window,
          "No submitable jobs", "No submitable jobs in dataset(s) "+datasetNames);
      return ok;
    }
    try{
      // First try a test job.
      setJobsRefresh();
      if(toSubmitJobDefIds.size()>1){
        runFirstJob(toSubmitJobDefIds, csName);
      }
      doSubmit(csName, toSubmitJobDefIds.toArray(new String [toSubmitJobDefIds.size()]));
    }
    catch(Exception e){
      stopJobsRefresh();
      ok = false;
      GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not submit first job.", e);
      try{
        workThread.interrupt();
        GridPilot.getClassMgr().getSubmissionControl().cancelSubmission();
      }
      catch(Exception ee){
        ee.printStackTrace();
      }
      e.printStackTrace();
      showSubmissionError(e, toSubmitJobDefIds, csName);
      return ok;
    }
    return ok;
  }
  
  private void runFirstJob(Vector<String> toSubmitJobDefIds, String csName) throws Exception {
    GridPilot.FIRST_JOB_SUBMITTED_WAIT_SECONDS = GridPilot.DEFAULT_FIRST_JOB_SUBMITTED_WAIT_SECONDS;
    String firstJobDefId = toSubmitJobDefIds.remove(0);
    doSubmit(csName, new String [] {firstJobDefId});
    waitForSubmitted(csName, firstJobDefId);
  }

  private void runFirstJobDefinition(Vector<DBRecord> toSubmitJobDefs, String csName) throws Exception {
    DBRecord firstJobDef = toSubmitJobDefs.remove(0);
    Vector<DBRecord> remainingJobDefs = new Vector<DBRecord>();
    remainingJobDefs.add(firstJobDef);
    GridPilot.getClassMgr().getSubmissionControl().submitJobDefinitions(
        remainingJobDefs, csName, dbPluginMgr);
    String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
    waitForSubmitted(csName, (String) firstJobDef.getValue(idField));
  }

  private void setJobsRefresh() {
    GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel(
        ).getJobMonitoringPanel().setAutoRefreshSeconds(AUTO_REFRESH_SECONDS);
  }

  private void stopJobsRefresh() {
    GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel(
        ).getJobMonitoringPanel().stopAutoRefresh();
  }

  private void setTransfersRefresh() {
    GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel(
        ).getTransferMonitoringPanel().setAutoRefreshSeconds(AUTO_REFRESH_SECONDS);
  }

  private void waitForSubmitted(String csName, String firstJobDefId) throws Exception {
    int i = 0;
    int dbStatus = -1;
    int sleepMillis = 5000;
    while(dbStatus!=DBPluginMgr.SUBMITTED){
      dbStatus = DBPluginMgr.getStatusId(dbPluginMgr.getJobDefStatus(firstJobDefId));
      Debug.debug("Waiting for first job; got status "+DBPluginMgr.getStatusName(dbStatus), 2);
      if(dbStatus==DBPluginMgr.SUBMITTED){
        return;
      }
      try{
        Thread.sleep(sleepMillis);
      }
      catch(InterruptedException e){
         e.printStackTrace();
         break;
      }
      if(dbStatus==DBPluginMgr.FAILED){
        throw new Exception("Job "+firstJobDefId+" failed.");
      }
      ++i;
      if(i*sleepMillis>1000L*GridPilot.FIRST_JOB_SUBMITTED_WAIT_SECONDS){
        break;
      }
    }
    throw new Exception("Timed out waiting for job "+firstJobDefId);
  }

  private boolean createJobDefsForDataset(String datasetId){
    JobCreationPanel panel = new JobCreationPanel(dbPluginMgr, getTable().getColumnNames(),
        new String [] {datasetId}, true);
    CreateEditDialog pDialog = new CreateEditDialog(panel, false, true, true, true, true);
    pDialog.focusCreateUpdateButton();
    pDialog.setTitle("Create "+GridPilot.getRecordDisplayName("jobDefinition")+"(s)");
    while(pDialog!=null && pDialog.isVisible()){
      try{
        Thread.sleep(1000);
      }
      catch(InterruptedException e){
        e.printStackTrace();
        break;
      }
    }
    return true;
  }
  
  /**
   * Monitor dataset(s).
   */
  private void monitorDatasets(){
    if(getSelectedIdentifiers()==null || getSelectedIdentifiers().length==0){
      return;
    }
    Debug.debug("Monitoring dataset(s): "+MyUtil.arrayToString(getSelectedIdentifiers()), 3);
    new Thread(){
      public void run(){
        boolean ok = true;
        String error = "";
        try{
          // Grab semaphore
          if(!waitForWorking()){
            GridPilot.getClassMgr().getLogFile().addMessage("WARNING: table busy, monitoring not done");
            return;
          }
          ok = doMonitorDatasets();
          stopWorking();
        }
        catch(Exception e){
          stopWorking();
          Debug.debug("Couldn't monitor dataset(s) " + "\n" +
                             "\tException\t : " + e.getMessage(), 2);
          e.printStackTrace();
          error = e.getMessage();
          ok = false;
        }
        if(!ok){
          MyUtil.showError(window, 
              "Problem monitoring dataset(s). "+
              (error==null||error.equals("")?"See the log for details.":error));
        }
      }
    }.start();
  }
 
  /**
   * Add all jobDefinitions of selected dataset(s) to monitor.
   */
  private boolean doMonitorDatasets() {
    String[] ids = getSelectedIdentifiers();
    boolean ok = true;
    if(ids==null || ids.length==0){
      return ok;
    }
    int jobCount = 0;
    Debug.debug("Monitoring jobs from "+(ids.length)+
        "dataset(s): "+MyUtil.arrayToString(ids), 3);
    String [] jobDefIds;
    DBResult jobDefs;
    String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
    for(int i=ids.length-1; i>=0; --i){
      try{
        jobDefs = dbPluginMgr.getJobDefinitions(ids[i],
            new String [] {idField}, null, null);
        if(jobDefs.values.length==0){
          continue;
        }
        jobDefIds = new String [jobDefs.values.length];
        for(int ii=0; ii<jobDefs.values.length; ++ii){
          jobDefIds[ii] = (String) jobDefs.get(ii).getValue(idField);
          ++jobCount;
        }
        monitorJobs(jobDefIds, false);
      }
      catch(Exception e){
        e.printStackTrace();
        ok = false;
      }
    }
    if(jobCount==0){
      MyUtil.showMessage(window, 
          "No jobs", "There are no jobs defined for the selected dataset(s)");
    }
    return ok;
  }

  /**
   * Cleanup dataset(s).<br>
   * If jobDefinitions exist:<br>
   * - ask if stdout/err or output files should be deleted
   * - delete files if so chosen - purge empty directories
   * - delete jobDefinitions if so chosen
   *
   */
  private void cleanupDatasets(){
    new Thread(){
      public void run(){
        // Grab semaphore
        if(!waitForWorking()){
          GridPilot.getClassMgr().getLogFile().addMessage("WARNING: table busy, cleanup not done");
          return;
        }
        Window frame = (Window) SwingUtilities.getWindowAncestor(getRootPane());
        frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        boolean anyDeleted = false;
        boolean ok = true;
        String error = "";
        int jobCount = 0;
        int fileCount = 0;
        try{
          String[] ids = getSelectedIdentifiers();
          if(ids==null || ids.length==0){
            return;
          }
          Debug.debug("cleaning jobs from dataset(s): "+MyUtil.arrayToString(ids), 3);
          String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
          for(int i=ids.length-1; i>=0; --i){
            DBResult jobDefs = dbPluginMgr.getJobDefinitions(ids[i],
                new String [] {idField}, null, null);
            DBResult files = dbPluginMgr.getFiles(ids[i]);
            try{
              cleanupJobsFromDataset(i, ids[i]);
            }
            catch(Exception ee){
              ee.printStackTrace();
            }
            try{
              anyDeleted = deleteJobDefsFromDataset(i, ids[i]);
              if(anyDeleted){
                jobCount = jobCount + jobDefs.size();
              }
            }
            catch(Exception ee){
              ee.printStackTrace();
              ok = false;
            }
            try{
              anyDeleted = deletefilesOfDataset(i, ids[i]);
              if(anyDeleted){
                fileCount = fileCount + files.size();
              }
            }
            catch(Exception ee){
              ee.printStackTrace();
              ok = false;
            }
          }
          MyUtil.showMessage(window, 
              "Cleanup done", "Cleaned up dataset(s) "+MyUtil.arrayToString(ids)+
              ".\n\n  job(s) cleaned: "+jobCount+"\n\n"+
              "  file(s) cleaned: "+fileCount+"\n\n");
        }
        catch(Exception e){
          ok = false;
          stopWorking();
          Debug.debug("Couldn't clean up dataset(s) " + "\n" +
                             "\tException\t : " + e.getMessage(), 2);
          e.printStackTrace();
          error = e.getMessage();
        }
        frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        deleteJobDefs = null;
        cleanupJobDefs = null;
        deleteFiles = null;
        cleanupFiles = null;        
        stopWorking();
        if(!ok){
          MyUtil.showError(window, 
              "Problem cleaning up dataset(s). "+
              (error==null||error.equals("")?"See the log for details.":error));
        }
      }
    }.start();
  }
 
  private boolean cleanupJobsFromDataset(int dsNum, String id) {
    boolean ok = true;
    if(!dbPluginMgr.isJobRepository()){
      return ok;
    }
    String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
    String nameField = MyUtil.getNameField(dbName, "jobDefinition");
    DBResult jobDefs = dbPluginMgr.getJobDefinitions(id,
        new String [] {idField, nameField, "computingSystem", "jobId"}, null, null);
    if(jobDefs.values.length>0){
      MyJobInfo job;
      DBRecord jobRecord;
      for(int i=0; i<jobDefs.values.length; ++i){
        jobRecord = jobDefs.get(i);
        // identifier, name, computingSystem, dbname
        job = new MyJobInfo((String) jobRecord.getValue(idField),
                            (String) jobRecord.getValue(nameField),
                            (String) jobRecord.getValue("computingSystem"),
                            dbName);
        // jobid
        job.setJobId((String) jobRecord.getValue("jobId"));
        GridPilot.getClassMgr().getCSPluginMgr().cleanup(job);
        job.setDBStatus(DBPluginMgr.DEFINED);
        GridPilot.getClassMgr().getJobMgr(job.getDBName()).updateDBCell(job);
      }
    }
    return ok;
  }

  private boolean deleteJobDefsFromDataset(int dsNum, String id) {
    boolean ok = true;
    if(!dbPluginMgr.isJobRepository()){
      return ok;
    }
    String idField = MyUtil.getIdentifierField(dbName, "jobDefinition");
    String nameField = MyUtil.getNameField(dbName, "jobDefinition");
    DBResult jobDefs = dbPluginMgr.getJobDefinitions(id,
        new String [] {idField, nameField, "computingSystem", "jobId"}, null, null);
    if(jobDefs.values.length==0){
      return ok;
    }
    int choice = -1;
    try{
      choice = getDeleteJobDefsConfirm(dsNum, dbPluginMgr.getDatasetName(id), jobDefs.values.length);
    }
    catch(Exception e){
      e.printStackTrace();
      return false;
    }
    if(choice!=0 && choice!=2){
      return false;
    }
    String [] jobDefIds = new String [jobDefs.values.length];
    for(int i=0; i<jobDefs.values.length; ++i){
      jobDefIds[i] = (String) jobDefs.get(i).getValue(idField);
    }
    return doDeleteJobDefs(jobDefIds, cleanupJobDefs);
  }

  private int getDeleteJobDefsConfirm(int dsNum, String name, int numJobs) throws Exception {
    if(deleteJobDefs!=null){
      return deleteJobDefs?2:3;
    }
    ConfirmBox confirmBox = new ConfirmBox(GridPilot.getClassMgr().getGlobalFrame()); 
    JCheckBox cbCleanup = new JCheckBox("Delete "+
          (dbPluginMgr.isFileCatalog()?"stdout/stderr of jobs":
            "output files and stdout/stderr of jobs"), true);
    int choice = -1;
    String title = "Confirm delete job definition"+(numJobs>1?"s":"");
    String msg =  "Do you want to delete the "+(numJobs>1?numJobs+" ":"")+"job definition"+(numJobs>1?"s":"")+" of dataset "+name+"?";
    Debug.debug("Buttons with icons? "+MyUtil.BUTTON_DISPLAY, 3);
    if(dsNum<1){
      choice = confirmBox.getConfirm(title, msg,
          new Object[] {
             MyUtil.mkOkObject(confirmBox.getOptionPane()),
             MyUtil.mkCancelObject(confirmBox.getOptionPane()),
             cbCleanup
          });
    }
    else{
      choice = confirmBox.getConfirm(title, msg,
          new Object[] {
             MyUtil.mkOkObject(confirmBox.getOptionPane()),
             MyUtil.mkSkipObject(confirmBox.getOptionPane()),
             MyUtil.mkOkAllObject(confirmBox.getOptionPane()),
             MyUtil.mkSkipAllObject(confirmBox.getOptionPane()),
             cbCleanup
          });
    }
    switch(choice){
    case 0:
       cleanupJobDefs = cbCleanup.isSelected();
       break; //OK
    case 1:
      break;  // Skip
    case 2:
       deleteJobDefs = true;
       cleanupJobDefs = cbCleanup.isSelected();
       break;  // OK for all
    case 3:
      deleteJobDefs = false;
      cleanupJobDefs = false;
      break; // Skip all
    }
    return choice;
  }

  private boolean deletefilesOfDataset(int dsNum, String id) {
    boolean ok = true;
    if(!dbPluginMgr.isFileCatalog()){
      return ok;
    }
    String idField = MyUtil.getIdentifierField(dbName, "file");
    DBResult files = dbPluginMgr.getFiles(id);
    if(files.values.length==0){
      return ok;
    }
    int choice = -1;
    try{
      choice = getDeleteFilesConfirm(dsNum, dbPluginMgr.getDatasetName(id), files.size());
    }
    catch(Exception e){
      e.printStackTrace();
      return false;
    }
    if(choice!=0 && choice!=2){
      return false;
    }
    if(files.values.length>0){
      String [] fileIds = new String [files.values.length];
      for(int i=0; i<files.values.length; ++i){
        fileIds[i] = (String) files.get(i).getValue(idField);
      }
      ok = ok && doDeleteFiles(fileIds, cleanupFiles);
    }
    return ok;
  }

  private int getDeleteFilesConfirm(int dsNum, String name, int numFiles) throws Exception {
    if(deleteFiles!=null){
      return deleteFiles?2:3;
    }
    ConfirmBox confirmBox = new ConfirmBox(GridPilot.getClassMgr().getGlobalFrame()); 
    JCheckBox cbCleanup = new JCheckBox("Delete physical file"+(numFiles>1?"s":""), true);
    String title = "Confirm delete file(s)";
    String msg = "Do you want to delete the "+(numFiles>1?numFiles+" ":"")+"file"+(numFiles>1?"s":"")+" of dataset "+name+"?";
    int choice = -1;
    if(dsNum<1){
      choice = confirmBox.getConfirm(title, msg,
          new Object[] {
             MyUtil.mkOkObject(confirmBox.getOptionPane()),
             MyUtil.mkCancelObject(confirmBox.getOptionPane()),
             cbCleanup
          });
    }
    else{
      choice = confirmBox.getConfirm(title, msg,
          new Object[] {
             MyUtil.mkOkObject(confirmBox.getOptionPane()),
             MyUtil.mkSkipObject(confirmBox.getOptionPane()),
             MyUtil.mkOkAllObject(confirmBox.getOptionPane()),
             MyUtil.mkSkipAllObject(confirmBox.getOptionPane()),
             cbCleanup
          });
    }
    switch(choice){
    case 0:
       cleanupFiles = cbCleanup.isSelected();
       break; //OK
    case 1:
      break;  // Skip
    case 2:
       deleteFiles = true;
       cleanupFiles = cbCleanup.isSelected();
       break;  // OK for all
    case 3:
      deleteFiles = false;
      cleanupFiles = false;
      break; // Skip all
    }
    return choice;
  }

  /**
   * Open job definition window.
   */
  private void defineJobDefinitions(){
    Debug.debug("defining job definitions, "+getSelectedIdentifiers().length, 3);
    if(getSelectedIdentifiers()!=null && getSelectedIdentifiers().length!=0){
      new Thread(){
        public void run(){
          try{
            createJobDefs();
          }
          catch(Exception e){
            Debug.debug("Couldn't create job definition window " + "\n" +
                               "\tException\t : " + e.getMessage(), 2);
            e.printStackTrace();
          }
        }
      }.start();
    }
  }
 
  /**
   * Called from the right-click menu
   */
  private void getInfo() {
    Debug.debug("Getting info on file(s) of dataset(s)", 2);
    new Thread(){
      public void run(){
        try{
          setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
          int [] rows = tableResults.getSelectedRows();
          if(rows==null || rows.length==0){
            return;
          }
          String fileIdField = MyUtil.getIdentifierField(dbName, "file");
          String [] ids = getSelectedIdentifiers();
          String name;
          int totalFiles = 0;
          long totalBytes = 0L;
          DBResult files;
          String fileId;
          String fileBytesStr;
          long fileBytes;
          for(int i=0; i<ids.length; ++i){
            name = dbPluginMgr.getDatasetName(ids[i]);
            Debug.debug("Checking dataset "+name, 1);
            files = dbPluginMgr.getFiles(ids[i]);
            for(int ii=0; ii<files.size(); ++ii){
              ++totalFiles;
              fileId = (String) files.get(ii).getValue(fileIdField);
              fileBytesStr = dbPluginMgr.getFileBytes(name, fileId);
              try{
                fileBytes = Long.parseLong(fileBytesStr);
                totalBytes += fileBytes;
              }
              catch(Exception e){
                e.printStackTrace();
              }
            }
            if(ids.length>1){
              Thread.sleep(1000);
            }
          }
          setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          String msg = "Number of files: "+totalFiles+"\nTotal size: "+totalBytes+" bytes";
          String exMsg = null;
          if(totalBytes>1000L){
            exMsg = " ("+totalBytes/1000L+" KB)";
            if(totalBytes>1000000L){
              exMsg = " ("+totalBytes/1000000L+" MB)";
              if(totalBytes>1000000000L){
                exMsg = " ("+totalBytes/1000000000L+" GB)";
              }
            }
            msg = msg + exMsg;
          }
          MyUtil.showMessage(window,
              "Information on selected dataset(s)", msg);
        }
        catch(Exception e){
          setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          e.printStackTrace();
        }
      }
    }.start();
  }
  
  /**
   * Called from the right-click menu
   */
  private void replicate() {
    Debug.debug("Replicating dataset(s)", 2);
    new Thread(){
      public void run(){
        replicating = true;
        int origFileRows = GridPilot.FILE_ROWS;
        try{
          String [] ids = getSelectedIdentifiers();
          if(ids==null || ids.length==0){
            return;
          }
          int [] rows = tableResults.getSelectedRows();
          TargetDBsPanel targetDBsPanel = makeTargetDBsPanel();
          JPanel pTargetDBs = targetDBsPanel.pTargetDBs;
          String dlUrl = MyUtil.getURL(defaultURL, pTargetDBs, true, "Choose destination directory");
          if(dlUrl==null){
            return;
          }
          if(dlUrl.startsWith("file:")){
            defaultURL = dlUrl;
          }
          GridPilot.FILE_ROWS = 0;
          setTransfersRefresh();
          for(int i=0; i<ids.length; ++i){
            if(GridPilot.getClassMgr().getTransferControl().allTransfersCancelled()){
              GridPilot.getClassMgr().getTransferControl().setAllTransfersCancelled(false);
              break;
            }
            Debug.debug("Replicating dataset "+ids[i], 1);
            replicateDataset(ids[i], rows[i], dlUrl, targetDBsPanel);
            if(ids.length>1){
              Thread.sleep(4000);
            }
          }
          GridPilot.FILE_ROWS = origFileRows;
          replicating = false;
        }
        catch(Exception e){
          GridPilot.FILE_ROWS = origFileRows;
          replicating = false;
          e.printStackTrace();
        }
      }
    }.start();
  }
  
  private void replicateDataset(String id, int row, String dlUrl, TargetDBsPanel targetDBsPanel) {
    Debug.debug("Creating new files panel from "+id, 2);
    dbPluginMgr.requestStopLookup();
    DBPanel filesPanel = viewFiles(true, id, row);
    dbPluginMgr.clearRequestStopLookup();
    Debug.debug("Waiting 4 seconds for "+filesPanel, 2);
    try{
      Thread.sleep(4000);
    }
    catch(InterruptedException e){
       e.printStackTrace();
    }
    // TODO: consider selecting only rows with non-empty pfns column
    Debug.debug("Selecting all", 2);
    filesPanel.tableResults.selectAll();
    Debug.debug("Starting download of "+filesPanel.tableResults.getSelectedRowCount()+
        " files.", 2);
    filesPanel.download(dlUrl, targetDBsPanel);
  }
  
  /**
   * Called from the right-click menu
   */
  private void exportDataset(){
    setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    new Thread(){
      public void run(){
        try{
          doExportDataset();
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
    }.start();
  }
  
  private void doExportDataset(){
    if(!getSelectedIdentifier().equals("-1") && !exportingDataset){
      new Thread(){
        public void run(){
          exportingDataset = true;
          // Get the dataset name and id
          String[] datasetIDs = getSelectedIdentifiers();
          try{
            if(exportDataset(datasetIDs)){
              MyUtil.showMessage(window, "Export successful",
                  "Thanks and congratulations! You've successfully exported "+
                  datasetIDs.length+" application"+(datasetIDs.length>1?"s":"")+
                  "/dataset"+(datasetIDs.length>1?"s":"")+".\n");
            }
          }
          catch(Exception e){
            String error = "ERROR: could not export dataset";
            Debug.debug(error, 1);
            GridPilot.getClassMgr().getStatusBar().setLabel(error);
            GridPilot.getClassMgr().getLogFile().addMessage(error, e);
            exportingDataset = false;
          }         
          setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          exportingDataset = false;
        }
      }.start();
    }
  }
  
  private boolean exportDataset(String[] datasetIDs) throws Exception {
    //String url = MyUtil.getURL("file:~/", null, true, "Choose destination directory");
    String url = MyUtil.getURL(GridPilot.APP_STORE_URL, null, true, "Choose destination directory");
    if(url!=null && !url.equals("")){
      Debug.debug("Exporting to "+url, 2);
      return ExportImport.exportDB(
          MyUtil.urlIsRemote(url)?
          url:
          MyUtil.clearTildeLocally(MyUtil.clearFile(url)),
          dbName, datasetIDs);
    }
    else{
      Debug.debug("Not exporting. "+url, 2);
      return false;
    }
  }

  
  private class TargetDBsPanel{
    protected JPanel pTargetDBs = null;
    protected JComboBox cbTargetDBSelection = null;
    TargetDBsPanel(JPanel _pTargetDBs, JComboBox _cbTargetDBSelection){
      pTargetDBs = _pTargetDBs;
      cbTargetDBSelection = _cbTargetDBSelection;
    }
  }
  
  private TargetDBsPanel makeTargetDBsPanel(){
    JPanel pTargetDBs = null;
    JComboBox cbTargetDBSelection = null;
    try{
      pTargetDBs = new JPanel();
      cbTargetDBSelection = new JComboBox();           
      cbTargetDBSelection.addItem("");
      for(int i=0; i<GridPilot.DB_NAMES.length; ++i){
        // If this DB has a job definition table, registration should not be done.
        // Files are defined by the jobDefinition table.
        try{
          if(!GridPilot.getClassMgr().getDBPluginMgr(GridPilot.DB_NAMES[i]).isFileCatalog()){
            continue;
          }
        }
        catch(Exception e){
        }
        cbTargetDBSelection.addItem(GridPilot.DB_NAMES[i]);
      }
      JLabel jlTargetDBSelection = new JLabel("Register new locations in DB:");
      pTargetDBs.add(jlTargetDBSelection, null);
      pTargetDBs.add(cbTargetDBSelection, null);
      if(cbTargetDBSelection.getItemCount()<2){
        pTargetDBs = null;
      }
    }
    catch(Exception e){
      pTargetDBs = null;
    }
    return new TargetDBsPanel(pTargetDBs, cbTargetDBSelection);
  }
  
  /**
   * Called when mouse is pressed on the Replicate button
   */
  private void download(final String _dlUrl, final TargetDBsPanel _targetDBsPanel){
    new Thread(){
      public void run(){        
        // if the table jobDefinition is present, we are using
        // the native tables and only one url/pfn per jobDefinition/file
        // is allowed, thus no db combobox needed
        TargetDBsPanel targetDBsPanel = null;
        if(_targetDBsPanel!=null){
          targetDBsPanel = _targetDBsPanel;
        }
        else{
          targetDBsPanel = makeTargetDBsPanel();
        }
        JPanel pTargetDBs = targetDBsPanel.pTargetDBs;
        JComboBox cbTargetDBSelection = targetDBsPanel.cbTargetDBSelection;

        String dlUrl = null;
        if(_dlUrl!=null){
          dlUrl = _dlUrl;
        }
        else{
          try{
            dlUrl = MyUtil.getURL(defaultURL, pTargetDBs, true, "Choose destination directory");
            if(dlUrl.startsWith("file:")){
              defaultURL = dlUrl;
            }
          }
          catch(Exception e){
            String error = "ERROR: not a valid directory: "+dlUrl;
            Debug.debug(error, 1);
            GridPilot.getClassMgr().getStatusBar().setLabel(error);
            return;
          }
        }
        // queue downloads, if drop-down of DBs is set, request registering new locations
        // by passing on corresponding dbPluginMgr
        if(startDownload(dlUrl, (pTargetDBs!=null && cbTargetDBSelection!=null &&
            cbTargetDBSelection.getSelectedItem()!=null &&
            !cbTargetDBSelection.getSelectedItem().toString().equals(""))?
                GridPilot.getClassMgr().getDBPluginMgr(
                    cbTargetDBSelection.getSelectedItem().toString()) : null)){
          GridPilot.getClassMgr().getGlobalFrame().showMonitoringPanel(MonitoringPanel.TAB_INDEX_TRANSFERS);
        }
      }
    }.start();
  }

  private void lookupPFNs(int [] rows, String [] identifiers){
    //new Thread(){
      //public void run(){
        String [] selectedFileIdentifiers = null;
        int [] selectedRows = null;
        if(rows==null){
          selectedFileIdentifiers = getSelectedIdentifiers();
          selectedRows = tableResults.getSelectedRows();
        }
        else{
          selectedFileIdentifiers = identifiers;
          selectedRows = rows;

        }
        // We assume that the dataset name is used as reference...
        // TODO: improve this
        String datasetColumn = "dsn";
        String [] fileDatasetReference =
          MyUtil.getFileDatasetReference(dbPluginMgr.getDBName());
        if(fileDatasetReference!=null){
          datasetColumn = fileDatasetReference[1];
        }
        dbPluginMgr.clearRequestStopLookup();
        String pfnsColumn = MyUtil.getPFNsField(dbName);
        String catalogsColumn = "catalogs";
        Debug.debug("PFNs column name: "+pfnsColumn, 2);
        JProgressBar pb = MyUtil.setProgressBar(selectedFileIdentifiers.length, dbName);
        for(int i=0; i<selectedFileIdentifiers.length; ++i){
          //pb.setValue(i+1);
          statusBar.incrementProgressBarValue(pb, 1);
          // Get the datasetName from the table.            
          HashMap<String, String> values = new HashMap<String, String>();
          int pfnsColumnIndex = -1;
          int catalogsColumnIndex = -1;
          for(int j=0; j<fieldNames.length; ++j){
            // Not displayed columns
            for(int k=0; k<tableResults.getColumnCount(); ++k){
              if(tableResults.getColumnName(k).equalsIgnoreCase(fieldNames[j])){
                values.put(fieldNames[j], tableResults.getUnsortedValueAt(selectedRows[i], k).toString());
                break;
              }
            }
            if(fieldNames[j].equalsIgnoreCase(pfnsColumn)){
              pfnsColumnIndex = j;
            }
            if(fieldNames[j].equalsIgnoreCase(catalogsColumn)){
              catalogsColumnIndex = j;
            }
          }
          String datasetName = null;
          try{
            datasetName = values.get(datasetColumn).toString();
          }
          catch(Exception e){
          }
          setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
          String [][] catsPfns = dbPluginMgr.getFileURLs(datasetName, selectedFileIdentifiers[i],
              findAll());
          if(catalogsColumnIndex>-1 && catsPfns!=null && catsPfns[0]!=null){
            tableResults.setValueAt(MyUtil.arrayToString(catsPfns[0]), selectedRows[i], catalogsColumnIndex);
          }
          if(pfnsColumnIndex>-1 && catsPfns!=null && catsPfns[1]!=null){
            tableResults.setValueAt(MyUtil.arrayToString(catsPfns[1]), selectedRows[i], pfnsColumnIndex);
          }
          setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        }
        GridPilot.getClassMgr().getStatusBar().removeProgressBar(pb);
        GridPilot.getClassMgr().getStatusBar().clearCenterComponent();
      //}
    //}.start();
  }

  private void copyPFNs(){
    String [] selectedFileIdentifiers = getSelectedIdentifiers();
    int [] selectedRows = tableResults.getSelectedRows();
    Object [] pfns = new String [selectedFileIdentifiers.length];
    for(int i=0; i<selectedFileIdentifiers.length; ++i){
      HashMap<String, String> values = new HashMap<String, String>();
      String pfnsColumn = MyUtil.getPFNsField(dbName);
      Debug.debug("PFNs column name: "+pfnsColumn, 2);
      int pfnsColumnIndex = -1;
      for(int j=0; j<fieldNames.length; ++j){
        // Not displayed colums
        for(int k=0; k<tableResults.getColumnCount(); ++k){
          if(tableResults.getColumnName(k).equalsIgnoreCase(fieldNames[j])){
            values.put(fieldNames[j], tableResults.getUnsortedValueAt(selectedRows[i], k).toString());
            break;
          }
        }
        if(fieldNames[j].equalsIgnoreCase(pfnsColumn)){
          pfnsColumnIndex = j;
        }
      }
      setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
      pfns[i] = tableResults.getUnsortedValueAt(selectedRows[i], pfnsColumnIndex);
      setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
    }
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    StringSelection stringSelection = new StringSelection(MyUtil.arrayToString(pfns));
    clipboard.setContents(stringSelection, this);
    clipboardOwned = false;
    menuEditPaste.setEnabled(false);
    GridPilot.getClassMgr().getGlobalFrame().cutting = false;
  }

  /**
   * Lookup the values of a row in the displayed table.
   * @param row
   * @return
   */
  private HashMap<String, Object> getValues(int row){
    HashMap<String, Object> values = new HashMap<String, Object>();
    for(int j=0; j<fieldNames.length; ++j){
      // Not displayed colums
      for(int k=0; k<tableResults.getColumnCount(); ++k){
        if(tableResults.getColumnName(k).equalsIgnoreCase(fieldNames[j])){
          values.put(fieldNames[j], tableResults.getUnsortedValueAt(row, k).toString());
          break;
        }
      }
    }
    return values;
  }
  
  /**
   * Starts the download of the selected files to the directory _dlUrl.
   * @param _dlUrl download directory.
   * @param regDBPluginMgr if not null, DBPluginMgr to use to register the new
   * file locations.
   * @return
   */
  private boolean startDownload(final String _dlUrl, final DBPluginMgr regDBPluginMgr){
    boolean ret = false;
    String [] selectedFileIdentifiers = getSelectedIdentifiers();
    int [] selectedRows = tableResults.getSelectedRows();
    Debug.debug("Starting download of "+selectedFileIdentifiers.length+" files", 2);
    GlobusURL srcUrl = null;
    GlobusURL destUrl = null;
    Vector<TransferInfo> transfers = new Vector<TransferInfo>();
    MyTransferControl transferControl = GridPilot.getClassMgr().getTransferControl();
    JProgressBar pb = MyUtil.setProgressBar(selectedFileIdentifiers.length, dbName);
    TransferInfo transfer;
    String pfnsColumn = MyUtil.getPFNsField(dbName);
    for(int i=0; i<selectedFileIdentifiers.length; ++i){
      pb.setValue(i+1);
      String [] urls = null;
      // First try and get the values from the table.            
      HashMap<String, Object> values = getValues(selectedRows[i]);
      String bytes = null;
      String checksum = null;
      try{
        bytes = (String) values.get(MyUtil.getFileSizeField(dbName));
      }
      catch(Exception e){
      }
      try{
        checksum = (String) values.get(MyUtil.getChecksumField(dbName));
      }
      catch(Exception e){
      }
      try{
        String urlsString = values.get(pfnsColumn).toString();
        // If the PFNs have not been looked up, do it.
        Debug.debug("urlsString: "+urlsString, 2);
        if(urlsString==null || urlsString.equals("")){
          lookupPFNs(new int [] {selectedRows[i]}, new String [] {selectedFileIdentifiers[i]});
          Thread.sleep(2000L);
          values = getValues(selectedRows[i]);
          urlsString = (String) values.get(pfnsColumn);
          Debug.debug("new urlsString: "+pfnsColumn+"-->"+urlsString, 2);
          // This may also cause some missing bytes and checksums to be filled in
          if(bytes==null || bytes.equals("")){
            bytes = (String) values.get(MyUtil.getFileSizeField(dbName));
          }
          if(checksum==null || checksum.equals("")){
            checksum = (String) values.get(MyUtil.getChecksumField(dbName));
          }
        }
        // If no PFNs were found, skip this transfer (an exception will be thrown and caught).
        urls = MyUtil.splitUrls(urlsString);
      }
      catch(Exception e){
      }
      // Just in case "pfns field" is not set in the config file and there is a "url" 
      // column...
      if(urls==null){
        try{
          String urlsString = values.get("url").toString();
          urls = new String [] {urlsString};
        }
        catch(Exception e){
        }
      }
      try{
        transfer = MyTransferControl.mkTransfer(srcUrl, destUrl, _dlUrl, urls,
           bytes, checksum, regDBPluginMgr,
           selectedFileIdentifiers[i], values, dbPluginMgr, findAll());
        MyUtil.setClosestSource(transfer);
        Debug.debug("adding transfer "+transfer.getSource().getURL()+" ---> "+transfer.getDestination().getURL(), 2);
        transfers.add(transfer);
      }
      catch(Exception e){
        String error = "WARNING: could not download file "+srcUrl+". Failed with all sources. "+e.getMessage();
        GridPilot.getClassMgr().getLogFile().addMessage(error);
        Debug.debug(error, 1);
        e.printStackTrace();
        continue;
      }
    }
    GridPilot.getClassMgr().getStatusBar().removeProgressBar(pb);
    GridPilot.getClassMgr().getStatusBar().clearCenterComponent();
    if(!transfers.isEmpty()){
      try{
        //Queue the transfers
        transferControl.queue(transfers);
        ret = true;
      }
      catch(Exception e){
        String error = "ERROR: could not queue transfers. "+e.getMessage();
        GridPilot.getClassMgr().getLogFile().addMessage(error);
        Debug.debug(error, 1);
        e.printStackTrace();
      }
    }
    return ret;
  }
  
  /**
   * Called when mouse is pressed on the Monitor button
   * on the jobDefinition panel
   */
  private void monitorJobs(){
    String [] jobIds = getSelectedIdentifiers();
    monitorJobs(jobIds, true);
  }
  
  private void monitorJobs(final String [] jobDefIds, boolean threaded){
    GridPilot.getClassMgr().getGlobalFrame().showMonitoringPanel(MonitoringPanel.TAB_INDEX_JOBS);
    Thread t = new Thread(){
      public void run(){        
        JobMgr jobMgr = null;
        
        statusBar.setLabel("Retrieving jobs...");
        // Group the file IDs by dataset
        HashMap<JobMgr, Vector<String>> jobMgrsAndIds = new HashMap<JobMgr, Vector<String>>();
        for(int i=0; i<jobDefIds.length; ++i){
          jobMgr = GridPilot.getClassMgr().getJobMgr(dbName);
          if(!jobMgrsAndIds.containsKey(jobMgr)){
            jobMgrsAndIds.put(jobMgr, new Vector<String>());
          }
          jobMgrsAndIds.get(jobMgr).add(jobDefIds[i]);
        }
        statusBar.setLabel("Retrieving jobs done.");
        // Add them
        String [] ids = null;
        Vector<String> idVec = null;
        for(Iterator<JobMgr> it=jobMgrsAndIds.keySet().iterator(); it.hasNext();){
          jobMgr = it.next();
          idVec = jobMgrsAndIds.get(jobMgr);
          ids = new String [idVec.size()];
          for(int i=0; i<idVec.size(); ++i){
            ids[i] = idVec.get(i).toString();
          }
          jobMgr.addJobs(ids);
        }

        jobMgr.updateJobsByStatus();
      }
    };
    if(threaded){
      t.start();
    }
    else{
      t.run();
    }
  }
  
  /**
   * Called when mouse is pressed on Define button on a dataset tab
   */
  private void bCreateDatasets_mousePressed(JButton invoker){
    // check if anything is selected
    String [] selectedDatasetIdentifiers = getSelectedIdentifiers();
    // if a dataset is selected, show the menu
    if(selectedDatasetIdentifiers.length!=0){
      pmCreateDSMenu.show(invoker, 0, 0); // without this, pmSubmitMenu.getWidth == 0
      pmCreateDSMenu.show(invoker, -pmCreateDSMenu.getWidth(),
                        -pmCreateDSMenu.getHeight() + invoker.getHeight());
    }
    else{
      createDatasets(false);
    }
  }
  
  /**
   * Called when mouse is pressed on Submit button
   */
  private void bSubmit_mousePressed(){
    // check if selected jobs are submittable
    /*String [] selectedJobIdentifiers = getSelectedIdentifiers();
    if(!areSubmitable(selectedJobIdentifiers)){
      statusBar.setLabel("ERROR: all selected jobs must be submittable.");
      return;
    }*/ // Done later.
    // if a jobDefinition is selected, show the menu with computing systems
    if(getSelectedIdentifiers().length!=0){
      pmSubmitMenu.show(this, 0, 0); // without this, pmSubmitMenu.getWidth == 0
      pmSubmitMenu.show(bSubmit, -pmSubmitMenu.getWidth(),
                        -pmSubmitMenu.getHeight() + bSubmit.getHeight());
    }
  }

  /**
   * Called when mouse is pressed on Run button
   */
  private void bProcess_mousePressed(){
    // if dataset is selected, show the menu with computing systems
    if(getSelectedIdentifiers().length!=0){
      Debug.debug("Processing "+getSelectedIdentifiers().length+" dataset(s) on one of "+
          GridPilot.CS_NAMES.length+" CS backends", 2);
      pmProcessMenu.show(this, 0, 0); // without this, pmSubmitMenu.getWidth == 0
      pmProcessMenu.show(bProcessDatasets, -pmProcessMenu.getWidth(),
                        -pmProcessMenu.getHeight() + bProcessDatasets.getHeight());
    }
  }

  /**
   * Called when a computing system in pmSubmitMenu is selected.
   * Submits all selected job definitions to computing system chosen in the popup menu.
   */
  private void submit(final ActionEvent e){
    workThread = new ResThread(){
      public void run(){
        String csName = ((JMenuItem)e.getSource()).getText();
        String [] jobDefIds = getSelectedIdentifiers();
        if(!areSubmitable(jobDefIds)){
          MyUtil.showMessage(window,
             "Job(s) not submitable", "Not all job(s) in selection are submitable.");
          return;
        }
        doSubmit(csName, jobDefIds);
      }
    };
    workThread.start();
  }
  
  private boolean areSubmitable(String[] jobDefIds) {
    for(int i=0; i<jobDefIds.length; ++i){
      if(!isSubmitable(jobDefIds[i])){
        return false;
      }
    }
    return true;
  }

  private boolean isSubmitable(String jobDefId) {
    String status;
    status = dbPluginMgr.getJobDefStatus(jobDefId);
    if(DBPluginMgr.getStatusId(status)!=DBPluginMgr.DEFINED){
      return false;
    }
    return true;
  }
  
  private void doSubmit(String csName, String [] jobDefIds){
    if(!waitForWorking()){
      GridPilot.getClassMgr().getLogFile().addMessage("WARNING: table busy, nothing submitted");
      return;
    }
    statusBar.setLabel("Preparing jobs, please wait...");
    setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    statusBar.animateProgressBar();
    statusBar.setIndeterminateProgressBarToolTip("click here to interrupt job queuing)");
    statusBar.addIndeterminateProgressBarMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent me){
        setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        statusBar.stopAnimation();
        workThread.interrupt();
      }
    });
    Vector<DBRecord> selectedJobDefinitions = new Vector<DBRecord>();
    for(int i=0; i<jobDefIds.length; ++i){
      selectedJobDefinitions.add(dbPluginMgr.getJobDefinition(jobDefIds[i]));
    }
    // First try a test job.
    statusBar.setLabel("Submitting first job. Please wait...");
    try{
      if(!GridPilot.ADVANCED_MODE){
        MyUtil.showMessage(window, "Submitting job(s)",
            "Your job(s) will be prepared and submitted.  This may involve downloading\n" +
            "and booting a virtual machine and downloading input file(s), so please\n" +
            "have patience. You can follow the progress on the various monitor tabs.");
      }
      runFirstJobDefinition(selectedJobDefinitions, csName);
    }
    catch(Exception e){
      e.printStackTrace();
      showSubmissionError(e, selectedJobDefinitions, csName);
      try{
        workThread.interrupt();
        GridPilot.getClassMgr().getSubmissionControl().cancelSubmission();
      }
      catch(Exception ee){
        ee.printStackTrace();
      }
      statusBar.stopAnimation();
      setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      return;
    }
    // submit the remaining jobs
    statusBar.setLabel("Submitting. Please wait...");
    GridPilot.getClassMgr().getSubmissionControl().submitJobDefinitions(selectedJobDefinitions, csName, dbPluginMgr);
    statusBar.stopAnimation();
    statusBar.setLabel("");
    setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
    stopWorking();
  }
  
  private void showSubmissionError(Exception e, Vector<?> selectedJobDefinitions, String csName) {
    String error = e.getMessage();
    MyUtil.showMessage(window,
        "Submission failed", "Submission of the "+
        (selectedJobDefinitions.isEmpty()?"":"first ")+"job failed." +
            "\nPlease check that you're allowed to run jobs on "+csName+
            "\nand that any runtime environments your job's executable is requiring" +
            "\nare available on the chosen computing system.\n\n"+
            (error==null||error.equals("")?"See the log for details.":" "+error));
  }

  public void copy(){
    Debug.debug("Copying!", 3);
    int [] rows = tableResults.getSelectedRows();
    String [] ids = getSelectedIdentifiers();
    String [] copyObjects = new String [ids.length];
    if(tableName.equalsIgnoreCase("dataset")){
      String nameField = MyUtil.getNameField(dbPluginMgr.getDBName(), "dataset");
      int nameIndex = -1;
      for(int i=0; i<tableResults.getColumnNames().length; ++i){
        if(tableResults.getColumnNames()[i].equalsIgnoreCase(nameField)){
          nameIndex = i;
        }
      }
      for(int i=0; i<ids.length; ++i){
        copyObjects[i] = 
          "'"+tableResults.getUnsortedValueAt(rows[i], nameIndex).toString()+"'::'"+
          ids[i]+"'";
      }
    }
    if(tableName.equalsIgnoreCase("file")){
      String nameField = MyUtil.getNameField(dbPluginMgr.getDBName(), "file");
      String [] datasetNameFields = MyUtil.getFileDatasetReference(dbPluginMgr.getDBName());
      String datasetNameField = datasetNameFields[1];
      int nameIndex = -1;
      int datasetNameIndex = -1;
      for(int i=0; i<tableResults.getColumnNames().length; ++i){
        if(tableResults.getColumnNames()[i].equalsIgnoreCase(nameField)){
          nameIndex = i;
        }
        if(tableResults.getColumnNames()[i].equalsIgnoreCase(datasetNameField)){
          datasetNameIndex = i;
        }
      }
      if(datasetNameIndex==-1){
        MyUtil.showError(window, "To copy from this table, "+datasetNameField+" must be displayed.");
        return;
      }
      if(nameIndex==-1){
        MyUtil.showError(window, "To copy from this table, "+nameField+" must be displayed.");
        return;
      }
      Debug.debug("Indices: "+datasetNameIndex+":"+nameIndex, 2);
      for(int i=0; i<ids.length; ++i){
        copyObjects[i] = 
          "'"+tableResults.getUnsortedValueAt(rows[i], datasetNameIndex)+"'::'"+
          tableResults.getUnsortedValueAt(rows[i], nameIndex)+"'::'"+
          ids[i]+"'";
      }
    }
    else{
      copyObjects = ids;
    }
    StringSelection stringSelection = new StringSelection(
        dbName+" "+tableName+" "+MyUtil.arrayToString(copyObjects));
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    clipboard.setContents(stringSelection, this);
    clipboardOwned = true;
    menuEditPaste.setEnabled(true);
    GridPilot.getClassMgr().getGlobalFrame().cutting = false;
  }
  
  public void cut(){
    Debug.debug("Cutting!", 3);
    copy();
    GridPilot.getClassMgr().getGlobalFrame().cutting = true;
    GridPilot.getClassMgr().getGlobalFrame().setCutPanel(this);
  }
  
  public void paste(){
    String clip = getClipboardContents();
    Debug.debug("Pasting "+clip, 3);
    // check if this is a 'normal' clipboard of the form <db> <table> <id1> <id2> ...
    // or the more fancy <db> <table> 'dsname1'::'id1' 'dsname2'::'id2' ...
    // or <db> <table> 'dsname1'::'name1'::'id1' 'dsname2'::'name2'::'id2' ...
    String [] records = null;
    if(clip.indexOf("'")>0){
      String [] recs = MyUtil.split(clip, "' '");
      records = new String [recs.length+2];
      String [] head = MyUtil.split(recs[0]);
      if(head.length<3){
        return;
      }
      records[0] = head[0];
      records[1] = head[1];
      records[2] = head[2]+(recs.length>1?"'":"");
      for(int i=1; i<recs.length-1; ++i){
        records[i+2] = "'"+recs[i]+"'";
      }
      if(recs.length>1){
        records[recs.length+1] = "'"+recs[recs.length-1];
      }
    }
    else{
      records = MyUtil.split(clip);
    }
    if(records.length<3){
      return;
    }
    if(!clipboardOwned){
      return;
    }
    boolean ok = true;
    String db = records[0];
    String table = records[1];
    try{
      // this can stay null for other than datasets and files
      for(int i=2; i<records.length; ++i){
        // Check if record is a dataset and already there
        // ask for prefix if this is the case.
        // Only datasets have unique names.
        try{
          Debug.debug("Pasting "+records[i], 2);
          pasteRecord(records[i], db, table);
        }
        catch(Exception e){
          ok = false;
          String error = "ERROR: could not insert record: "+
          db+", "+table+", "+dbName+", "+tableName+", "+records[i];
          GridPilot.getClassMgr().getLogFile().addMessage(error, e);
          e.printStackTrace();
        }
      }
    }
    catch(Exception e){
      return;
    }
    // If records were inserted in target table and we're cutting,
    // delete source records
    if(GridPilot.getClassMgr().getGlobalFrame().cutting){
      Debug.debug("Deleting "+(records.length-2)+" rows", 2);
      statusBar.setLabel(
      "Deleting job definition(s). Please wait...");
      JProgressBar pb = new JProgressBar();
      statusBar.setProgressBar(pb);
      statusBar.setProgressBarMax(pb, records.length-2);
      for(int i=2; i<records.length; ++i){
        try{
          deleteRecord(db, table, records[i]);
        }
        catch(Exception e){
          String msg = "Deleting record "+(i-2)+" failed. "+
             GridPilot.getClassMgr().getDBPluginMgr(db).getError();
          Debug.debug(msg, 1);
          statusBar.setLabel(msg);
          GridPilot.getClassMgr().getLogFile().addMessage(msg);
          continue;
        }
        pb.setValue(pb.getValue()+1);
      }
      statusBar.setLabel(
         "Deleting job definition(s) done.");
      statusBar.removeProgressBar(pb);
    }
    GridPilot.getClassMgr().getGlobalFrame().cutting = false;
    if(ok){
      refresh();
    }
    GridPilot.getClassMgr().getGlobalFrame().refreshCutPanel();
  }
  
  private void pasteRecord(String record, String db, String table) throws Exception {
    String id = record;
    // this can stay null for other than files
    String datasetName = null;
    String name = null;
    if(tableName.equalsIgnoreCase("dataset")){
      // Now, DQ2 cannot even lookup dataset names from ID...
      // Another ugly hack: we pass both id and name
      // e.g. ATLAS dataset b2a80235-0be0-4b9d-9785-b12565c21fcd user.FrederikOrellana5894-ATLAS.csc11.002.Gee_500_pythia_photos_reson
      //name = GridPilot.getClassMgr().getDBPluginMgr(db).getDatasetName(
      //    record);
      int index = record.indexOf("'::'");
      if(index>-1){
        datasetName = record.substring(1, index);
        String rest = record.substring(index+4);
        index = rest.indexOf("'::'");
        id = rest.substring(0, index);
        name = datasetName;
      }
      // Get the name of the dataset from the source db
      String dsName = GridPilot.getClassMgr().getDBPluginMgr(db).getDatasetName(id);
      // See if the name exists in the destination db
      String testDsName = null;
      try{
        testDsName = GridPilot.getClassMgr().getDBPluginMgr(dbName).getDatasetName(id);
      }
      catch(Exception e){
      }
      Debug.debug("Dataset name: "+dbName+"-->"+dsName+"-->"+testDsName, 2);
      if(testDsName!=null && !testDsName.equals("-1")){            
        name = MyUtil.getName("Cannot overwrite, please give new name", "new-"+testDsName);
        if(name==null || name.equals("")){
          return;
        }
      }
    }
    else if(tableName.equalsIgnoreCase("file")){
      // Well, once more, because DQ2 cannot lookup files we have
      // to introduce an ugly hack: we pass both dataset name, file name and id
      // ('dsname'::'name'::'id') when copy-pasting
      int index = record.indexOf("'::'");
      datasetName = record.substring(1, index);
      String rest = record.substring(index+4);
      index = rest.indexOf("'::'");
      name = rest.substring(0, index);
      id = rest.substring(index+4, rest.length()-1);
    }
    insertRecord(db, table, id, name, datasetName);
  }

  /**
   * 
   * The method called when copy-pasting. Inserts a DB record.
   * 
   * @param sourceDB
   * @param sourceTable
   * @param targetDB
   * @param targetTable
   * @param id
   * @param name
   * @param datasetName    only relevant for job definitions and files
   * @throws Exception
   */
  private void insertRecord(String sourceDB, String sourceTable,
      String id, String name, String datasetName) throws Exception{
    
    DBPluginMgr sourceMgr = GridPilot.getClassMgr().getDBPluginMgr(sourceDB);
    
    DBRecord record = null;

    if(tableName.equalsIgnoreCase("jobDefinition")){
      try{
        record = sourceMgr.getJobDefinition(id);
        insertJobDefinition(sourceMgr, record);
      }
      catch(Exception e){
        String msg = "ERROR: job definition "+id+" could not be created, "+sourceDB+
        "."+sourceTable+"->"+dbName+"."+tableName+". "+e.getMessage();
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
    else if(tableName.equalsIgnoreCase("dataset")){
      try{
        record = sourceMgr.getDataset(id);
        insertDataset(record, sourceMgr, name, id);
      }
      catch(Exception e){
        String msg = "ERROR: dataset "+id+" could not be created, "+sourceDB+
        "."+sourceTable+"->"+dbName+"."+tableName+". "+e.getMessage();
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
    else if(tableName.equalsIgnoreCase("executable")){
      try{
        record = sourceMgr.getExecutable(id);
        insertExecutable(record, sourceMgr);
      }
      catch(Exception e){
        String msg = "ERROR: executable "+id+" could not be created, "+sourceDB+
        "."+sourceTable+"->"+dbName+"."+tableName+". "+e.getMessage();
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
    else if(tableName.equalsIgnoreCase("runtimeEnvironment")){
      try{
        record = sourceMgr.getRuntimeEnvironment(id);
        insertRuntimeEnvironment(record);
      }
      catch(Exception e){
        String msg = "ERROR: runtime environment "+id+" could not be created, "+sourceDB+
        "."+sourceTable+"->"+dbName+"."+tableName+". "+e.getMessage();
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
    else if(tableName.equalsIgnoreCase("file")){
      try{
        insertFile(sourceMgr, name, datasetName, id);
      }
      catch(Exception e){
        String msg = "ERROR: file "+id+" could not be inserted, "+sourceDB+
        "."+sourceTable+"->"+dbName+"."+tableName+". "+e.getMessage();
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
  }
  
  private boolean insertJobDefinition(DBPluginMgr sourceMgr, DBRecord jobDef) throws Exception{  
    try{
      // Check if parent dataset exists
      String sourceJobDefIdentifier = MyUtil.getIdentifierField(sourceMgr.getDBName(), "jobDefinition");
      String datasetName = sourceMgr.getDatasetName(
          sourceMgr.getJobDefDatasetID((String) jobDef.getValue(sourceJobDefIdentifier)));
      String targetDsId = dbPluginMgr.getDatasetID(datasetName);
      if(!targetDsId.equals("-1")){
        dbPluginMgr.createJobDef(jobDef.fields, jobDef.values);
      }
      else{
        String error = "ERROR: parent dataset for job definition does not exist.";
        throw(new Exception(error));
      }
    }
    catch(Exception e){
      throw e;
    }
    return true;
  }
  
  private boolean insertFile(DBPluginMgr sourceMgr, String name, String datasetName,
      String fileID) throws Exception{
    if(!dbPluginMgr.isFileCatalog()){
      ConfirmBox confirmBox = new ConfirmBox(GridPilot.getClassMgr().getGlobalFrame());
      String msg = "Cannot create file(s) in virtual table.";
      confirmBox.getConfirm("Confirm delete", msg, new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane())});
      throw new SQLException(msg);
    }
    try{
      // Check if parent dataset exists - NO, not necessary, it will be created...
      /*String targetDatasetId = dbPluginMgr.getDatasetID(datasetName);
      if(!targetDatasetId.equals("-1")){*/
        dbPluginMgr.createFil(sourceMgr, datasetName, name, fileID);
      /*}
      else{
        throw(new Exception("ERROR: Parent dataset for file "+
            name+" does not exist."));
      }*/
    }
    catch(Exception e){
      throw e;
    }
    return true;
  }
  
  private void insertDataset(DBRecord dataset, DBPluginMgr sourceMgr,
      String name, String id) throws Exception{
    boolean ok = false;
    try{
      // If there are no executables in source or target, there's no point
      // in checking
      if(!sourceMgr.isJobRepository() || !dbPluginMgr.isJobRepository()){
        ok = true;
      }
      else{
        // Check if referenced executable exists     
        String sourceExeName = sourceMgr.getDatasetExecutableName(
            dataset.getValue(MyUtil.getIdentifierField(sourceMgr.getDBName(), "dataset")).toString());
        String sourceExeVersion = sourceMgr.getDatasetExecutableVersion(
            dataset.getValue(MyUtil.getIdentifierField(sourceMgr.getDBName(), "dataset")).toString());          
        DBResult targetExecutable = dbPluginMgr.getExecutables();
        Vector<DBRecord> transVec = new Vector<DBRecord>();
        for(int i=0; i<targetExecutable.values.length; ++i){
          if(targetExecutable.getValue(i, MyUtil.getNameField(dbPluginMgr.getDBName(),
              "executable")).toString().equalsIgnoreCase(sourceExeName)){
            transVec.add(targetExecutable.get(i));
          }
        }
        for(int i=0; i<transVec.size(); ++i){
          if(((DBRecord) transVec.get(i)).getValue(MyUtil.getVersionField(dbPluginMgr.getDBName(),
              "executable")).toString().equalsIgnoreCase(sourceExeVersion)){
            ok = true;
            break;
          }
        }
      }
    }
    catch(Exception ee){
      ee.printStackTrace();
    }
    // If this is a job-only database (no file catalog) we deny creating
    // orphaned datasets (data provenance enforcement).
    if(!ok && (!dbPluginMgr.isFileCatalog() || dbPluginMgr.isJobRepository())){
      String error = "ERROR: executable for dataset does not exist.";
      throw(new Exception(error));
    }
    boolean success = doInsertDataset(dataset, sourceMgr, name, id);
    if(!success){
      throw(new Exception("ERROR: "+dbPluginMgr.getError()));
    }
  }
  
  private boolean doInsertDataset(DBRecord dataset, DBPluginMgr sourceMgr,
      String name, String id) throws Exception{
    String [] targetFields = dbPluginMgr.getFieldNames("dataset");
    String [] targetValues = new String[targetFields.length];
    String [] sourceFields = sourceMgr.getFieldNames("dataset");
    String [] sourceValues = new String[sourceFields.length];
    for(int j=0; j<targetFields.length; ++j){ 
      targetValues[j] = "";
      // Do the mapping.
      for(int k=0; k<sourceFields.length; ++k){
        if(sourceFields[k].equalsIgnoreCase(targetFields[j])){
          targetValues[j] = sourceValues[k];
          break;
        }
      }
      // See if attribute is in target dataset and set. If not, ignore.
      if(targetValues[j]==null || targetValues[j].equals("")){
        boolean fieldPresent = false;
        for(int l=0; l<sourceFields.length; ++l){
          if(targetFields[j].equalsIgnoreCase(sourceFields[l])){
            fieldPresent = true;
            break;
          }
        }
        if(fieldPresent){
          try{
            if(dataset.getValue(targetFields[j])!=null){
              targetValues[j] = (String) dataset.getValue(targetFields[j]);
            }
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
    }
    dataset = new DBRecord(targetFields, targetValues);
    Debug.debug("Creating dataset: " + MyUtil.arrayToString(dataset.fields, ":") + " ---> " +
        MyUtil.arrayToString(dataset.values, ":"), 3);
    try{
      // If name is specified, use it
      if(name!=null && !name.equals("")){
        Debug.debug("Setting name "+name, 3);
        dataset.setValue(MyUtil.getNameField(dbPluginMgr.getDBName(), "dataset"),
            name);
      }
    }
    catch(Exception e){
      Debug.debug("WARNING: Could not set dataset name "+name, 3);
      e.printStackTrace();
    }
    try{
      // If id is specified, use it - except when copying from a
      // job-only database - in which case the id will be a useless
      // autoincremented number or if source and target db are the same
      // - in which case we clear the id
      if(id!=null && !id.equals("")){
        if(sourceMgr.getDBName().equals(dbPluginMgr.getDBName())){
          Debug.debug("Clearing id", 3);
          dataset.setValue(MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "dataset"),
              "''");
        }
        else if(sourceMgr.isFileCatalog()){
          Debug.debug("Setting id "+id, 3);
          dataset.setValue(MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "dataset"),
              id);
        }
      }
    }
    catch(Exception e){
      Debug.debug("WARNING: Could not set dataset id "+id, 3);
      e.printStackTrace();
    }
    return dbPluginMgr.createDataset("dataset", dataset.fields, dataset.values);
  }
  
  private boolean insertExecutable(DBRecord executable, DBPluginMgr sourceMgr)
     throws Exception{
    try{
      // Check if referenced runtime environment exists
      String sourceExecutableIdentifier = MyUtil.getIdentifierField(sourceMgr.getDBName(),
          "executable");
      String sourceExecutableName = MyUtil.getNameField(sourceMgr.getDBName(),
         "executable");
      String targetRuntimeEnvironmentName = MyUtil.getNameField(dbPluginMgr.getDBName(),
          "runtimeEnvironment");
      String runtimeEnvironment = sourceMgr.getExecutableRuntimeEnvironment(
          executable.getValue(sourceExecutableIdentifier).toString());
      DBResult targetRuntimes = dbPluginMgr.getRuntimeEnvironments();
      Vector<String> runtimeNames = new Vector<String>();
      for(int i=0; i<targetRuntimes.values.length; ++i){
        runtimeNames.add(targetRuntimes.getValue(i, targetRuntimeEnvironmentName).toString());
      }
      if(runtimeEnvironment==null || runtimeNames==null || !runtimeNames.contains(runtimeEnvironment)){
        String msg = "WARNING: runtime environment "+runtimeEnvironment+", of executable "+
           executable.getValue(sourceExecutableName)+" does not exist.";
        MyUtil.showError(window,
            msg+" You will not be able to use this executable until you've loaded\n" +
        		"a computing system that provides the needed runtime environment.");
        GridPilot.getClassMgr().getLogFile().addInfo(msg);
      }
      dbPluginMgr.createExecutable(executable.fields, executable.values);
    }
    catch(Exception e){
      throw e;
    }
    return true;
  }
  
  private boolean insertRuntimeEnvironment(DBRecord pack) throws Exception{
    try{
      dbPluginMgr.createRuntimeEnv(pack.fields, pack.values);
    }
    catch(Exception e){
      throw e;
    }
    return true;
  }
  
  private void deleteRecord(String sourceDB, String sourceTable, String id) throws Exception{
    DBPluginMgr sourceMgr = GridPilot.getClassMgr().getDBPluginMgr(sourceDB);
    if(tableName.equalsIgnoreCase("jobDefinition")){
      try{
        sourceMgr.deleteJobDefinition(id, false);
      }
      catch(Exception e){
        String msg = "ERROR: job definition "+id+" could not be deleted from, "+sourceDB+
        "."+sourceTable;
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
    else if(tableName.equalsIgnoreCase("dataset")){
      try{
        sourceMgr.deleteDataset(id, true);
      }
      catch(Exception e){
        String msg = "ERROR: dataset "+id+" could not be deleted from, "+sourceDB+
        "."+sourceTable;
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
    else if(tableName.equalsIgnoreCase("executable")){
      try{
        sourceMgr.deleteExecutable(id);
      }
      catch(Exception e){
        String msg = "ERROR: executable "+id+" could not be deleted from, "+sourceDB+
        "."+sourceTable;
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
    else if(tableName.equalsIgnoreCase("runtimeEnvironment")){
      try{
        sourceMgr.deleteRuntimeEnvironment(id);
      }
      catch(Exception e){
        String msg = "ERROR: runtime environment "+id+" could not be deleted from, "+sourceDB+
        "."+sourceTable;
        Debug.debug(msg, 1);
        statusBar.setLabel(msg);
        GridPilot.getClassMgr().getLogFile().addMessage(msg, e);
        throw e;
      }
    }
  }
  
  /**
   * Implementation of the ClipboardOwner interface.
   */
   public void lostOwnership(Clipboard aClipboard, Transferable aContents){
   }

  /**
  * Place a String on the clipboard, and make this class the
  * owner of the Clipboard's contents.
  */
  protected void setClipboardContents(String aString){
    StringSelection stringSelection = new StringSelection(aString);
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    clipboard.setContents(stringSelection, this);
  }

  /**
  * Get the String residing on the clipboard.
  *
  * @return any text found on the Clipboard; if none found, return an
  * empty String.
  */
  protected String getClipboardContents(){
    String result = "";
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    //odd: the Object param of getContents is not currently used
    Transferable contents = clipboard.getContents(null);
    boolean hasTransferableText = (contents!=null) &&
      contents.isDataFlavorSupported(DataFlavor.stringFlavor);
    if(hasTransferableText){
      try{
        result = (String) contents.getTransferData(DataFlavor.stringFlavor);
      }
      catch(UnsupportedFlavorException ex){
        // highly unlikely since we are using a standard DataFlavor
        Debug.debug(ex.getMessage(), 1);
        ex.printStackTrace();
      }
      catch (IOException ex){
        Debug.debug(ex.getMessage(), 1);
        ex.printStackTrace();
      }
    }
    return result;
  }

  protected void setConstraint(String nameField, String runtimeEnvironmentName, int i) {
    selectPanel.setConstraint(nameField, runtimeEnvironmentName, i);
  }

}
