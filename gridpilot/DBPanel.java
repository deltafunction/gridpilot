package gridpilot;

import gridpilot.Table;
import gridpilot.DBResult;
import gridpilot.DBRecord;
import gridpilot.SelectPanel.SPanel;
import gridpilot.Debug;
import gridpilot.GridPilot;

import javax.swing.*;
import javax.swing.event.*;

import org.globus.util.GlobusURL;
import org.safehaus.uuid.UUIDGenerator;

import java.awt.*;
import java.awt.event.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.ClipboardOwner;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.awt.Toolkit;
import java.io.*;

/**
 * This panel contains one SelectPanel.
 *
 */
public class DBPanel extends JPanel implements ListPanel, ClipboardOwner{

  private static final long serialVersionUID = 1L;
  private JScrollPane spSelectPanel = new JScrollPane();
  private JPanel pButtonSelectPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
  private JScrollPane spTableResults = new JScrollPane();
  private Table tableResults = null;
  private JPanel pButtonTableResults = new JPanel(new FlowLayout(FlowLayout.CENTER));
  private JButton bCreateRecords = new JButton("Define new record(s)");
  private JButton bEditRecord = new JButton("Edit record");
  private JCheckBox cbFindAllFiles = new JCheckBox();
  private JButton bDownload = new JButton("Replicate file(s)");
  private JPopupMenu pmSubmitMenu = new JPopupMenu();
  private JButton bSubmit = new JButton("Submit job(s)");
  private JButton bMonitor = new JButton("Monitor job(s)");
  private JButton bDeleteRecord = new JButton("Delete record(s)");
  private JButton bSearch = new JButton("Search");
  private JButton bClear = new JButton("Clear");
  private JButton bViewJobDefinitions = new JButton("Show jobDefinition(s)");
  private JButton bViewFiles = new JButton("Show file(s)");
  private JButton bDefineJobDefinitions = new JButton("Create jobDefinition(s)");
  private JMenuItem miEdit = null;
  private JMenuItem miImportFiles = new JMenuItem("Import file(s)");
  private String [] identifiers;
  // lists of field names with table name as key
  private String [] fieldNames = null;
  private String dbName = null;
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
  private Thread workThread;
  // WORKING THREAD SEMAPHORE
  // The idea is to ignore new requests when working on a request
  private boolean working = false;
  private SubmissionControl submissionControl = null;
  
  private static String defaultURL;
  
  public JPanel panelSelectPanel = new JPanel(new GridBagLayout());
  public SelectPanel selectPanel;

  // try grabbing the semaphore
  private synchronized boolean getWorking(){
    if(!working){
      working = true;
      return true;
    }
    return false;
  }
  // release the semaphore
  private synchronized void stopWorking(){
    working = false;
  }
  
  // see if the find all checkbox is checked
  private boolean findAll(){
    return cbFindAllFiles.isSelected();
  }
  
  /**
   * Create a new DBPanel from scratch.
   */
   public DBPanel(/*name of database*/
                  String _dbName,
                  /*name of tables for the select*/
                  String _tableName) throws Exception{
  
     dbName = _dbName;     
     tableName = _tableName;
     dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
     statusBar = GridPilot.getClassMgr().getStatusBar();
          
     identifier = Util.getIdentifierField(dbPluginMgr.getDBName(), tableName);
     
     if(tableName.equalsIgnoreCase("jobDefinition")){
       jobDefIdentifier = Util.getIdentifierField(dbPluginMgr.getDBName(), "jobDefinition");
     }

     defaultFields = dbPluginMgr.getDBDefFields(tableName);
     Debug.debug("Default fields "+defaultFields.length, 3);

     fieldNames = dbPluginMgr.getFieldNames(tableName);
     
     hiddenFields = dbPluginMgr.getDBHiddenFields(tableName);
     Debug.debug("Hidden fields "+Util.arrayToString(hiddenFields), 3);
     
     submissionControl = GridPilot.getClassMgr().getSubmissionControl();
     
     // Pass on only non-hidden fields to
     // Table. Perhaps rethink: - Table hides fields...
     Vector fieldSet = new Vector();
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
     
     tableResults = new Table(hiddenFields, fieldNames,
         GridPilot.jobColorMapping);
     
     setFieldArrays();
     
     menuEditCopy = GridPilot.getClassMgr().getGlobalFrame().menuEditCopy;
     menuEditCut = GridPilot.getClassMgr().getGlobalFrame().menuEditCut;
     menuEditPaste = GridPilot.getClassMgr().getGlobalFrame().menuEditPaste;
     initGUI();
   }
   
   public Table getTable(){
     return tableResults;
   }

   private void setFieldArrays(){
     Vector shownSet = new Vector();  
     boolean ok = true;
     boolean hiddenOk = true;
     Debug.debug("Finding fields to show from: "+Util.arrayToString(defaultFields), 3);
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
       Debug.debug("Checking "+selectPanel.sPanel.spDisplayList.getComponentCount()+":"+shownFields.length, 3);
       boolean fieldOk = true;
       for(int i=0; i<selectPanel.sPanel.spDisplayList.getComponentCount(); ++i){
         fieldOk = true;
         SPanel.DisplayPanel cb =
           ((SPanel.DisplayPanel) selectPanel.sPanel.spDisplayList.getComponent(i));
         for(int j=0; j<shownFields.length; ++j){
           Debug.debug("Checking fields "+
               cb.cbDisplayAttribute.getSelectedItem().toString()+"<->"+shownFields[j], 3);
           if((cb.cbDisplayAttribute.getSelectedItem().toString()
                   ).equalsIgnoreCase(shownFields[j])){
             fieldOk = false;
             break;
           }
         }
         if(fieldOk){
           shownSet.add(cb.cbDisplayAttribute.getSelectedItem().toString());
         }
       }    
     }

     shownFields = new String[shownSet.size()];
     for(int i=0; i<shownSet.size(); ++i){
       shownFields[i] = shownSet.get(i).toString();
     }
     
     for(int k=0; k<shownFields.length; ++k){
       shownFields[k] = tableName+"."+shownFields[k];
     }
     
     // Set the default values of the selection drop downs.
     // selectFields only used by clear().
     Vector selectSet = new Vector();  
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
   
   /**
    * Create a new DBPanel from a parent panel.
    */
  public DBPanel(/*name of tables for the select*/
                 String _tableName,
                 /*pointer to the db in use for this panel*/
                 DBPluginMgr _dbPluginMgr,
                 /*identifier of the parent record (dataset <- jobDefinition)*/
                 String _parentId) throws Exception{
      this(_dbPluginMgr.getDBName(), _tableName);
      dbPluginMgr = _dbPluginMgr;
      parentId = _parentId;
  }
    
  /**
  * GUI initialisation
  */

  private void initGUI() throws Exception{
    
    this.setLayout(new BorderLayout());

    // SelectPanel
    selectPanel = new SelectPanel(tableName, fieldNames);
    selectPanel.initGUI();
    clear();

    spSelectPanel.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_NEVER);
    spSelectPanel.getViewport().add(selectPanel);

    panelSelectPanel.add(spSelectPanel,
        new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    panelSelectPanel.add(new JLabel(dbName),
        new GridBagConstraints(0, 1, 1, 1, 1.0, 0.0,
            GridBagConstraints.WEST,
            GridBagConstraints.NONE, new Insets(10, 10, 10, 10), 0, 0));
    panelSelectPanel.add(pButtonSelectPanel,
        new GridBagConstraints(0, 1, 1, 1, 1.0, 0.0,
            GridBagConstraints.EAST,
            GridBagConstraints.NONE,new Insets(10, 10, 10, 10), 0, 0));
    panelSelectPanel.validate();

    selectPanel.setConstraint(Util.getNameField(dbPluginMgr.getDBName(), tableName), "", 1);
    
    // Listen for enter key in text field
    this.selectPanel.spcp.tfConstraintValue.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        switch(e.getKeyCode()){
          case KeyEvent.VK_ENTER:
            search();
        }
        if(KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("c") ||
            KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("x")){
          if(e.isControlDown()){
            clipboardOwned = false;
            menuEditPaste.setEnabled(clipboardOwned);
          }
        }

      }
    });
    
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
    
    tableResults.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        //Debug.debug("key code: "+KeyEvent.getKeyText(e.getKeyCode()), 3);
        if(e.getKeyCode()==KeyEvent.VK_F1){
          //menuHelpAbout_actionPerformed();
        }
        else if(KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("x")){
          if(e.isControlDown()){
            cut();
          }
        }
        else if(KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("c")){
          if(e.isControlDown()){
            copy();
          }
        }
        else if(KeyEvent.getKeyText(e.getKeyCode()).equalsIgnoreCase("v")){
          if(e.isControlDown()){
            paste();
          }
        }
      }
    });
        
    //// buttons
    // Costumized for each type of table
    bSearch.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        search();
      }
    });
    bSearch.setToolTipText("Search results for this request");
    bClear.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        clear();
      }
    });
    
    // This is to pad with empty space and keep the table results
    // full width
    GridBagConstraints ct = new GridBagConstraints();
    ct.fill = GridBagConstraints.HORIZONTAL;
    pButtonTableResults.add(new JLabel(), ct);
    
    if(tableName.equalsIgnoreCase("dataset")){
      
      tableResults.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          if(e.getClickCount()==2){
            editDataset();
          }
        }
      });

      bViewJobDefinitions.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){
          viewJobDefinitions();
        }
      }
      );
      
      bViewFiles.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){
          viewFiles();
        }
      }
      );

      bDefineJobDefinitions.addActionListener(new java.awt.event.ActionListener(){
        public void actionPerformed(ActionEvent e){
          defineJobDefinitions();
        }
      }
      );
      
      bCreateRecords.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          createDatasets();
        }
      });

      bEditRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          editDataset();
        }
      });

      bDeleteRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          deleteDatasets();
        }
      });
      
      addButtonResultsPanel(bViewFiles);
      addButtonResultsPanel(bViewJobDefinitions);
      addButtonResultsPanel(bDefineJobDefinitions);
      addButtonResultsPanel(new JLabel("|"));
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonResultsPanel(bDeleteRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      bViewFiles.setEnabled(false);
      bViewJobDefinitions.setEnabled(false);
      bDefineJobDefinitions.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecord.setEnabled(false);
      updateUI();
    }
    else if(tableName.equalsIgnoreCase("file")){
      
      bEditRecord.setText("View record");
      
      tableResults.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          if(e.getClickCount()==2){
            editFile();
          }
        }
      });

      bEditRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          //editJobDef();
          editFile();
        }
      });

      bDeleteRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          //deleteJobDefs();
          deleteFiles();
        }
      });

      bDownload.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          download();
        }
      });
            
      addButtonResultsPanel(bDownload);
      addButtonResultsPanel(new JLabel("|"));
      addButtonResultsPanel(bEditRecord);
      //addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bDeleteRecord);
      // Add tickbox to search panel
      JPanel pFindAll = new JPanel();
      pFindAll.add(new JLabel("Find all PFNs"));
      pFindAll.add(cbFindAllFiles);
      addButtonSelectPanel(pFindAll);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      bEditRecord.setEnabled(false);
      bDeleteRecord.setEnabled(false);
      bDownload.setEnabled(false);
      updateUI();
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

      bEditRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          editJobDef();
        }
      });

      bDeleteRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          deleteJobDefs();
        }
      });

      bCreateRecords.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          createJobDefinitions();
        }
      });
      
      for(int i=0; i<GridPilot.csNames.length ; ++i){
        JMenuItem mi = new JMenuItem(GridPilot.csNames[i]);
        //mi.setMnemonic(i);
        mi.addActionListener(new ActionListener(){
          public void actionPerformed(final ActionEvent e){
                submit(e);
          }});
        pmSubmitMenu.add(mi);
      }

      addButtonResultsPanel(bSubmit);
      addButtonResultsPanel(bMonitor);
      addButtonResultsPanel(new JLabel("|"));
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonResultsPanel(bDeleteRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      bSubmit.setEnabled(false);
      bMonitor.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecord.setEnabled(false);
      updateUI();
    }
    else if(tableName.equalsIgnoreCase("transformation")){
      
      tableResults.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          if(e.getClickCount()==2){
            editTransformation();
          }
        }
      });

      bCreateRecords.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          createTransformation();
        }
      });

      bEditRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          editTransformation();
        }
      });

      bDeleteRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          deleteTransformations();
        }
      });
      
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonResultsPanel(bDeleteRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      //bViewFiles.setEnabled(false);
      //bViewJobDefinitions.setEnabled(false);
      //bDefineJobDefinitions.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecord.setEnabled(false);
      updateUI();
    }    
    else if(tableName.equalsIgnoreCase("runtimeEnvironment")){
      
      tableResults.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          if(e.getClickCount()==2){
            editRuntimeEnvironment();
          }
        }
      });
      
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

      bDeleteRecord.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          deleteRuntimeEnvironments();
        }
      });
      
      addButtonResultsPanel(bCreateRecords);
      addButtonResultsPanel(bEditRecord);
      addButtonResultsPanel(bDeleteRecord);
      addButtonSelectPanel(bClear);
      addButtonSelectPanel(bSearch);
      //bViewFiles.setEnabled(false);
      //bViewJobDefinitions.setEnabled(false);
      //bDefineJobDefinitions.setEnabled(false);
      bEditRecord.setEnabled(false);
      bDeleteRecord.setEnabled(false);
      menuEditCopy.setEnabled(false);
      menuEditCut.setEnabled(false);
      menuEditPaste.setEnabled(false);
      updateUI();
    }    
  }

  public String getTitle(){
    return tableName+"s"/*jobTranss looks bad...*/;
  }

  public void panelShown(){
    Debug.debug("panelShown", 1);
    boolean rowsAreSelected = tableResults.getSelectedRows().length>0;
    menuEditCopy.setEnabled(rowsAreSelected);
    menuEditCut.setEnabled(rowsAreSelected);
    // Check if clipboard is of the form "db table id1 id2 id3 ..."
    String clip = getClipboardContents();
    String clips [] = null;
    if(clip!=null){
      clips = Util.split(clip);
    }
    if(clips!=null && clips.length>2 && clips[1].equalsIgnoreCase(tableName)){
      clipboardOwned = true;
    }
    else{
      clipboardOwned = false;
    }
    menuEditPaste.setEnabled(clipboardOwned);
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
   * Adds a button on the left of the buttons shown when the panel with results is shown.
   */
  public void addButtonResultsPanel(JComponent b){
    pButtonTableResults.add(b);
  }

  /**
   * Adds a button on the select panel.
   */
  public void addButtonSelectPanel(JComponent b){
    pButtonSelectPanel.add(b);
  }

  /**
   * Carries out search according to selection
   */
  public void search(){
    if(tableResults==null){
      searchRequest();
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
    updateUI();
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
    selectPanel.setDisplayFieldValue(values);
    selectPanel.resetConstraintList(tableName);
    selectPanel.setConstraint(Util.getNameField(dbPluginMgr.getDBName(), tableName), "", 1);
    selectPanel.updateUI();
  }

  /**
   * Request DBPluginMgr for select request from SelectPanel, and fill tableResults
   * with results. The request is performed in a separeted thread, avoiding to block
   * all the GUI during this action.
   * Called when button "Search" is clicked
   */
  public void searchRequest(final int sortColumn, final boolean isAscending,
      final int [] columnWidths){
        
    workThread = new Thread(){
      public void run(){
        for(int i=0; i<3; ++i){
          if(!getWorking()){
            // retry 3 times with 3 seconds in between
            statusBar.setLabel("Busy, please wait ...", 2);
            try{
              Thread.sleep(3000);
            }
            catch(Exception e){
            }
            if(i==2){
              return;
            }
            else{
              continue;
            }
          }
          else{
            break;
          }
        }
        statusBar.setLabel("Searching, please wait ...", 2);
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
        setFieldArrays();
        String selectRequest;
        selectRequest = selectPanel.getRequest(shownFields);
        if(selectRequest==null){
            return;
        }
        DBResult res = null;
        res = dbPluginMgr.select(selectRequest, identifier, findAll());

        bViewFiles.setEnabled(false);
        bViewJobDefinitions.setEnabled(false);
        bDefineJobDefinitions.setEnabled(false);
        bEditRecord.setEnabled(false);
        bDeleteRecord.setEnabled(false);
        bSubmit.setEnabled(false);
        bMonitor.setEnabled(false);
        menuEditCopy.setEnabled(false);
        menuEditCut.setEnabled(false);
        menuEditPaste.setEnabled(clipboardOwned);
        
        tableResults.setTable(res.values, res.fields);
        
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
              // runtime/transformation/dataset/job catalogs and dataset/file catalogs.
              bViewJobDefinitions.setEnabled(dbPluginMgr.isJobRepository() && !lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              bDefineJobDefinitions.setEnabled(dbPluginMgr.isJobRepository() && !lsm.isSelectionEmpty());
              bDeleteRecord.setEnabled(!lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              miEdit.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              miImportFiles.setEnabled(dbPluginMgr.isFileCatalog() && !lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
              menuEditCut.setEnabled(!lsm.isSelectionEmpty());
              menuEditPaste.setEnabled(clipboardOwned);
            }
          });

          makeDatasetMenu();
        }
        else if(tableName.equalsIgnoreCase("file")){
          tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
          tableResults.addListSelectionListener(new ListSelectionListener(){
            public void valueChanged(ListSelectionEvent e){
              if (e.getValueIsAdjusting()) return;
              ListSelectionModel lsm = (ListSelectionModel)e.getSource();
              //Debug.debug("lsm indices: "+
              //    lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
              bDownload.setEnabled(!lsm.isSelectionEmpty());
              bDeleteRecord.setEnabled(dbPluginMgr.isFileCatalog() && !lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              miEdit.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
              menuEditCut.setEnabled(!lsm.isSelectionEmpty());
              menuEditPaste.setEnabled(clipboardOwned);
            }
          });

          makeFileMenu();
        }
        else if(tableName.equalsIgnoreCase("jobDefinition")){
          tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
          tableResults.addListSelectionListener(new ListSelectionListener(){
            public void valueChanged(ListSelectionEvent e){
              if (e.getValueIsAdjusting()) return;
              ListSelectionModel lsm = (ListSelectionModel)e.getSource();
              //Debug.debug("lsm indices: "+
              //    lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
              bSubmit.setEnabled(!lsm.isSelectionEmpty());
              bMonitor.setEnabled(!lsm.isSelectionEmpty());
              bDeleteRecord.setEnabled(!lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              miEdit.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
              menuEditCut.setEnabled(!lsm.isSelectionEmpty());
              menuEditPaste.setEnabled(clipboardOwned);
            }
          });

          makeJobDefMenu();
        }
        else if(tableName.equalsIgnoreCase("transformation")){
          tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
          tableResults.addListSelectionListener(new ListSelectionListener(){
            public void valueChanged(ListSelectionEvent e){
              if (e.getValueIsAdjusting()) return;
              ListSelectionModel lsm = (ListSelectionModel)e.getSource();
              //Debug.debug("lsm indices: "+
              //    lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
              bDeleteRecord.setEnabled(!lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              miEdit.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
              menuEditCut.setEnabled(!lsm.isSelectionEmpty());
              menuEditPaste.setEnabled(clipboardOwned);
            }
          });

          makeTransformationMenu();
        }
        else if(tableName.equalsIgnoreCase("runtimeEnvironment")){
          tableResults.getSelectionModel().setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
          tableResults.addListSelectionListener(new ListSelectionListener(){
            public void valueChanged(ListSelectionEvent e){
              if (e.getValueIsAdjusting()) return;
              ListSelectionModel lsm = (ListSelectionModel)e.getSource();
              //Debug.debug("lsm indices: "+
              //    lsm.getMaxSelectionIndex()+" : "+lsm.getMinSelectionIndex(), 3);
              bDeleteRecord.setEnabled(!lsm.isSelectionEmpty());
              bEditRecord.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              miEdit.setEnabled(!lsm.isSelectionEmpty() &&
                  lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
              menuEditCopy.setEnabled(!lsm.isSelectionEmpty());
              menuEditCut.setEnabled(!lsm.isSelectionEmpty());
              menuEditPaste.setEnabled(clipboardOwned);
            }
          });
          makeRuntimeEnvironmentMenu();
        }
        
        GridPilot.getClassMgr().getGlobalFrame().menuEdit.updateUI();
        statusBar.stopAnimation();
        statusBar.setLabel("Records found: "+tableResults.getRowCount(), 20);
        setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        
        if(sortColumn>-1){
          Debug.debug("Sorting: "+sortColumn+":"+isAscending, 3);
          ((DBVectorTableModel) tableResults.getModel()).sort(sortColumn, isAscending);
        }
        
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
        stopWorking();
      }
    };
    workThread.start();

  }
  
  public void searchRequest(){
    DBVectorTableModel tableModel = (DBVectorTableModel) tableResults.getModel();
    tableModel.ascending = true;
    searchRequest(-1, tableModel.ascending, null);
  }
 
  /**
   * Add menu items to the table with search results. This function is called from within DBPanel
   * after the results table is filled
   */
  public void makeDatasetMenu(){
    Debug.debug("Making dataset menu", 3);
    JMenuItem miViewJobDefinitions = new JMenuItem("Show job definition(s)");
    miViewJobDefinitions.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        viewJobDefinitions();
      }
    });
    miImportFiles.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        importFiles();
      }
    });
    JMenuItem miViewFiles = new JMenuItem("Show file(s)");
    miViewFiles.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        viewFiles();
      }
    });
    JMenuItem miDelete = new JMenuItem("Delete");
    miEdit = new JMenuItem("Edit");
    miDelete.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteDatasets();
      }
    });
    miEdit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editDataset();
      }
    });
    miImportFiles.setEnabled(dbPluginMgr.isFileCatalog());
    miViewFiles.setEnabled(true);
    miViewJobDefinitions.setEnabled(dbPluginMgr.isJobRepository());
    miDelete.setEnabled(true);
    miEdit.setEnabled(true);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miImportFiles);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miViewFiles);
    tableResults.addMenuItem(miViewJobDefinitions);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miEdit);
    tableResults.addMenuItem(miDelete);
  }

  public void makeTransformationMenu(){
    Debug.debug("Making transformation menu", 3);
    JMenuItem miDelete = new JMenuItem("Delete");
    miEdit = new JMenuItem("Edit");
    miDelete.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        deleteTransformations();
      }
    });
    miEdit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        editTransformation();
      }
    });
    miDelete.setEnabled(true);
    miEdit.setEnabled(true);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miEdit);
    tableResults.addMenuItem(miDelete);
  }
  public void makeRuntimeEnvironmentMenu(){
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

  public void makeFileMenu(){
    Debug.debug("Making file menu", 3);
    JMenuItem miDelete = new JMenuItem("Delete");
    miEdit = new JMenuItem("View");
    JMenuItem miDownload = new JMenuItem("Replicate file(s)");
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
        download();
      }
    });
    miDelete.setEnabled(dbPluginMgr.isFileCatalog());
    miEdit.setEnabled(true);
    miDownload.setEnabled(true);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miDownload);
    tableResults.addMenuSeparator();
    tableResults.addMenuItem(miEdit);
    tableResults.addMenuItem(miDelete);
  }

  public void makeJobDefMenu(){
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
               String choiceStr = JOptionPane.showInputDialog(JOptionPane.getRootFrame(),
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
    for(int i=0; i<GridPilot.csNames.length; ++i){
      JMenuItem mi = new JMenuItem(GridPilot.csNames[i]);
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
    tableResults.addMenuItem(jmSetFieldValue);
    tableResults.addMenuSeparator();
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
   * Open dialog with jobDefintion creation panel (from datasets)
   */ 
  private void createJobDefs(){
    Debug.debug("Creating job definition(s), "+getSelectedIdentifiers().length, 3);
    //JobDefCreationPanel panel = new JobDefCreationPanel(dbName, datasetMgr, this, false);
    JobCreationPanel panel = new JobCreationPanel(dbPluginMgr, this);
    CreateEditDialog pDialog = new CreateEditDialog(panel, false, true, true);
    pDialog.setTitle("jobDefinition");
  }

  private void createJobDefinitions(){
    Debug.debug("Creating job definition(s), "+getSelectedIdentifiers().length, 3);
    JobDefCreationPanel panel = new JobDefCreationPanel(dbName, null, this, new Boolean(false));
    CreateEditDialog pDialog = new CreateEditDialog(panel, false, false, true);
    pDialog.setTitle("jobDefinition");
  }

  private void editJobDef(){
    DatasetMgr datasetMgr = null;
    String selectedDatasetID = dbPluginMgr.getJobDefDatasetID(getSelectedIdentifier());
    if(parentId.equals("-1")){
      parentId = selectedDatasetID;
    }
    Debug.debug("Got dbPluginMgr:"+dbPluginMgr+":"+parentId, 1);
    try{
      datasetMgr = GridPilot.getClassMgr().getDatasetMgr(dbName, selectedDatasetID);
    }
    catch(Throwable e){
      Debug.debug("ERROR: could not get DatasetMgr. "+e.getMessage(), 1);
      e.printStackTrace();
      return;
    }
    JobDefCreationPanel panel = null;
    panel = new JobDefCreationPanel(dbName, datasetMgr, this,
        new Boolean(true));
    CreateEditDialog pDialog = new CreateEditDialog(panel, true, false, true);
    pDialog.setTitle(tableName);
    //pDialog.setVisible(true);
  }

  private void editFile(){
    // This should be safe. Only mysql and hsqldb contain jobDefinitions and are directly editable.
    // TODO: support editing other file catalogs
    if(dbPluginMgr.isJobRepository() && !dbPluginMgr.isFileCatalog()){
      editJobDef();
    }
    else{
      // Just display the content
      int i = tableResults.getSelectedRow();
      String [] values = new String [fieldNames.length];
      for(int j=0; j<fieldNames.length; ++j){
        // Not displayed colums
        values[j] = "--- not displayed ---";
        for(int k=0; k<tableResults.getColumnCount(); ++k){
          if(tableResults.getColumnName(k).equalsIgnoreCase(fieldNames[j])){
            values[j] = tableResults.getUnsortedValueAt(i, k).toString();
            break;
          }
        }
        // This would work if all file catalogs would support searches on individual files.
        // DQ2 doesn't
        /*values[j] = dbPluginMgr.getFile(tableResults.getValueAt(i,
            // the identifier is the last column
            tableResults.getColumnCount()-1).toString()).getValue(fieldNames[j]).toString();*/
      }
      Util.showResult(fieldNames, values, "file", 0);
    }
  }
  
  private void deleteFiles(){
    
    if(!dbPluginMgr.isFileCatalog()){
      MessagePane.showMessage("This is a virtual file table. " +
            "Entries cannot be modified directly.", "Cannot delete");
    }
    // Should be safe, only mysql and hsqldb contain jobDefinitions
    else{
      String msg = "Are you sure you want to delete file";
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
      
      final JCheckBox cbCleanup = new JCheckBox("Delete physical files", true);
      ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
      try{
        int choice = confirmBox.getConfirm("Confirm delete",
            msg, new Object[] {"OK", "Cancel", cbCleanup});
        if(choice==1){
          return;
        }
      }
      catch(Exception e){
        e.printStackTrace();
        return;
      }
      workThread = new Thread(){
        public void run(){
          if(!getWorking()){
            return;
          }
          boolean anyDeleted = false;
          String [] ids = getSelectedIdentifiers();

          //int [] rows = tableResults.getSelectedRows();
          Debug.debug("Deleting "+ids.length+" rows", 2);
          if(ids.length != 0){
            GridPilot.getClassMgr().getStatusBar().setLabel(
               "Deleting file(s). Please wait ...");
            JProgressBar pb = new JProgressBar();
            pb.setMaximum(ids.length);
            statusBar.setProgressBar(pb);
            statusBar.setProgressBar(pb);
            boolean success = true;
            try{
              success = dbPluginMgr.deleteFiles("", ids, cbCleanup.isSelected());
            }
            catch(Exception e){
              e.printStackTrace();
              success = false;
            }
            if(!success){
              String msg = "Deleting files "+Util.arrayToString(ids)+" failed.";
              Debug.debug(msg, 1);
              GridPilot.getClassMgr().getStatusBar().setLabel(msg);
              GridPilot.getClassMgr().getLogFile().addMessage(msg);
            }
            else{
              anyDeleted = true;
            }
            for(int i=ids.length-1; i>=0; i--){
              pb.setValue(pb.getValue()+1);
              // Not necessary, we refresh below
              //tableResults.removeRow(rows[i]);
            }
            tableResults.tableModel.fireTableDataChanged();
            GridPilot.getClassMgr().getStatusBar().setLabel(
               "Deleting file(s) done.");
            statusBar.removeProgressBar(pb);
          }
          if(anyDeleted){
            refresh();
          }
          stopWorking();
        }
      };
      workThread.start();
    }
  }
  
  // From AtCom1
  // TODO: try if not better than deleteJobDefs
  /**
   * Deletes selected job definitions from the database. 
   * Returns list of successfully deleted job definitions.
   */
  /*public synchronized HashSet deleteJobdefinitions(boolean showResults) {
  // TODO: Delete job definitions only if jobs not running.
  
  boolean skipAll = false;
  boolean skip = false;

  boolean showThis;
  int choice = 3;
  HashSet deleted = new HashSet();
  int [] selectedJobDefs = getSelectedIdentifiers();
  JProgressBar pb = new JProgressBar();
  pb.setMaximum(pb.getMaximum()+selectedJobDefs.length);
  statusBar.setProgressBar(pb);
  showThis = showResults;
  Debug.debug("Deleting "+selectedJobDefs.length+" logical files",2);
  JCheckBox cbCleanup = null;
  
  for(int i=selectedJobDefs.length-1; i>=0 && !skipAll; --i){
    
    if(skipAll){
      break;
    }
    
    if(showThis && !skipAll){
        
      ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()); 
      cbCleanup = new JCheckBox("Cleanup runtime info", true);
      try{
        if(i>0){
          choice = confirmBox.getConfirm("Confirm delete",
             "Really delete logical file # "+selectedJobDefs[i]+"?",
             new Object[] {"OK", "Skip", "OK for all", "Skip all", cbCleanup});
        }
        else{
          choice = confirmBox.getConfirm("Confirm delete",
           "Really delete logical file # "+selectedJobDefs[i]+"?",
           new Object[] {"OK",  "Skip", cbCleanup});        
        }
      }
      catch(java.lang.Exception e){Debug.debug("Could not get confirmation, "+e.getMessage(),1);}
        switch(choice){
          case 0  : skip = false;  break;  // OK
          case 1  : skip = true;   break; // Skip
          case 2  : skip = false;  showThis = false ; break;   //OK for all
          case 3  : skip = true;   showThis = false ; skipAll = true; break;// Skip all
          default : skip = true;   skipAll = true; break;// other (closing the dialog). Same action as "Skip all"
        }
      }
      if(!skipAll && !skip){
        Debug.debug("deleting logical file # " + selectedJobDefs[i], 2);
        pb.setValue(pb.getValue()+1);
        if(cbCleanup.isSelected()){
          if(!dbPluginMgr.cleanRunInfo(selectedJobDefs[i])){
            GridPilot.getClassMgr().getLogFile().addMessage(
                "WARNING: Deleting runtime record for logicalFile # "+selectedJobDefs[i]+
                " failed."+"Please clean up by hand."+dbPluginMgr.getError());
          }
        }
        if(dbPluginMgr.deleteJobDefinition(selectedJobDefs[i])){
          deleted.add(Integer.toString(selectedJobDefs[i]));
          statusBar.setLabel("Job definition # " + selectedJobDefs[i] + " deleted.");
        }
        else{
          statusBar.setLabel("Job definition # " + selectedJobDefs[i] + " NOT deleted.");
          Debug.debug("WARNING: Job definition "+selectedJobDefs[i]+" could not be deleted",1);
        }   
      }
    }
    return deleted;
  }*/

  private void deleteJobDefs(){
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

    final JCheckBox cbCleanup = new JCheckBox("Delete physical files", true);
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    try{
      int choice = confirmBox.getConfirm("Confirm delete",
          msg, new Object[] {"OK", "Cancel", cbCleanup});
      if(choice==1){
        return;
      }
    }
    catch(Exception e){
      e.printStackTrace();
      return;
    }

    workThread = new Thread(){
      public void run(){
        if(!getWorking()){
          return;
        }
        boolean anyDeleted = false;
        String [] ids = getSelectedIdentifiers();

        // Update job monitoring display
        for(int i=ids.length-1; i>=0; --i){
          String currentDatasetID = dbPluginMgr.getJobDefDatasetID(ids[i]);
          Debug.debug("Got dbPluginMgr:"+dbPluginMgr+":"+parentId, 1);
          DatasetMgr datasetMgr = null;
          try{
            datasetMgr = GridPilot.getClassMgr().getDatasetMgr(dbName, currentDatasetID);
          }
          catch(Throwable e){
            Debug.debug("ERROR: could not get DatasetMgr. "+e.getMessage(), 1);
            e.printStackTrace();
            return;
          }
          datasetMgr.removeRow(ids[i]);
        }
        
        Debug.debug("Deleting "+ids.length+" rows", 2);
        if(ids.length != 0){
          GridPilot.getClassMgr().getStatusBar().setLabel(
             "Deleting job definition(s). Please wait ...");
          JProgressBar pb = new JProgressBar();
          pb.setMaximum(ids.length);
          statusBar.setProgressBar(pb);
          statusBar.setProgressBar(pb);
          for(int i=ids.length-1; i>=0; i--){
            boolean success = dbPluginMgr.deleteJobDefinition(ids[i], cbCleanup.isSelected());
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
          GridPilot.getClassMgr().getStatusBar().setLabel(
             "Deleting job definition(s) done.");
          statusBar.removeProgressBar(pb);
        }
        if(anyDeleted){
          refresh();
        }
        stopWorking();
      }
    };
    workThread.start();
  }

  /**
   * Open dialog with dataset creation panel in creation mode
   */ 
  private void createDatasets(){
    CreateEditDialog pDialog = new CreateEditDialog(
        new DatasetCreationPanel(dbPluginMgr, this, false), false, false, true);
    pDialog.setTitle(tableName);
 }
  
  /**
   * Open dialog with dataset creation panel in editing mode
   */ 
 private void editDataset(){
   CreateEditDialog pDialog = new CreateEditDialog(
     new DatasetCreationPanel(dbPluginMgr, this, true), true, false, true);
   pDialog.setTitle(tableName);
 }

  /**
   *  Delete datasets. Returns HashSet of identifier strings.
   *  From AtCom1.
   */
  public void deleteDatasets(){
    statusBar.setLabel("Deleting dataset(s).");
    final HashSet deleted = new HashSet();
    workThread = new Thread(){
      public void run(){
        if(!getWorking()){
          return;
        }
        boolean skip = false;
        boolean okAll = false;
        int choice = 3;
        JCheckBox cbCleanup = null;
        String [] datasetIdentifiers = getSelectedIdentifiers();
        for(int i=datasetIdentifiers.length-1; i>=0; --i){
          if(!datasetIdentifiers[i].equals("-1")){
            if(!okAll){
              ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()/*,"",""*/); 
              cbCleanup = new JCheckBox("Delete job definitions", true);    
              if(i<1){
                try{
                  choice = confirmBox.getConfirm("Confirm delete",
                                       "Really delete dataset # "+datasetIdentifiers[i]+"?",
                                       dbPluginMgr.isJobRepository() ?
                                       new Object[] {"OK", "Skip", cbCleanup} :
                                         new Object[] {"OK", "Skip"});
                }
                catch(java.lang.Exception e){
                  Debug.debug("Could not get confirmation, "+e.getMessage(),1);
                }
              }
              else{
                try{
                  choice = confirmBox.getConfirm("Confirm delete",
                                       "Really delete dataset # "+datasetIdentifiers[i]+"?",
                                       dbPluginMgr.isJobRepository() ?
                                       new Object[] {"OK", "Skip", "OK for all", "Skip all", cbCleanup} :
                                         new Object[] {"OK", "Skip", "OK for all", "Skip all"});
                  }
                catch(java.lang.Exception e){
                  Debug.debug("Could not get confirmation, "+e.getMessage(),1);
                }
              }
        
              switch(choice){
              case 0  : skip = false;break;  // OK
              case 1  : skip = true ; break;  // Skip
              case 2  : skip = false; okAll = true ;break;  // OK for all
              case 3  : skip = true ; return; // Skip all
              default : skip = true;    // other (closing the dialog). Same action as "Skip"
              }
            }
            if(!skip || okAll){
              Debug.debug("deleting dataset # " + datasetIdentifiers[i], 2);
              if(dbPluginMgr.deleteDataset(datasetIdentifiers[i],
                  dbPluginMgr.isJobRepository() && cbCleanup.isSelected())){
                deleted.add(datasetIdentifiers[i]);
                statusBar.setLabel("Dataset # " + datasetIdentifiers[i] + " deleted.");
              }
              else{
                Debug.debug("WARNING: dataset "+datasetIdentifiers[i]+" could not be deleted",1);
                statusBar.setLabel("Dataset # " + datasetIdentifiers[i] + " NOT deleted.");
              }
            }
          }
          else{
            Debug.debug("WARNING: dataset undefined and could not be deleted",1);
          }
        }
        //statusBar.setLabel("Deleting dataset(s) done.");
        if(deleted.size()>0){
          refresh();
          statusBar.setLabel(deleted.size()+" of "+
              datasetIdentifiers.length+" datasets deleted.");
        }
        stopWorking();
      }
    };
    workThread.start();
    //return deleted;
  }
  
  /**
   * Refresh search results.
   */ 
  public void refresh(){
    Debug.debug("Refreshing search results", 3);
    if(tableResults==null /*|| tableResults.getRowCount()==0*/){
      searchRequest();
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
   * Open dialog with transformation creation panel.
   */ 
  private void createTransformation(){
    CreateEditDialog pDialog = new CreateEditDialog(
       new TransformationCreationPanel(dbPluginMgr, this, false),
       false, false, true);
    pDialog.setTitle(tableName);
  }
  /**
   * Open dialog with runtime environment creation panel
   */ 
  private void createRuntimeEnvironment(){
    CreateEditDialog pDialog = new CreateEditDialog(
       new RuntimeCreationPanel(dbPluginMgr, this, false),
       false, false, true);
    pDialog.setTitle(tableName);
  }

  private void editTransformation(){
    CreateEditDialog pDialog = new CreateEditDialog(
       new TransformationCreationPanel(dbPluginMgr, this, true),
       true, false, true);
    pDialog.setTitle(tableName);
  }
  
  private void editRuntimeEnvironment(){
    CreateEditDialog pDialog = new CreateEditDialog(
       new RuntimeCreationPanel(dbPluginMgr, this, true),
       true, false, true);
    pDialog.setTitle(tableName);
  }

  private void deleteTransformations(){
    String msg = "Are you sure you want to delete transformation record";
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
    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(),
        msg, "Delete?",
        JOptionPane.YES_NO_OPTION);
    if(choice==JOptionPane.NO_OPTION){
      return;
    }
    workThread = new Thread(){
      public void run(){
        if(!getWorking()){
          return;
        }
        boolean anyDeleted = false;
        String [] ids = getSelectedIdentifiers();
        //int [] rows = tableResults.getSelectedRows();
        Debug.debug("Deleting "+ids.length+" rows", 2);
        if(ids.length!=0){
          GridPilot.getClassMgr().getStatusBar().setLabel(
             "Deleting transformation(s). Please wait ...");
          JProgressBar pb = new JProgressBar();
          statusBar.setProgressBar(pb);
          pb.setMaximum(ids.length);
          for(int i = ids.length-1; i>=0; i--){
            boolean success = dbPluginMgr.deleteTransformation(ids[i]);
            if(!success){
              String msg = "Deleting transformation "+ids[i]+" failed";
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
             "Deleting transformation(s) done.");
          statusBar.removeProgressBar(pb);
        }
        stopWorking();
        if(anyDeleted){
          refresh();
        }
      }
    };
    workThread.start();
  }
  private void deleteRuntimeEnvironments(){
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
    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(),
        msg, "Delete?",
        JOptionPane.YES_NO_OPTION);
    if(choice==JOptionPane.NO_OPTION){
      return;
    }
    workThread = new Thread(){
      public void run(){
        if(!getWorking()){
          return;
        }
        boolean anyDeleted = false;
        String [] ids = getSelectedIdentifiers();
        //int [] rows = tableResults.getSelectedRows();
        Debug.debug("Deleting "+ids.length+" rows", 2);
        if(ids.length != 0){
          GridPilot.getClassMgr().getStatusBar().setLabel(
             "Deleting runtime environment(s). Please wait ...");
          JProgressBar pb = new JProgressBar();
          pb.setMaximum(ids.length);
          statusBar.setProgressBar(pb);
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
          GridPilot.getClassMgr().getStatusBar().setLabel(
             "Deleting runtime environment(s) done.");
          statusBar.removeProgressBar(pb);
        }
        stopWorking();
        if(anyDeleted){
          refresh();
        }
      }
    };
    workThread.start();
  }

  /**
   * Open new pane with list of files.
   */
  private void viewFiles(){
    if(!getSelectedIdentifier().equals("-1")){
      new Thread(){
        public void run(){
          try{
            // Create new panel with jobDefinitions.         
            String id = getSelectedIdentifier();
            DBPanel dbPanel = new DBPanel("file", dbPluginMgr, id);
            // We assume that the dataset name is used as reference...
            // TODO: improve this
            String datasetColumn = "dsn";
            String [] fileDatasetReference = Util.getFileDatasetReference(dbPluginMgr.getDBName());
            if(fileDatasetReference!=null){
              datasetColumn = fileDatasetReference[1];
            }
            dbPanel.selectPanel.setConstraint(datasetColumn,
                dbPluginMgr.getDataset(id).getValue(
                    fileDatasetReference[0]).toString(), 0);
            dbPanel.searchRequest();
            dbPanel.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
            GridPilot.getClassMgr().getGlobalFrame().addPanel(dbPanel);                   
          }
          catch(Exception e){
            Debug.debug("Couldn't create panel for dataset " + "\n" +
                               "\tException\t : " + e.getMessage(), 2);
            e.printStackTrace();
          }
        }
      }.start();
    }
  }
 
  /**
   * Open new pane with list of jobDefinitions.
   */
  private void viewJobDefinitions(){
    if(!getSelectedIdentifier().equals("-1")){
      new Thread(){
        public void run(){
          try{
            // Create new panel with jobDefinitions.         
            String id = getSelectedIdentifier();
            DBPanel dbPanel = new DBPanel("jobDefinition",
                dbPluginMgr, id);
            String [] jobDefDatasetReference =
              Util.getJobDefDatasetReference(dbPluginMgr.getDBName());
            dbPanel.selectPanel.setConstraint(jobDefDatasetReference[1],
                dbPluginMgr.getDataset(id).getValue(
                    jobDefDatasetReference[0]).toString(),
                0);
            dbPanel.searchRequest();           
            GridPilot.getClassMgr().getGlobalFrame().addPanel(dbPanel);                   
          }
          catch(Exception e){
            Debug.debug("Couldn't create panel for dataset " + "\n" +
                               "\tException\t : " + e.getMessage(), 2);
            e.printStackTrace();
          }
        }
      }.start();
    }
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
   * Called when mouse is pressed on the Import button
   */
  private void importFiles(){
    if(!getSelectedIdentifier().equals("-1")){
      new Thread(){
        public void run(){
          // Get the dataset name and id
          String datasetID = getSelectedIdentifier();
          String datasetName = dbPluginMgr.getDatasetName(datasetID);
          // Find the list of files
          String [] regUrls = null;
          try{
            regUrls = getImportFiles();
          }
          catch(Exception e){
            String error = "ERROR: could not get URLs to register";
            Debug.debug(error, 1);
            GridPilot.getClassMgr().getStatusBar().setLabel(error);
            GridPilot.getClassMgr().getLogFile().addMessage(error, e);
            return;
          }         
          // Register the files
          String uuid = null;
          String lfn = null;
          String pfn = null;
          for(int i=0; i<regUrls.length; ++i){
            try{
              pfn = regUrls[i];
              int lastSlash = pfn.lastIndexOf("/");
              lfn = pfn;
              if(lastSlash>-1){
                lfn = pfn.substring(lastSlash + 1);
              }
              uuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
              String message = "Generated new UUID "+uuid.toString()+" for "+lfn;
              GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar.setLabel(message);
              GridPilot.getClassMgr().getLogFile().addInfo(message);
              dbPluginMgr.registerFileLocation(
                  datasetID, datasetName, uuid, lfn, pfn, false);
            }
            catch(Exception e){
              GridPilot.getClassMgr().getLogFile().addMessage(
                  "ERROR: could not register "+pfn+" for file "+
                  lfn+" in dataset "+datasetName, e);
              GridPilot.getClassMgr().getGlobalFrame(
              ).monitoringPanel.statusBar.setLabel("ERROR: could not register "+pfn);
            }
          }
          GridPilot.getClassMgr().getGlobalFrame(
             ).monitoringPanel.statusBar.setLabel("Registration done");
        }
      }.start();
    }
  }
  
  /**
   * Called when mouse is pressed on the Download button
   */
  private void download(){
    new Thread(){
      public void run(){
        // First get download dir or URL.
        // Default to either home dir on "home gridftp server"
        // as defined in config file, or system.home
        if((defaultURL==null || defaultURL.equals("")) && GridPilot.gridftpHomeURL!=null){
          defaultURL = GridPilot.gridftpHomeURL;
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
        
        // if the table jobDefinition is present, we are using
        // the native tables and only one url/pfn per jobDefinition/file
        // is allowed, thus no db combobox needed
        JPanel pTargetDBs = null;
        JComboBox cbTargetDBSelection = null;
        try{
          pTargetDBs = new JPanel();
          cbTargetDBSelection = new JComboBox();           
          cbTargetDBSelection.addItem("");
          for(int i=0; i<GridPilot.dbNames.length; ++i){
            // If this DB has a job definition table, registration should not be done.
            // Files are defined by the jobDefinition table.
            try{
              if(!GridPilot.getClassMgr().getDBPluginMgr(GridPilot.dbNames[i]).isFileCatalog()){
                continue;
              }
            }
            catch(Exception e){
            }
            cbTargetDBSelection.addItem(GridPilot.dbNames[i]);
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

        String dlUrl = null;
        try{
          dlUrl = getReplicaURL(defaultURL, pTargetDBs);
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
        // queue downloads, if drop-down of DBs is set, request registering new locations
        // by passing on corresponding dbPluginMgr
        if(startDownload(dlUrl, (pTargetDBs!=null && cbTargetDBSelection!=null &&
            cbTargetDBSelection.getSelectedItem()!=null &&
            !cbTargetDBSelection.getSelectedItem().toString().equals(""))?
                GridPilot.getClassMgr().getDBPluginMgr(
                    cbTargetDBSelection.getSelectedItem().toString()) : null)){
          GridPilot.getClassMgr().getGlobalFrame().showMonitoringPanel();
          GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.tpStatLog.setSelectedIndex(1);
        }
      }
    }.start();
  }

  private String [] getImportFiles() throws IOException{
    JFrame frame = (JFrame) SwingUtilities.getWindowAncestor(getRootPane());
    frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    final String finUrl = "";
    final String finBaseUrl = "";
    BrowserPanel wb = null;
    try{
      wb = new BrowserPanel(
                      GridPilot.getClassMgr().getGlobalFrame(),
                      "Choose files",
                      finUrl,
                      finBaseUrl,
                      true,
                      true,
                      true,
                      null,
                      "");
    }
    catch(Exception eee){
      Debug.debug("Could not open URL "+finBaseUrl+". "+eee.getMessage(), 1);
      eee.printStackTrace();
      GridPilot.getClassMgr().getStatusBar().setLabel("Could not open URL "+finBaseUrl+". "+eee.getMessage());
      ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()/*,"",""*/); 
      try{
        confirmBox.getConfirm("URL could not be opened",
                             "The URL "+finBaseUrl+" could not be opened. \n"+eee.getMessage(),
                          new Object[] {"OK"});
      }
      catch(Exception eeee){
        Debug.debug("Could not get confirmation, "+eeee.getMessage(), 1);
      }
    }
    if(wb!=null && wb.lastURL!=null &&
        wb.lastURL.startsWith(finBaseUrl)){
        //GridPilot.getClassMgr().getStatusBar().setLabel("");
    }
    else{
      // Don't do anything if we cannot get a URL
      Debug.debug("ERROR: Could not open URL "+finBaseUrl, 1);
    }
    frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
    //GridPilot.getClassMgr().getStatusBar().setLabel("");
    String [] ret = wb.lastUrlList;
    Debug.debug("Returning last URL list "+Util.arrayToString(ret), 2);
    return ret;
  }
  
  private String getReplicaURL(String url, JComponent jcb) throws IOException{
    Debug.debug("URL: "+url, 3);
    JFrame frame = (JFrame) SwingUtilities.getWindowAncestor(getRootPane());
    frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    final String finUrl = url;
    final String finBaseUrl = "";//url;
    BrowserPanel wb = null;
    try{
      wb = new BrowserPanel(
                      GridPilot.getClassMgr().getGlobalFrame(),
                      "Choose directory",
                      finUrl,
                      finBaseUrl,
                      true,
                      false,
                      true,
                      jcb,
                      "*/");
    }
    catch(Exception eee){
      Debug.debug("Could not open URL "+finBaseUrl+". "+eee.getMessage(), 1);
      eee.printStackTrace();
      GridPilot.getClassMgr().getStatusBar().setLabel("Could not open URL "+finBaseUrl+". "+eee.getMessage());
      ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()/*,"",""*/); 
      try{
        confirmBox.getConfirm("URL could not be opened",
                             "The URL "+finBaseUrl+" could not be opened. \n"+eee.getMessage(),
                          new Object[] {"OK"});
      }
      catch(Exception eeee){
        Debug.debug("Could not get confirmation, "+eeee.getMessage(), 1);
      }
    }
    if(wb!=null && wb.lastURL!=null &&
        wb.lastURL.startsWith(finBaseUrl)){
        //GridPilot.getClassMgr().getStatusBar().setLabel("");
    }
    else{
      // Don't do anything if we cannot get a URL
      Debug.debug("ERROR: Could not open URL "+finBaseUrl, 1);
    }
    frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
    //GridPilot.getClassMgr().getStatusBar().setLabel("");
    String ret = wb.lastURL.substring(finBaseUrl.length());
    Debug.debug("Returning last URL "+ret, 2);
    if(!ret.endsWith("/")){
      throw new IOException("ERROR: not a directory: "+ret);
    }
    return ret;
  }
  
  private boolean startDownload(final String _dlUrl,
      final DBPluginMgr regDBPluginMgr){
    boolean ret = false;
    String [] selectedFileIdentifiers = getSelectedIdentifiers();
    int [] selectedRows = tableResults.getSelectedRows();
    Debug.debug("Starting download of "+selectedFileIdentifiers.length+" files", 2);
    String nameField = Util.getNameField(dbPluginMgr.getDBName(), "file");
    String idField = Util.getIdentifierField(dbPluginMgr.getDBName(), "file");
    GlobusURL srcUrl = null;
    GlobusURL destUrl = null;
    TransferInfo transfer = null;
    Vector transfers = new Vector();
    String dlUrl = null;
    TransferControl transferControl = GridPilot.getClassMgr().getTransferControl();
    // Correct file url not understood by Globus: file:/... -> file://...
    String dlUrlDir = _dlUrl.replaceFirst("^file:/([^/])", "file://$1");
    // GlobusURL does not accept file://C:/... or file://C:\..., but
    // file:////C:/... or file:////C:\...
    dlUrlDir = dlUrlDir.replaceFirst("\\\\", "/");
    dlUrlDir = dlUrlDir.replaceFirst("^file://(\\w)://", "file:////$1://");
    dlUrlDir = dlUrlDir.replaceFirst("^file://(\\w):/", "file:////$1:/");
    dlUrlDir = dlUrlDir.replaceFirst("^file://(\\w)", "file:////$1");
    for(int i=0; i<selectedFileIdentifiers.length; ++i){
      String [] urls = null;
      String guid = null;
      String name = null;
      String datasetName = null;
      String datasetID = null;
      // We assume that the dataset name is used as reference...
      // TODO: improve this
      String datasetColumn = "dsn";
      String [] fileDatasetReference =
        Util.getFileDatasetReference(dbPluginMgr.getDBName());
      if(fileDatasetReference!=null){
        datasetColumn = fileDatasetReference[1];
      }
      // First try and get the values from the table.            
      HashMap values = new HashMap();
      for(int j=0; j<fieldNames.length; ++j){
        // Not displayed colums
        for(int k=0; k<tableResults.getColumnCount(); ++k){
          if(tableResults.getColumnName(k).equalsIgnoreCase(fieldNames[j])){
            values.put(fieldNames[j], tableResults.getUnsortedValueAt(selectedRows[i], k).toString());
            break;
          }
        }
      }
      try{
        guid = values.get(idField).toString();
      }
      catch(Exception e){
      }
      try{
        name = values.get(nameField).toString();
      }
      catch(Exception e){
      }
      try{
        datasetName = values.get(datasetColumn).toString();
      }
      catch(Exception e){
      }
      try{
        // Non-general.
        // TODO: perhaps make this configurable like identifier and name,
        // dbPluginMgr.getPfnsField("file");
        String urlsString = values.get("pfns").toString();
        urls = Util.split(urlsString);
      }
      catch(Exception e){
      }
      if(urls==null){
        try{
          String urlsString = values.get("url").toString();
          urls = new String [] {urlsString};
        }
        catch(Exception e){
        }
      }
      DBRecord file = null;
      if(guid==null || name==null || urls==null || datasetName==null){
        file = dbPluginMgr.getFile(datasetName, selectedFileIdentifiers[i]);
        // In the case of DQ2 these are too slow or will fail and return null.
        // All information must be in the table...
        if(guid==null){
          guid = file.getValue(idField).toString();
        }
        if(name==null){
          guid = file.getValue(nameField).toString();
        }
        if(urls==null){
          urls = dbPluginMgr.getFileURLs(datasetName, selectedFileIdentifiers[i]);
        }
        if(datasetName==null){
          datasetName = file.getValue(datasetColumn).toString();
        }
      }
      try{
        datasetID = dbPluginMgr.getDatasetID(datasetName);
      }
      catch(Exception e){
      }
      if(urls==null){
        String error = "ERROR: URLs not found. Cannot queue this transfer. "+
           selectedFileIdentifiers[i];
        GridPilot.getClassMgr().getLogFile().addMessage(error);
        continue;
      }
      if(regDBPluginMgr!=null && name==null){
        String error = "ERROR: LFN not found. Cannot queue this transfer. "+
           selectedFileIdentifiers[i];
        GridPilot.getClassMgr().getLogFile().addMessage(error);
        continue;
      }
      if(regDBPluginMgr!=null && guid==null){
        String error = "WARNING: guid not found for "+name;
        GridPilot.getClassMgr().getLogFile().addMessage(error);
      }
      if(regDBPluginMgr!=null && guid==null){
        String error = "WARNING: guid not found for "+name;
        GridPilot.getClassMgr().getLogFile().addMessage(error);
      }
      if(regDBPluginMgr!=null && datasetName==null){
        String error = "WARNING: dataset name not found for "+name;
        GridPilot.getClassMgr().getLogFile().addMessage(error);
      }
      if(regDBPluginMgr!=null && datasetID==null){
        String error = "WARNING: dataset ID not found for "+name;
        GridPilot.getClassMgr().getLogFile().addMessage(error);
      }
      for(int j=0; j<urls.length; ++j){
        try{
          srcUrl = new GlobusURL(urls[j]);
          // Add :8443 to srm urls without port number
          if(srcUrl.getPort()<1 && srcUrl.getProtocol().toLowerCase().equals("srm")){
            srcUrl = new GlobusURL(urls[j].replaceFirst("(srm://[^/]+)/", "$1:8443/"));
          }
          dlUrl = dlUrlDir+(new File (srcUrl.getPath())).getName();
          destUrl = new GlobusURL(dlUrl);
          Debug.debug("Preparing download of file "+name, 2);
          Debug.debug(srcUrl.getURL()+" ---> "+destUrl.getURL(), 2);
          transfer = new TransferInfo(srcUrl, destUrl);
          transfer.setDBPluginMgr(regDBPluginMgr);
          try{
            // If the file is in a file catalog, we should reuse the lfn, guid
            // and the dataset name and id if possible.
            if(dbPluginMgr.isFileCatalog()){
              // This file query may fail (in the case of DQ2).
              transfer.setGUID(guid);
            }
            // The LFN we always reuse.
            // This file query may fail (in the case of DQ2).
            transfer.setLFN(name);
            transfer.setDatasetName(datasetName);
            transfer.setDatasetID(datasetID);
            transfers.add(transfer);
            // We simply take the first file in the list of PFNs.
            // TODO: implement some seletion algorithm; choose the
            // file which is "closest".
            break;
          }
          catch(Exception ee){
            // Try next URL
            continue;
          }
        }
        catch(Exception e){
          String error = "WARNING: could not download file "+
             name+". "+e.getMessage();
          GridPilot.getClassMgr().getLogFile().addMessage(error);
          Debug.debug(error, 1);
          e.printStackTrace();
          continue;
        }
      }
    }
    if(!transfers.isEmpty()){
      try{
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
    GridPilot.getClassMgr().getGlobalFrame().showMonitoringPanel();
    GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.tpStatLog.setSelectedIndex(0);
    new Thread(){
      public void run(){        
        DBRecord jobDef;
        String [] selectedJobIdentifiers = getSelectedIdentifiers();
        String idField = Util.getIdentifierField(dbPluginMgr.getDBName(), "jobDefintition");
        for(int i=0; i<selectedJobIdentifiers.length; ++i){
          jobDef = dbPluginMgr.getJobDefinition(selectedJobIdentifiers[i]);
          DatasetMgr datasetMgr = GridPilot.getClassMgr().getDatasetMgr(dbName,
              dbPluginMgr.getJobDefDatasetID(
                  jobDef.getValue(idField).toString()));
          datasetMgr.addJobs(new String [] {selectedJobIdentifiers[i]});
        }
      }
    }.start();
  }

  /**
   * Called when mouse is pressed on Submit button
   */
  private void bSubmit_mousePressed(){
    // check if selected jobs are submittable
    String [] selectedJobIdentifiers = getSelectedIdentifiers();
    for(int i=0; i<selectedJobIdentifiers.length; ++i){
      if(DBPluginMgr.getStatusId(
          dbPluginMgr.getJobDefStatus(selectedJobIdentifiers[i]))!=DBPluginMgr.DEFINED){
        statusBar.setLabel("ERROR: all selected jobs must be submittable.");
        return;
      }
    }
    // if a partition is selected, shows the menu with computing systems
    if(getSelectedIdentifiers().length != 0){
      pmSubmitMenu.show(this, 0, 0); // without this, pmSubmitMenu.getWidth == 0

      pmSubmitMenu.show(bSubmit, -pmSubmitMenu.getWidth(),
                        -pmSubmitMenu.getHeight() + bSubmit.getHeight());
    }
  }

  /**
   * Called when a computing system in pmSubmitMenu is selected
   * Submits all selected logicalFiles (partitions) in computing system chosen in the popupMenu
   */
  private void submit(ActionEvent e){
    String [] selectedJobDefIdentifiers = getSelectedIdentifiers();
    Vector selectedJobDefinitions = new Vector();
    for(int i=0; i<selectedJobDefIdentifiers.length; ++i){
      selectedJobDefinitions.add(dbPluginMgr.getJobDefinition(selectedJobDefIdentifiers[i]));
    }
    String csName = ((JMenuItem)e.getSource()).getText();
    // submit the jobs
    submissionControl.submitJobDefinitions(selectedJobDefinitions, csName, dbPluginMgr);
  }
  
  public void copy(){
    Debug.debug("Copying!", 3);
    int [] rows = tableResults.getSelectedRows();
    String [] ids = getSelectedIdentifiers();
    String [] copyObjects = new String [ids.length];
    if(tableName.equalsIgnoreCase("file")){
      String nameField = Util.getNameField(dbPluginMgr.getDBName(), "file");
      String [] datasetNameFields = Util.getFileDatasetReference(dbPluginMgr.getDBName());
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
      for(int i=0; i<ids.length; ++i){
        copyObjects[i] = 
          "'"+tableResults.getValueAt(rows[i], datasetNameIndex).toString()+"'::'"+
          tableResults.getValueAt(rows[i], nameIndex).toString()+"'::'"+
          ids[i]+"'";
      }
    }
    else{
      copyObjects = ids;
    }
    StringSelection stringSelection = new StringSelection(
        dbName+" "+tableName+" "+Util.arrayToString(copyObjects));
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
    GridPilot.getClassMgr().getGlobalFrame().cutPanel = this;
  }
  
  public void paste(){
    String clip = getClipboardContents();
    Debug.debug("Pasting "+clip, 3);
    String [] records = Util.split(clip);
    if(records.length<3){
      return;
    }
    if(!clipboardOwned){
      return;
    }
    boolean ok = true;
    try{
      // this can stay null for other than files
      String datasetName = null;
      // this can stay null for other than datasets and files
      String name = null;
      // this is always needed
      String id = null;
      for(int i=2; i<records.length; ++i){
        name = null;
        id = records[i];
        // Check if record is a dataset and already there
        // ask for prefix if this is the case.
        // Only datasets have unique names.
        try{
          if(tableName.equalsIgnoreCase("dataset")){
            name = GridPilot.getClassMgr().getDBPluginMgr(records[0]).getDatasetName(
                records[i]);
            if(!GridPilot.getClassMgr().getDBPluginMgr(dbName).getDatasetID(name).equals("-1")){            
              name = Util.getName("Cannot overwrite, please give new name",
                "new-"+name);
            }
          }
          else if(tableName.equalsIgnoreCase("file")){
            // Well, once more, because DQ2 cannot lookup files we have
            // to introduce an ugly hack: we pass both dataset name, file name and id
            // ('dsname'::'name'::'id') when copy-pasting
            int index = records[i].indexOf("'::'");
            datasetName = records[i].substring(1, index);
            String rest = records[i].substring(index+4);
            index = rest.indexOf("'::'");
            name = rest.substring(0, index);
            id = rest.substring(index+4, rest.length()-1);
            
          }
          insertRecord(records[0], records[1], id, name, datasetName);
        }
        catch(Exception e){
          ok = false;
          String error = "ERROR: could not insert record: "+
          records[0]+", "+records[1]+", "+dbName+", "+tableName+", "+
          records[i]+", "+name;
          GridPilot.getClassMgr().getLogFile().addMessage(error);
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
      "Deleting job definition(s). Please wait ...");
      JProgressBar pb = new JProgressBar();
      pb.setMaximum((records.length-2));
      statusBar.setProgressBar(pb);
      for(int i=2; i<records.length; ++i){
        try{
          deleteRecord(records[0], records[1],
              records[i]);
        }
        catch(Exception e){
          String msg = "Deleting record "+(i-2)+" failed. "+
             GridPilot.getClassMgr().getDBPluginMgr(records[0]).getError();
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
    try{
      if(GridPilot.getClassMgr().getGlobalFrame().cutPanel!=null){
        ((DBPanel) GridPilot.getClassMgr().getGlobalFrame().cutPanel).refresh();
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
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
  public void insertRecord(String sourceDB, String sourceTable,
      String id, String name, String datasetName) throws Exception{
    
    DBPluginMgr sourceMgr = GridPilot.getClassMgr().getDBPluginMgr(sourceDB);
    
    DBRecord record = null;

    if(tableName.equalsIgnoreCase("jobDefinition")){
      try{
        record = sourceMgr.getJobDefinition(id);
        insertJobDefinition(record);
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
    else if(tableName.equalsIgnoreCase("transformation")){
      try{
        record = sourceMgr.getTransformation(id);
        insertTransformation(record, sourceMgr);
      }
      catch(Exception e){
        String msg = "ERROR: transformation "+id+" could not be created, "+sourceDB+
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
        //record = sourceMgr.getFile(datasetName, id);
        // TODO: copy also file metadata?
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
  
  public boolean insertJobDefinition(DBRecord jobDef) throws Exception{  
    try{
      // Check if parent dataset exists
      String targetJobDefIdentifier = Util.getIdentifierField(dbPluginMgr.getDBName(), "jobDefinition");
      String targetDsId = dbPluginMgr.getJobDefDatasetID(jobDef.getValue(targetJobDefIdentifier).toString());
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
  
  public boolean insertFile(DBPluginMgr sourceMgr, String name, String datasetName,
      String fileID) throws Exception{
    if(!dbPluginMgr.isFileCatalog()){
      throw new SQLException("Cannot create file(s) in virtual table.");
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
  
  public boolean insertDataset(DBRecord dataset, DBPluginMgr sourceMgr,
      String name, String id) throws Exception{
    try{
      boolean ok = false;
      boolean success = true;
      try{
        // Check if referenced transformation exists     
        String sourceTransName = sourceMgr.getDatasetTransformationName(
            dataset.getValue(Util.getIdentifierField(sourceMgr.getDBName(),
                "dataset")).toString());
        String sourceTransVersion = sourceMgr.getDatasetTransformationVersion(
            dataset.getValue(Util.getIdentifierField(sourceMgr.getDBName(),
                "dataset")).toString());  
        
        DBResult targetTransformations = dbPluginMgr.getTransformations();
        Vector transVec = new Vector();
        for(int i=0; i<targetTransformations.values.length; ++i){
          if(targetTransformations.getValue(i, Util.getNameField(dbPluginMgr.getDBName(),
              "transformation")).toString(
              ).equalsIgnoreCase(sourceTransName)){
            transVec.add(targetTransformations.getRow(i));
          }
        }
        for(int i=0; i<transVec.size(); ++i){
          if(((DBRecord) transVec.get(i)).getValue(Util.getVersionField(dbPluginMgr.getDBName(),
              "transformation")).toString().equalsIgnoreCase(sourceTransVersion)){
            ok = true;
            break;
          }
        }
      }
      catch(Exception ee){
        ee.printStackTrace();
      }
      // only if this is a job-only database (no file catalog) do we deny creating
      // orphaned datasets (data provenance enforcement).
      if(ok || dbPluginMgr.isFileCatalog()){
        Debug.debug("Creating dataset: " + Util.arrayToString(dataset.fields, ":") + " ---> " +
            Util.arrayToString(dataset.values, ":"), 3);
        try{
          // if name is specified, use it
          if(name!=null && !name.equals("")){
            dataset.setValue(Util.getNameField(sourceMgr.getDBName(), "dataset"),
                name);
          }
        }
        catch(Exception e){
          Debug.debug("WARNING: Could not set dataset name "+name, 3);
          e.printStackTrace();
        }
        try{
          // if id is specified, use it - except when copying from a
          // job-only database - in which case the id will be a useless
          // autoincremented number
          if(sourceMgr.isFileCatalog() && id!=null && !id.equals("")){
            dataset.setValue(Util.getIdentifierField(sourceMgr.getDBName(), "dataset"),
                id);
          }
        }
        catch(Exception e){
          Debug.debug("WARNING: Could not set dataset name "+name, 3);
          e.printStackTrace();
        }
        success = dbPluginMgr.createDataset("dataset", dataset.fields, dataset.values);
        if(!success){
          throw(new Exception("ERROR: "+dbPluginMgr.getError()));
        }
      }
      else{
        String error = "ERROR: transformation for dataset does not exist.";
        throw(new Exception(error));
      }
    }
    catch(Exception e){
      throw e;
    }
    return true;
  }
  
  public boolean insertTransformation(DBRecord transformation, DBPluginMgr sourceMgr)
     throws Exception{
    try{
      // Check if referenced runtime environment exists
      String sourceTransformationIdentifier = Util.getIdentifierField(sourceMgr.getDBName(),
          "transformation");
      String targetTransformationIdentifier = Util.getIdentifierField(dbPluginMgr.getDBName(),
          "transformation");
      String targetRuntimeEnvironmentName = Util.getNameField(dbPluginMgr.getDBName(),
          "runtimeEnvironment");
      String runtimeEnvironment = sourceMgr.getTransformationRuntimeEnvironment(
          transformation.getValue(
              sourceTransformationIdentifier).toString());
      DBResult targetRuntimes = dbPluginMgr.getRuntimeEnvironments();
      Vector runtimeNames = new Vector();
      for(int i=0; i<targetRuntimes.values.length; ++i){
        runtimeNames.add(targetRuntimes.getValue(i, targetRuntimeEnvironmentName).toString());
      }
      if(runtimeEnvironment==null || runtimeNames==null ||
          !runtimeNames.contains(runtimeEnvironment)){
        GridPilot.getClassMgr().getLogFile().addInfo("WARNING: runtime environment for transformation "+
              transformation.getValue(targetTransformationIdentifier)+" does not exist.");
      }
      dbPluginMgr.createTrans(transformation.fields, transformation.values);
    }
    catch(Exception e){
      throw e;
    }
    return true;
  }
  
  public boolean insertRuntimeEnvironment(DBRecord pack) throws Exception{
    try{
      dbPluginMgr.createRuntimeEnv(pack.fields, pack.values);
    }
    catch(Exception e){
      throw e;
    }
    return true;
  }
  
  public void deleteRecord(String sourceDB, String sourceTable, String id) throws Exception{
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
    else if(tableName.equalsIgnoreCase("transformation")){
      try{
        sourceMgr.deleteTransformation(id);
      }
      catch(Exception e){
        String msg = "ERROR: transformation "+id+" could not be deleted from, "+sourceDB+
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
  public void setClipboardContents(String aString){
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
  public String getClipboardContents(){
    String result = "";
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    //odd: the Object param of getContents is not currently used
    Transferable contents = clipboard.getContents(null);
    boolean hasTransferableText =
      (contents != null) &&
      contents.isDataFlavorSupported(DataFlavor.stringFlavor);
    if(hasTransferableText){
      try{
        result = (String)contents.getTransferData(DataFlavor.stringFlavor);
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

}
