package gridpilot;

import javax.swing.*;

import java.awt.*;
import java.awt.event.*;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.event.*;

/**
 * Shows a table with informations about runnings transfers
 *
 * <p><a href="TransferMonitoringPanel.java.html">see sources</a>
 */
public class TransferMonitoringPanel extends CreateEditPanel implements ListPanel{

  private static final long serialVersionUID = 1L;

  private final int ALL_JOBS = 0;
  private final int ONLY_RUNNING_JOBS = 1;
  private final int ONLY_DONE_JOBS = 2;
  
  private int showRows = ALL_JOBS;
  private Table statusTable = null;
  // Central panel
  private JPanel mainPanel = new JPanel();
  private JScrollPane spStatusTable = new JScrollPane();
  private StatisticsPanel statisticsPanel =
    GridPilot.getClassMgr().getTransferStatisticsPanel();
  // Options panel
  private JPanel pOptions = new JPanel();
  // view options
  private ButtonGroup bgView = new ButtonGroup();
  private JRadioButton rbAllJobs = new JRadioButton("View all transfers", true);
  private JRadioButton rbRunningJobs = new JRadioButton("View only running transfers");
  private JRadioButton rbDoneJobs = new JRadioButton("View only done transfers");
  // clear
  private JButton bClearTable = new JButton("Clear");
  // Buttons panel
  private JPanel pButtons = new JPanel();
  private JButton bKill = new JButton("Kill");
  private JButton bRefresh = new JButton("Refresh");
  // auto refresh
  private JCheckBox cbAutoRefresh = new JCheckBox("each");
  private JSpinner sAutoRefresh = new JSpinner();
  private JComboBox cbRefreshUnits = new JComboBox(new Object []{"sec", "min"});
  private int MIN = 1;
  private JMenuItem miShowInfo = new JMenuItem("Show Information");
  private JMenuItem miRefresh = new JMenuItem("Refresh");
  private JMenuItem miKill = new JMenuItem("Stop transfer");
  private JMenuItem miResubmit = new JMenuItem("Retry transfer");
  private JMenuItem miClear = new JMenuItem("Clear");
  private JMenuItem miStopUpdate = new JMenuItem("Stop update");
  private TransferControl transferControl;
  
  public TransferStatusUpdateControl statusUpdateControl = null;
  
  private Timer timerRefresh = new Timer(0, new ActionListener (){
    public void actionPerformed(ActionEvent e){
      updateStatus(null);
    }
  });

  /**
   * Constructor
   */
  public TransferMonitoringPanel() throws Exception{
    statusTable = GridPilot.getClassMgr().getTransferStatusTable();
    transferControl = new TransferControl();
    statusUpdateControl = new TransferStatusUpdateControl();
  }
  
  public String getTitle(){
    return "Transfer Monitor";
  }

  public void initGUI(){

    statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
    this.setLayout(new BorderLayout());
    mainPanel.setLayout(new BorderLayout());
    
    statusTable.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        selectionEvent(e);
      }
    });
    spStatusTable.getViewport().add(statusTable);
    
    makeMenu();

    //options panel
    pOptions.setLayout(new GridBagLayout());

    // view
    pOptions.add(rbAllJobs, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
        GridBagConstraints.WEST, GridBagConstraints.NONE,
        new Insets(0, 10, 0, 0), 0, 0));
    pOptions.add(rbRunningJobs, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
        GridBagConstraints.WEST,
        GridBagConstraints.NONE, new Insets(0, 10, 0, 0), 0, 0));
    pOptions.add(rbDoneJobs, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0,
        GridBagConstraints.WEST,
        GridBagConstraints.NONE, new Insets(0, 10, 0, 0), 0, 0));

    bgView.add(rbAllJobs);
    bgView.add(rbRunningJobs);
    bgView.add(rbDoneJobs);

    rbAllJobs.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        onlyJobsSelected();
      }
    });

    rbRunningJobs.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        onlyJobsSelected();
      }
    });

    rbDoneJobs.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        onlyJobsSelected();
      }
    });

    rbAllJobs.setMnemonic(ALL_JOBS);
    rbRunningJobs.setMnemonic(ONLY_RUNNING_JOBS);
    rbDoneJobs.setMnemonic(ONLY_DONE_JOBS);

    pOptions.add(bClearTable, new GridBagConstraints(0, 5, 1, 1, 0.0, 0.0,
        GridBagConstraints.CENTER,
        GridBagConstraints.NONE,
        new Insets(10, 10, 0, 0), 0, 0));

    bClearTable.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        clearTable();
      }
    });

    pOptions.add(statisticsPanel, new GridBagConstraints(0, 6, 1, 1, 0.1, 0.1,
        GridBagConstraints.WEST,
        GridBagConstraints.BOTH,
        new Insets(30, 5, 0, 5), 0, 0));

    // Buttons panel
    bKill.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        kill();
      }
    });
    bKill.setEnabled(false);

    bRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        updateStatus(null);
      }
    });
    cbAutoRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        cbAutoRefresh_clicked();
      }
    });

    sAutoRefresh.setPreferredSize(new Dimension(50, 21));
    sAutoRefresh.setModel(new SpinnerNumberModel(5, 1, 9999, 1));
    sAutoRefresh.addChangeListener(new ChangeListener(){
      public void stateChanged(ChangeEvent e){
        delayChanged();
      }
    });

    cbRefreshUnits.setSelectedIndex(MIN);
    cbRefreshUnits.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        delayChanged();
      }
    });
    
    pButtons.add(bKill);
    pButtons.add(new JLabel(" | "));
    pButtons.add(bRefresh);
    pButtons.add(cbAutoRefresh);
    pButtons.add(sAutoRefresh);
    pButtons.add(cbRefreshUnits);

    bKill.setToolTipText("Kills the selected jobs");
    bRefresh.setToolTipText("Refresh all jobs");

    mainPanel.add(pOptions, BorderLayout.EAST);
    mainPanel.add(pButtons, BorderLayout.SOUTH);
    mainPanel.add(spStatusTable);
    this.add(mainPanel);
    
    //this.setPreferredSize(new Dimension(700, 500));
    
  }

  /**
   * Makes the menu shown when the user right-clicks on the status table
   */
  private void makeMenu(){

    miKill.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        kill();
      }
    });

    miRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        updateStatus(statusTable.getSelectedRows());
      }
    });

    miClear.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        clear(statusTable.getSelectedRows());
      }
    });

    miResubmit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        transferControl = GridPilot.getClassMgr().getTransferControl();
        try{
          transferControl.cancel(
              getTransfersAtRows(statusTable.getSelectedRows()));
        }
        catch(Exception ee){
          statusBar.setLabel("ERROR: cancel jobs failed.");
          return;
        }
        try{
          transferControl.queue(
              getTransfersAtRows(statusTable.getSelectedRows()));
        }
        catch(Exception ee){
          statusBar.setLabel("ERROR: queueing jobs failed.");
        }
      }
    });

    miShowInfo.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        showInfo();
      }
    });

    miStopUpdate.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        reset();
      }
    });

    miKill.setEnabled(false);
    miResubmit.setEnabled(false);
    miShowInfo.setEnabled(false);

    statusTable.addMenuSeparator();
    statusTable.addMenuItem(miRefresh);
    statusTable.addMenuItem(miKill);
    statusTable.addMenuItem(miResubmit);
    statusTable.addMenuItem(miStopUpdate);
    statusTable.addMenuItem(miClear);

  }

  /**
   * Called when this panel is shown.
   */
  public void panelShown(){
    Debug.debug("panelShown",1);
    statusBar.setLabel(GridPilot.getClassMgr().getSubmittedJobs().size() + " job(s) monitored");
  }

  /**
   * Called when this panel is hidden
   */
  public void panelHidden(){
    Debug.debug("panelHidden", 1);
    statusBar.removeLabel();
  }


  /**
   * Called when check box for auto refresh is selected
   */
  void cbAutoRefresh_clicked(){
    if(cbAutoRefresh.isSelected()){
      delayChanged();
      timerRefresh.restart();
    }
    else{
      timerRefresh.stop();
    }
  }

  /**
   * Called when either the spinner valuer is changed or combo box "sec/min" is changed
   */
  void delayChanged(){
    int delay = ((Integer) (sAutoRefresh.getValue())).intValue();
    if(cbRefreshUnits.getSelectedIndex()==MIN){
      timerRefresh.setDelay(delay * 1000);
    }
    else{
      timerRefresh.setDelay(delay * 1000 * 60);
    }
  }

  /**
   * Called when button or menu item "Kill" is selected
   */
  void kill(){
    (new Thread(){
      public void run(){
        transferControl = GridPilot.getClassMgr().getTransferControl();
        try{
          transferControl.cancel(
              getTransfersAtRows(statusTable.getSelectedRows()));
        }
        catch(Exception ee){
          statusBar.setLabel("ERROR: cancel jobs failed.");
        }
      }
    }).start();
  }

  /**
   * Shows information about the job at the selected row. <p>
   */
  private void showInfo(){
    final Thread t = new Thread(){
      public void run(){
        JobInfo job = DatasetMgr.getJobAtRow(statusTable.getSelectedRow());
        String status = GridPilot.getClassMgr().getCSPluginMgr().getFullStatus(job);
        statusBar.removeLabel();
        statusBar.stopAnimation();
        MessagePane.showMessage(status, "Job status");
      }
    };
    statusBar.setLabel("Getting full status...");
    statusBar.animateProgressBar();
    statusBar.setIndeterminateProgressBarToolTip("click here to stop");
    statusBar.addIndeterminateProgressBarMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent me){
        t.interrupt();
      }
    });
    t.start();
  }

  /**
   * Updates selected and unselected menu items or button regarding to the selection.
   */
  private void selectionEvent(ListSelectionEvent e){
    //Ignore extra messages.
    if (e.getValueIsAdjusting()) return;

    ListSelectionModel lsm = (ListSelectionModel)e.getSource();
    if(lsm.isSelectionEmpty()){
      bKill.setEnabled(false);
      miKill.setEnabled(false);
      miResubmit.setEnabled(false);
    }
    else{
      int [] rows = statusTable.getSelectedRows();
      if(DatasetMgr.areKillables(rows)){
        bKill.setEnabled(true);
        miKill.setEnabled(true);
      }
      else{
        bKill.setEnabled(false);
        miKill.setEnabled(false);
      }

      if(DatasetMgr.areResumbitables(rows)){
        miResubmit.setEnabled(true);
      }
      else{
        miResubmit.setEnabled(false);
      }

      miShowInfo.setEnabled(true);
    }
  }

  /**
   * Called when user selectes one of the radio button (view all jobs,
   * view only running jobs, ...).
   */
  private void onlyJobsSelected(){
    showRows = bgView.getSelection().getMnemonic();
    switch(showRows){
      case ALL_JOBS:
        statusTable.showAllRows();
        break;
      case ONLY_RUNNING_JOBS:
      case ONLY_DONE_JOBS:
        showOnlyRows();
        break;
      default:
        Debug.debug("WARNING: Selection choice doesn't exist : " + showRows, 1);
      break;
    }
  }
  
  /**
   * Shows/Hides rows according to the user's choice.
   */
  private void showOnlyRows(){
    Vector submittedJobs = GridPilot.getClassMgr().getSubmittedJobs();
    Enumeration e =  submittedJobs.elements();
    while(e.hasMoreElements()){
      JobInfo job = (JobInfo) e.nextElement();
      if(DatasetMgr.isRunning(job)){
        if(showRows==ONLY_RUNNING_JOBS){
          statusTable.showRow(job.getTableRow());
        }
        else{
          statusTable.hideRow(job.getTableRow());
        }
      }
      else{
        if(showRows==ONLY_RUNNING_JOBS){
          statusTable.hideRow(job.getTableRow());
        }
        else{
          statusTable.showRow(job.getTableRow());
        }
      }
    }
    statusTable.updateUI();
  }

  private void updateStatus(int[] rows){
    // TODO
  }
  
  private void reset(){
    // TODO
  }
  
  /**
   * Removes selected jobs from this status table.
   */
  public void clear(int[] rows){
    // TODO: check if running or queueing
  }
  
  /**
   * Removes all jobs from this status table.
   */
  public boolean clearTable(){
    transferControl = GridPilot.getClassMgr().getTransferControl();
    if(transferControl.isSubmitting()){
      Debug.debug("cannot clear table during submission", 3);
      return false;
    }
    if(GridPilot.getClassMgr().getJobValidation().isValidating()){
      Debug.debug("cannot clear table during validation", 3);
      return false;
    }

    reset();

    boolean ret = true;
    GridPilot.getClassMgr().getSubmittedJobs().removeAllElements();
    statusTable.createRows(0);
    DatasetMgr mgr = null;
    try{
      for(Iterator it = GridPilot.getClassMgr().getDatasetMgrs().iterator(); it.hasNext();){
        mgr = ((DatasetMgr) it.next());
        mgr.initChanges();
        mgr.updateJobsByStatus();
      }
    }
    catch(Exception e){
      ret = false;
      e.printStackTrace();
      Debug.debug("WARNING: failed to clear jobs from "+mgr.dbName, 1);
    }
    
    return ret;
  }
  
  DatasetMgr getDatasetMgr(JobInfo job){
    return GridPilot.getClassMgr().getDatasetMgr(job.getDBName(),
        GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).getJobDefDatasetID(
            job.getJobDefId()));
  }

  /**
   * Returns the transfer at the specified row in the statusTable
   * @see #getTransfersAtRows(int[])
   */
  public static TransferInfo getTransferAtRow(int row){
    Vector submTransfers = GridPilot.getClassMgr().getSubmittedTransfers();
    //Debug.debug("Got transfers at row "+row+". "+submTransfers(), 3);
    return (TransferInfo) submTransfers.get(row);
  }

  /**
   * Returns the transfers at the specified rows in statusTable
   * @see #getTransferAtRow(int)
   */
  public static Vector getTransfersAtRows(int[] row){
    Vector transfers = new Vector(row.length);
    for(int i=0; i<row.length; ++i){
      transfers.add(getTransferAtRow(row[i]));
    }
    return transfers;
  }

  public void copy(){
  }
  public void cut(){
  }
  public void paste(){
  }
}

