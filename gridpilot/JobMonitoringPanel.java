package gridpilot;

import javax.swing.*;

import java.awt.*;
import java.awt.event.*;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.event.*;

import gridpilot.DBRecord;
import gridpilot.DBResult;

/**
 * Shows a table with informations about runnings jobs
 *
 * <p><a href="JobMonitoringPanel.java.html">see sources</a>
 */
public class JobMonitoringPanel extends CreateEditPanel implements ListPanel{

  private static final long serialVersionUID = 1L;
  private Table statusTable = null;
  // use status bar on main window
  private StatusBar statusBar = null;
  private final int ALL_JOBS = 0;
  private final int ONLY_RUNNING_JOBS = 1;
  private final int ONLY_DONE_JOBS = 2;
  private int showRows = ALL_JOBS;
  // Central panel
  private JTabbedPane tpStatLog = new JTabbedPane();
  private JScrollPane spStatusTable = new JScrollPane();
  private JScrollPane spLogView = new JScrollPane();
  public static StatisticsPanel statisticsPanel =
    GridPilot.getClassMgr().getStatisticsPanel();
  // Options panel
  private JPanel pOptions = new JPanel();
  // view options
  private ButtonGroup bgView = new ButtonGroup();
  private JRadioButton rbAllJobs = new JRadioButton("View all jobs", true);
  private JRadioButton rbRunningJobs = new JRadioButton("View only running jobs");
  private JRadioButton rbDoneJobs = new JRadioButton("View only done jobs");
  // jobs loading
  private JButton bLoadJobs = new JButton("Load all jobs");
  private JButton bLoadMyJobs = new JButton("Load my jobs");
  private JButton bClearTable = new JButton("Clear");
  // Buttons panel
  private JPanel pButtons = new JPanel();
  private JButton bDecide = new JButton("Decide");
  private JButton bKill = new JButton("Kill");
  private JButton bRefresh = new JButton("Refresh all");
  // auto refresh
  private JCheckBox cbAutoRefresh = new JCheckBox("Refresh each");
  private JSpinner sAutoRefresh = new JSpinner();
  private JComboBox cbRefreshUnits = new JComboBox(new Object []{"sec", "min"});
  //private int SEC = 0;
  private int MIN = 1;
  private JMenuItem miKill = new JMenuItem("Kill");
  private JMenuItem miDecide = new JMenuItem("Decide");
  private JMenuItem miRefresh = new JMenuItem("Refresh");
  private JMenu mSubmit = new JMenu("Submit");
  private JMenuItem miResubmit = new JMenuItem("Resubmit");
  private JMenu mShow = new JMenu("Show");
  private JMenuItem miShowOutput = new JMenuItem("Outputs");
  private JMenuItem miShowFullStatus = new JMenuItem("Full status");
  private JMenuItem miShowInfo = new JMenuItem("Infos");
  private JMenuItem miShowScripts = new JMenuItem("Scripts");
  private JMenuItem miStopUpdate = new JMenuItem("Stop update");
  private JMenuItem miRevalidate = new JMenuItem("Revalidate");
  //private JMenuItem miResetChanges = new JMenuItem("Reset changes");
  private JMenu mDB = new JMenu("Set DB Status");
  private LogViewerPanel logViewerPanel = new LogViewerPanel();
  public StatusUpdateControl statusUpdateControl;
  private SubmissionControl submissionControl;
  Timer timerRefresh = new Timer(0, new ActionListener (){
    public void actionPerformed(ActionEvent e){
      statusUpdateControl.updateStatus(null);
    }
  });


  /**
   * Constructor
   */
  public JobMonitoringPanel() throws Exception{
    
    statusBar = GridPilot.getClassMgr().getStatusBar();
    statusTable = GridPilot.getClassMgr().getStatusTable();
    
    statusUpdateControl = new StatusUpdateControl();
    submissionControl = GridPilot.getClassMgr().getSubmissionControl();
  }
  
  public String getTitle(){
    return "Job Monitor";
  }

  public void initGUI(){
    
    this.setLayout(new BorderLayout());

    // central panel
    tpStatLog.setTabPlacement(JTabbedPane.BOTTOM);
    
    spLogView.getViewport().add(logViewerPanel);
    statusTable.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        selectionEvent(e);
      }
    });
    spStatusTable.getViewport().add(statusTable);
    tpStatLog.addTab("Monitor", spStatusTable);
    tpStatLog.addTab("Logs", spLogView);

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

    pOptions.add(bLoadJobs, new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0,
        GridBagConstraints.WEST,
        GridBagConstraints.NONE,
        new Insets(30, 10, 0, 0), 0, 0));

    pOptions.add(bLoadMyJobs, new GridBagConstraints(0, 4, 1, 1, 0.0, 0.0,
        GridBagConstraints.WEST,
        GridBagConstraints.NONE,
        new Insets(10, 10, 0, 0), 0, 0));


    bLoadJobs.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        loadDBJobs(true);
      }
    });

    bLoadMyJobs.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        loadDBJobs(false);
      }
    });

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
    bDecide.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        decide();
      }
    });
    bDecide.setEnabled(false);

    bKill.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        kill();
      }
    });
    bKill.setEnabled(false);

    bRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        statusUpdateControl.updateStatus(null);
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

    pButtons.add(bDecide);
    pButtons.add(bKill);
    pButtons.add(bRefresh);
    pButtons.add(cbAutoRefresh);
    pButtons.add(sAutoRefresh);
    pButtons.add(cbRefreshUnits);

    bDecide.setToolTipText("Shows the outputs of the selected jobs");
    bKill.setToolTipText("Kills the selected jobs");
    bRefresh.setToolTipText("Refresh all jobs");

    this.getTopLevelAncestor().add(tpStatLog, BorderLayout.CENTER);
    this.getTopLevelAncestor().add(pOptions, BorderLayout.EAST);
    this.getTopLevelAncestor().add(pButtons, BorderLayout.SOUTH);
    
    //this.setPreferredSize(new Dimension(700, 500));
    
  }

  public void windowClosing(){
    GridPilot.getClassMgr().getGlobalFrame().cbMonitor.setSelected(false);
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

    miDecide.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        decide();
      }
    });

    miRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        statusUpdateControl.updateStatus(statusTable.getSelectedRows());
      }
    });

    miResubmit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        submissionControl.resubmit(DatasetMgr.getJobsAtRows(statusTable.getSelectedRows()));
      }
    });

    miShowOutput.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        showOutput();
      }
    });

    miShowFullStatus.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        showFullStatus();
      }
    });

    miShowInfo.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        showInfo();
      }
    });

    miShowScripts.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        showScripts();
      }
    });

    miRevalidate.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        DatasetMgr.revalidate(statusTable.getSelectedRows());
      }
    });

    JMenuItem miDBUnexpected = new JMenuItem("UnexpectedErrors");
    JMenuItem miDBFailed = new JMenuItem("Failed");
    JMenuItem miDBAborted = new JMenuItem("Aborted");
    JMenuItem miDBDefined = new JMenuItem("Defined");

    miDBAborted.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        setDBStatus(DBPluginMgr.ABORTED);
      }
    });

    miDBDefined.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        setDBStatus(DBPluginMgr.DEFINED);
      }
    });

    miDBFailed.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        setDBStatus(DBPluginMgr.FAILED);
      }
    });

    miDBUnexpected.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        setDBStatus(DBPluginMgr.UNEXPECTED);
      }
    });

    miStopUpdate.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        statusUpdateControl.reset();
      }
    });

    for(int i=0; i<GridPilot.csNames.length; ++i){
      JMenuItem mi = new JMenuItem(GridPilot.csNames[i], i);
      mi.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          submissionControl.submitJobs((Vector) DatasetMgr.getJobsAtRows(statusTable.getSelectedRows()),
              /*computingSystem*/
              /*((JMenuItem) e.getSource()).getMnemonic()*/
              ((JMenuItem) e.getSource()).getText());
        }
      });
      mSubmit.add(mi);
    }

    /*miResetChanges.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        jobControl.resetChanges();
      }
    });*/

    miKill.setEnabled(false);
    miDecide.setEnabled(false);
    mSubmit.setEnabled(false);
    miResubmit.setEnabled(false);

    mShow.setEnabled(false);

    //miShowOutput.setEnabled(false);
    //miShowFullStatus.setEnabled(false);
    //miShowInfo.setEnabled(false);
    miRevalidate.setEnabled(false);
    mDB.setEnabled(false);

    //miResetChanges.setEnabled(true);

    mDB.add(miDBDefined);
    mDB.add(miDBFailed);
    mDB.add(miDBAborted);

    mShow.add(miShowFullStatus);
    mShow.add(miShowInfo);
    mShow.add(miShowOutput);
    mShow.add(miShowScripts);



    statusTable.addMenuSeparator();
    statusTable.addMenuItem(miKill);
    statusTable.addMenuItem(miDecide);
    statusTable.addMenuItem(miRefresh);
    statusTable.addMenuItem(mSubmit);
    statusTable.addMenuItem(miResubmit);
    statusTable.addMenuItem(mShow);
    statusTable.addMenuItem(miRevalidate);
    statusTable.addMenuItem(miStopUpdate);
    //statusTable.addMenuItem(miResetChanges);


    statusTable.addMenuItem(mDB);

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
        DatasetMgr.killJobs(statusTable.getSelectedRows());
      }
    }).start();
  }

  /**
   * Called when button or menu item "Decide" is clicked
   */
  void decide(){
    int [] rows = statusTable.getSelectedRows();

    if(!DatasetMgr.areDecidables(rows))
      return;

    Vector jobs = DatasetMgr.getJobsAtRows(rows);

    int [] options = {DBPluginMgr.VALIDATED, DBPluginMgr.FAILED, DBPluginMgr.UNDECIDED, DBPluginMgr.ABORTED};
    String [] sOptions = {
        DBPluginMgr.getStatusName(options[0]),
        DBPluginMgr.getStatusName(options[1]),
        DBPluginMgr.getStatusName(options[2]),
        DBPluginMgr.getStatusName(options[3])
    };

    int choices [] = ShowOutputsJobsDialog.show(JOptionPane.getRootFrame(), jobs, sOptions);
    int dbChoices [] = new int[choices.length];

    for(int i=0; i<jobs.size(); ++i){
      if(choices[i]==-1){
        dbChoices[i] = DBPluginMgr.UNDECIDED;
      }
      else{
        dbChoices[i]  = options[choices[i]];
      }
    }

    //jobControl.undecidedChoices(jobs, dbChoices);

    DatasetMgr datasetMgr = null;
    for(int i=0; i<jobs.size(); ++i){
      JobInfo job = (JobInfo) jobs.get(i);
      if(job.getDBStatus()!=dbChoices[i]){
        datasetMgr = getDatasetMgr(job);
        datasetMgr.updateDBStatus(job, dbChoices[i]);
      }
    }
    statusTable.updateSelection();
  }

  /**
   * Shows outputs of the job at the selected job. <p>
   * If this job is running, asks them to jobControl, otherwise, read files directly in
   * job.StdOut, job.StdErr
   */
  private void showOutput(){
    Debug.debug("show outputs", 1);
    
    final int selectedRow = statusTable.getSelectedRow();
    if(selectedRow==-1){
      return;
    }
    statusBar.setLabel("Waiting for outputs ...");
    statusBar.animateProgressBar();
    ((JFrame) SwingUtilities.getWindowAncestor(getRootPane())).setCursor(
        Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));

    final Thread t = new Thread(){
      public void run(){
        JobInfo job = DatasetMgr.getJobAtRow(selectedRow);
        String [] outNames = new String [] {"stdout", "stderr"};
        String [] outs = null;
        try{
          outs = GridPilot.getClassMgr().getCSPluginMgr(
          ).getCurrentOutputs(job);
        }
        catch(Exception e){
          outs = new String [] {"Could not read stdout "+e.getMessage(),
              "Could not read stdout "+e.getMessage()};
        }
        if(job.getStdErr()==null){
          outNames = new String [] {"stdout"};
          outs = new String[] {outs[0]};
        }
        //statusBar.removeLabel();
        ((JFrame) SwingUtilities.getWindowAncestor(getRootPane())).setCursor(
            Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        statusBar.stopAnimation();

        String message = "";
        if(DatasetMgr.isRunning(selectedRow)){
          message = "Current outputs of job";
        }
        else{
          message = "Final outputs of job";
        }
        ShowOutputsJobsDialog.showTabs(JOptionPane.getRootFrame(),
            message + " " + job.getName(),
            outNames,
            outs);
      }
    };
    statusBar.setIndeterminateProgressBarToolTip("click here to stop");
    statusBar.addIndeterminateProgressBarMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent me){
        t.interrupt();
      }
    });
    t.start();
  }

  /**
   * Shows full status of the job at the selected row. <p>
   * @see atcom.jobcontrol.ComputingSystem#getFullStatus(atcom.jobcontrol.JobInfo)
   * @see atcom.jobcontrol.JobControl#getFullStatus(int)
   */
  private void showFullStatus(){
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
   * Shows information about the job at the selected row. <p>
   * @see atcom.jobcontrol.JobControl#getJobInfo(int)
   */
  private void showInfo(){
    String info = DatasetMgr.getJobInfo(statusTable.getSelectedRow());
    MessagePane.showMessage(info, "Job Infos");
  }

  private void showScripts(){
    JobInfo job = DatasetMgr.getJobAtRow(statusTable.getSelectedRow());
    ShowOutputsJobsDialog.showTabs(JOptionPane.getRootFrame(),
        "Scripts for job " + job.getName(),
        job,
        GridPilot.getClassMgr().getCSPluginMgr().getScripts(job)            
        );
  }

  /**
   * Load jobs from database.
   */
  private void loadDBJobs(final boolean allJobs){
    new Thread(){
      public void run(){
        statusBar.setLabel("Waiting for DB Server ...");
        statusBar.animateProgressBar();
        bLoadJobs.setEnabled(false);
        bLoadMyJobs.setEnabled(false);
        if(statusTable.getRowCount()>0){
          statusTable.selectAll();
          statusTable.clearSelection();
        }
        //DatasetMgr mgr = null;
        try{
          /*Vector datasetMgrs = GridPilot.getClassMgr().getDatasetMgrs();
          if(datasetMgrs!=null && datasetMgrs.size()>0){
            for(Iterator it = datasetMgrs.iterator(); it.hasNext();){
              Debug.debug("getting DatasetMgr...", 3);
              //mgr = ((DatasetMgr) it.next());
              Debug.debug("loading jobs...", 3);
              loadJobs(allJobs, new int [] {Database.SUBMITTED, Database.UNDECIDED,
                  Database.UNEXPECTED, Database.FAILED});
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
          }
          else{
            statusBar.setLabel("");
          }*/
          Debug.debug("loading jobs...", 3);
          loadJobs(allJobs, new int [] {Database.SUBMITTED, Database.UNDECIDED,
              Database.UNEXPECTED, Database.FAILED});
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
        catch(Exception e){
          Debug.debug("WARNING: failed to load jobs. "+e.getMessage(), 1);
          e.printStackTrace();
        }
        statusBar.stopAnimation();
        statusBar.setLabel("");
        bLoadJobs.setEnabled(true);
        bLoadMyJobs.setEnabled(true);
      }
    }.start();
  }

  // Display jobs of a status from statusList on monitoring panel.
  public static void loadJobs(boolean allJobs, int [] statusList){
    
    DBPluginMgr dbPluginMgr;
    String [] shownFields;
    String jobStatus = null;
    String user = "";
    String csName = "";
    for(int ii=0; ii<GridPilot.dbs.length; ++ii){
      dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(GridPilot.dbs[ii]);
      shownFields = dbPluginMgr.getFieldnames("jobDefinition");//dbPluginMgr.getDBDefFields("jobDefinition");
      DBResult allJobDefinitions = 
        dbPluginMgr.getJobDefinitions(
            /*datasetID*/-1, shownFields);
      for (int i=0; i<allJobDefinitions.fields.length; ++i){
        Debug.debug(allJobDefinitions.fields[i] + " = " +
            allJobDefinitions.getValue(0, allJobDefinitions.fields[i]), 3);
      }
      Debug.debug ("number of jobs for "+GridPilot.dbs[ii]+
          ": "+allJobDefinitions.values.length, 2);

      for(int i=0; i<allJobDefinitions.values.length; ++i){
        user = null;
        DBRecord job = new DBRecord(allJobDefinitions.fields,
            allJobDefinitions.values[i]);
        String idField = dbPluginMgr.getIdentifierField("jobDefinition");
        Debug.debug("Checking:"+Util.arrayToString(job.values), 3);
        int id = Integer.parseInt(job.getValue(idField).toString());
        // if not showing all jobs and job not submitted by me, continue
        csName = job.getValue("computingSystem").toString();
        if(csName!=null){
          user = GridPilot.getClassMgr().getCSPluginMgr().getUserInfo(csName);
          Debug.debug("user: "+user, 3);
          Debug.debug("userInfo: "+job.getValue("userInfo"), 3);
        }
        if(!allJobs &&
            (user==null || !job.getValue("userInfo").toString().equalsIgnoreCase(user))){
          continue;
        }
        // if status ok, add the job
        for(int j=0; j<statusList.length; ++j){
          Debug.debug("Getting status: "+idField+":"+id, 3);
          jobStatus = dbPluginMgr.getJobDefStatus(id);
          if(statusList[j]==DBPluginMgr.getStatusId(jobStatus)){
            DatasetMgr mgr = GridPilot.getClassMgr().getDatasetMgr(GridPilot.dbs[ii], id);
            mgr.addJobs(new int [] {id});
            break;
          }
        }
      }
    }
  }

  /**
   * Updates selected and unselected menu items or button regarding to the selection.
   */
  private void selectionEvent(ListSelectionEvent e){
    //Ignore extra messages.
    if (e.getValueIsAdjusting()) return;

    ListSelectionModel lsm = (ListSelectionModel)e.getSource();
    if (lsm.isSelectionEmpty()){

      bKill.setEnabled(false);

      bDecide.setEnabled(false);

      miKill.setEnabled(false);
      miDecide.setEnabled(false);
      mSubmit.setEnabled(false);
      miResubmit.setEnabled(false);

      mShow.setEnabled(false);
      miRevalidate.setEnabled(false);
      mDB.setEnabled(false);

    }
    else{
      int [] rows = statusTable.getSelectedRows();

      if(DatasetMgr.areDecidables(rows)){
        bDecide.setEnabled(true);
        miDecide.setEnabled(true);
        miRevalidate.setEnabled(true);
      }
      else{
        bDecide.setEnabled(false);
        miDecide.setEnabled(false);
        miRevalidate.setEnabled(false);
      }

      if(DatasetMgr.areKillables(rows)){
        bKill.setEnabled(true);
        miKill.setEnabled(true);
      }
      else{
        bKill.setEnabled(false);
        miKill.setEnabled(false);
      }

      if(DatasetMgr.areResumbitables(rows))
        miResubmit.setEnabled(true);
      else
        miResubmit.setEnabled(false);

      if(DatasetMgr.areSubmitables(rows))
        mSubmit.setEnabled(true);
      else
        mSubmit.setEnabled(false);

      miShowFullStatus.setEnabled(true);
      miShowOutput.setEnabled(true);
      miShowInfo.setEnabled(true);

      mDB.setEnabled(true);
      mShow.setEnabled(true);
    }
  }

  private void setDBStatus(final int dbStatus){
    new Thread(){
      public void run(){
        Vector jobs = DatasetMgr.getJobsAtRows(statusTable.getSelectedRows());
        JobInfo job;
        DatasetMgr datasetMgr;
        for(int i=0; i<jobs.size(); ++i){
          job = (JobInfo) jobs.get(i);
          datasetMgr = getDatasetMgr(job);
          datasetMgr.setDBStatus(new int [] {statusTable.getSelectedRows()[i]}, dbStatus);
        }
      }
    }.start();
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

  /**
   * Removes all jobs from this status table.
   */
  public boolean clearTable(){
    if(submissionControl!=null && submissionControl.isSubmitting()){
      Debug.debug("cannot clear table during submission", 3);
      return false;
    }
    if(GridPilot.getClassMgr().getJobValidation().isValidating()){
      Debug.debug("cannot clear table during validation", 3);
      return false;
    }

    statusUpdateControl.reset();

    boolean ret = true;
    GridPilot.getClassMgr().clearSubmittedJobs();
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

  public void copy(){
  }
  public void cut(){
  }
  public void paste(){
  }
}

