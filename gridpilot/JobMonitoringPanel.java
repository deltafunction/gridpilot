package gridpilot;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.event.*;

/**
 * Shows a table with informations about runnings jobs
 *
 * <p><a href="JobMonitoringPanel.java.html">see sources</a>
 */
public class JobMonitoringPanel extends CreateEditPanel implements ListPanel{

  private static final long serialVersionUID = 1L;
  private Table statusTable = null;
  private StatusBar statusBar = null;
  
  private final int ALL_JOBS = 0;
  private final int ONLY_RUNNING_JOBS = 1;
  private final int ONLY_DONE_JOBS = 2;

  private int showRows = ALL_JOBS;

  Timer timerRefresh = new Timer(0, new ActionListener (){
    public void actionPerformed(ActionEvent e) {
      refresh();
    }
  });

  // Central panel
  private JTabbedPane tpStatLog = new JTabbedPane();
  private JScrollPane spStatusTable = new JScrollPane();
  private JScrollPane spLogView = new JScrollPane();
  public static StatisticsPanel statisticPanel = new StatisticsPanel();;
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
  private JButton bRefresh = new JButton("Refresh");
  // auto refresh
  private JCheckBox cbAutoRefresh = new JCheckBox("Refresh each");
  private JSpinner sAutoRefresh = new JSpinner();
  private JComboBox cbRefreshUnits = new JComboBox(new Object []{"sec", "min"});
  private int SEC = 0;

  //private JMenu menu = new JMenu("Job options");

  private JMenuItem miKill = new JMenuItem("Kill");
  private JMenuItem miDecide = new JMenuItem("Decide");
  private JMenuItem miRefresh = new JMenuItem("Refresh");
  private JMenu mSubmit = new JMenu("Submit");
  private JMenuItem miResubmit = new JMenuItem("Resubmit");

  private JMenu mShow = new JMenu("Show");
  private JMenuItem miShowOutput = new JMenuItem("Outputs");
  private JMenuItem miShowFullStatus = new JMenuItem("Full status");
  private JMenuItem miShowInfo = new JMenuItem("Infos");
  private JMenuItem miShowFiles = new JMenuItem("Files");
  private JMenuItem miShowScripts = new JMenuItem("Scripts");

  private JMenuItem miStopUpdate = new JMenuItem("Stop update");
  private JMenuItem miRevalidate = new JMenuItem("Revalidate");
  //private JMenuItem miResetChanges = new JMenuItem("Reset changes");
  private JMenu mDB = new JMenu("Set DB Status");

  private LogViewerPanel logViewerPanel = new LogViewerPanel();

  public StatusUpdateControl statusUpdateControl;
  private SubmissionControl submissionControl;


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
    
    this.setLayout(new GridBagLayout());

    // central panel
    this.add(tpStatLog, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
        GridBagConstraints.CENTER,
        GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));

    tpStatLog.setTabPlacement(JTabbedPane.BOTTOM);

    tpStatLog.add("Monitor", spStatusTable);
    tpStatLog.add("Logs", spLogView);

    spLogView.getViewport().add(logViewerPanel);

    spStatusTable.getViewport().add(statusTable);
    statusTable.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        selectionEvent(e);
      }
    });

    makeMenu();

    //// options panel

    pOptions.setLayout(new GridBagLayout());

    this.add(pOptions, new GridBagConstraints(1, 0, 1, 1, 0.1, 0.1
    ,GridBagConstraints.NORTH, GridBagConstraints.VERTICAL, new Insets(30, 10, 0, 0), 0, 0));

    // view
    pOptions.add(rbAllJobs, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0
    ,GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 10, 0, 0), 0, 0));
    pOptions.add(rbRunningJobs, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0
    ,GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 10, 0, 0), 0, 0));
    pOptions.add(rbDoneJobs, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0
    ,GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 10, 0, 0), 0, 0));

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

    pOptions.add(statisticPanel, new GridBagConstraints(0, 6, 1, 1, 0.1, 0.1,
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
        refresh();
      }
    });
    cbAutoRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        cbAutoRefresh_clicked();
      }
    });

    sAutoRefresh.setPreferredSize(new Dimension(50, 21));
    sAutoRefresh.setModel(new SpinnerNumberModel(30, 1, 9999, 1));
    sAutoRefresh.addChangeListener(new ChangeListener(){
      public void stateChanged(ChangeEvent e){
        delayChanged();
      }
    });

    cbRefreshUnits.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        delayChanged();
      }
    });

    this.add(pButtons,  new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
        GridBagConstraints.CENTER,
        GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));

    pButtons.add(bDecide);
    pButtons.add(bKill);
    pButtons.add(bRefresh);
    pButtons.add(cbAutoRefresh);
    pButtons.add(sAutoRefresh);
    pButtons.add(cbRefreshUnits);

    bDecide.setToolTipText("Shows the outputs of the selected jobs");
    bKill.setToolTipText("Kills the selected jobs");
    bRefresh.setToolTipText("Refresh all jobs");
    
    this.setPreferredSize(new Dimension(700, 500));
    
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
        statusUpdateControl.updateStatus();
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

    miShowFiles.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        showFiles();
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

    String [] csNames = GridPilot.getClassMgr().getCSPluginMgr().getCSNames();
    for(int i=0; i<csNames.length; ++i){
      JMenuItem mi = new JMenuItem(csNames[i], i);
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

    mShow.add(miShowOutput);
    mShow.add(miShowFullStatus);
    mShow.add(miShowInfo);
    mShow.add(miShowFiles);
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
   * Called when timeout on timer occurs or the user clicks on "Refresh"
   */
  private void refresh(){
    Debug.debug("Refresh", 1);
    statusUpdateControl.updateStatus();
  }

  /**
   * Called when check box for auto refresh is selected
   */
  void cbAutoRefresh_clicked() {
    if(cbAutoRefresh.isSelected()){
      delayChanged();
      timerRefresh.restart();
    }
    else
      timerRefresh.stop();
  }

  /**
   * Called when either the spinner valuer is changed or combo box "sec/min" is changed
   */
  void delayChanged() {
    int delay = ((Integer) (sAutoRefresh.getValue())).intValue();
    if(cbRefreshUnits.getSelectedIndex() == SEC)
      timerRefresh.setDelay(delay * 1000);
    else
      timerRefresh.setDelay(delay * 1000 * 60);
  }

  /**
   * Called when button or menu item "Kill" is selected
   */
  void kill() {
    DatasetMgr.killJobs(statusTable.getSelectedRows());
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

    for(int i = 0; i< jobs.size() ; ++i){
      if(choices[i] == -1)
        dbChoices[i] = DBPluginMgr.UNDECIDED;
      else
        dbChoices[i]  = options[choices[i]];
    }

    //jobControl.undecidedChoices(jobs, dbChoices);

    DatasetMgr datasetMgr = null;
    for(int i = 0; i < jobs.size(); ++i){
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
    if(selectedRow == -1)
      return;

    if(DatasetMgr.isRunning(selectedRow)){
      statusBar.setLabel("Waiting for outputs ...");
      statusBar.animateProgressBar();


      final Thread t = new Thread(){
        public void run(){
          String [] outs = GridPilot.getClassMgr().getCSPluginMgr().getCurrentOutputs(
              DatasetMgr.getJobAtRow(selectedRow));

          JobInfo job = DatasetMgr.getJobAtRow(selectedRow);
          String path[] = job.getStdErr() == null ?
              new String[]{job.getStdOut()} :
              new String[]{job.getStdOut(), job.getStdErr()};
          if (job.getStdErr() == null)
            outs = new String[] {
                outs[0]};
          statusBar.removeLabel();
          statusBar.stopAnimation();

          ShowOutputsJobsDialog.showFilesTabs(JOptionPane.getRootFrame(),
                                              "Current outputs of job " + job.getName(),
                                              path,
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
    else{
      //jobControl.setFinalDest(selectedRow);
      JobInfo job = DatasetMgr.getJobAtRow(selectedRow);
      if((job.getStdOut() == null || job.getStdOut().equals("")) &&
         (job.getStdErr() == null || job.getStdErr().equals(""))){
        DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
        job.setOutputs(dbPluginMgr.getStdOutFinalDest(job.getJobDefId()),
                       dbPluginMgr.getStdErrFinalDest(job.getJobDefId()));
      }
      ShellMgr shell = null;
      try{
        shell = GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job);
      }
      catch(Exception e){
        Debug.debug("ERROR getting shell manager: "+e.getMessage(), 1);
      }
      String [] files;
      if(job.getStdOut() == null){
        Debug.debug("No stdout, trying to get...", 2);
        final Thread t = new Thread(){
          public void run(){
            String [] outs = GridPilot.getClassMgr().getCSPluginMgr().getCurrentOutputs(
                  DatasetMgr.getJobAtRow(selectedRow));

            JobInfo job = DatasetMgr.getJobAtRow(selectedRow);
            String path[] = job.getStdErr() == null ?
                new String[]{job.getStdOut()} :
                new String[]{job.getStdOut(), job.getStdErr()};
            if (job.getStdErr() == null){
              outs = new String[] {outs[0]};
              // Save the obtained stdout
              try{
                GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job).writeFile(job.getStdOut(), outs[0], false);
              }
              catch(Exception e){
                Debug.debug("WARNING: Could not save. "+e.getMessage(), 1);
              }
            }
            else{
              // Save the obtained stdout/stderr
              try{
                GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job).writeFile(job.getStdOut(), outs[0], false);
                GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job).writeFile(job.getStdErr(), outs[1], false);
              }
              catch(Exception e){
                Debug.debug("WARNING: Could not save. "+e.getMessage(), 1);
              }
            }
            statusBar.removeLabel();
            statusBar.stopAnimation();
            

            ShowOutputsJobsDialog.showFilesTabs(JOptionPane.getRootFrame(),
                                                "Current outputs of job " + job.getName(),
                                                path,
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
      else{
        if(job.getStdErr() == null){
          Debug.debug("No stderr", 3);
          files = new String [] {job.getStdOut()};
        }
        else{
          files = new String [] {job.getStdOut(), job.getStdErr()};
        }
        ShowOutputsJobsDialog.showFilesTabs(this,
            "Final outputs of job " + job.getName(),
            shell,
            files);
      }
    }
  }

  /**
   * Shows full status of the job at the selected row. <p>
   * @see atcom.jobcontrol.ComputingSystem#getFullStatus(atcom.jobcontrol.JobInfo)
   * @see atcom.jobcontrol.JobControl#getFullStatus(int)
   */
  private void showFullStatus(){
    JobInfo job = DatasetMgr.getJobAtRow(statusTable.getSelectedRow());
    String status = GridPilot.getClassMgr().getCSPluginMgr().getFullStatus(job);
    MessagePane.showMessage(status, "Job status");
  }

  /**
   * Shows information about the job at the selected row. <p>
   * @see atcom.jobcontrol.JobControl#getJobInfo(int)
   */
  private void showInfo(){
    String info = DatasetMgr.getJobInfo(statusTable.getSelectedRow());
    MessagePane.showMessage(info, "Job Infos");
  }

  /**
   * Shows a window with as many tabs as the number of files in the same
   * directory than job.getStdOut (for the first selected job). <p>
   * In each tab, a file is shown.
   */
  private void showFiles(){
    JobInfo job = DatasetMgr.getJobAtRow(statusTable.getSelectedRow());
    ShellMgr shell = null;
    try{
      shell = GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job);
    }
    catch(Exception e){
      Debug.debug("ERROR getting shell manager: "+e.getMessage(), 1);
    }
    // looks for the directory
    String dir = job.getStdOut();
    if(dir ==null){
      GridPilot.getClassMgr().getLogFile().addMessage("Stdout is null", job);
      return;

    }
    dir = dir.substring(0, dir.lastIndexOf("/")+1);
    // asks all files in this directory
    String[] files = shell.listFiles(dir);
    // checks if dir was a directory
    if(files == null){
      GridPilot.getClassMgr().getLogFile().addMessage("This directory (" + dir +
                                                  ") doesn't exist");
      return;
    }
    // replaces directories by null (won't be shown)
    for(int i=0; i<files.length ; ++i){
      Debug.debug("file : " + files[i], 2);
      if(shell.isDirectory(files[i]))
        files[i] = null;
    }
    // shows the window
    ShowOutputsJobsDialog.showFilesTabs(JOptionPane.getRootFrame(),
                                        "Files of job " + job.getName(),
                                        shell, files);
  }

  private void showScripts(){
    JobInfo job = DatasetMgr.getJobAtRow(statusTable.getSelectedRow());
    ShellMgr shell = null;
    try{
      shell = GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job);
    }
    catch(Exception e){
      Debug.debug("ERROR getting shell manager: "+e.getMessage(), 1);
    }
    ShowOutputsJobsDialog.showFilesTabs(JOptionPane.getRootFrame(),
        "Scripts for job " + job.getName(),
        shell,
        getDatasetMgr(job).getScripts(statusTable.getSelectedRow())            
        );
  }

  /**
   * Load jobs from database.
   */
  private void loadDBJobs(final boolean allJobs){
    new Thread(){
  public void run(){
    statusBar.setLabel("Waiting for AMI Server ...");
    statusBar.animateProgressBar();
    bLoadJobs.setEnabled(false);
    bLoadMyJobs.setEnabled(false);
    statusTable.clearSelection();
    DatasetMgr mgr = null;
    try{
      for(Iterator it = GridPilot.getClassMgr().getDatasetMgrs().iterator(); it.hasNext();){
        mgr = ((DatasetMgr) it.next());
        mgr.loadJobDefs(new int [] {ONLY_RUNNING_JOBS});
      }
    }
    catch(Exception e){
      Debug.debug("WARNING: failed to load jobs from "+mgr.dbName, 1);
      e.printStackTrace();
    }
    statusBar.stopAnimation();
    bLoadJobs.setEnabled(true);
    bLoadMyJobs.setEnabled(true);
  }
    }.start();
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
      public void run() {
        //jobControl.setDBStatus(statusTable.getSelectedRows(), dbStatus);
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
    int choice = bgView.getSelection().getMnemonic();
    switch(choice){
      case ALL_JOBS:
        statusTable.showAllRows();
        break;
      case ONLY_RUNNING_JOBS:
      case ONLY_DONE_JOBS:
        showOnlyRows();
        break;
      default:
        Debug.debug("WARNING: Selection choice doesn't exist : " + choice, 1);
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
  }

  /**
   * Removes all jobs from this status table.
   */
  public boolean clearTable(){
    if(submissionControl.isSubmitting()){
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

