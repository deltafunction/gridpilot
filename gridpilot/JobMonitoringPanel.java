package gridpilot;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.Vector;

import javax.swing.event.*;

/**
 * Third panel of the main frame. <br>
 *
 * Shows a table with informations about runnings jobs
 *
 * <p><a href="JobMonitoringPanel.java.html">see sources</a>
 */

public class JobMonitoringPanel extends CreateEditPanel implements ListPanel{

  private static final long serialVersionUID = 1L;
  private Table statusTable;
  private StatusBar statusBar;

  Timer timerRefresh = new Timer(0, new ActionListener (){
    public void actionPerformed(ActionEvent e) {
      refresh();
    }
  });

  //// Central panel
  private JTabbedPane tpStatLog = new JTabbedPane();

  private JScrollPane spStatusTable = new JScrollPane();
  private JScrollPane spLogView = new JScrollPane();

  public static StatisticsPanel statisticPanel = new StatisticsPanel();;

  //// Buttons panel
  private JPanel pButtons = new JPanel();

  private JButton bDecide = new JButton("Decide");
  private JButton bKill = new JButton("Kill");
  private JButton bRefresh = new JButton("Refresh");

  // auto refresh
  private JCheckBox cbAutoRefresh = new JCheckBox("Refresh each");
  private JSpinner sAutoRefresh = new JSpinner();
  private JComboBox cbRefreshUnits = new JComboBox(new Object []{"sec", "min"});
  private int SEC = 0;

//  private JMenu menu = new JMenu("Job options");

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
   
    initGUI();
  }
  
  public String getTitle(){
    return "Job Monitor";
  }

  /**
   * GUI initialisation
   */

  public void initGUI(){
    
    this.setLayout(new GridBagLayout());


    //// central panel

    this.add(tpStatLog, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0
        ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));

    tpStatLog.setTabPlacement(JTabbedPane.BOTTOM);

    tpStatLog.add("Monitor", spStatusTable);
    tpStatLog.add("Logs", spLogView);

    spLogView.getViewport().add(logViewerPanel);

    spStatusTable.getViewport().add(statusTable);
    statusTable.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e) {
        selectionEvent(e);
      }
    });

    makeMenu();

    //// Buttons panel

    bDecide.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e) {
        decide();
      }
    });
    bDecide.setEnabled(false);

    bKill.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        kill();
      }
    });
    bKill.setEnabled(false);

    bRefresh.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        refresh();
      }
    });
    cbAutoRefresh.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        cbAutoRefresh_clicked();
      }
    });


    sAutoRefresh.setPreferredSize(new Dimension(50, 21));
    sAutoRefresh.setModel(new SpinnerNumberModel(30, 1, 9999, 1));
    sAutoRefresh.addChangeListener(new ChangeListener() {
      public void stateChanged(ChangeEvent e) {
        delayChanged();
      }
    });

    cbRefreshUnits.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        delayChanged();
      }
    });


    this.add(pButtons,  new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0
        ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));

    pButtons.add(bDecide);
    pButtons.add(bKill);
    pButtons.add(bRefresh);
    pButtons.add(cbAutoRefresh);
    pButtons.add(sAutoRefresh);
    pButtons.add(cbRefreshUnits);

    bDecide.setToolTipText("Shows the outputs of the selected jobs");
    bKill.setToolTipText("Kills the selected jobs");
    bRefresh.setToolTipText("Refresh all jobs");
  }

  public void windowClosing(){
    GridPilot.getClassMgr().getGlobalFrame().cbMonitor.setSelected(false);
  }
  
  /**
   * Makes the menu shown when the user right-clicks on the status table
   */
  private void makeMenu(){
    // menu items

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

//    miShowOutput.setEnabled(false);
//    miShowFullStatus.setEnabled(false);
//    miShowInfo.setEnabled(false);
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
   * public methods
   */

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
   * Action Events
   */

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

    DatasetMgr datasetMgr;
    for(int i = 0; i < jobs.size(); ++i){
      JobInfo job = (JobInfo) jobs.get(i);
      if(job.getDBStatus()!=dbChoices[i]){
        datasetMgr = GridPilot.getClassMgr().getDatasetMgr(job.getDBName(),
            GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).getJobDefDatasetID(
                job.getJobDefId()));
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

      ShellMgr shell = GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job);
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
              }catch(Exception e){
                Debug.debug("WARNING: Could not save. "+e.getMessage(), 1);
              }
            }
            else{
              // Save the obtained stdout/stderr
              try{
                GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job).writeFile(job.getStdOut(), outs[0], false);
                GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job).writeFile(job.getStdErr(), outs[1], false);
              }catch(Exception e){
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

    ShellMgr shell = GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job);

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
    ShellMgr shell = GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job);
    DatasetMgr datasetMgr;
    datasetMgr = GridPilot.getClassMgr().getDatasetMgr(job.getDBName(),
        GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).getJobDefDatasetID(
            job.getJobDefId()));
    ShowOutputsJobsDialog.showFilesTabs(JOptionPane.getRootFrame(),
        "Scripts for job " + job.getName(),
        shell,
        datasetMgr.getScripts(statusTable.getSelectedRow())            
        );
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
          datasetMgr = GridPilot.getClassMgr().getDatasetMgr(job.getDBName(),
              GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()).getJobDefDatasetID(
                  job.getJobDefId()));
          datasetMgr.setDBStatus(new int [] {statusTable.getSelectedRows()[i]}, dbStatus);
        }
      }
    }.start();
  }

  public void copy(){
  }
  public void cut(){
  }
  public void paste(){
  }
}

