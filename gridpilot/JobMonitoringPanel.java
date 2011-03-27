package gridpilot;

import gridfactory.common.ConfirmBox;
import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.MyLinkedHashSet;
import gridfactory.common.ResThread;

import javax.swing.*;

import java.awt.*;
import java.awt.event.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import javax.swing.event.*;

/**
 * Shows a table with informations about running jobs.
 */
public class JobMonitoringPanel extends CreateEditPanel implements ListPanel{

  private static final long serialVersionUID = 1L;
  
  private final int ALL_JOBS = 0;
  private final int ONLY_RUNNING_JOBS = 1;
  private final int ONLY_DONE_JOBS = 2;
  
  private int showRows = ALL_JOBS;
  private MyJTable statusTable = null;
  // Central panel
  private JPanel mainPanel = new JPanel();
  private JScrollPane spStatusTable = new JScrollPane();
  private StatisticsPanel statisticsPanel =
    GridPilot.getClassMgr().getJobStatisticsPanel();
  // Options panel
  private JPanel pOptions = new JPanel();
  // view options
  private ButtonGroup bgView = new ButtonGroup();
  private JRadioButton rbAllJobs = new JRadioButton("View all jobs", true);
  private JRadioButton rbRunningJobs = new JRadioButton("View only running jobs");
  private JRadioButton rbDoneJobs = new JRadioButton("View only done jobs");
  // jobs loading
  private JButton bLoadJobs;
  private JButton bLoadMyJobs;
  private JButton bClearTable;
  // Buttons panel
  private JPanel pButtons = new JPanel();
  // TODO: discard bKill
  private JButton bKill;
  private JButton bRefresh;
  // auto refresh
  private JCheckBox cbAutoRefresh = new JCheckBox("each");
  private JSpinner sAutoRefresh = new JSpinner();
  private JSpinner sAutoResubmit = new JSpinner();
  private JComboBox cbRefreshUnits = new JComboBox(new Object []{"sec", "min"});
  private int SEC = 0;
  private int MIN = 1;
  private JMenuItem miStopUpdate = new JMenuItem("Stop updating");
  private JMenuItem miKill = new JMenuItem("Kill");
  private JMenuItem miClean = new JMenuItem("Clean");
  private JMenuItem miDecide = new JMenuItem("Decide");
  private JMenuItem miRefresh = new JMenuItem("Refresh");
  private JMenu mSubmit = new JMenu("Submit");
  private JMenuItem miResubmit = new JMenuItem("Resubmit");
  private JMenu mShow = new JMenu("Show");
  private JMenuItem miShowOutput = new JMenuItem("Outputs");
  private JMenuItem miShowFullStatus = new JMenuItem("Full status");
  private JMenuItem miShowInfo = new JMenuItem("Information");
  private JMenuItem miShowScripts = new JMenuItem("Script(s)");
  private JMenuItem miRevalidate = new JMenuItem("Revalidate");
  private JMenu mDB = new JMenu("Set DB Status");
  private JobStatusUpdateControl statusUpdateControl;
  private SubmissionControl submissionControl;

  
  private Timer timerRefresh = new Timer(0, new ActionListener (){
    public void actionPerformed(ActionEvent e){
      statusUpdateControl.updateStatus(null);
    }
  });

  private int decideTimeout;

  /**
   * Constructor
   * @throws Exception 
   */
  public JobMonitoringPanel() throws Exception{
    statusTable = GridPilot.getClassMgr().getJobStatusTable();
    String timeout = GridPilot.getClassMgr().getConfigFile().getValue(GridPilot.TOP_CONFIG_SECTION, "Decide timeout");
    if(timeout!=null){
      try{
        decideTimeout = Integer.parseInt(timeout);
      }
      catch(Exception e){
        GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not parse decide timeout", e);
      }
    }
  }
  
  public void activate() throws Exception {
    statusUpdateControl = new JobStatusUpdateControl();
    submissionControl = GridPilot.getClassMgr().getSubmissionControl();
  }
  
  public JobStatusUpdateControl getJobStatusUpdateControl() {
    return statusUpdateControl;
  }
  
  public String getTitle(){
    return "Job Monitor";
  }
  
  private void initButtons(){
    bKill = MyUtil.mkButton("stop.png", "Kill", "Kill selected job(s)");
    bLoadJobs = MyUtil.mkButton("load_all_jobs.png", "Load active job(s)", "Load active job(s) submitted by anyone");
    bLoadMyJobs = MyUtil.mkButton("load_my_jobs.png", "Load my active job(s)", "Load only my active job(s)");
    bClearTable = MyUtil.mkButton("clear_table.png", "Clear", "Clear monitor");
    bRefresh = MyUtil.mkButton("refresh.png", "Refresh", "Refresh status of job(s)");
  }

  public void initGUI(){

    try{
      initButtons();
    }
    catch(Exception e){
    }
    
    statusBar = GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel().getStatusBar();
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

    pOptions.add(bLoadJobs, new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0,
        GridBagConstraints.WEST,
        GridBagConstraints.NONE,
        new Insets(30, 10, 0, 0), 0, 0));

    if(GridPilot.ADVANCED_MODE){
      pOptions.add(bLoadMyJobs, new GridBagConstraints(0, 4, 1, 1, 0.0, 0.0,
          GridBagConstraints.WEST,
          GridBagConstraints.NONE,
          new Insets(10, 10, 0, 0), 0, 0));
    }

    bLoadJobs.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        loadDBJobs(true);
      }
    });
    bLoadJobs.setToolTipText("Load all jobs from DB");

    bLoadMyJobs.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        loadDBJobs(false);
      }
    });
    bLoadMyJobs.setToolTipText("Load my jobs from DB");

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
        miStopUpdate.setEnabled(true);
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
        delayRefreshChanged();
      }
    });
    cbRefreshUnits.setSelectedIndex(MIN);
    cbRefreshUnits.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        delayRefreshChanged();
      }
    });
    
    sAutoResubmit.setPreferredSize(new Dimension(40, 21));
    sAutoResubmit.setModel(new SpinnerNumberModel(0, 0, 9, 1));
    sAutoResubmit.addChangeListener(new ChangeListener(){
      public void stateChanged(ChangeEvent e){
        //TODO
      }
    });
    sAutoResubmit.setToolTipText("Number of times to resubmit failed jobs");

    pButtons.add(bRefresh);
    pButtons.add(cbAutoRefresh);
    pButtons.add(sAutoRefresh);
    pButtons.add(cbRefreshUnits);
    
    pButtons.add(new JLabel("  |  "));
    pButtons.add(new JLabel("Resubmit "));
    pButtons.add(sAutoResubmit);
    pButtons.add(new JLabel("times"));
    
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

    miStopUpdate.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        statusUpdateControl.reset();
      }
    });
    miStopUpdate.setEnabled(false);

    miStopUpdate.setToolTipText("Stop refreshing status of jobs");

    miKill.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        kill();
      }
    });

    miClean.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        clean();
      }
    });

    miDecide.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        ResThread t = new ResThread(){
          public void run(){
            decide();
            setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          }
        };
        t.start();
      }
    });

    miRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        miStopUpdate.setEnabled(true);
        statusUpdateControl.updateStatus(statusTable.getSelectedRows());
      }
    });

    miResubmit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        (new Thread(){
          public void run(){
            submissionControl = GridPilot.getClassMgr().getSubmissionControl();
            submissionControl.resubmit(
                JobMgr.getJobsAtRows(statusTable.getSelectedRows()));
          }
        }).start();
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
        JobMgr.revalidate(statusTable.getSelectedRows());
      }
    });

    JMenuItem miDBUnexpected = new JMenuItem(DBPluginMgr.getStatusName(DBPluginMgr.UNEXPECTED));
    JMenuItem miDBFailed = new JMenuItem(DBPluginMgr.getStatusName(DBPluginMgr.FAILED));
    JMenuItem miDBAborted = new JMenuItem(DBPluginMgr.getStatusName(DBPluginMgr.ABORTED));
    JMenuItem miDBDefined = new JMenuItem(DBPluginMgr.getStatusName(DBPluginMgr.DEFINED));

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

    for(int i=0; i<GridPilot.CS_NAMES.length; ++i){
      if(!MyUtil.checkCSEnabled(GridPilot.CS_NAMES[i])){
        continue;
      }
      JMenuItem mi = new JMenuItem(GridPilot.CS_NAMES[i], i);
      mi.addActionListener(new ActionListener(){
        public void actionPerformed(ActionEvent e){
          submissionControl = GridPilot.getClassMgr().getSubmissionControl();
          submissionControl.submitJobs(JobMgr.getJobsAtRows(statusTable.getSelectedRows()),
              /*computingSystem*/
              /*((JMenuItem) e.getSource()).getMnemonic()*/
              ((JMenuItem) e.getSource()).getText());
        }
      });
      mSubmit.add(mi);
    }

    miKill.setEnabled(false);
    miClean.setEnabled(false);
    miDecide.setEnabled(false);
    mSubmit.setEnabled(false);
    miResubmit.setEnabled(false);

    mShow.setEnabled(false);

    miRevalidate.setEnabled(false);
    mDB.setEnabled(false);

    mDB.add(miDBDefined);
    mDB.add(miDBFailed);
    mDB.add(miDBAborted);

    mShow.add(miShowFullStatus);
    mShow.add(miShowInfo);
    mShow.add(miShowOutput);
    mShow.add(miShowScripts);

    statusTable.addMenuSeparator();
    statusTable.addMenuItem(miStopUpdate);
    statusTable.addMenuSeparator();
    statusTable.addMenuItem(miKill);
    statusTable.addMenuItem(miClean);
    statusTable.addMenuItem(miDecide);
    statusTable.addMenuItem(miRefresh);
    statusTable.addMenuItem(mSubmit);
    statusTable.addMenuItem(miResubmit);
    statusTable.addMenuItem(mShow);
    statusTable.addMenuItem(miRevalidate);
    statusTable.addMenuItem(mDB);

  }

  /**
   * Called when this panel is shown.
   */
  public void panelShown(){
    Debug.debug("panelShown",1);
    statusBar.setLabel(GridPilot.getClassMgr().getMonitoredJobs().size() + " job(s) monitored");
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
  private void cbAutoRefresh_clicked(){
    if(cbAutoRefresh.isSelected()){
      delayRefreshChanged();
      timerRefresh.restart();
    }
    else{
      timerRefresh.stop();
    }
  }
  
  public void setAutoRefreshSeconds(int secs){
    cbRefreshUnits.setSelectedIndex(SEC);
    sAutoRefresh.setValue(secs);
    cbAutoRefresh.setSelected(true);
    cbAutoRefresh_clicked();
  }
  
  public void stopAutoRefresh(){
    cbAutoRefresh.setSelected(false);
    cbAutoRefresh_clicked();
  }

  /**
   * Called when either the spinner valuer is changed or combo box "sec/min" is changed
   */
  private void delayRefreshChanged(){
    int delay = ((Integer) (sAutoRefresh.getValue())).intValue();
    if(cbRefreshUnits.getSelectedIndex()==MIN){
      timerRefresh.setDelay(delay * 1000 * 60);
    }
    else{
      timerRefresh.setDelay(delay * 1000);
    }
  }

  /**
   * Called when button or menu item "Kill" is selected
   */
  private void kill(){
    (new Thread(){
      public void run(){
        JobMgr.killJobs(statusTable.getSelectedRows());
      }
    }).start();
  }

  /**
   * Called when button or menu item "Clean" is selected
   */
  private void clean(){
    (new Thread(){
      public void run(){
        JobMgr.cleanJobs(statusTable.getSelectedRows());
      }
    }).start();
  }

  /**
   * Called when button or menu item "Decide" is clicked
   */
  private void decide(){
    int [] rows = statusTable.getSelectedRows();
    
    Debug.debug("Deciding rows "+MyUtil.arrayToString(rows), 3);

    if(!JobMgr.areDecidable(rows)){
      return;
    }

    Set<MyJobInfo> jobs = JobMgr.getJobsAtRows(rows);

    int [] options = {DBPluginMgr.VALIDATED, DBPluginMgr.FAILED, DBPluginMgr.UNDECIDED, DBPluginMgr.ABORTED};
    
    String [] sOptions = {
        DBPluginMgr.getStatusName(options[0]),
        DBPluginMgr.getStatusName(options[1]),
        DBPluginMgr.getStatusName(options[2]),
        DBPluginMgr.getStatusName(options[3])
    };

    int choices[] = null;
    try{
      choices = ShowOutputsJobsDialog.show((Window) SwingUtilities.getRoot(this), jobs, sOptions);
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not show scripts.", e);
    }
    if(choices!=null){
      int dbChoices [] = new int[choices.length];

      for(int i=0; i<jobs.size(); ++i){
        if(choices[i]==-1){
          dbChoices[i] = DBPluginMgr.UNDECIDED;
        }
        else{
          dbChoices[i]  = options[choices[i]];
        }
      }
      doDecide(jobs, dbChoices);
    }
  }

  private void doDecide(final Set<MyJobInfo> jobs, final int[] dbChoices) {
    setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    ResThread t = new ResThread(){
      public void run(){
        try{
          int i = 0;
          MyJobInfo job;
          for(Iterator<MyJobInfo> it=jobs.iterator(); it.hasNext();){
            job = it.next();
            if(!doDoDecide(job, dbChoices[i]) && it.hasNext()){             
              ConfirmBox confirmBox = new ConfirmBox(GridPilot.getClassMgr().getGlobalFrame());
              String confirmString =
                  "Would you like to continue deciding the remaining job(s)?\n";
              int choice = -1;
              choice = confirmBox.getConfirmPlainText("Continue deciding?", confirmString,
                  new Object[] {
                  MyUtil.mkButton(null, "Continue", "Continue with remaining job(s)"),
                  MyUtil.mkButton(null, "Stop", "Stop deciding job(s)")},
                  null, Color.WHITE, true, false);
              if(choice!=0){
                return;
              }           
            }
            ++i;
          }
          statusTable.updateSelection();
        }
        catch(Exception e){
          e.printStackTrace();
        }
        finally{
          setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        }
      }
    };
    t.start();
  }

  private boolean doDoDecide(final MyJobInfo job, final int dbChoice) {
    ResThread t = new ResThread(){
      public void run(){
        try{
          if(job.getDBStatus()!=dbChoice){
            JobMgr jobMgr = getJobMgr(job);
            jobMgr.updateDBStatus(job, dbChoice);
            if(dbChoice!= DBPluginMgr.UNDECIDED){
              job.setNeedsUpdate(false);
            }
            jobMgr.updateJobsByStatus();
          }
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
    };
    t.start();
    return MyUtil.waitForThread(t, "Decide job status", decideTimeout, "decide", GridPilot.getClassMgr().getLogFile());
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
    statusBar.setLabel("Waiting for outputs...");
    statusBar.animateProgressBar();
    ((Window) SwingUtilities.getWindowAncestor(getRootPane())).setCursor(
        Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));

    final Thread t = new Thread(){
      public void run(){
        MyJobInfo job = JobMgr.getJobAtRow(selectedRow);
        String [] outNames = new String [] {"stdout", "stderr"};
        String [] outs = null;
        try{
          outs = GridPilot.getClassMgr().getCSPluginMgr(
          ).getCurrentOutput(job);
        }
        catch(Exception e){
          outs = new String [] {"Could not read stdout "+e.getMessage(),
              "Could not read stdout "+e.getMessage()};
        }
        if(job.getErrTmp()==null){
          outNames = new String [] {"stdout"};
          outs = new String[] {outs[0]};
        }
        ((Window) SwingUtilities.getWindowAncestor(getRootPane())).setCursor(
            Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        statusBar.stopAnimation();
        statusBar.setLabel("");

        String message = "";
        if(JobMgr.isRunning(selectedRow)){
          message = "Current outputs of job";
        }
        else{
          message = "Final outputs of job";
        }
        doShowOutput(job, message, outNames, outs);     
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

  private void doShowOutput(final MyJobInfo job, final String message,
      final String [] outNames, final String [] outs){
    ShowOutputsJobsDialog.showTabs((Window) SwingUtilities.getRoot(this),
        message + " " + job.getName(),
        outNames,
        outs);
  }

  /**
   * Shows full status of the job at the selected row. <p>
   */
  private void showFullStatus(){
    final Thread t = new Thread(){
      public void run(){
        MyJobInfo job = JobMgr.getJobAtRow(statusTable.getSelectedRow());
        final String status = GridPilot.getClassMgr().getCSPluginMgr().getFullStatus(job);
        statusBar.removeLabel();
        statusBar.stopAnimation();
        SwingUtilities.invokeLater(
            new Runnable(){
              public void run(){
                try{
                  MyUtil.showLongMessage(status, "Job status");
                }
                catch(Exception ex){
                  Debug.debug("Could not create panel ", 1);
                  ex.printStackTrace();
                }
              }
            }
          );
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
   */
  private void showInfo(){
    String info = JobMgr.getJobInformation(statusTable.getSelectedRow());
    MyUtil.showLongMessage(info, "Job Infos");
  }

  private void showScripts(){

    final int selectedRow = statusTable.getSelectedRow();
    if(selectedRow==-1){
      return;
    }
    statusBar.setLabel("Waiting for scripts...");
    statusBar.animateProgressBar();
    ((Window) SwingUtilities.getWindowAncestor(getRootPane())).setCursor(
        Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));

    final Thread t = new Thread(){
      public void run(){
        MyJobInfo job = JobMgr.getJobAtRow(statusTable.getSelectedRow());
        try{
          String [] scripts = GridPilot.getClassMgr().getCSPluginMgr().getScripts(job);
          ((Window) SwingUtilities.getWindowAncestor(getRootPane())).setCursor(
              Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          statusBar.stopAnimation();
          statusBar.setLabel("");
          ShowOutputsJobsDialog.showTabs((Window) SwingUtilities.getRoot(getThis()),
              "Scripts for job " + job.getName(),
              job,
              scripts            
              );
        }
        catch(Exception e){
          GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not show scripts.", e);
        }
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
  
  private JobMonitoringPanel getThis(){
    return this;
  }

  /**
   * Load jobs from database.
   */
  private void loadDBJobs(final boolean allJobs){
    new Thread(){
      public void run(){
        statusBar.setLabel("Waiting for DB Server...");
        statusBar.animateProgressBar();
        bLoadJobs.setEnabled(false);
        bLoadMyJobs.setEnabled(false);
        if(statusTable.getRowCount()>0){
          statusTable.selectAll();
          statusTable.clearSelection();
        }
        try{
          Debug.debug("loading jobs...", 3);
          loadJobs(allJobs, new int [] {Database.SUBMITTED, Database.UNDECIDED,
              Database.UNEXPECTED/*, Database.FAILED*/});
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
    
    DBPluginMgr dbPluginMgr = null;
    String [] shownFields = null;
    String user = "";
    String csName = "";
    DBResult allJobDefinitions = null;
    // Group the file IDs by dataset
    HashMap<JobMgr, Vector<String>> jobMgrsAndIds = new HashMap<JobMgr, Vector<String>>();

    for(int ii=0; ii<GridPilot.DB_NAMES.length; ++ii){
      dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(GridPilot.DB_NAMES[ii]);
      shownFields = null;
      try{
        shownFields = dbPluginMgr.getFieldnames("jobDefinition");//dbPluginMgr.getDBDefFields("jobDefinition");
      }
      catch(Exception e){
        Debug.debug("Skipping DB "+dbPluginMgr.getDBName(), 1);
        continue;
      }
      String [] statusStrings = new String [statusList.length];
      for(int j=0; j<statusList.length; ++j){
        statusStrings[j] = DBPluginMgr.getStatusName(statusList[j]);
      }
      allJobDefinitions = dbPluginMgr.getJobDefinitions(
            /*datasetID*/"-1", shownFields, statusStrings, null);
      Debug.debug ("number of jobs for "+GridPilot.DB_NAMES[ii]+
          ": "+allJobDefinitions.values.length, 2);

      JobMgr mgr = GridPilot.getClassMgr().getJobMgr(GridPilot.DB_NAMES[ii]);
      for(int i=0; i<allJobDefinitions.values.length; ++i){
        user = null;
        DBRecord job = new DBRecord(allJobDefinitions.fields,
            allJobDefinitions.values[i]);
        String idField = MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "jobDefinition");
        Debug.debug("Checking:"+MyUtil.arrayToString(job.values), 3);
        String id = job.getValue(idField).toString();
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
        if(!jobMgrsAndIds.containsKey(mgr)){
          jobMgrsAndIds.put(mgr, new Vector<String>());
        }
        Debug.debug("Adding job #"+id, 3);
        jobMgrsAndIds.get(mgr).add(id);
      }
    }
    // Add them
    String [] ids = null;
    Vector<String> idVec = null;
    JobMgr jobMgr = null;
    for(Iterator<JobMgr> it=jobMgrsAndIds.keySet().iterator(); it.hasNext();){
      jobMgr = it.next();
      idVec = jobMgrsAndIds.get(jobMgr);
      ids = new String [idVec.size()];
      for(int i=0; i<idVec.size(); ++i){
        ids[i] = idVec.get(i).toString();
      }
      jobMgr.addJobs(ids);
      jobMgr.updateJobsByStatus();
    }
  }

  /**
   * Updates selected and unselected menu items or button regarding to the selection.
   */
  private void selectionEvent(ListSelectionEvent e){
    //Ignore extra messages.
    if (e.getValueIsAdjusting()){
      return;
    }
    
    miStopUpdate.setEnabled(statusUpdateControl.checkingThreads.size()>0);
    
    ListSelectionModel lsm = (ListSelectionModel)e.getSource();
    if(lsm.isSelectionEmpty()){
      bKill.setEnabled(false);
      miClean.setEnabled(false);
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
      if(JobMgr.areDecidable(rows)){
        miDecide.setEnabled(true);
        miRevalidate.setEnabled(true);
      }
      else{
        miDecide.setEnabled(false);
        miRevalidate.setEnabled(false);
      }

      if(JobMgr.areKillable(rows)){
        bKill.setEnabled(true);
        miKill.setEnabled(true);
      }
      else{
        bKill.setEnabled(false);
        miKill.setEnabled(false);
      }

      if(JobMgr.areCleanable(rows)){
        miClean.setEnabled(true);
      }
      else{
        miClean.setEnabled(false);
      }

      if(JobMgr.areResumbitable(rows)){
        miResubmit.setEnabled(true);
      }
      else{
        miResubmit.setEnabled(false);
      }

      if(JobMgr.areSubmitable(rows)){
        mSubmit.setEnabled(true);
      }
      else{
        mSubmit.setEnabled(false);
      }

      miShowFullStatus.setEnabled(!lsm.isSelectionEmpty() &&
          lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
      miShowOutput.setEnabled(!lsm.isSelectionEmpty() &&
          lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
      miShowInfo.setEnabled(!lsm.isSelectionEmpty() &&
          lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
      miShowScripts.setEnabled(!lsm.isSelectionEmpty() &&
          lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());

      mDB.setEnabled(true);
      mShow.setEnabled(true);
    }
  }

  private void setDBStatus(final int dbStatus){
    new Thread(){
      public void run(){    
        int [] rows = statusTable.getSelectedRows();       
        Debug.debug("Setting status of rows "+MyUtil.arrayToString(rows)+" from "+statusTable.getRowCount(), 2);
        MyLinkedHashSet<MyJobInfo> jobs = JobMgr.getJobsAtRows(rows);
        MyJobInfo job = null;
        JobMgr jobMgr = null;
        HashMap<JobMgr, Vector<Integer>> datasetJobs = new HashMap<JobMgr, Vector<Integer>>();
        int i = 0;
        for(Iterator<MyJobInfo> it=jobs.iterator(); it.hasNext();){
          job = it.next();
          jobMgr = getJobMgr(job);
          if(!datasetJobs.containsKey(jobMgr)){
            datasetJobs.put(jobMgr, new Vector<Integer>());
          }
          datasetJobs.get(jobMgr).add(new Integer(rows[i]));
          ++i;
        }
        Vector<Integer> jobRows;
        for(Iterator<JobMgr> it=datasetJobs.keySet().iterator(); it.hasNext();){
          jobMgr = it.next();
          jobRows = datasetJobs.get(jobMgr);
          int [] dsRows = new int [jobRows.size()];
          for(int j=0; j<jobRows.size(); ++j){
            dsRows[j] = ((Integer) jobRows.get(j)).intValue();
          }
          Debug.debug("Setting status "+dbStatus +" of rows "+MyUtil.arrayToString(dsRows), 2);
          jobMgr.setDBStatus(dsRows, dbStatus);
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
    Set<MyJobInfo> submittedJobs = GridPilot.getClassMgr().getMonitoredJobs();
    for(Iterator<MyJobInfo>it=submittedJobs.iterator(); it.hasNext();){
      MyJobInfo job = it.next();
      if(JobMgr.isRunning(job)){
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
  private boolean clearTable(){
    submissionControl = GridPilot.getClassMgr().getSubmissionControl();
    if(submissionControl!=null && submissionControl.isSubmitting()){
      Debug.debug("cannot clear table during submission", 3);
      return false;
    }
    if(GridPilot.getClassMgr().getJobValidation().isValidating()){
      Debug.debug("cannot clear table during validation", 3);
      return false;
    }
    JobMgr mgr = null;
    for(Iterator<JobMgr> it = GridPilot.getClassMgr().getJobMgrs().iterator(); it.hasNext();){
      mgr = it.next();
      if(mgr.isPostProcessing()){
        Debug.debug("cannot clear table during post-processing", 3);
        return false;
      }
    }
    statusUpdateControl.reset();
    boolean ret = true;
    GridPilot.getClassMgr().getMonitoredJobs().clear();
    statusTable.createRows(0);
    try{
      for(Iterator<JobMgr> it = GridPilot.getClassMgr().getJobMgrs().iterator(); it.hasNext();){
        mgr = it.next();
        mgr.initChanges();
      }
      mgr.updateJobsByStatus();
    }
    catch(Exception e){
      ret = false;
      e.printStackTrace();
      Debug.debug("WARNING: failed to clear jobs from "+mgr.dbName, 1);
    }
    
    return ret;
  }
  
  JobMgr getJobMgr(MyJobInfo job){
    return GridPilot.getClassMgr().getJobMgr(job.getDBName());
  }
  
  public void exit(){
  }

  public void copy(){
  }
  public void cut(){
  }
  public void paste(){
  }

  public JSpinner getSAutoResubmit() {
    return sAutoResubmit;
  }

}

