package gridpilot;

import gridfactory.common.Debug;
import gridfactory.common.FileTransfer;
import gridfactory.common.TransferInfo;

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
  private MyJTable statusTable = null;
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
  private JButton bClearTable;
  // Buttons panel
  private JPanel pButtons = new JPanel();
  private JButton bKill;
  private JButton bRefresh;
  // auto refresh
  private JCheckBox cbAutoRefresh = new JCheckBox("each");
  private JSpinner sAutoRefresh = new JSpinner();
  private JComboBox cbRefreshUnits = new JComboBox(new Object []{"sec", "min"});
  private int SEC = 0;
  private int MIN = 1;
  private JMenuItem miShowInfo = new JMenuItem("Show Information");
  private JMenuItem miRefresh = new JMenuItem("Refresh");
  private JMenuItem miKill = new JMenuItem("Stop transfer(s)");
  private JMenuItem miResubmit = new JMenuItem("Retry transfer(s)");
  private JMenuItem miClear = new JMenuItem("Clear");
  private MyTransferControl transferControl;
  private MyTransferStatusUpdateControl statusUpdateControl = null;
  
  private Timer timerRefresh = new Timer(0, new ActionListener (){
    public void actionPerformed(ActionEvent e){
      Debug.debug("Refreshing download status", 3);
      statusUpdateControl.updateStatus(null);
    }
  });

  /**
   * Constructor
   * @throws Exception 
   */
  public TransferMonitoringPanel() throws Exception {
    statusTable = GridPilot.getClassMgr().getTransferStatusTable();
    statusTable.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        selectionEvent(e);
      }
    });
  }
  
  public void activate() {
    transferControl = GridPilot.getClassMgr().getTransferControl();
    statusUpdateControl = GridPilot.getClassMgr().getTransferStatusUpdateControl();
  }
  
  public String getTitle(){
    return "Transfer Monitor";
  }
  
  private void initButtons(){
    bClearTable = MyUtil.mkButton("clear_table.png", "Clear done/failed", "Remove done/failed transfers from monitor");
    bRefresh = MyUtil.mkButton("refresh.png", "Refresh", "Refresh status of transfer(s)");
    bKill = MyUtil.mkButton("stop.png", "Stop", "Cancel transfer(s)");
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
        statusUpdateControl.updateStatus(null);
      }
    });
    cbAutoRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        cbAutoRefresh_clicked();
      }
    });

    sAutoRefresh.setPreferredSize(new Dimension(50, 21));
    sAutoRefresh.setModel(new SpinnerNumberModel(20, 1, 9999, 1));
    sAutoRefresh.addChangeListener(new ChangeListener(){
      public void stateChanged(ChangeEvent e){
        delayChanged();
      }
    });

    cbRefreshUnits.setSelectedIndex(SEC);
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

    mainPanel.add(pOptions, BorderLayout.EAST);
    mainPanel.add(pButtons, BorderLayout.SOUTH);
    mainPanel.add(spStatusTable);
    this.add(mainPanel);
    
    //this.setPreferredSize(new Dimension(700, 500));
    
  }

  /**
   * Makes the menu shown when the user right-clicks on the status table
   */
  protected void makeMenu(){

    miKill.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        kill();
      }
    });

    miRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        statusUpdateControl.updateStatus(statusTable.getSelectedRows());
      }
    });

    miClear.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        clear(statusTable.getSelectedRows());
      }
    });

    miResubmit.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        resubmit();
      }
    });

    miShowInfo.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        showInfo();
      }
    });

    miKill.setEnabled(false);
    miResubmit.setEnabled(false);
    miShowInfo.setEnabled(false);

    statusTable.addMenuSeparator();
    statusTable.addMenuItem(miRefresh);
    statusTable.addMenuItem(miShowInfo);
    statusTable.addMenuItem(miKill);
    statusTable.addMenuItem(miResubmit);
    statusTable.addMenuItem(miClear);

  }

  /**
   * Called when this panel is shown.
   */
  public void panelShown(){
    Debug.debug("panelShown",1);
    statusBar.setLabel(GridPilot.getClassMgr().getSubmittedTransfers().size() +
        " transfer(s) monitored");
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
    setAutoRefresh(cbAutoRefresh.isSelected());
  }
  
  /**
   * Start or stop auto refreshing.
   * @param refresh
   */
  public void setAutoRefresh(boolean refresh){
    if(refresh){
      Debug.debug("restarting auto refresh timer", 3);
      delayChanged();
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

  /**
   * Called when either the spinner valuer is changed or combo box "sec/min" is changed
   */
  private void delayChanged(){
    int delay = ((Integer) (sAutoRefresh.getValue())).intValue();
      Debug.debug("Changing refresh interval to "+delay, 3);
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
        transferControl = GridPilot.getClassMgr().getTransferControl();
        try{
          transferControl.cancel(
              getTransfersAtRows(statusTable.getSelectedRows()));
          statusUpdateControl.updateTransfersByStatus();
        }
        catch(Exception ee){
          statusBar.setLabel("ERROR: cancel transfers failed.");
        }
      }
    }).start();
  }

  /**
   * Shows information about the transfer at the selected row. <p>
   */
  private void showInfo(){
    final Thread t = new Thread(){
      public void run(){
        String info = "<html>";
        TransferInfo transfer = getTransferAtRow(statusTable.getSelectedRow());
        try{
          info += "File transfer system : "+transfer.getFTName()+"<br>\n";
          try{
            info += "User information : "+GridPilot.getClassMgr().getFTPlugin(
                transfer.getFTName()).getUserInfo()+"<br>\n";
          }
          catch(Exception e){
          }
          info += "File catalog : "+(transfer.getDBName()==null?
              "none":transfer.getDBName())+"<br>\n";
          info += transfer.toString().replaceAll("\\n", "<br>\n")+"<br>\n";
          try{
            info += "Status : "+transfer.getStatus()+"<br>\n";
          }
          catch(Exception e){
          }
          info += "Internal status : "+transfer.getInternalStatus()+"<br>\n";
          try{
            info += "Plugin "+transferControl.getFullStatus(transfer.getTransferID());
          }
          catch(Exception e){
            info += "ERROR: could not get full status. "+e.getMessage();
          }
        }
        catch(Exception e){
          info += "ERROR: could not get status from plugin. " +
                "The transfer may not have started yet. "+e.getMessage();
        }
        info += "</html>";
        statusBar.removeLabel();
        statusBar.stopAnimation();
        //MyUtil.showLongMessage(info, "Transfer info");
        MyUtil.showMessage("Transfer info", info);
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
    if(e.getValueIsAdjusting()){
      return;
    }

    ListSelectionModel lsm = (ListSelectionModel)e.getSource();
    if(lsm.isSelectionEmpty()){
      bKill.setEnabled(false);
      miKill.setEnabled(false);
      miResubmit.setEnabled(false);
    }
    else{
      int [] rows = statusTable.getSelectedRows();
      if(areKillables(rows)){
        bKill.setEnabled(true);
        miKill.setEnabled(true);
      }
      else{
        bKill.setEnabled(false);
        miKill.setEnabled(false);
      }

      if(areResubmitables(rows)){
        miResubmit.setEnabled(true);
      }
      else{
        miResubmit.setEnabled(false);
      }

      miShowInfo.setEnabled(!lsm.isSelectionEmpty() &&
          lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
    }
  }

  /**
   * Called when user selectes one of the radio button (view all transfers,
   * view only running transfers, ...).
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
    Vector submittedJobs = GridPilot.getClassMgr().getSubmittedTransfers();
    Enumeration e =  submittedJobs.elements();
    while(e.hasMoreElements()){
      TransferInfo transfer = (TransferInfo) e.nextElement();
      if(MyTransferControl.isRunning(transfer)){
        if(showRows==ONLY_RUNNING_JOBS){
          statusTable.showRow(transfer.getTableRow());
        }
        else{
          statusTable.hideRow(transfer.getTableRow());
        }
      }
      else{
        if(showRows==ONLY_RUNNING_JOBS){
          statusTable.hideRow(transfer.getTableRow());
        }
        else{
          statusTable.showRow(transfer.getTableRow());
        }
      }
    }
    statusTable.updateUI();
  }
  
  /**
   * Removes selected transfers from this status table.
   */
  private void clear(final int [] selectedRows){
    if(SwingUtilities.isEventDispatchThread()){
      doClear(selectedRows);
    }
    else{
      SwingUtilities.invokeLater(
        new Runnable(){
          public void run(){
            try{
              doClear(selectedRows);
            }
            catch(Exception ex){
              Debug.debug("Could not clear...", 1);
              ex.printStackTrace();
            }
          }
        }
      );
    }
  }
  
  private void doClear(int [] selectedRows){
    transferControl = GridPilot.getClassMgr().getTransferControl();
    /*if(transferControl.isSubmitting()){
      Debug.debug("cannot clear table during submission", 3);
      return;
    }*/
    Vector<Integer> notRunningRowsVector = new Vector<Integer>();
    Vector<TransferInfo> transferVector = getTransfersAtRows(selectedRows);
    transferControl.cancel(transferVector);
    TransferInfo transfer = null;
    for(int i=0; i<transferVector.toArray().length;++i){
      transfer = transferVector.get(i);
      if(!MyTransferControl.isRunning(transfer)){
        statusTable.removeRow(i);
        statusTable.repaint();
        notRunningRowsVector.add(new Integer(i));
      }
      else{
        Debug.debug("Cannot clear running transfer", 2);
      }
    }

    int [] runningRows = new int [notRunningRowsVector.size()];
    for(int i=0; i<runningRows.length; ++i){
      runningRows[i] = ((Integer) notRunningRowsVector.get(i)).intValue();
    }
    GridPilot.getClassMgr().getTransferControl().clearTableRows(runningRows);

    statusUpdateControl.updateStatus(null);
    statusUpdateControl.updateTransfersByStatus();
    
    statusTable.tableModel.fireTableDataChanged();
  }
  
  /**
   * Requeues selected transfers.
   */
  private void resubmit(){
    transferControl = GridPilot.getClassMgr().getTransferControl();
    Vector transfers = getTransfersAtRows(statusTable.getSelectedRows());
    try{
      GridPilot.getClassMgr().getTransferControl().resubmit(transfers);
    }
    catch(Exception ee){
      statusBar.setLabel("ERROR: resubmit failed.");
      return;
    }
  }
  
  /**
   * Removes all transfers from this status table.
   */
  private void clearTable(){
    if(SwingUtilities.isEventDispatchThread()){
      doClearTable();
    }
    else{
      SwingUtilities.invokeLater(
        new Runnable(){
          public void run(){
            try{
              doClearTable();
            }
            catch(Exception ex){
              Debug.debug("Could not clear...", 1);
              ex.printStackTrace();
            }
          }
        }
      );
    }
  }
  

  private void doClearTable(){
    transferControl = GridPilot.getClassMgr().getTransferControl();
    /*if(transferControl.isSubmitting()){
      String error = "Cannot clear table during submission";
      statusBar.setLabel(error);
      Debug.debug(error, 2);
      return;
    }*/
    
    Vector<TransferInfo> notRunningVector = new Vector<TransferInfo>();
    Vector<Integer> notRunningRowsVector = new Vector<Integer>();
    for(int i=0; i<statusTable.getRowCount(); ++i){
      TransferInfo transfer = getTransferAtRow(i);
      if(!MyTransferControl.isRunning(transfer)){
        Debug.debug("Removing row "+i, 3);
        notRunningVector.add(transfer);
        notRunningRowsVector.add(new Integer(i));
      }
    }
    transferControl.cancel(notRunningVector);
    
    int [] notRunningRows = new int [notRunningRowsVector.size()];
    for(int i=0; i<notRunningRows.length; ++i){
      notRunningRows[i] = ((Integer) notRunningRowsVector.get(i)).intValue();
    }
    GridPilot.getClassMgr().getTransferControl().clearTableRows(notRunningRows);
    
    statusUpdateControl.updateStatus(null);
    statusUpdateControl.updateTransfersByStatus();
    
    statusTable.tableModel.fireTableDataChanged();

  }
  
  /**
   * Returns the transfer at the specified row in the statusTable
   * @see #getTransfersAtRows(int[])
   */
  public static TransferInfo getTransferAtRow(int row){
    Vector submTransfers = GridPilot.getClassMgr().getSubmittedTransfers();
    Debug.debug("Got transfers at row "+row+". "+submTransfers.size(), 3);
    return (TransferInfo) submTransfers.get(row);
  }

  /**
   * Returns the transfers at the specified rows in statusTable
   * @see #getTransferAtRow(int)
   */
  public static Vector<TransferInfo> getTransfersAtRows(int[] rows){
    Vector transfers = new Vector(rows.length);
    for(int i=0; i<rows.length; ++i){
      transfers.add(getTransferAtRow(rows[i]));
    }
    return transfers;
  }
  
  private boolean areKillables(int [] rows){
    Vector transferVector = getTransfersAtRows(rows);
    for(Iterator it=transferVector.iterator(); it.hasNext();){
      TransferInfo transfer = (TransferInfo) it.next();
      int internalStatus = transfer.getInternalStatus();
      if(internalStatus==FileTransfer.STATUS_DONE ||
          //internalStatus==FileTransfer.STATUS_ERROR ||
          internalStatus==FileTransfer.STATUS_FAILED){
        return false;
      }
    }
    return true;
  }
 
  private boolean areResubmitables(int [] rows){
    Vector transferVector = getTransfersAtRows(rows);
    for(Iterator it=transferVector.iterator(); it.hasNext();){
      TransferInfo transfer = (TransferInfo) it.next();
      int internalStatus = transfer.getInternalStatus();
      if(internalStatus==FileTransfer.STATUS_WAIT ||
          internalStatus==FileTransfer.STATUS_RUNNING){
        return false;
      }
    }
    return true;
  }
  
  public void copy(){
  }
  public void cut(){
  }
  public void paste(){
  }
}

