package gridpilot.csplugins.ec2;

import gridpilot.ConfirmBox;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.StatusBar;
import gridpilot.Table;
import gridpilot.Util;

import java.awt.*;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.ClipboardOwner;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.ListSelectionModel;
import javax.swing.TransferHandler;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.xerox.amazonws.ec2.EC2Exception;
import com.xerox.amazonws.ec2.ImageDescription;
import com.xerox.amazonws.ec2.ReservationDescription;
import com.xerox.amazonws.ec2.ReservationDescription.Instance;

/**
 * Panel showing the status of EC2 and containing buttons for
 * starting and stopping virtual machines.
 * 
 * Available images: <selectable list>  [launch]
 * Running images: <selectable list> [configure access] [stop]
 * 
 */
public class EC2MonitoringPanel extends JPanel implements ClipboardOwner{

  private static final long serialVersionUID = 1L;
  private Table amiTable = null;
  private Table instanceTable = null;
  private String [] amiColorMapping = null;  
  private String [] instanceColorMapping = null;  
  private EC2Mgr ec2mgr = null;
  private JButton bTerminate = new JButton("Terminate");
  private JButton bLaunch = new JButton("Launch instance(s)");
  private JMenuItem miCopyDNS = new JMenuItem("Copy DNS to clipboard");
  private JMenuItem miCopyKeyFile = new JMenuItem("Copy key file location to clipboard");
  private JMenuItem miRunShell = new JMenuItem("Run shell on instance");
  
  private static String [] AMI_FIELDS = new String [] {"AMI ID", "Manifest", "State", "Owner"};
  private static String [] INSTANCE_FIELDS = new String [] {"Reservation ID", "Owner", "Instance ID", "AMI", "State",
      "Public DNS", "Key"};
 
  public StatusBar statusBar = null;

  public EC2MonitoringPanel(EC2Mgr _ec2mgr){
    ec2mgr = _ec2mgr;
    // use status bar on main window until a monitoring panel is actually created
    statusBar = GridPilot.getClassMgr().getStatusBar();
    amiColorMapping = GridPilot.getClassMgr().getConfigFile().getValues("EC2", "AMI color mapping");  
    instanceColorMapping = GridPilot.getClassMgr().getConfigFile().getValues("EC2", "Instance color mapping");  
    initGUI();
    bLaunch.setEnabled(false);
    bTerminate.setEnabled(false);
  }
  
  public String getTitle(){
    return "EC2 Monitor";
  }

  public void initGUI(){
    
    this.setLayout(new BorderLayout());
    
    statusBar = new StatusBar();
    
    JPanel jp = new JPanel(new BorderLayout());
    JPanel availableAMIsPanel = createAMIsPanel();
    JPanel runningInstancesPanel = createInstancesPanel();
    availableAMIsPanel.setPreferredSize(new Dimension(600, 150));
    runningInstancesPanel.setPreferredSize(new Dimension(600, 150));
    availableAMIsPanel.setMinimumSize(new Dimension(600, 100));
    runningInstancesPanel.setMinimumSize(new Dimension(600, 100));

    JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT,
        availableAMIsPanel, runningInstancesPanel);
    splitPane.setOneTouchExpandable(true);
    splitPane.setContinuousLayout(true);
    jp.add(splitPane);
    jp.add(statusBar, BorderLayout.SOUTH);
    
    JScrollPane sp = new JScrollPane();
    sp.getViewport().add(jp);
    this.add(sp);

    /*this.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
    this.addWindowListener(new WindowAdapter(){
      public void windowClosing(WindowEvent we){
        Debug.debug("Thwarted user attempt to close window.", 3);
      }
    });

    jp.setPreferredSize(new Dimension(600, 300));
    this.pack();*/
    splitPane.setDividerLocation(0.5);
    splitPane.setResizeWeight(0.5);
    
    // Disable clipboard handling inherited from JPanel
    TransferHandler th = new TransferHandler(null);
    instanceTable.setTransferHandler(th);
    
    makeMenu();
  }
  
  String [][] getAvailableAMIs() throws EC2Exception{
    List amiList = ec2mgr.listAvailableAMIs();
    String [][] amiArray = new String [amiList.size()][AMI_FIELDS.length];
    ImageDescription ami = null;
    int i = 0;
    // "AMI ID", "Manifest", "State", "Owner"
    for(Iterator it=amiList.iterator(); it.hasNext();){
      ami = (ImageDescription) it.next();
      amiArray[i][0] = ami.getImageId();
      amiArray[i][1] = ami.getImageLocation();
      amiArray[i][2] = ami.getImageState();
      amiArray[i][3] = ami.getImageOwnerId();
      ++i;
    }
    return amiArray;
  }
  
  String [][] getRunningInstances() throws EC2Exception{
    List reservationList = ec2mgr.listReservations();
    Vector instanceVector = new Vector();
    List instanceList = null;
    String [] row = new String [INSTANCE_FIELDS.length];
    Instance instance = null;
    ReservationDescription reservation = null;
    for(Iterator it=reservationList.iterator(); it.hasNext();){
      reservation = (ReservationDescription) it.next();
      instanceList = ec2mgr.listInstances(reservation);
      // "Reservation ID", "Owner", "Instance ID", "AMI", "State", "Public DNS", "Key"
      for(Iterator itt=instanceList.iterator(); itt.hasNext();){
        row = new String [INSTANCE_FIELDS.length];
        instance = (Instance) itt.next();
        row[0] = reservation.getReservationId();
        row[1] = reservation.getOwner();
        row[2] = instance.getInstanceId();
        row[3] = instance.getImageId();
        row[4] = instance.getState();
        row[5] = instance.getDnsName();
        row[6] = instance.getKeyName();
        instanceVector.add(row);
      }
    }
    String [][] instanceArray = new String[instanceVector.size()][INSTANCE_FIELDS.length];
    for(int i=0; i<instanceVector.size(); ++i){
      row = (String []) instanceVector.get(i);
      for(int j=0; j<INSTANCE_FIELDS.length; ++j){
        instanceArray[i][j] = row[j];
      }
    }
    return instanceArray;
  }
  
  private JPanel createAMIsPanel(){
    JPanel panel = new JPanel(new BorderLayout()); 
    amiTable = new Table(new String [] {}, AMI_FIELDS, amiColorMapping);
    amiTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    amiTable.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        amiSelectionEvent(e);
      }
    });
    amiTable.setTable(AMI_FIELDS);
    amiTable.updateUI();
    JScrollPane sp = new JScrollPane();
    sp.getViewport().add(amiTable);
    sp.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Available AMIs"));
    panel.add(sp);
    // buttons
    JPanel pButtons = new JPanel();
    JButton bRefresh = new JButton("Refresh");
    bRefresh.setToolTipText("Refresh the list of AMIs");
    bRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          amiTable.setTable(getAvailableAMIs(), AMI_FIELDS);
          bLaunch.setEnabled(false);
          bTerminate.setEnabled(false);
          makeMenu();
        }
        catch(Exception e1){
           e1.printStackTrace();
        }
      }
    });
    bLaunch.setToolTipText("Launch an instance of the selected AMI");
    bLaunch.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          launchAMIs();
        }
        catch(Exception e1){
           e1.printStackTrace();
        }
      }
    });
    pButtons.add(bRefresh);
    pButtons.add(new JLabel("|"));
    pButtons.add(bLaunch);
    panel.add(pButtons, BorderLayout.SOUTH);
    return panel;
  }
  
  protected void launchAMIs() throws Exception {
    // get the selected AMI
    int row = amiTable.getSelectedRow();
    if(row==-1){
      return;
    }
    String amiID = (String) amiTable.getUnsortedValueAt(row, 0);
    // get the number of instances we want to start
    int instances = Util.getNumber("Number of instances to start", "Instances", 1);
    if(instances<1){
      return;
    }
    ec2mgr.launchInstances(amiID, instances);
  }

  protected void terminateInstances() throws Exception {
    // get the selected instances
    int [] rows = instanceTable.getSelectedRows();
    if(rows==null || rows.length==0){
      Debug.debug("Nothing selected", 2);
      return;
    }
    String [] ids = new String [rows.length];
    for(int i=0; i<rows.length; ++i){
      ids[i] = (String) instanceTable.getUnsortedValueAt(rows[i], 2);
    }
    String msg = "Are you sure you want to terminate the instance(s) "+Util.arrayToString(ids)+"?";
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    try{
      int choice = confirmBox.getConfirm("Confirm terminate",
          msg, new Object[] {"OK", "Cancel"});
      if(choice!=0){
        return;
      }
    }
    catch(Exception e){
      e.printStackTrace();
      return;
    }
    Debug.debug("Terminating "+ids.length+" instances.", 2);
    ec2mgr.terminateInstances(ids);
  }

  private JPanel createInstancesPanel(){
    JPanel panel = new JPanel(new BorderLayout()); 
    instanceTable = new Table(new String [] {}, INSTANCE_FIELDS, instanceColorMapping);
    amiTable.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
    instanceTable.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        instanceSelectionEvent(e);
      }
    });
    instanceTable.setTable(INSTANCE_FIELDS);
    instanceTable.updateUI();
    JScrollPane sp = new JScrollPane();
    sp.getViewport().add(instanceTable);
    sp.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Your instances"));
    panel.add(sp);
    JPanel pButtons = new JPanel();
    JButton bRefresh = new JButton("Refresh");
    bRefresh.setToolTipText("Refresh the list of instances");
    bRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          instanceTable.setTable(getRunningInstances(), INSTANCE_FIELDS);
          bLaunch.setEnabled(false);
          bTerminate.setEnabled(false);
          makeMenu();
        }
        catch(Exception e1){
           e1.printStackTrace();
        }
      }
    });
    bTerminate.setToolTipText("Terminate the selected instance(s)");
    bTerminate.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        try{
          terminateInstances();
        }
        catch(Exception e1){
           e1.printStackTrace();
        }
      }
    });
    pButtons.add(bRefresh);
    pButtons.add(new JLabel("|"));
    pButtons.add(bTerminate);
    panel.add(pButtons, BorderLayout.SOUTH);
    return panel;
  }

  public void windowClosing(){
    // disallow closing this window
  }

  /**
   * Updates selected and unselected menu items or button regarding to the selection.
   */
  private void amiSelectionEvent(ListSelectionEvent e){
    //Ignore extra messages.
    if(e.getValueIsAdjusting()){
      return;
    }
    ListSelectionModel lsm = (ListSelectionModel)e.getSource();
    if(lsm.isSelectionEmpty()){
      bLaunch.setEnabled(false);
    }
    else{
      //int [] rows = amiTable.getSelectedRows();
      bLaunch.setEnabled(!lsm.isSelectionEmpty() && lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
    }
  }
  
  /**
   * Updates selected and unselected menu items or button regarding to the selection.
   */
  private void instanceSelectionEvent(ListSelectionEvent e){
    // Ignore extra messages.
    if(e.getValueIsAdjusting()){
      return;
    }
    ListSelectionModel lsm = (ListSelectionModel)e.getSource();
    if(lsm.isSelectionEmpty()){
      bTerminate.setEnabled(false);
      miCopyDNS.setEnabled(false);
      miCopyKeyFile.setEnabled(false);
      miRunShell.setEnabled(false);
    }
    else{
      int [] rows = instanceTable.getSelectedRows();
      bTerminate.setEnabled(true);
      for(int i=0; i<rows.length; ++i){
        if(!((String) instanceTable.getUnsortedValueAt(rows[i], 4)).equalsIgnoreCase("running")){
          bTerminate.setEnabled(false);
          boolean ok = !lsm.isSelectionEmpty() && lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex();
          miCopyDNS.setEnabled(ok);
          miCopyKeyFile.setEnabled(ok);
          miRunShell.setEnabled(ok);
        }
      }
      //bTerminate.setEnabled(!lsm.isSelectionEmpty());
    }
  }

  /**
   * Makes the menu shown when the user right-clicks on the status table
   */
  private void makeMenu(){
    miCopyDNS.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        (new Thread(){
          public void run(){
            copyDNSToClipBoard();
          }
        }).start();
      }
    });
    miCopyKeyFile.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        (new Thread(){
          public void run(){
            copyKeyFileToClipBoard();
          }
        }).start();
      }
    });
    miRunShell.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        (new Thread(){
          public void run(){
            runShell();
          }
        }).start();
      }
    });
    //instanceTable.addMenuSeparator();
    instanceTable.addMenuItem(miCopyDNS);
    instanceTable.addMenuItem(miCopyKeyFile);
    instanceTable.addMenuItem(miRunShell);
  }
  
  private void copyDNSToClipBoard(){
    int row = instanceTable.getSelectedRow();
    String dns = (String) instanceTable.getUnsortedValueAt(row, 5);
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    StringSelection stringSelection = new StringSelection(dns);
    clipboard.setContents(stringSelection, this);
  }

  private void copyKeyFileToClipBoard(){
    int row = instanceTable.getSelectedRow();
    String name = (String) instanceTable.getUnsortedValueAt(row, 6);
    name = ec2mgr.keyFile.getPath();
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    StringSelection stringSelection = new StringSelection(name);
    clipboard.setContents(stringSelection, this);
  }

  private void runShell(){
    int row = instanceTable.getSelectedRow();
    String dns = (String) instanceTable.getUnsortedValueAt(row, 5);
  }

  public void lostOwnership(Clipboard clipboard, Transferable contents) {
  }
  
}

