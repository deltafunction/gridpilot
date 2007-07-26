package gridpilot.csplugins.ec2;

import gridpilot.Debug;
import gridpilot.GPFrame;
import gridpilot.GridPilot;
import gridpilot.StatusBar;
import gridpilot.Table;
import gridpilot.Util;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.ListSelectionModel;
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
public class EC2MonitoringPanel extends GPFrame {

  private static final long serialVersionUID = 1L;
  private Table amiTable = null;
  private Table instanceTable = null;
  private String [] amiColorMapping = null;  
  private String [] instanceColorMapping = null;  
  private EC2Mgr ec2mgr = null;
  
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
  }
  
  public String getTitle(){
    return "EC2 Monitor";
  }

  public void initGUI(){
    
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

    this.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
    this.addWindowListener(new WindowAdapter(){
      public void windowClosing(WindowEvent we){
        Debug.debug("Thwarted user attempt to close window.", 3);
      }
    });

    jp.setPreferredSize(new Dimension(600, 300));
    this.pack();
    splitPane.setDividerLocation(0.5);
    splitPane.setResizeWeight(0.5);
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
        }
        catch(Exception e1){
           e1.printStackTrace();
        }
      }
    });
    JButton bLaunch = new JButton("Launch instance(s)");
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
    String instancesStr = Util.getName("Number of instances to start", "1");
    int instances = Integer.parseInt(instancesStr);
    ec2mgr.launchInstances(amiID, instances);
  }

  protected void terminateInstances() throws Exception {
    // get the selected instances
    int [] rows = amiTable.getSelectedRows();
    if(rows==null || rows.length==0){
      return;
    }
    String [] ids = new String [rows.length];
    for(int i=0; i<rows.length; ++i){
      ids[i] = (String) instanceTable.getUnsortedValueAt(rows[i], 0);
    }
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
        }
        catch(Exception e1){
           e1.printStackTrace();
        }
      }
    });
    JButton bTerminate = new JButton("Terminate");
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
    }
    else{
      //int [] rows = amiTable.getSelectedRows();
      //miShowInfo.setEnabled(!lsm.isSelectionEmpty() && lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
    }
  }
  
  /**
   * Updates selected and unselected menu items or button regarding to the selection.
   */
  private void instanceSelectionEvent(ListSelectionEvent e){
    //Ignore extra messages.
    if(e.getValueIsAdjusting()){
      return;
    }
    ListSelectionModel lsm = (ListSelectionModel)e.getSource();
    if(lsm.isSelectionEmpty()){
    }
    else{
      //int [] rows = instanceTable.getSelectedRows();
      //miShowInfo.setEnabled(!lsm.isSelectionEmpty() && lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
    }
  }


  
}

