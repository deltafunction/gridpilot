package gridpilot.csplugins.ec2;

import gridpilot.Debug;
import gridpilot.GPFrame;
import gridpilot.GridPilot;
import gridpilot.StatusBar;
import gridpilot.Table;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.Iterator;
import java.util.List;

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
  private String [] amiFields = new String [] {"AMI ID", "Manifest", "State", "Owner"};
  private String [] instanceColorMapping = null;  
  private String [] instanceFields = new String [] {"Reservation ID", "Owner", "Instance ID", "AMI", "State",
      "Public DNS", "Key"};
  private EC2Mgr ec2mgr = null;
  
  
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
    String [][] amiArray = new String [amiList.size()][amiFields.length];
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
  
  private JPanel createAMIsPanel(){
    JPanel panel = new JPanel(new BorderLayout()); 
    amiTable = new Table(new String [] {}, amiFields, amiColorMapping);
    amiTable.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        amiSelectionEvent(e);
      }
    });
    amiTable.setTable(amiFields);
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
          amiTable.setTable(getAvailableAMIs(), amiFields);
        }
        catch (EC2Exception e1) {
           e1.printStackTrace();
        }
      }
    });
    JButton bLaunch = new JButton("Launch instance(s)");
    bLaunch.setToolTipText("Launch an instance of the selected AMI");
    bRefresh.addActionListener(new ActionListener(){
      public void actionPerformed(ActionEvent e){
        //launchAMIs();
      }
    });
    pButtons.add(bRefresh);
    pButtons.add(new JLabel("|"));
    pButtons.add(bLaunch);
    panel.add(pButtons, BorderLayout.SOUTH);
    return panel;
  }
  
  private JPanel createInstancesPanel(){
    JPanel panel = new JPanel(new BorderLayout()); 
    instanceTable = new Table(new String [] {}, instanceFields, instanceColorMapping);
    instanceTable.addListSelectionListener(new ListSelectionListener(){
      public void valueChanged(ListSelectionEvent e){
        instanceSelectionEvent(e);
      }
    });
    instanceTable.setTable(instanceFields);
    instanceTable.updateUI();
    JScrollPane sp = new JScrollPane();
    sp.getViewport().add(instanceTable);
    sp.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), "Your instances"));
    panel.add(sp);
    JPanel pButtons = new JPanel();
    JButton bRefresh = new JButton("Refresh");
    JButton bTerminate = new JButton("Terminate");
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
      int [] rows = amiTable.getSelectedRows();
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
      int [] rows = instanceTable.getSelectedRows();
      //miShowInfo.setEnabled(!lsm.isSelectionEmpty() && lsm.getMaxSelectionIndex()==lsm.getMinSelectionIndex());
    }
  }


  
}

