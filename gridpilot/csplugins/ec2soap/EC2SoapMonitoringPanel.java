package gridpilot.csplugins.ec2soap;

import gridfactory.common.ConfirmBox;
import gridfactory.common.Debug;
import gridfactory.common.LocalShell;
import gridpilot.GridPilot;

import gridpilot.MyUtil;
import gridpilot.VMMonitoringPanel;

import java.awt.datatransfer.ClipboardOwner;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.JOptionPane;

import com.amazon.aes.webservices.client.ImageDescription;
import com.amazon.aes.webservices.client.ReservationDescription;
import com.amazon.aes.webservices.client.ReservationDescription.Instance;

/**
 * Panel showing the status of EC2 and containing buttons for
 * starting and stopping virtual machines.
 * 
 * Available images: <selectable list>  [launch]
 * Running images: <selectable list> [configure access] [stop]
 * 
 */
public class EC2SoapMonitoringPanel extends VMMonitoringPanel implements ClipboardOwner{

  private static final long serialVersionUID = 1L;

  private EC2SoapMgr ec2mgr = null;
  private boolean runningShell = false;
  
  protected String [] imageColorMapping = null;  
  protected String [] instanceColorMapping = null;  
  protected String [] sshCommand = null;
  
  protected static String [] IMAGE_FIELDS = new String [] {"AMI ID", "Manifest", "State", "Owner"};
  protected static String [] INSTANCE_FIELDS = new String [] {"Reservation ID", "Owner", "Instance ID", "AMI", "State",
      "Public DNS", "Key"};

  public EC2SoapMonitoringPanel(EC2SoapMgr _ec2mgr) throws Exception{
    super();
    idField = 2;
    ec2mgr = _ec2mgr;
    imageColorMapping = GridPilot.getClassMgr().getConfigFile().getValues("Eucalyptus", "AMI color mapping");  
    instanceColorMapping = GridPilot.getClassMgr().getConfigFile().getValues("Eucalyptus", "Instance color mapping");  
    sshCommand = GridPilot.getClassMgr().getConfigFile().getValues("EC2", "Ssh command");  
    imageTable.setTable(IMAGE_FIELDS);
    instanceTable.setTable(getRunningInstances(), INSTANCE_FIELDS);
  }
  
  public String getName(){
    return "EC2 virtual machines";
  }
  
  protected String [][] getAvailableImages() throws Exception{
    List<ImageDescription> amiList = ec2mgr.listAvailableAMIs();
    String [][] amiArray = new String [amiList.size()][IMAGE_FIELDS.length];
    ImageDescription ami = null;
    int i = 0;
    // "AMI ID", "Manifest", "State", "Owner"
    for(Iterator<ImageDescription> it=amiList.iterator(); it.hasNext();){
      ami = it.next();
      amiArray[i][0] = ami.imageId;
      amiArray[i][1] = ami.imageLocation;
      amiArray[i][2] = ami.imageState;
      amiArray[i][3] = ami.imageOwnerId;
      ++i;
    }
    return amiArray;
  }
  
  protected String [][] getRunningInstances() throws Exception{
    List reservationList = ec2mgr.listReservations();
    Vector instanceVector = new Vector();
    List instanceList = null;
    String [] row = new String [INSTANCE_FIELDS.length];
    Instance instance = null;
    ReservationDescription reservation = null;
    for(Iterator<ReservationDescription> it=reservationList.iterator(); it.hasNext();){
      reservation = it.next();
      instanceList = ec2mgr.listInstances(reservation);
      // "Reservation ID", "Owner", "Instance ID", "AMI", "State", "Public DNS", "Key"
      for(Iterator<Instance> itt=instanceList.iterator(); itt.hasNext();){
        row = new String [INSTANCE_FIELDS.length];
        instance = itt.next();
        row[0] = reservation.resId;
        row[1] = reservation.owner;
        row[2] = instance.instanceId;
        row[3] = instance.imageId;
        row[4] = instance.state;
        row[5] = instance.dnsName;
        row[6] = instance.keyName;
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
  
  protected void launchImages() throws Exception {
    // get the selected AMI
    int row = imageTable.getSelectedRow();
    if(row==-1){
      return;
    }
    String amiID = (String) imageTable.getUnsortedValueAt(row, imIdField);
    // get the number of instances we want to start
    int instances = MyUtil.getNumber("Number of instances to start", "Instances", 1);
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
      ids[i] = (String) instanceTable.getUnsortedValueAt(rows[i], idField);
    }
    String msg = "Are you sure you want to terminate "+MyUtil.arrayToString(ids, ", ")+"?";
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
  
  protected String getCredentials(){
    return "SSH key: "+ec2mgr.getKeyFile().getPath();
  }

  protected void runShell(){
    if(runningShell){
      return;
    }
    runningShell = true;
    StringBuffer stdout = new StringBuffer();
    StringBuffer stderr = new StringBuffer();
    String [] fullCommand = new String [0];
    try{
      int row = instanceTable.getSelectedRow();
      String dns = (String) instanceTable.getUnsortedValueAt(row, dnsField);
      fullCommand = new String [sshCommand.length+2];
      for(int i=0; i<sshCommand.length; ++i){
        fullCommand[i] = sshCommand[i];
      }
      fullCommand[fullCommand.length-2] = ec2mgr.getKeyFile().getPath();
      fullCommand[fullCommand.length-1] = dns;
      Debug.debug("Connecting to "+dns+" with "+ec2mgr.getKeyFile().getPath(), 1);
      (new LocalShell()).exec(fullCommand, null, null, stdout, stderr, 0, null);
    }
    catch(Exception e){
      e.printStackTrace();
      MyUtil.showError("Could not connect to host with "+MyUtil.arrayToString(fullCommand)+"; "+stdout+" : "+stderr);
    }
    finally{
      runningShell = false;
    }
  }

}

