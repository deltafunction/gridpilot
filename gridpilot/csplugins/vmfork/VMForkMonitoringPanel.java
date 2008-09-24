package gridpilot.csplugins.vmfork;

import gridfactory.common.ConfirmBox;
import gridfactory.common.Debug;
import gridfactory.common.LocalShell;
import gridfactory.common.ResThread;
import gridfactory.common.jobrun.VMMgr;
import gridfactory.common.jobrun.VirtualMachine;
import gridfactory.common.jobrun.RTECatalog.MetaPackage;
import gridpilot.GridPilot;

import gridpilot.MyUtil;
import gridpilot.RteRdfParser;
import gridpilot.VMMonitoringPanel;

import java.awt.datatransfer.ClipboardOwner;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.swing.JOptionPane;

import org.safehaus.uuid.UUIDGenerator;

/**
 * Panel showing the status of EC2 and containing buttons for
 * starting and stopping virtual machines.
 * 
 * Available images: <selectable list>  [launch]
 * Running images: <selectable list> [configure access] [stop]
 * 
 */
public class VMForkMonitoringPanel extends VMMonitoringPanel implements ClipboardOwner{

  private static final long serialVersionUID = 1L;

  private VMMgr vmMgr = null;
  private String [] rteCatalogUrls;
  private boolean runningShell = false;

  public VMForkMonitoringPanel(VMMgr _vmMgr, String [] _rteCatalogUrls) throws Exception{
    super();
    IMAGE_FIELDS = new String [] {"Image name", "OS", "Services", "Privileges"};
    INSTANCE_FIELDS = new String [] {"Image name", "Identifier", "State", "DNS", "SSH port"};
    dnsField = 3;
    stateField = 2;
    vmMgr = _vmMgr;
    rteCatalogUrls = _rteCatalogUrls;
    imageColorMapping = GridPilot.getClassMgr().getConfigFile().getValues("VMFork", "Image color mapping");  
    instanceColorMapping = GridPilot.getClassMgr().getConfigFile().getValues("VMFork", "Instance color mapping");  
    sshCommand = GridPilot.getClassMgr().getConfigFile().getValues("VMFork", "Shell command"); 
    imageTable.setTable(IMAGE_FIELDS);
    instanceTable.setTable(getRunningInstances(), INSTANCE_FIELDS);
  }
  
  public String getName(){
    return "Local virtual machines";
  }
  
  protected String [][] getAvailableImages() throws Exception{
    RteRdfParser rdfParser = GridPilot.getClassMgr().getRteRdfParser(rteCatalogUrls);
    Set<MetaPackage> mps = rdfParser.getRteCatalog().getMetaPackages();
    MetaPackage mp;
    HashSet<String []> images = new HashSet<String []>();
    String[] imRow;
    // "Image name", "OS", "Services", "Privileges"
    for(Iterator it=mps.iterator(); it.hasNext();){
      mp = (MetaPackage) it.next();
      if(mp.virtualMachine!=null){
        imRow = new String[IMAGE_FIELDS.length];
        imRow[0] = mp.name;
        imRow[1] = mp.virtualMachine.os;
        imRow[2] = MyUtil.arrayToString(mp.virtualMachine.services);
        imRow[3] = MyUtil.arrayToString(mp.virtualMachine.privileges);
        images.add(imRow);
      }
    }
    String [][] imArray = new String [images.size()][IMAGE_FIELDS.length];
    int i = 0;
    for(Iterator<String []> it=images.iterator(); it.hasNext();){
      imArray[i] = it.next();
      ++i;
    }
    return imArray;
  }
  
  protected String [][] getRunningInstances() throws Exception{
    HashMap<String, VirtualMachine> vms = vmMgr.getVMs();
    String [][] instanceArray = new String[vms.size()][INSTANCE_FIELDS.length];
    String id;
    VirtualMachine vm;
    int i = 0;
    for(Iterator<String> it=vms.keySet().iterator(); it.hasNext();){
      id = it.next();
      vm = vms.get(id);
      // "Image name", "Identifier", "State", "DNS", "SSH port"
      instanceArray[i][0] = vm.getOS();
      instanceArray[i][1] = id;
      instanceArray[i][2] = vm.getState();
      instanceArray[i][3] = vm.getHostName();
      instanceArray[i][4] = Integer.toString(vm.getSshPort());
      ++i;
    }
    return instanceArray;
  }
  
  protected void launchImages() throws Exception {
    // get the selected AMI
    int row = imageTable.getSelectedRow();
    if(row==-1){
      return;
    }
    final String imageName = (String) imageTable.getUnsortedValueAt(row, 0);
    // get the number of instances we want to start
    final int memory = MyUtil.getNumber("Memory (in MB) to assign to this VM ", "Memory", 512);
    if(memory<=0){
      return;
    }
    (new ResThread(){
          public void run(){
            try{
              String uuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
              vmMgr.launchVM(imageName, memory, uuid);
            }
            catch(Exception e){
              e.printStackTrace();
              this.setException(e);
              this.requestStop();
            }
          }
        }
    ).start();
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
      ids[i] = (String) instanceTable.getUnsortedValueAt(rows[i], 1);
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
    for(int i=0; i<ids.length; ++i){
      Debug.debug("Terminating "+ids[i], 2);
      vmMgr.terminateInstance(ids[i]);
    }
  }

  protected String getCredentials(){
    int row = instanceTable.getSelectedRow();
    String id = (String) instanceTable.getUnsortedValueAt(row, 1);
    VirtualMachine vm = vmMgr.getVM(id);
    String creds = "User: "+vm.getUserName()+"\nPassword: "+vm.getPassword();
    return creds;
  }

  protected void runShell(){
    if(runningShell){
      return;
    }
    runningShell = true;
    StringBuffer stdout = new StringBuffer();
    StringBuffer stderr = new StringBuffer();
    String [] fullCommand = null;
    try{
      int row = instanceTable.getSelectedRow();
      String id = (String) instanceTable.getUnsortedValueAt(row, 1);
      VirtualMachine vm = vmMgr.getVM(id);
      String dns = (String) instanceTable.getUnsortedValueAt(row, 3);
      fullCommand = new String [sshCommand.length+6];
      for(int i=0; i<sshCommand.length; ++i){
        fullCommand[i] = sshCommand[i];
      }
      fullCommand[fullCommand.length-6] = "/usr/bin/ssh";
      fullCommand[fullCommand.length-5] = "-l";
      fullCommand[fullCommand.length-4] = vm.getUserName();
      fullCommand[fullCommand.length-3] = "-p";
      fullCommand[fullCommand.length-2] = Integer.toString(vm.getSshPort());
      fullCommand[fullCommand.length-1] = dns;
      Debug.debug("Connecting to "+dns, 1);
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

