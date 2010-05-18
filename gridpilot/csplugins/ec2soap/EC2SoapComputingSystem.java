package gridpilot.csplugins.ec2soap;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import javax.swing.JOptionPane;

import com.amazon.aes.webservices.client.ReservationDescription;
import com.amazon.aes.webservices.client.ReservationDescription.Instance;
import com.jcraft.jsch.JSchException;

import gridfactory.common.ConfirmBox;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.Shell;
import gridpilot.MyComputingSystem;
import gridpilot.GridPilot;
import gridpilot.MySecureShell;
import gridpilot.MyUtil;
import gridpilot.csplugins.ec2.EC2Mgr;
import gridpilot.csplugins.forkpool.ForkPoolComputingSystem;

public class EC2SoapComputingSystem extends ForkPoolComputingSystem implements MyComputingSystem {

  private EC2SoapMgr ec2mgr = null;
  private String amiID = null;
  // max time to wait for booting a virtual machine when submitting a job
  private static long MAX_BOOT_WAIT = 5*60*1000;
  // the user to use for running jobs on the virtual machines
  private static String USER = "root";
  private int maxMachines = 0;

  public EC2SoapComputingSystem(String _csName) throws Exception {
    super(_csName);
    
    amiID = GridPilot.getClassMgr().getConfigFile().getValue(csName,
      "Fallback AMI id");
    String ec2ServiceUrl = GridPilot.getClassMgr().getConfigFile().getValue(csName,
       "Service url");
    if(ec2ServiceUrl==null || ec2ServiceUrl.equals("")){
      ec2ServiceUrl = "https://ec2.amazonaws.com";
    }
    String sshAccessSubnet = GridPilot.getClassMgr().getConfigFile().getValue(csName,
       "SSH access subnet");
    if(sshAccessSubnet==null || sshAccessSubnet.equals("")){
      // Default to global access
      sshAccessSubnet = "0.0.0.0/0";
    }
    String runDir = MyUtil.clearTildeLocally(MyUtil.clearFile(workingDir));
    Debug.debug("Using workingDir "+workingDir, 2);
    // Save the unencrypted secret key to a temporary file
    Debug.debug("Using key in format "+GridPilot.getClassMgr().getSSL().getPrivateKeyFormat(), 2);
    File tmpKeyFile = File.createTempFile(MyUtil.getTmpFilePrefix(), ".pkcs8");
    GridPilot.getClassMgr().getSSL().exportPrivateKey(tmpKeyFile);
    tmpKeyFile.setWritable(false, false);
    tmpKeyFile.setReadable(false, false);
    tmpKeyFile.setReadable(true, true);
    // have the file deleted on exit
    GridPilot.addTmpFile(tmpKeyFile.getAbsolutePath(), tmpKeyFile);
    tmpKeyFile.deleteOnExit();
    ec2mgr = new EC2SoapMgr(ec2ServiceUrl,
        MyUtil.clearTildeLocally(MyUtil.clearFile(GridPilot.getClassMgr().getSSL().getCertFile())),
        MyUtil.clearTildeLocally(tmpKeyFile.getAbsolutePath()),
        sshAccessSubnet, getUserInfo(csName),
        runDir, transferControl);
 
    Debug.debug("Adding EC2 monitor", 2);
    EC2SoapMonitoringPanel panel = new EC2SoapMonitoringPanel(ec2mgr);
    // This causes the panel to be added to the monitoring window as a tab,
    // right after the transfer monitoring tab and before the log tab.
    GridPilot.EXTRA_MONITOR_TABS.add(panel);
        
    try{
      String mms = GridPilot.getClassMgr().getConfigFile().getValue(csName,
         "Max machines");
      maxMachines = Integer.parseInt(mms);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    String jobsPerMachine = "1";
    try{
      jobsPerMachine = GridPilot.getClassMgr().getConfigFile().getValue(csName,
         "Max running jobs per host");
    }
    catch(Exception e){
      e.printStackTrace();
    }
    // Fill hosts with nulls and assign values as jobs are submitted.
    hosts = new String[maxMachines];
    // Fill maxJobs with a constant number
    maxRunningJobs = new String[maxMachines];
    Arrays.fill(maxRunningJobs, jobsPerMachine);
    
    preprocessingHostJobs = new HashMap<String, HashSet<JobInfo>>();
    
    // Reuse running VMs
    discoverInstances();

  }

  /**
   * Override in order to postpone setting up shellMgrs till submission time (preProcess).
   */
  protected void setupRemoteShellMgrs(){
    remoteShellMgrs = new HashMap<String, Shell>();
  }
  
  /**
   * Discover running virtual machines and ask to have them
   * included in the pool.
   */
  private void discoverInstances(){
    List<ReservationDescription> reservations;
    try{
      reservations = ec2mgr.listReservations();
    }
    catch(Exception e1){
      e1.printStackTrace();
      return;
    }
    if(reservations==null){
      Debug.debug("No reservations found.", 2);
      return;
    }
    ReservationDescription res = null;
    Instance inst = null;
    ArrayList<Instance> instances = new ArrayList<Instance>();
    for(Iterator<ReservationDescription> it=reservations.iterator(); it.hasNext();){
      res = it.next();
      Debug.debug("checking reservation"+res.resId, 2);
      for(Iterator<Instance> itt=res.instances.iterator(); itt.hasNext();){
        inst = itt.next();
        if(inst.isShuttingDown() || inst.isTerminated()){
          continue;
        }
        Debug.debug("checking instance "+inst.dnsName, 2);
        // If we have no key for the host, we cannot use it.
        if(inst.keyName.equalsIgnoreCase(EC2Mgr.KEY_NAME)){
          instances.add(inst);
        }
      }
    }
    if(instances.isEmpty()){
      return;
    }
    String msg = "You have "+instances.size()+" running EC2 AMI instance(s).\n" +
       "Do you want to include it/them in the pool of compute hosts?";
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    if(GridPilot.SPLASH!=null){
      GridPilot.SPLASH.hide();
    }
    int choice = -1;
    try{
      choice = confirmBox.getConfirm("Confirm inclusion of hosts",
          msg, new Object[] {"Yes", "No"});
    }
    catch(Exception e){
      e.printStackTrace();
      return;
    }
    if(choice!=0){
      return;
    }
    try {
      ec2mgr.getKey();
    }
    catch(Exception e){
      e.printStackTrace();
      MyUtil.showError("Could not get SSH key.");
      return;
    }
    int i = 0;
    String hostName = null;
    for(Iterator<Instance> it=instances.iterator(); it.hasNext();){
      hostName = it.next().dnsName;
      remoteShellMgrs.put(hostName, null);
      preprocessingHostJobs.put(hostName, new HashSet<JobInfo>());
      if(i<maxMachines && hosts[i]==null){
        hosts[i] = hostName;
        ++i;
      }
    }
  }

  /**
   * Halt virtual machines with no running jobs -
   * ask for confirmation.
   * @throws Exception 
   */
  private void haltNonBusy() throws Exception{
    List<ReservationDescription> reservations;
    try{
      reservations = ec2mgr.listReservations();
    }
    catch(Exception e1){
      e1.printStackTrace();
      return;
    }
    if(reservations==null){
      Debug.debug("No reservations found.", 2);
      return;
    }
    ReservationDescription res = null;
    Instance inst = null;
    ArrayList<String> passiveInstances = new ArrayList<String>();
    ArrayList<String> activeInstances = new ArrayList<String>();
    for(Iterator<ReservationDescription> it=reservations.iterator(); it.hasNext();){
      res = it.next();
      Debug.debug("checking reservation"+res.resId, 2);
      for(Iterator<Instance> itt=res.instances.iterator(); itt.hasNext();){
        inst = itt.next();
        if(inst.isShuttingDown() || inst.isTerminated()){
          continue;
        }
        Debug.debug("checking instance "+inst.dnsName, 2);
        if(remoteShellMgrs.containsKey(inst.dnsName) &&
            remoteShellMgrs.get(inst.dnsName)!=null &&
            ((MySecureShell) remoteShellMgrs.get(inst.dnsName)).getJobsNumber()>0){
          activeInstances.add(inst.instanceId);
        }
        else{
          passiveInstances.add(inst.instanceId);
        }
      }
    }
    if(passiveInstances.isEmpty() && activeInstances.isEmpty()){
      return;
    }
    String msg = "You have running EC2 AMI instance(s).\n" +
    (passiveInstances.isEmpty()?"":"The following are not executing any GridPilot jobs:\n"+
    MyUtil.arrayToString(passiveInstances.toArray(), ",\n")+".\n") +
    (activeInstances.isEmpty()?"":"The following are executing GridPilot jobs:\n"+
    MyUtil.arrayToString(activeInstances.toArray(), ",\n")+".\n" )+
    "What do you want to do?";
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    int choice = -1;
    try{
      if(passiveInstances.isEmpty() || activeInstances.isEmpty()){
        choice = confirmBox.getConfirm("Confirm terminate instances",
            msg, new Object[] {"Do nothing", "Terminate"});
      }
      else{
        choice = confirmBox.getConfirm("Confirm terminate instances",
            msg, new Object[] {"Do nothing", "Terminate all", "Terminate passive"});
      }
      if(choice==0){
        return;
      }
    }
    catch(Exception e){
      e.printStackTrace();
      return;
    }
    if(choice==1 || choice==2){
      int i = 0;
      String [] termArr = new String [passiveInstances.size()];
      for(Iterator<String> it=passiveInstances.iterator(); it.hasNext();){
        termArr[i] = (String) it.next();
        ++i;
      }
      try{
        ec2mgr.terminateInstances(termArr);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    if(choice==1){
      int i = 0;
      String [] termArr = new String [activeInstances.size()];
      for(Iterator<String> it=activeInstances.iterator(); it.hasNext();){
        termArr[i] = (String) it.next();
        ++i;
      }
      try{
        ec2mgr.terminateInstances(termArr);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
  }

  public void exit() {
    try{
      super.exit();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    try{
      haltNonBusy();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    ec2mgr.exit();
  }

  /**
   * Finds a Shell for the host/user/password of the job.
   * If the shellMgr is dead it is attempted to start a new one.
   * If no shellMgr exists for this host, a new one is created.
   * @param job
   * @return a Shell
   * @throws JSchException 
   */
  protected Shell getShell(String host) throws JSchException{
    Shell mgr = null;
    /*
     * If there is no keyFile set, this is a VM reused from a previous GridPilot session.
     * If the secret key corresponding to the public key currently uploaded with EC2 is
     * available either on the hard disk or in the grid homedir, ec2mgr.getKey() will
     * set the keyFile.
     * If this fails, we cannot use this host and it is dropped from remoteShellMgrs and
     * hosts.
     */
    if(ec2mgr.getKeyFile()==null){
      try{
        ec2mgr.getKey();
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    if(ec2mgr.getKeyFile()==null){
      remoteShellMgrs.remove(host);
      for(int i=0; i<hosts.length; ++i){
        if(hosts[i].equals(host)){
          hosts[i] = null;
          break;
        }
      }
    }
    Debug.debug("host: "+host+"-->"+remoteShellMgrs.get(host)+"-->"+ec2mgr.getKeyFile(), 2);
    Debug.debug("remoteShellMgrs: "+MyUtil.arrayToString(remoteShellMgrs.keySet().toArray()), 2);
    if(host!=null && !host.equals("") &&
        !host.startsWith("localhost") && !host.equals("127.0.0.1")){
      if(!remoteShellMgrs.containsKey(host)){
        // This means the VM has just been booted
        Shell newShellMgr = new MySecureShell(host, USER, ec2mgr.getKeyFile(), "");
        remoteShellMgrs.put(host, newShellMgr);
        setupRuntimeEnvironmentsSSH(newShellMgr);
      }
      else if(remoteShellMgrs.get(host)==null){
        // This means the VM was running before starting
        // GridPilot and we need to reconnect. RTEs should already be setup.
        // -- well, we do it anyway, just in case
        Shell newShellMgr = new MySecureShell(host, USER, ec2mgr.getKeyFile(), "");
        Debug.debug("Added ShellMgr on already running host "+newShellMgr.getHostName(), 2);
        remoteShellMgrs.put(host, newShellMgr);
        setupRuntimeEnvironmentsSSH(newShellMgr);
      }
      MySecureShell sMgr = (MySecureShell) remoteShellMgrs.get(host);
      if(!sMgr.isConnected()){
        sMgr.reconnect();
      }
      mgr = sMgr;
    }
    else if(host!=null && !host.equals("") &&
        (host.startsWith("localhost") || host.equals("127.0.0.1"))){
      mgr = (Shell) remoteShellMgrs.get(host);
    }
    return mgr;
  }

  /**
   * The brokering algorithm. As simple as possible: FIFO.
   * Slight extension as compared to ForkPoolComputingSystem:
   * start shell and get host if none is running on the slot.
   */
  protected synchronized String selectHost(JobInfo job){
    Shell mgr = null;
    String host = null;
    int maxR = 1;
    int submitting = 0;
    // First try to use an already used instance
    for(int i=0; i<hosts.length; ++i){
      try{
        if(hosts[i]==null){
          continue;
        }
        host = hosts[i];
        maxR = 1;
        mgr = getShell(host);
        if(maxRunningJobs!=null && maxRunningJobs.length>i && maxRunningJobs[i]!=null){
          maxR = Integer.parseInt(maxRunningJobs[i]);
        }
        submitting = (host!=null&&preprocessingHostJobs.get(host)!=null?((HashSet<JobInfo>)preprocessingHostJobs.get(host)).size():0);
        if(mgr.getJobsNumber()+submitting<maxR){
          return host;
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    // then try to boot an instance
    for(int i=0; i<hosts.length; ++i){
      try{
        if(hosts[i]==null){
          ReservationDescription desc = ec2mgr.launchInstances(amiID, 1);
          Instance inst = ((Instance) desc.instances.get(0));
          // Wait for the machine to boot
          long startMillis = MyUtil.getDateInMilliSeconds(null);
          long nowMillis = MyUtil.getDateInMilliSeconds(null);
          List<ReservationDescription> reservationList = null;
          List<Instance> instanceList = null;
          Instance instance = null;
          while(!inst.isRunning()){
            nowMillis = MyUtil.getDateInMilliSeconds(null);
            if(nowMillis-startMillis>MAX_BOOT_WAIT){
              logFile.addMessage("ERROR: timeout waiting for image "+inst.imageId+
                   "to boot for job "+job.getIdentifier());
              return null;
            }
            reservationList = ec2mgr.listReservations();
            instanceList = null;
            instance = null;
            ReservationDescription reservation = null;
            Debug.debug("Finding reservations... ", 1);
            for(Iterator<ReservationDescription> it=reservationList.iterator(); it.hasNext();){
              reservation = it.next();
              instanceList = ec2mgr.listInstances(reservation);
              // "Reservation ID", "Owner", "Instance ID", "AMI", "State", "Public DNS", "Key"
              for(Iterator<Instance> itt=instanceList.iterator(); itt.hasNext();){
                instance = itt.next();
                if(reservation.resId.equalsIgnoreCase(reservation.resId)){
                  inst = instance;
                }
              }
            }
            Debug.debug("Waiting for EC2 machine to boot... "+inst.state+":"+inst.stateCode, 1);
            if(inst.isRunning() || inst.state.equalsIgnoreCase("running")){
              break;
            }
            Thread.sleep(5000);
          }
          hosts[i] = inst.dnsName;
          Debug.debug("Returning host "+hosts[i]+" "+inst.state, 1);
          preprocessingHostJobs.put(hosts[i], new HashSet<JobInfo>());
          return hosts[i];
        }
      }
      catch(Exception e){
        e.printStackTrace();
        return null;
      }
    }
    return null;
  }

  //public void cleanupRuntimeEnvironments(String csName) {
    // TODO
  //}
  
  //public void setupRuntimeEnvironments(String csName) {
    // TODO
  //}

}
