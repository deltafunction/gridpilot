package gridpilot.csplugins.ec2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.swing.JOptionPane;

import com.xerox.amazonws.ec2.EC2Exception;
import com.xerox.amazonws.ec2.ReservationDescription;
import com.xerox.amazonws.ec2.ReservationDescription.Instance;

import gridpilot.ComputingSystem;
import gridpilot.ConfirmBox;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.JobInfo;
import gridpilot.SecureShellMgr;
import gridpilot.ShellMgr;
import gridpilot.Util;
import gridpilot.csplugins.fork.ForkPoolComputingSystem;

public class EC2ComputingSystem extends ForkPoolComputingSystem implements ComputingSystem {

  private EC2Mgr ec2mgr = null;
  private String amiID = null;
  // max time to wait for booting a virtual machine when submitting a job
  private static long MAX_BOOT_WAIT = 5*60*1000;
  // the user to use for running jobs on the virtual machines
  private static String USER = "root";

  public EC2ComputingSystem(String _csName) throws Exception {
    super(_csName);
    
    amiID = GridPilot.getClassMgr().getConfigFile().getValue("EC2",
      "AMI id");
    String accessKey = GridPilot.getClassMgr().getConfigFile().getValue("EC2",
       "AWS access key id");
    String secretKey = GridPilot.getClassMgr().getConfigFile().getValue("EC2",
       "AWS secret access key");
    String sshAccessSubnet = GridPilot.getClassMgr().getConfigFile().getValue("EC2",
       "SSH access subnet");
    if(sshAccessSubnet==null || sshAccessSubnet.equals("")){
      // Default to global access
      sshAccessSubnet = "0.0.0.0/0";
    }
    String runDir = Util.clearTildeLocally(Util.clearFile(workingDir));
    Debug.debug("Using workingDir "+workingDir, 2);
    ec2mgr = new EC2Mgr(accessKey, secretKey, sshAccessSubnet, this.getUserInfo(csName),
        runDir);
 
    System.out.println("Adding EC2 monitor");
    EC2MonitoringPanel panel = new EC2MonitoringPanel(ec2mgr);
    // This causes the panel to be added to the monitoring window as a tab,
    // right after the transfer monitoring tab and before the log tab.
    GridPilot.extraMonitorTabs.add(panel);
    
    int mm = 0;
    try{
      String maxMachines = GridPilot.getClassMgr().getConfigFile().getValue("EC2",
         "Maximum machines");
      mm = Integer.parseInt(maxMachines);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    String jobsPerMachine = "1";
    try{
      jobsPerMachine = GridPilot.getClassMgr().getConfigFile().getValue("EC2",
         "Jobs per machine");
    }
    catch(Exception e){
      e.printStackTrace();
    }
    // Fill hosts with nulls and assign values as jobs are submitted.
    hosts = new String[mm];
    // Fill maxJobs with a constant number
    maxJobs = new String[mm];
    Arrays.fill(maxJobs, jobsPerMachine);
  }

  /**
   * Override in order to postpone setting up shellMgrs till submission time (preProcess).
   */
  protected void setupRemoteShellMgrs(){
    remoteShellMgrs = new HashMap();
  }
  
  // Halt machines with no running jobs
  private void haltNonBusy(){
    List reservations;
    try{
      reservations = ec2mgr.listReservations();
    }
    catch (EC2Exception e1){
      e1.printStackTrace();
      return;
    }
    ReservationDescription res = null;
    Instance inst = null;
    ArrayList passiveInstances = new ArrayList();
    ArrayList activeInstances = new ArrayList();
    if(reservations!=null){
      for(Iterator it=reservations.iterator(); it.hasNext();){
        res = (ReservationDescription) it.next();
        for(Iterator itt=res.getInstances().iterator(); it.hasNext();){
          inst = (Instance) itt.next();
          if(remoteShellMgrs.containsKey(inst.getDnsName())){
            activeInstances.add(inst.getInstanceId());
          }
          else{
            passiveInstances.add(inst.getInstanceId());
          }
        }
      }
      if(!passiveInstances.isEmpty() || !activeInstances.isEmpty()){
        String msg = "You have running EC2 AMI instance(s).\n" +
           (passiveInstances.isEmpty()?"":"The following are not executing any GridPilot jobs: "+
           Util.arrayToString(passiveInstances.toArray())+".\n") +
           (activeInstances.isEmpty()?"":"The following ARE executing GridPilot jobs: "+
           Util.arrayToString(activeInstances.toArray())+".\n" )+
           "What do you want to do?";
        ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
        int choice = -1;
        try{
          choice = confirmBox.getConfirm("Confirm terminate instances",
              msg, new Object[] {"Terminate all", "Terminate passive", "Do nothing"});
          if(choice==2){
            return;
          }
        }
        catch(Exception e){
          e.printStackTrace();
          return;
        }
        if(choice==0 || choice==1){
          int i = 0;
          String [] termArr = new String [passiveInstances.size()];
          for(Iterator it=passiveInstances.iterator(); it.hasNext();){
            termArr[i] = (String) it.next();
            ++i;
          }
          try{
            ec2mgr.terminateInstances(termArr);
          }
          catch (EC2Exception e) {
            e.printStackTrace();
          }
        }
        if(choice==1){
          int i = 0;
          String [] termArr = new String [activeInstances.size()];
          for(Iterator it=activeInstances.iterator(); it.hasNext();){
            termArr[i] = (String) it.next();
            ++i;
          }
          try{
            ec2mgr.terminateInstances(termArr);
          }
          catch (EC2Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  public void exit() {
    super.exit();
    haltNonBusy();
    ec2mgr.exit();
  }

  /**
   * Finds a ShellMgr for the host/user/password of the job.
   * If the shellMgr is dead it is attempted to start a new one.
   * If no shellMgr exists for this host, a new one is created.
   * @param job
   * @return a ShellMgr
   */
  protected ShellMgr getShellMgr(String host){
    ShellMgr mgr = null;
    if(host!=null &&
        !host.startsWith("localhost") && !host.equals("127.0.0.1")){
      if(!remoteShellMgrs.containsKey(host)){
        // This means it has just been booted
        ShellMgr newShellMgr = new SecureShellMgr(host, USER, ec2mgr.keyFile, "");
        remoteShellMgrs.put(host, newShellMgr);
        setupRuntimeEnvironmentsSSH(newShellMgr);
      }
      SecureShellMgr sMgr = (SecureShellMgr) remoteShellMgrs.get(host);
      if(!sMgr.isConnected()){
        sMgr.reconnect();
      }
      mgr = sMgr;
    }
    else if(host!=null &&
        (host.startsWith("localhost") || host.equals("127.0.0.1"))){
      mgr = (ShellMgr) remoteShellMgrs.get(host);
    }
    return mgr;
  }

  /**
   * The brokering algorithm. As simple as possible: FIFO.
   * Slight extension as compared to ForkPoolComputingSystem:
   * start shell and get host if none is running on the slot.
   */
  protected String selectHost(JobInfo job){
    ShellMgr mgr = null;
    String host = null;
    int maxR = 1;
    // First try to use a running instance
    for(int i=0; i<hosts.length; ++i){
      try{
        if(hosts[i]==null){
          continue;
        }
        host = hosts[i];
        maxR = 1;
        mgr = getShellMgr(host);
        if(maxJobs!=null && maxJobs.length>i && maxJobs[i]!=null){
          maxR = Integer.parseInt(maxJobs[i]);
        }
        if(mgr.getJobsNumber()<maxR){
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
          Instance inst = ((Instance) desc.getInstances().get(0));
          // Wait for the machine to boot
          long startMillis = Util.getDateInMilliSeconds(null);
          long nowMillis = Util.getDateInMilliSeconds(null);
          List reservationList = null;
          List instanceList = null;
          Instance instance = null;
          while(!inst.isRunning()){
            nowMillis = Util.getDateInMilliSeconds(null);
            if(nowMillis-startMillis>MAX_BOOT_WAIT){
              logFile.addMessage("ERROR: timeout waiting for image "+inst.getImageId()+
                   "to boot for job "+job.getJobDefId());
              return null;
            }
            reservationList = ec2mgr.listReservations();
            instanceList = null;
            instance = null;
            ReservationDescription reservation = null;
            Debug.debug("Finding reservations... ", 1);
            for(Iterator it=reservationList.iterator(); it.hasNext();){
              reservation = (ReservationDescription) it.next();
              instanceList = ec2mgr.listInstances(reservation);
              // "Reservation ID", "Owner", "Instance ID", "AMI", "State", "Public DNS", "Key"
              for(Iterator itt=instanceList.iterator(); itt.hasNext();){
                instance = (Instance) itt.next();
                if(reservation.getReservationId().equalsIgnoreCase(reservation.getReservationId())){
                  inst = instance;
                }
              }
            }
            Debug.debug("Waiting for EC2 machine to boot... "+inst.getState()+":"+inst.getStateCode(), 1);
            if(inst.isRunning() || inst.getState().equalsIgnoreCase("running")){
              break;
            }
            Thread.sleep(5000);
          }
          hosts[i] = inst.getDnsName();
          Debug.debug("Returning host "+inst.getState()+hosts[i], 1);
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
