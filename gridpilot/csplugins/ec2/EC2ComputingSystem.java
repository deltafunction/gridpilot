package gridpilot.csplugins.ec2;

import java.util.Vector;

import gridpilot.ComputingSystem;
import gridpilot.GridPilot;
import gridpilot.JobInfo;
import gridpilot.Util;
import gridpilot.csplugins.fork.ForkPoolComputingSystem;

public class EC2ComputingSystem extends ForkPoolComputingSystem implements ComputingSystem {

  private EC2Mgr ec2mgr = null;

  public EC2ComputingSystem(String _csName) {
    super(_csName);
    
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
    ec2mgr = new EC2Mgr(accessKey, secretKey, sshAccessSubnet, this.getUserInfo(csName),
        runDir);
 
    System.out.println("Adding EC2 monitor");
    EC2MonitoringPanel panel = new EC2MonitoringPanel(ec2mgr);
    GridPilot.extraMonitorTabs.add(panel);
  }

  public void cleanupRuntimeEnvironments(String csName) {
    // TODO Auto-generated method stub

  }

  public void exit() {
    ec2mgr.exit();
    // TODO: cleanup runtimeenvironments
  }

  public String getFullStatus(JobInfo job) {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean killJobs(Vector jobs) {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean postProcess(JobInfo job) {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean preProcess(JobInfo job) {
    // TODO Auto-generated method stub
    return false;
  }

  public void setupRuntimeEnvironments(String csName) {
    // TODO Auto-generated method stub

  }

  public boolean submit(JobInfo job) {
    // TODO Auto-generated method stub
    return false;
  }

  public void updateStatus(Vector jobs) {
    // TODO Auto-generated method stub

  }

}
