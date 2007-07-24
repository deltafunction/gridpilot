package gridpilot.csplugins.ec2;

import java.io.IOException;
import java.util.Vector;

import gridpilot.ComputingSystem;
import gridpilot.JobInfo;

public class EC2ComputingSystem implements ComputingSystem {

  public void cleanupRuntimeEnvironments(String csName) {
    // TODO Auto-generated method stub

  }

  public void clearOutputMapping(JobInfo job) {
    // TODO Auto-generated method stub

  }

  public void exit() {
    // TODO Auto-generated method stub

  }

  public String[] getCurrentOutputs(JobInfo job) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public String getError(String csName) {
    // TODO Auto-generated method stub
    return null;
  }

  public String getFullStatus(JobInfo job) {
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getScripts(JobInfo job) {
    // TODO Auto-generated method stub
    return null;
  }

  public String getUserInfo(String csName) {
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
