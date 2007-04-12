package gridpilot.csplugins.gpss;

import java.io.IOException;
import java.util.Vector;

import gridpilot.ComputingSystem;
import gridpilot.JobInfo;

public class GPSSComputingSystem implements ComputingSystem{

  public GPSSComputingSystem(){
  }

  public boolean submit(JobInfo job){
    // TODO Auto-generated method stub
    return false;
  }

  public void updateStatus(Vector jobs){
    // TODO Auto-generated method stub

  }

  public boolean killJobs(Vector jobs){
    // TODO Auto-generated method stub
    return false;
  }

  public void clearOutputMapping(JobInfo job){
    // TODO Auto-generated method stub

  }

  public void exit(){
    // TODO Auto-generated method stub

  }

  public String getFullStatus(JobInfo job){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getCurrentOutputs(JobInfo job,boolean resyncFirst)
      throws IOException{
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getScripts(JobInfo job){
    // TODO Auto-generated method stub
    return null;
  }

  public String getUserInfo(String csName){
    // TODO Auto-generated method stub
    return null;
  }

  public boolean postProcess(JobInfo job){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean preProcess(JobInfo job){
    // TODO Auto-generated method stub
    return false;
  }

  public String getError(String csName){
    // TODO Auto-generated method stub
    return null;
  }

  public void setupRuntimeEnvironments(String csName){
    // TODO Auto-generated method stub

  }

}
