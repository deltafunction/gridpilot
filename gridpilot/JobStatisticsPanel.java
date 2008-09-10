package gridpilot;

import gridfactory.common.Debug;

import java.util.*;

/**
 * Shows some charts about the jobs/transfers status.
 */
public class JobStatisticsPanel extends StatisticsPanel{

  public JobStatisticsPanel(String title) {
    super(title);
    colors = DBPluginMgr.getStatusColors();
    statusNames = DBPluginMgr.getDBStatusNames();
  }

  private static final long serialVersionUID = 1L;

  public void update(){
    Vector jobMgrs = GridPilot.getClassMgr().getJobMgrs();
    //Debug.debug("jobMgrs: "+jobMgrs.size(), 3);
    if(style<painters.size()){
      statusNames = DBPluginMgr.getDBStatusNames();
      // Set the number of jobs in each state to 0
      values = new int[statusNames.length];
      for(int i=0; i<values.length; ++i){
        values[i]= 0;
      }
      int [] theseValues = new int[values.length];
      //Debug.debug("JobMgrs: "+jobMgrs.size(), 3);
      for(int i=0; i<jobMgrs.size(); ++i){
        theseValues = ((JobMgr) jobMgrs.get(i)).getJobsByDBStatus();
        for(int j=0; j<values.length; ++j){
          //Debug.debug("Increasing value "+j+" from "+values[j]+" with "+theseValues[j], 3);
          values[j] += theseValues[j];
        }
        //fix
        break;
      }
    }
    else{
      statusNames = DBPluginMgr.getStatusNames();
      // Set the number of jobs in each state to 0
      values = new int[statusNames.length];
      Debug.debug("resetting number of jobs for each status, "+
          MyUtil.arrayToString(statusNames), 3);
      for(int i=0; i<values.length; ++i){
        values[i]= 0;
      }
      int [] theseValues = new int [values.length];
      for(int i=0; i<jobMgrs.size(); ++i){
        theseValues = ((JobMgr) jobMgrs.get(i)).getJobsByStatus();
        for(int j=0; j<values.length; ++j){
          values[j] += theseValues[j];
        }
        // fix
        break;
      }
    }
    repaint();
  }
}
