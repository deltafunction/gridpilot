package gridpilot;

import java.util.*;

/**
 * Shows some charts about the jobs/transfers status.
 */
public class JobStatisticsPanel extends StatisticsPanel{

  public JobStatisticsPanel(String title) {
    super(title);
  }

  private static final long serialVersionUID = 1L;

  public void update(){
    Vector datasetMgrs = GridPilot.getClassMgr().getDatasetMgrs();
    //Debug.debug("datasetMgrs: "+datasetMgrs.size(), 3);
    if(style<painters.size()){
      statusNames = DBPluginMgr.getDBStatusNames();
      // Set the number of jobs in each state to 0
      values = new int[statusNames.length];
      for(int i=0; i<values.length; ++i){
        values[i]= 0;
      }
      int [] theseValues = new int[values.length];
      //Debug.debug("DatasetMgrs: "+datasetMgrs.size(), 3);
      for(int i=0; i<datasetMgrs.size(); ++i){
        theseValues = ((DatasetMgr) datasetMgrs.get(i)).getJobsByDBStatus();
        for(int j=0; j<values.length; ++j){
          //Debug.debug("Increasing value "+j+" from "+values[j]+" with "+theseValues[j], 3);
          values[j] += theseValues[j];
        }
      }
    }
    else{
      statusNames = DBPluginMgr.getStatusNames();
      // Set the number of jobs in each state to 0
      values = new int[statusNames.length];
      Debug.debug("resetting number of jobs for each status, "+
          Util.arrayToString(statusNames), 3);
      for(int i=0; i<values.length; ++i){
        values[i]= 0;
      }
      int [] theseValues = new int [values.length];
      for(int i=0; i<datasetMgrs.size(); ++i){
        theseValues = ((DatasetMgr) datasetMgrs.get(i)).getJobsByStatus();
        for(int j=0; j<values.length; ++j){
          values[j] += theseValues[j];
        }
      }
    }
    repaint();
  }
}
