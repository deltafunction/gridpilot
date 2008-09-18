package gridpilot;

import gridfactory.common.Debug;

import java.awt.Color;

/**
 * Shows some charts about the jobs/transfers status.
 */
public class TransferStatisticsPanel extends StatisticsPanel{
  
  private MyTransferStatusUpdateControl statusUpdateControl = null;

  /**
   * Colors corresponding to getStatusNames for statistics panel.
   */
  private static Color [] statusColors = new Color [] {
                         Color.blue,
                         Color.orange,
                         Color.green,
                         Color.red,
                         Color.magenta
                         };

  public TransferStatisticsPanel(String title){
    super(title);
    statusNames = MyTransferStatusUpdateControl.ftStatusNames;
    colors = statusColors;
  }

  private static final long serialVersionUID = 1L;

  public void update(){
    
    MyTransferStatusUpdateControl statusUpdateControl =
      GridPilot.getClassMgr().getTransferStatusUpdateControl();

    if(style<painters.size()){
      statusNames = MyTransferStatusUpdateControl.ftStatusNames;
      // Set the number of jobs in each state to 0
      values = new int[statusNames.length];
      for(int i=0; i<values.length; ++i){
        values[i]= 0;
      }
      int [] theseValues = new int[values.length];
      theseValues = statusUpdateControl.getTransfersByFTStatus();
      for(int j=0; j<values.length; ++j){
        //Debug.debug("Increasing value "+j+" from "+values[j]+" with "+theseValues[j], 3);
        values[j] += theseValues[j];
      }
    }
    else{
      statusNames = MyTransferStatusUpdateControl.statusNames;
      // Set the number of jobs in each state to 0
      values = new int[statusNames.length];
      Debug.debug("resetting number of jobs for each status, "+
          MyUtil.arrayToString(statusNames), 3);
      for(int i=0; i<values.length; ++i){
        values[i]= 0;
      }
      int [] theseValues = new int [values.length];
      theseValues = statusUpdateControl.getTransfersByStatus();
      for(int j=0; j<values.length; ++j){
        values[j] += theseValues[j];
      }
    }
    repaint();
  }
}
