package gridpilot;

import java.awt.Color;

/**
 * Shows some charts about the jobs/transfers status.
 */
public class TransferStatisticsPanel extends StatisticsPanel{
  
  private TransferStatusUpdateControl statusUpdateControl = null;

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
    statusNames = TransferStatusUpdateControl.ftStatusNames;
    colors = statusColors;
  }

  private static final long serialVersionUID = 1L;

  public void update(){
    
    TransferStatusUpdateControl statusUpdateControl =
      GridPilot.getClassMgr().getTransferStatusUpdateControl();

    if(style<painters.size()){
      statusNames = TransferStatusUpdateControl.ftStatusNames;
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
      statusNames = TransferStatusUpdateControl.statusNames;
      // Set the number of jobs in each state to 0
      values = new int[statusNames.length];
      Debug.debug("resetting number of jobs for each status, "+
          Util.arrayToString(statusNames), 3);
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
