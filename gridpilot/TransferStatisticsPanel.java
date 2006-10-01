package gridpilot;

/**
 * Shows some charts about the jobs/transfers status.
 */
public class TransferStatisticsPanel extends StatisticsPanel{
  
  private TransferStatusUpdateControl statusUpdateControl = null;

  public TransferStatisticsPanel(String title) {
    super(title);
  }

  private static final long serialVersionUID = 1L;

  public void update(){
    
    statusUpdateControl = GridPilot.getClassMgr().getGlobalFrame(
      ).monitoringPanel.transferMonitor.statusUpdateControl;

    if(style<painters.size()){
      statusNames = TransferInfo.getFTStatusNames();
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
      statusNames = DBPluginMgr.getStatusNames();
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
