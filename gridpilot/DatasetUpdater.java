package gridpilot;

import java.util.*;
import javax.swing.*;
import java.awt.*;

/**
 * Creates the dataset with data given by DatasetCreationPanel.
 */
public class DatasetUpdater{

  private int identifier;
  private boolean showResults;
  private String [] cstAttr;
  private StatusBar statusBar;
  private String [] resCstAttr;
  private String [] cstAttrNames;
  private static Vector vCstAttr = new Vector();
  private static Vector vDataset = new Vector();
  private static JProgressBar pb = new JProgressBar();
  private static Object semaphoreDBCreation = new Object();
  private DBPluginMgr dbPluginMgr;
  private Object[] showResultsOptions = {"OK",  "Cancel"};

  public DatasetUpdater(  DBPluginMgr _dbPluginMgr,
                          boolean _showResults,
                          String [] _cstAttr,
                          String [] _cstAttrNames,
                          int _identifier
                          ){

    identifier = _identifier;
    showResults = _showResults;
    cstAttr = _cstAttr;

    cstAttrNames =  _cstAttrNames;

 	  resCstAttr = _cstAttr;

    statusBar = GridPilot.getClassMgr().getStatusBar();
    dbPluginMgr = _dbPluginMgr;

	  updateDataset();
  }

  private void updateDataset(){

    boolean skipAll = false;
    boolean skip = false;
    boolean showThis;

    showThis = showResults;

    if(showThis){
      int choice = showResult(resCstAttr);
      switch(choice){
        case 0  : skip = false;  break;  // OK
        case 1  : skip = true;   skipAll = true; // Skip all
        default : skip = true;   skipAll = true; // other (closing the dialog). Same action as "Cancel"
      }
    }
    if(!skip){
      vDataset.add(new Integer(0));
      vCstAttr.add(resCstAttr.clone());
    }
    if(!skipAll){
      updateDBDataset();
    }
  }

  private void updateDBDataset(){
    synchronized(semaphoreDBCreation){
      while(!vDataset.isEmpty()){
		    int part = ((Integer) vDataset.remove(0)).intValue();
        resCstAttr = (String [] ) vCstAttr.remove(0);

        statusBar.setLabel("Updating dataset  ...");
        pb.setValue(pb.getValue()+1);
        
        if(!dbPluginMgr.updateDataset(identifier, cstAttrNames, resCstAttr)){
            JOptionPane.showMessageDialog(JOptionPane.getRootFrame(), "Datatset cannot be updated");
            vDataset.removeAllElements();
          statusBar.setLabel("Updated NOT succeeded.");
        }
        else{
          statusBar.setLabel("Updated succeeded.");
        }
      }
    }
  }


  private int showResult(String [] resCstAttr){

    JPanel pResult = new JPanel(new GridBagLayout());
    int row = 0;

    for(int i =0; i<cstAttr.length; ++i, ++row){
      if(cstAttrNames[i].equals("init")){
        JTextArea ta = new JTextArea(resCstAttr[i]);
        ta.setWrapStyleWord(true);
        ta.setLineWrap(true);
        ta.setEditable(false);
        pResult.add(ta, new GridBagConstraints(1, row, 3, 1, 1.0, 0.0
            ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
        }
      else{
        pResult.add(new JLabel(cstAttrNames[i] + " : "), new GridBagConstraints(0, row, 1, 1, 0.0, 0.0
            ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));
            pResult.add(new JLabel(resCstAttr[i]), new GridBagConstraints(1, row, 3, 1, 1.0, 0.0
                ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
      }
    }


    JScrollPane sp = new JScrollPane(pResult);
    sp.setPreferredSize(new Dimension(500,
                                      (int)pResult.getPreferredSize().getHeight() +
                                      (int)sp.getHorizontalScrollBar().getPreferredSize().getHeight() + 5));

    JOptionPane op = new JOptionPane(sp,
                                     JOptionPane.QUESTION_MESSAGE,
                                     JOptionPane.YES_NO_CANCEL_OPTION,
                                     null,
                                     showResultsOptions,
                                     showResultsOptions[0]);

    op.setPreferredSize(new Dimension(550, 500));
    op.setMinimumSize(new Dimension(550, 500));
    JDialog dialog = op.createDialog(JOptionPane.getRootFrame(),
        "Dataset");
    dialog.setResizable(true);
    dialog.setVisible(true);
    dialog.dispose();

    Object selectedValue = op.getValue();

    if (selectedValue==null){
      return JOptionPane.CLOSED_OPTION;
    }
    for (int i=0; i<showResultsOptions.length; ++i){
      if (showResultsOptions[i]==selectedValue){
        return i;
      }
    }
    return JOptionPane.CLOSED_OPTION;
  }
}
