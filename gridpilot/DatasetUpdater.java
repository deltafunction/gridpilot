package gridpilot;

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
  private String [] cstAttrNames;
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
    statusBar = GridPilot.getClassMgr().getStatusBar();
    dbPluginMgr = _dbPluginMgr;

    updateDataset();
  }

  private void updateDataset(){

    boolean skip = false;
    boolean showThis;

    showThis = showResults;

    if(showThis){
      int choice = showResult();
      switch(choice){
        case 0  : skip = false;  break;  // OK
        case 1  : skip = true; // Skip
        default : skip = true;
      }
    }
    if(!skip){
      updateDBDataset();
    }
  }

  private void updateDBDataset(){
    if(!dbPluginMgr.updateDataset(identifier, cstAttrNames, cstAttr)){
      JOptionPane.showMessageDialog(JOptionPane.getRootFrame(),
          "ERROR: dataset cannot be updated.\n"+
          dbPluginMgr.getError(),
          "", JOptionPane.PLAIN_MESSAGE);
      statusBar.setLabel("Updated NOT succeeded.");
    }
    else{
      statusBar.setLabel("Updated succeeded.");
    }
  }


  private int showResult(){

    JPanel pResult = new JPanel(new GridBagLayout());
    int row = 0;

    for(int i =0; i<cstAttr.length; ++i, ++row){
      if(cstAttrNames[i].equals("init")){
        JTextArea ta = new JTextArea(cstAttr[i]);
        ta.setWrapStyleWord(true);
        ta.setLineWrap(true);
        ta.setEditable(false);
        pResult.add(ta, new GridBagConstraints(1, row, 3, 1, 1.0, 0.0
            ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
        }
      else{
        pResult.add(new JLabel(cstAttrNames[i] + " : "), new GridBagConstraints(0, row, 1, 1, 0.0, 0.0
            ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));
            pResult.add(new JLabel(cstAttr[i]), new GridBagConstraints(1, row, 3, 1, 1.0, 0.0
                ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
      }
    }


    JScrollPane sp = new JScrollPane(pResult);
    int height = (int)pResult.getPreferredSize().getHeight() +
    (int)sp.getHorizontalScrollBar().getPreferredSize().getHeight() + 5;
    int width = (int)pResult.getPreferredSize().getWidth() +
    (int)sp.getVerticalScrollBar().getPreferredSize().getWidth() + 5;
    Dimension screenSize = new Dimension(Toolkit.getDefaultToolkit().getScreenSize());
    if (height>screenSize.height){
      height = 700;
      Debug.debug("Screen height exceeded, setting "+height, 2);
    }
    if (width>screenSize.width){
      width = 550;
      Debug.debug("Screen width exceeded, setting "+width, 2);
    }
    Debug.debug("Setting size "+width+":"+height, 3);
    sp.setPreferredSize(new Dimension(width, height));

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
