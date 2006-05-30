package gridpilot;

import javax.swing.*;
import java.awt.*;

/**
 * Creates the runtime environment records with data given by RuntimeCreationPanel.
 * This object removes all known constants from the attributes, and evaluates them.
 */
public class RuntimeCreator{

  private boolean showResults;
  private String [] cstAttr;
  private String [] cstAttrNames;
  private boolean editing;
  private Object[] showResultsOptions = {"OK", "Skip"};
  private DBPluginMgr dbPluginMgr = null;

  public RuntimeCreator(
                          DBPluginMgr _dbPluginMgr,
                          boolean _showResults,
                          String [] _cstAttr,
                          String [] _cstAttrNames,
                          boolean _editing
                          ){

    dbPluginMgr = _dbPluginMgr;
    showResults = _showResults;
    cstAttr = _cstAttr;
    cstAttrNames =  _cstAttrNames;
    editing = _editing;

   Debug.debug("Are we editing? "+editing,3);
   createRuntimeEnvironmentRecord();
  }

  private void createRuntimeEnvironmentRecord(){
    int choice = 0;
    if(showResults){
      choice = showResult();
    }
    switch(choice){
      case 0  : break;  // OK
      case 1  : return; // Skip
      default : return;
    }
    if(editing){
      String tranformationIdentifier =
        dbPluginMgr.getIdentifierField("runtimeEnvironment");
      int id = -1;
      for(int i=0; i<cstAttrNames.length; ++i){
        Debug.debug("Checking name "+tranformationIdentifier+":"+cstAttrNames[i].toString(), 3);
        if(cstAttrNames[i].toString().equalsIgnoreCase(
            tranformationIdentifier)){
          id = Integer.parseInt(cstAttr[i]);
          break;
        }
      }
      Debug.debug("Updating...", 3);
      if(!dbPluginMgr.updateRuntimeEnvironment(id, cstAttrNames, cstAttr)){
        JOptionPane.showMessageDialog(JOptionPane.getRootFrame(),
            "Runtime environment cannot be updated.\n"+
          dbPluginMgr.getError(), "", JOptionPane.PLAIN_MESSAGE);
      }
    }
    else{
      if(!dbPluginMgr.createRuntimeEnvironment(cstAttr)){
        JOptionPane.showMessageDialog(JOptionPane.getRootFrame(),
            "Runtime environment cannot be created.\n"+
          dbPluginMgr.getError(), "", JOptionPane.PLAIN_MESSAGE);
      }
    }     
  }

  private int showResult(){

    JPanel pResult = new JPanel(new GridBagLayout());
    int row = 0;
    JComponent jval;
    for(int i =0; i<cstAttr.length; ++i, ++row){
      if(cstAttrNames[i].equalsIgnoreCase("initLines")){
        jval = new JTextArea(cstAttr[i].toString());
        ((JTextArea) jval).setLineWrap(true);
        ((JTextArea) jval).setWrapStyleWord(true);
        ((JTextArea) jval).setEditable(false);
        Util.setBackgroundColor(jval);
      }
      else{
        jval = new JLabel(cstAttr[i].toString());
      }
      pResult.add(new JLabel(cstAttrNames[i] + " : "),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0 ,
              GridBagConstraints.CENTER, GridBagConstraints.BOTH,
              new Insets(5, 25, 5, 5), 0, 0));
      pResult.add(jval, new GridBagConstraints(1, row, 3, 1, 1.0, 0.0
          ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
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
    

    JDialog dialog = op.createDialog(JOptionPane.getRootFrame(), "runtimeEnvironment");    
    dialog.requestFocusInWindow();    
    dialog.setResizable(true);
    dialog.setVisible(true);
    dialog.dispose();

    Object selectedValue = op.getValue();

    if(selectedValue==null){
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
