package gridpilot;

import javax.swing.*;
import java.awt.*;
import gridpilot.dbplugins.proddb.ProdDBXmlNode;

/**
 * Creates the transformation records with data given by TransformationCreationPanel.
 * This object removes all known constants from the attributes, and evaluates them.
 */
public class TransformationCreator{

  private boolean showResults;
  private String [] cstAttr;

  private String [] resCstAttr;

  private String [] cstAttrNames;
  
  private boolean editing;

  private static Object semaphoreDBCreation = new Object();

  private Object[] showResultsOptions = {"OK", "Skip"};

  private DBPluginMgr dbPluginMgr = null;

  public TransformationCreator(
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

    resCstAttr = new String[cstAttr.length];

    createTransformationRecord();
  }

  private void createTransformationRecord(){
    int choice = showResult(resCstAttr);

    switch(choice){
      case 0  : break;  // OK
      case 2  : return; // Skip
      default : return;
    }

    if(editing){
      String tranformationIdentifier =
        dbPluginMgr.getIdentifier(dbPluginMgr.getDBName() ,"transformation");
      int id = -1;
      for(int i=0; i<cstAttrNames.length; ++i){
        Debug.debug("Checking name "+tranformationIdentifier+":"+cstAttrNames[i].toString(), 3);
        if(cstAttrNames[i].toString().equalsIgnoreCase(
            tranformationIdentifier)){
          id = Integer.parseInt(resCstAttr[i]);
          break;
        }
      }
      Debug.debug("Updating...", 3);
      if(!dbPluginMgr.updateTransformation(id, cstAttrNames, resCstAttr)){
        JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), "transformation" +
            " cannot be updated", "", JOptionPane.OK_OPTION);
      }
    }
    else{
      if(!dbPluginMgr.createTransformation(resCstAttr)){
        JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), "transformation" +
            " cannot be created", "", JOptionPane.OK_OPTION);
      }
    }     
  }

  private int showResult(String [] resCstAttr){

    JPanel pResult = new JPanel(new GridBagLayout());
    int row = 0;
    ProdDBXmlNode xmlNode = null;

    for(int i =0; i<cstAttr.length; ++i, ++row){
      pResult.add(new JLabel(cstAttrNames[i] + " : "), new GridBagConstraints(0, row, 1, 1, 0.0, 0.0
      ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));
      JComponent jval;
      JTextArea textArea = new JTextArea("");
      if(cstAttrNames[i].equalsIgnoreCase("formalPars")){
        try{
          if(resCstAttr[i]!=null && !resCstAttr[i].equals("null") &&
              !resCstAttr[i].equals("")){
            xmlNode = ProdDBXmlNode.parseString(resCstAttr[i], 0);
            xmlNode.fillText();
          }
        }
        catch(Exception e){
          textArea = new JTextArea(resCstAttr[i]);
        }

        if(xmlNode!=null && xmlNode.parsedText!=null){
          textArea = new JTextArea(xmlNode.parsedText);
        }
        textArea.setLineWrap(true);
        textArea.setWrapStyleWord(true);
        textArea.setEditable(false);
        jval = textArea;
      }
      else{
        jval = new JLabel(resCstAttr[i]);
      }
      pResult.add(jval, new GridBagConstraints(1, row, 3, 1, 1.0, 0.0
          ,GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5), 0, 0));
    }

    JScrollPane sp = new JScrollPane(pResult);
    int size1 = (int)pResult.getPreferredSize().getHeight() +
    	(int)sp.getHorizontalScrollBar().getPreferredSize().getHeight() + 5;
    Dimension screenSize = new Dimension(Toolkit.getDefaultToolkit().getScreenSize());
    if (size1 > screenSize.height) size1 = 500;
      Debug.debug(Integer.toString(size1), 2);
    sp.setPreferredSize(new Dimension(500,size1));

    JOptionPane op = new JOptionPane(sp,
                                     JOptionPane.QUESTION_MESSAGE,
                                     JOptionPane.YES_NO_CANCEL_OPTION,
                                     null,
                                     showResultsOptions,
                                     showResultsOptions[0]);
    

    JDialog dialog = op.createDialog(JOptionPane.getRootFrame(), "transformation");    
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
