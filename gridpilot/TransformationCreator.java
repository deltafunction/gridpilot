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
  private String [] cstAttrNames;
  private boolean editing;
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
    createTransformationRecord();
  }

  private void createTransformationRecord(){
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
        dbPluginMgr.getIdentifierField(dbPluginMgr.getDBName() ,"transformation");
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
      if(!dbPluginMgr.updateTransformation(id, cstAttrNames, cstAttr)){
        JOptionPane.showMessageDialog(JOptionPane.getRootFrame(),
            "Transformation cannot be updated.\n"+
          dbPluginMgr.getError(), "", JOptionPane.PLAIN_MESSAGE);
      }
    }
    else{
      if(!dbPluginMgr.createTransformation(cstAttr)){
        JOptionPane.showMessageDialog(JOptionPane.getRootFrame(),
            "Transformation cannot be created.\n"+
          dbPluginMgr.getError(), "", JOptionPane.PLAIN_MESSAGE);
      }
    }     
  }

  private int showResult(){

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
          if(cstAttr[i]!=null && !cstAttr[i].equals("null") &&
              !cstAttr[i].equals("")){
            xmlNode = ProdDBXmlNode.parseString(cstAttr[i], 0);
            xmlNode.fillText();
          }
        }
        catch(Exception e){
          textArea = new JTextArea(cstAttr[i]);
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
        jval = new JLabel(cstAttr[i]);
      }
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
