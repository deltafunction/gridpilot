package gridpilot;

import gridpilot.TaskMgr;
import java.util.*;
import javax.swing.*;
import java.awt.*;
import gridpilot.SyntaxException;
import gridpilot.ArithmeticExpression;
import gridpilot.dbplugins.proddb.ProdDBXmlNode;

/**
 * Creates the partititons with datas given by JobDefCreationPanel.
 * This object removes all known constants from the attributes, and evaluates them.
 *
 */
public class JobDefCreator {

  private TaskMgr taskMgr;
  private DBPluginMgr dbPluginMgr;
  private int from;
  private int to;
  private boolean showResults;
  private Vector constants;
  private String [] cstAttr;

  private String [] resCstAttr;

  private String [] cstAttrNames;
  
  private boolean editing;
  
  private String dbName;

  private static Vector vJobDef = new Vector();
  private static Vector vCstAttr = new Vector();

  private static JProgressBar pb = new JProgressBar();

  private static Object semaphoreDBCreation = new Object();

  private Object[] showResultsOptions = {"OK", "OK for all", "Skip", "Skip all"};

  public JobDefCreator(String _dbName,
                       TaskMgr _taskMgr,
                       int _from,
                       int _to,
                       boolean _showResults,
                       Vector _constants,
                       String [] _cstAttr,
                       String [] _cstAttrNames,
                       boolean _editing
                       ){

    taskMgr = _taskMgr;
    dbName = _dbName;
    
    if(taskMgr!=null){
      dbPluginMgr = taskMgr.getDBPluginMgr();
    }
    else{
      dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    }
    
    from = _from;
    to = _to;
    showResults = _showResults;
    constants = _constants;
    cstAttr = _cstAttr;

    cstAttrNames =  _cstAttrNames;
    
    editing = _editing;
    
    Debug.debug("Are we editing? "+editing,3);

    resCstAttr = new String[cstAttr.length];

    createJobDefs();
  }

  private void createJobDefs(){
    Debug.debug("createJobDefs", 1);
    
    try{
      removeConstants();
    }catch(SyntaxException se){

      String msg = "Syntax error  : \n" + se.getMessage() + "\nCannot create jobDefinition";
      String title = "Syntax error";
      MessagePane.showMessage(msg, title);
      return;
    }


    int firstJobDef = from;
    int lastJobDef  = to;

    if(firstJobDef > lastJobDef){
      GridPilot.getClassMgr().getLogFile().addMessage(
          "first value (from) cannot be greater then last value (To) : " +
          firstJobDef + ">" + lastJobDef);
      return;
    }


    boolean skipAll = false;
    boolean skip = false;

    boolean showThis;

    int jobDefCount = 0;

    for(int currentJobDef = firstJobDef ; currentJobDef <= lastJobDef && !skipAll; ++currentJobDef){

      showThis = showResults;

      try{
        evaluate(currentJobDef);
      }catch(Exception ex){
        String msg = "";
        String title = "";
        if(ex instanceof ArithmeticException){
          msg = "Arithmetic error in jobDefinition " + currentJobDef+" : \n" +
                ex.getMessage() +
                "\n\nDo you want to continue jobDefinition creation ?";
          title = "Arithmetic error";
        }
        else{
          if(ex instanceof SyntaxException){
            msg = "Syntax error in jobDefinition " + currentJobDef+" : \n" +
                  ex.getMessage() +
                  "\n\nDo you want to continue jobDefinition creation ?";
            title = "Syntax error";
          }
          else{ // ??? should not happen
            msg = "Unexpected " + ex.getClass().getName() + " : " + ex.getMessage();
            title = "Unexpected exception";
            GridPilot.getClassMgr().getLogFile().addMessage("JobDef creation", ex);
          }
        }

        int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), msg, title,
            JOptionPane.YES_NO_OPTION);
        if(choice == JOptionPane.NO_OPTION){
          showThis = false;
          skip = true;
          skipAll = true;
        }
        else{
          showThis = false;
          skip = true;
        }
      }

      /*for(int i =0; i<cstAttr.length; ++i){
        if(cstAttrNames[i].equals("jobXML")){
          if(resCstAttr[i]==null || resCstAttr[i].equals("null") || resCstAttr[i].equals("")){
            resCstAttr[i] = "";
          }
          else{
            if(!editing && resCstAttr[i].indexOf("</jobDef>")<0 && resCstAttr[i].indexOf("<jobDef>")<0){
              resCstAttr[i] = "<jobDef>"+resCstAttr[i]+"</jobDef>";
            }
          }
          break;
        }
      }*/

      
      if(showThis){
        int choice = showResult(currentJobDef, resCstAttr,
            currentJobDef<lastJobDef);

        switch(choice){
          case 0  : skip = false;  break;  // OK
          case 1  : skip = false;  showResults = false ; break;   //OK for all
          case 2  : skip = true;   break; // Skip
          case 3  : skip = true;   skipAll = true; // Skip all
          default : skip = true;   skipAll = true; // other (closing the dialog). Same action than "Skip all"
        }
      }
      if(!skip){
        Debug.debug("creating jobDefinition # " + currentJobDef, 2);
        vJobDef.add(new Integer(currentJobDef));
        vCstAttr.add(resCstAttr.clone());
 
        ++jobDefCount;

      }
    }


    if(!skipAll){

      Debug.debug("going to set pb", 2);
      //pb.setMaximum(pb.getMaximum() + jobDefCount);
      pb.setMaximum(jobDefCount);

      Debug.debug("going to call createDBJobDefs", 2);
      createDBJobDefs();

    }
  }

  private void createDBJobDefs(){
    synchronized(semaphoreDBCreation){
      while(!vJobDef.isEmpty()){
        int part = ((Integer) vJobDef.remove(0)).intValue();
        resCstAttr = (String [] ) vCstAttr.remove(0);

        Debug.debug("Creating jobDefinition # " + part + " ...", 2);
        pb.setValue(pb.getValue()+1);
        Debug.debug(this.getClass().getName() + " is calling DB", 2);

        if(editing){
          String jobDefIdentifier = GridPilot.getClassMgr().getConfigFile().getValue(
              taskMgr.getDBPluginMgr().getDBName(),
          "job definition table identifier");
          int id = -1;
          for(int i=0; i<cstAttrNames.length; ++i){
            if(cstAttrNames[i].toString().equalsIgnoreCase(
                jobDefIdentifier)){
              id = Integer.parseInt(resCstAttr[i]);
              break;
            }
          }
          Debug.debug("Updating...", 3);
          if(!taskMgr.getDBPluginMgr().updateJobDefinition(id, cstAttrNames, resCstAttr)){
            if(JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), "JobDef " + part +
                " cannot be updated", "", JOptionPane.OK_CANCEL_OPTION) == JOptionPane.CANCEL_OPTION)
            //cancel creation
            vJobDef.removeAllElements();
          }
        }
        else{
          if(taskMgr!=null){
            if(!taskMgr.createJobDef(cstAttrNames, resCstAttr)){
              if(JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), "JobDef " + part +
                  " cannot be created", "", JOptionPane.OK_CANCEL_OPTION) == JOptionPane.CANCEL_OPTION)
              //cancel creation
              vJobDef.removeAllElements();
            }
          }
          else{
            try{
              dbPluginMgr.createJobDef(cstAttrNames, resCstAttr);
            }
            catch(Exception e){
              Debug.debug(e.getMessage(), 1);
              if(JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), "JobDef " + part +
                  " cannot be created", "", JOptionPane.OK_CANCEL_OPTION) == JOptionPane.CANCEL_OPTION){
                //cancel creation
                vJobDef.removeAllElements();
              }
            }
          }
        }
      }
      //taskMgr.getVectorTableModel().fireTableDataChanged();
    }
  }




  private String evaluate(String s, int var) throws ArithmeticException, SyntaxException {
    // expression format : ${<arithmExpr>[:length]}
    // arithmExpr : operator priority : (*,/,%), (+,-), left associative

    String result = "";
    int previousPos = 0;
    int currentPos;
    boolean end=false;
    do{
      currentPos = s.indexOf("${", previousPos);
      if(currentPos == -1){
        currentPos = s.length();
        end = true;
      }
      result += s.substring(previousPos, currentPos);
      if(end)
        break;
      previousPos = currentPos + 2;
      int colon = s.indexOf(':', previousPos);
      if(colon == -1)
        colon = s.length();
      int brace = s.indexOf('}', previousPos);
      if(brace == -1){
        throw new SyntaxException(s + " : misformed expression - } missing");
      }

      if (colon<brace){
        // with colon
        currentPos = colon;
        int value = evaluateExpression(s.substring(previousPos, currentPos), var);
        previousPos = currentPos +1;
        currentPos = s.indexOf('}', previousPos);
        if(currentPos == -1){
          throw new SyntaxException(s + " : misformed expression - } missing");
        }
        int l;
        try{
          l = new Integer(s.substring(previousPos, currentPos)).intValue();
        }catch(NumberFormatException nfe){
          throw new SyntaxException(s + " : " + s.substring(previousPos, currentPos) +
                                    " is not an integer");
        }

        result += format(value, l);
      }
      else{
        currentPos = brace;
        int value = evaluateExpression(s.substring(previousPos, currentPos), var);
        result += value;
      }
      previousPos = currentPos + 1;
    }while(true);

    return result.trim();
  }

  private String format(int val, int length){
    Debug.debug("format : "+val+", "+length, 1);
    StringBuffer res = new StringBuffer();
    res.setLength(length);
    int currentVal = val;
    for(int i=length-1; i>=0; --i){
      res.setCharAt(i, (char)('0'+(currentVal%10)));
      currentVal /=10;
    }
    return res.toString();
  }

  /**
   * Gets the value of the constant c, or null if this constant is not defined.
   */
  private String getConstantValue(char c){
    int index = (int) (c - 'A');
    if(index < 0 || index >=constants.size())
      return null;
    else
      return ((JTextField ) (constants.get(index))).getText();
  }

  /**
   * Returns the arithmetic value of the expression <code>s</code>, when
   * the variable 'i' has the value <code>var</code>
   * @throws ArithmeticException if the expression s in not syntaxically correct.
   */
  private int evaluateExpression(String s, int var) throws ArithmeticException{
    Debug.debug("evaluate : "+s,1);
    ArithmeticExpression ae = new ArithmeticExpression(s, 'i', var);
    int res=ae.getValue();
    return res;
  }


  private void evaluate(int currentJobDef)throws ArithmeticException, SyntaxException{

    for(int i=0; i< resCstAttr.length; ++i)
      resCstAttr[i] = evaluate(cstAttr[i], currentJobDef);

  }

  /**
   * Removes constants in every String
   * @throws SyntaxException if a constant is not defined
   */
  private void removeConstants() throws SyntaxException{
    for(int i=0; i< resCstAttr.length; ++i)
      cstAttr[i] = removeConstants(cstAttr[i]);
  }

  /**
   * Creates a String which contains the String s, in which each constant ($A, $B, etc)
   * has been replaced by the value of this constant.
   * @throws SyntaxException if a constant has been found but is not defined
   */
  private String removeConstants(String s) throws SyntaxException{
    int begin = 0;
    int end;
    String res = "";

    while(true){
      end = s.indexOf('$', begin);
      if(end == -1)
        end = s.length();

      /**
       * s.substring(begin, end) = the substring from the last constant (or the begin)
       * to the last character before the next constant (or the last character of s)
       */
      res += s.substring(begin, end);
      if(end == s.length())
        break;
      if(end +1 < s.length() && s.charAt(end + 1) != '{'){
        // a constants has been found : s.charAt(end) = '$', and s.charAt(end+1) =
        // the name of the constant
        String cstValue = getConstantValue(s.charAt(end+1));
        if(cstValue == null) // this constant is not defined
          throw new SyntaxException(s + " : Constant " + s.charAt(end + 1) + " unknown");
        res += cstValue;
        begin = end + 2; // skip $<constant name>
      }
      else{
        // an arithmetic expression has been found : adds the '$', and skips it
        res += '$';
        begin = end+1;
      }
    }

//    Debug.debug(s + " -> " + res,this, 3);
    return res;

  }
  private int showResult(int currentJobDef, String [] resCstAttr,
                         boolean moreThanOne){

    JPanel pResult = new JPanel(new GridBagLayout());
    int row = 0;
    ProdDBXmlNode xmlNode = null;

    for(int i =0; i<cstAttr.length; ++i, ++row){
      pResult.add(new JLabel(cstAttrNames[i] + " : "), new GridBagConstraints(0, row, 1, 1, 0.0, 0.0
      ,GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 25, 5, 5), 0, 0));
      JComponent jval = null;
      JTextArea textArea = null;
      if(cstAttrNames[i].equalsIgnoreCase("jobXML") ||
          cstAttrNames[i].equalsIgnoreCase("jobPars") ||
         cstAttrNames[i].equalsIgnoreCase("jobOutputs") ||
         cstAttrNames[i].equalsIgnoreCase("jobLogs") ||
         cstAttrNames[i].equalsIgnoreCase("jobInputs")){
        try{
          // Just give it a try with the proddb schema...
          if(resCstAttr[i]!=null && !resCstAttr[i].equals("null") &&
              !resCstAttr[i].equals("")){
            xmlNode = ProdDBXmlNode.parseString(resCstAttr[i], 0);
            xmlNode.fillText();
          }

          textArea = new JTextArea(xmlNode.parsedText);

        }
        catch(Exception e){
          Debug.debug("Could not parse XML. "+e.getMessage(), 2);
          e.printStackTrace();
          // If it doesn't work, show raw XML
          textArea = new JTextArea(resCstAttr[i]);
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
    

    JDialog dialog = op.createDialog(JOptionPane.getRootFrame(), "JobDef # "+currentJobDef);
    
    dialog.requestFocusInWindow();
    
    if(!moreThanOne){
      ((JComponent) op.getComponent(1)).getComponent(1).setEnabled(false);
      ((JComponent) op.getComponent(1)).getComponent(3).setEnabled(false);
    }
    
    dialog.setResizable(true);
    dialog.show();
    dialog.dispose();


    Object selectedValue = op.getValue();

    if (selectedValue == null)
      return JOptionPane.CLOSED_OPTION;
    for (int i = 0; i < showResultsOptions.length; ++i)
      if (showResultsOptions[i] == selectedValue)
        return i;
    return JOptionPane.CLOSED_OPTION;



    /*

    int choice = JOptionPane.showOptionDialog(JOptionPane.getRootFrame(),
        sp, "JobDef # " +
        currentJobDef, JOptionPane.YES_NO_CANCEL_OPTION,JOptionPane.QUESTION_MESSAGE,
        null, showResultsOptions, showResultsOptions[0]);

    return choice;*/
  }
}
