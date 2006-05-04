package gridpilot;

import java.awt.Color;
import java.util.StringTokenizer;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.UIManager;
import javax.swing.plaf.UIResource;
import javax.swing.text.JTextComponent;

/**
 * @author Frederik.Orellana@cern.ch
 *
 */

public class Util{

  public static String [] split(String s){
    StringTokenizer tok = new StringTokenizer(s);
    int len = tok.countTokens();
    String [] res = new String[len];
    for (int i=0; i<len ; i++){
      res[i] = tok.nextToken();
      /* remove leading whitespace */
      res[i] = res[i].replaceAll("^\\s+", "");
      /* remove trailing whitespace */
      res[i] = res[i].replaceAll("\\s+$", "");
    }
    return res ;
  }
  
  public static String [] split(String s, String delim){
    StringTokenizer tok = new StringTokenizer(s, delim);
    int len = tok.countTokens();
    String [] res = new String[len];
    for (int i=0 ; i<len ; i++){
      res[i] = tok.nextToken();
      /* remove leading whitespace */
      res[i] = res[i].replaceAll("^\\s+", "");
      /* remove trailing whitespace */
      res[i] = res[i].replaceAll("\\s+$", "");
    }
    return res ;
  }

  /**
   * Converts an array of object in a String representing this array.
   * Example : {"a", new Integer(32), "ABCE", null} is converted in "a 32 ABCE null"
   */
  public static String arrayToString(Object [] values){
    String res = "";
    if(values==null){
      return "(null)";
    }
    for(int i=0; i<values.length ; ++i){
      res += (values[i]==null ? "null" : values[i].toString());
      if(i<values.length-1){
        res += " ";
      }
    }
    return res;
  }

  /**
   * Converts an array of integers in a String representing this array.
   */
  public static String arrayToString(int [] values){
    String res = "";
    if(values==null){
      return "(null)";
    }
    for(int i=0; i<values.length ; ++i){
      res += values[i];
      if(i<values.length-1){
        res += " ";
      }
    }
    return res;
  }
  
  /**
   * Converts an array of object in a String representing this array,
   * using the delimiter string delim to separate the records.
   */
  public static String arrayToString(Object [] values, String delim){
    String res = "";
    if(values==null)
      return "(null)";
    for(int i=0; i<values.length ; ++i){
      res += (values[i]==null ? "" : values[i].toString());
      if(i<values.length-1){
        res += delim;
      }       
    }
    return res;
  }

  /**
   * Returns the text of a JComponent.
   */
  public static String getJTextOrEmptyString(JComponent comp){
    String text = "";
    if(comp.getClass().isInstance(new JTextArea())){
      text = ((JTextComponent) comp).getText();
    }
    else if(comp.getClass().isInstance(new JTextField())){
      text = ((JTextField) comp).getText();
    }
    else if(comp.getClass().isInstance(new JComboBox())){
      text = ((JComboBox) comp).getSelectedItem().toString();
    }
    else{
      Debug.debug("WARNING: component type "+comp.getClass()+
          " not known. Failed to set text "+text, 3);
    }
    return text;
  }
  
  /**
   * Sets the text of a JComponent.
   */
  public static String setJText(JComponent comp, String text){
    if(comp.getClass().isInstance(new JTextArea())){
      Debug.debug("Setting text "+((JTextArea) comp).getText()+"->"+text, 3);
      ((JTextArea) comp).setText(text);
    }
    else if(comp.getClass().isInstance(new JTextField())){
      Debug.debug("Setting text "+((JTextField) comp).getText()+"->"+text, 3);
      ((JTextField) comp).setText(text);
    }
    else if(comp.getClass().isInstance(new JComboBox())){
      ((JComboBox) comp).setSelectedItem(text);
    }
    else{
      Debug.debug("WARNING: component type "+comp.getClass()+
          " not known. Failed to set text "+text, 3);
    }
    return text;
  }
  
  /**
   * Enables or disables a JComponent.
   */
  public static void setJEditable(JComponent comp, boolean edi){
    if(comp.getClass().isInstance(new JTextArea())){
      ((JTextComponent) comp).setEditable(edi);
      //((JTextComponent) comp).setEnabled(edi);
    }
    else if(comp.getClass().isInstance(new JTextField())){
      ((JTextField) comp).setEditable(edi);
      //((JTextField) comp).setEnabled(edi);
    }
    else if(comp.getClass().isInstance(new JComboBox())){
      ((JComboBox) comp).setEditable(edi);
      //((JComboBox) comp).setEnabled(edi);
    }
    if(!edi){
      comp.setBackground(Color.lightGray);
    }
    else{
      comp.setBackground(Color.white);
    }
  }

  public static String encode(String s){
    // Put quotes around and escape all enclosed quotes/dollars.
    // Tried to get this working using replaceAll without luck.
    String tmp = "";
    for(int i=0; i<s.length(); i++){
      if(s.charAt(i)=='"'){
        tmp = tmp + '\\';
        tmp = tmp + '"';
      }
      else if(s.charAt(i)=='$'){
        tmp = tmp + '\\';
        tmp = tmp + '$';
      }
      else {
        tmp = tmp + s.charAt(i);
      }
    }
    return "\"" + tmp + "\"";
  }
  
  public static JTextComponent createTextArea(){
    JTextArea ta = new JTextArea();
    ta.setBorder(new JTextField().getBorder());
    ta.setWrapStyleWord(true);
    ta.setLineWrap(true);
    return ta;
  }
  
  public static String dbEncode(String str){
    if(str==null || str.length()==0){
      return str;
    }
    String retStr = str;
    retStr = retStr.replaceAll("\\$", "\\\\\\$");
    retStr = str.replace('\n',' ');
    retStr = str.replace('\r',' ');
    retStr = retStr.replaceAll("\n","\\\\n");
    Debug.debug("Encoded: "+str+"->"+retStr, 3);
    return str;
  }
  
  public static String [] dbEncode(String [] strArray){
    String [] retStrArray = new String [strArray.length];
    for(int i=0; i<strArray.length; ++i){
      retStrArray[i] = dbEncode(strArray[i]);
    }
    return retStrArray;
  }
  
  public static void setBackgroundColor(JComponent c){
    Color background = c.getBackground();
    if (background instanceof UIResource){
      c.setBackground(UIManager.getColor("TextField.inactiveBackground"));
    }
  }
 
}
