package gridpilot;

import java.awt.Color;
import java.util.StringTokenizer;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.text.JTextComponent;

/**
 * @author Frederik.Orellana@cern.ch
 *
 */

public class Util{

  public static String [] split(String s) {
    StringTokenizer tok = new StringTokenizer(s);
    int len = tok.countTokens();
    String [] res = new String[len];
    for (int i = 0 ; i < len ; i++) {
      res[i] = tok.nextToken();
    }
    return res ;
  }
  
  public static String [] split(String s, String delim) {
    StringTokenizer tok = new StringTokenizer(s, delim);
    int len = tok.countTokens();
    String [] res = new String[len];
    for (int i = 0 ; i < len ; i++) {
      res[i] = tok.nextToken();
    }
    return res ;
  }

  /**
   * Converts an array of object in a String representing this array.
   * Example : {"a", new Integer(32), "ABCE", null} is converted in "a 32 ABCE null"
   */
  public static String arrayToString(Object [] values){
    String res = "";
    if(values == null)
      return "(null)";
    for(int i=0; i< values.length ; ++i){
      res += (values[i] == null ? "null" : values[i].toString()) + " ";
    }
    return res;
  }

  /**
   * Returns the text of a JComponent.
   */
  public static String getJTextOrEmptyString(JComponent comp){
    String text = "";
    if(comp.getClass().isInstance(new JTextArea())){
      text =  ((JTextComponent) comp).getText();
    }
    else if(comp.getClass().isInstance(new JComboBox())){
      text =  ((JComboBox) comp).getSelectedItem().toString();
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
      ((JTextComponent) comp).setEnabled(edi);
    }
    else if(comp.getClass().isInstance(new JTextField())){
      ((JTextField) comp).setEditable(edi);
      ((JTextField) comp).setEnabled(edi);
    }
    else if(comp.getClass().isInstance(new JComboBox())){
      ((JComboBox) comp).setEditable(edi);
      ((JComboBox) comp).setEnabled(edi);
    }
    if(!edi){
      comp.setBackground(Color.lightGray);
    }
    else{
      comp.setBackground(Color.white);
    }
  }

  /**
   * Converts a local path (<code>file</code>) into a absolute path by prepending the "prefix" attribute
   * of the gridpilot section in config file. <br>
   * If the file name begins by '/' or the prefix is not defined, nothing is prepend, <br>
   * If prepend doesn't end by '/', a '/' is added between <code>file</code> and
   * <code>prepend</code>.
   */
  public static String getFullPath(String file){
    if(file.startsWith("/"))
      return file;

    if(GridPilot.prefix == null)
      return file;
    else
      return GridPilot.prefix + file;
  }

  /**
   * Converts a local path (<code>file</code>) into a URL by prepending the "url" attribute
   * of the gridpilot section in config file. <br>
   * If the file name begins by 'http://' or the prefix is not defined, nothing is prepend, <br>
   * If prepend doesn't end by '/', a '/' is added between <code>file</code> and
   * <code>prepend</code>.
   */
  public static String getURL(String file){
    if(file.startsWith("http://") || file.startsWith("https://"))
      return file;

    if(GridPilot.url == null)
      return file;
    else
      return GridPilot.url + file;
  }  
}
