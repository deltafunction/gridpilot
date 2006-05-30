package gridpilot;

import java.awt.Color;
import java.awt.Cursor;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.File;
import java.util.StringTokenizer;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.UIManager;
import javax.swing.event.HyperlinkEvent;
import javax.swing.event.HyperlinkListener;
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
  
  public static String webEncode(String [] strs) {
    String tmp = "";
    String alltmp = "";
    for (int i=0 ; i<strs.length ; i++) {
      // encode % into %25
      // encode spaces into %20
      tmp = strs[i].replaceAll("%","%25");
      tmp = tmp.replaceAll(" ","%20");
      alltmp += tmp + " ";
    }
    return alltmp ;
  }
  
  public static String [] decode(String s) {
    //split on spaces and replace "\0" by " "
    StringTokenizer tok = new StringTokenizer(s);
    int len = tok.countTokens();
    String [] res = new String[len];
    for (int i = 0 ; i < len ; i++) {
      res[i] = tok.nextToken().replaceAll("\0"," ");
      res[i] = res[i].replaceAll("%20"," ");
      res[i] = res[i].replaceAll("%25","%");
    }
    return res ;
  }

 public static void setBackgroundColor(JComponent c){
    Color background = c.getBackground();
    if (background instanceof UIResource){
      c.setBackground(UIManager.getColor("TextField.inactiveBackground"));
    }
  }
 
  public static JEditorPane createCheckPanel(
      final Frame frame, 
      final String name, final JTextComponent jt,
      final DBPluginMgr dbPluginMgr){
    //final Frame frame = (Frame) SwingUtilities.getWindowAncestor(getRootPane());
    String markup = "<b>"+name+" : </b><br>"+
      "<a href=\"http://check/\">check</a>";
    JEditorPane checkPanel = new JEditorPane("text/html", markup);
    checkPanel.setEditable(false);
    checkPanel.setOpaque(false);
    checkPanel.addHyperlinkListener(
      new HyperlinkListener(){
      public void hyperlinkUpdate(HyperlinkEvent e){
        if(e.getEventType()==HyperlinkEvent.EventType.ACTIVATED){
          Debug.debug("URL: "+e.getURL().toExternalForm(), 3);
          if(e.getURL().toExternalForm().equals("http://check/")){
            String httpScript = jt.getText();
            String url = null;
            try{
              if(httpScript.startsWith("/")){
                url = (new File(httpScript)).toURL().toExternalForm();
              }
              else if(httpScript.startsWith("file://")){
                url = (new File(httpScript.substring(6))).toURL().toExternalForm();
              }
              else if(httpScript.startsWith("file://")){
                url = (new File(httpScript.substring(5))).toURL().toExternalForm();
              }
              else{
                url = httpScript;
              }
            }
            catch(Exception ee){
              Debug.debug("Could not open URL "+httpScript+". "+ee.getMessage(), 1);
              ee.printStackTrace();
              GridPilot.getClassMgr().getStatusBar().setLabel("Could not open URL "+httpScript);
              return;
            }
            Debug.debug("URL: "+url, 3);
            frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            final String finUrl = url;
            final String finBaseUrl = "";//url;
            MyThread t = new MyThread(){
              public void run(){
                WebBox wb = null;
                try{
                  wb = new WebBox(//GridPilot.getClassMgr().getGlobalFrame(),
                                  frame,
                                  "Choose file",
                                  finUrl,
                                  finBaseUrl,
                                  true,
                                  true,
                                  false);
                }
                catch(Exception eee){
                  Debug.debug("Could not open URL "+finBaseUrl+". "+eee.getMessage(), 1);
                  GridPilot.getClassMgr().getStatusBar().setLabel("Could not open URL "+finBaseUrl+". "+eee.getMessage());
                  ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()/*,"",""*/); 
                  try{
                    confirmBox.getConfirm("URL could not be opened",
                                         "The URL "+finBaseUrl+" could not be opened. \n"+eee.getMessage(),
                                      new Object[] {"OK"});
                  }
                  catch(Exception eeee){
                    Debug.debug("Could not get confirmation, "+eeee.getMessage(), 1);
                  }
                }
                if(wb!=null && wb.lastURL!=null &&
                    wb.lastURL.startsWith(finBaseUrl)){
                  // Set the text: the URL browsed to with case URL removed
                  jt.setText(wb.lastURL.substring(
                      finBaseUrl.length()));
                  GridPilot.getClassMgr().getStatusBar().setLabel("");
                }
                else{
                  // Don't do anything if we cannot get a URL
                  Debug.debug("ERROR: Could not open URL "+finBaseUrl, 1);
                }
                frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
                GridPilot.getClassMgr().getStatusBar().setLabel("");
              }
            };
            t.start();
          }
        }
      }
    });
    return checkPanel;
  }
  
  /**
   * Loads class.
   * @throws Throwable if an exception or an error occurs during loading
   */
  public static Object loadClass(String dbClass, Class [] argTypes,
     Object [] args) throws Throwable{
    Debug.debug("Loading plugin: "+" : "+dbClass, 2);
    // Arguments and class name for <DatabaseName>Database
    boolean loadfailed = false;
    Object ret = null;
    Debug.debug("argument types: "+Util.arrayToString(argTypes), 3);
    Debug.debug("arguments: "+Util.arrayToString(args), 3);
    try{
      //Class newClass = this.getClass().getClassLoader().loadClass(dbClass);
      Class newClass = (new MyClassLoader()).loadClass(dbClass);
      ret = (newClass.getConstructor(argTypes).newInstance(args));
      Debug.debug("plugin " + "(" + dbClass + ") loaded, "+ret.getClass(), 2);
    }
    catch(Exception e){
      e.printStackTrace();
      loadfailed = true;
      //do nothing, will try with MyClassLoader.
    }
    if(loadfailed){
      try{
        // loading of this plug-in
       MyClassLoader mcl = new MyClassLoader();
       ret = (mcl.findClass(dbClass).getConstructor(argTypes).newInstance(args)); 
       Debug.debug("plugin " + "(" + dbClass + ") loaded", 2);
      }
      catch(IllegalArgumentException iae){
        GridPilot.getClassMgr().getLogFile().addMessage("Cannot load class " + dbClass + ".\nThe plugin constructor " +
                          "must have one parameter (String)", iae);
        throw iae;
      }
      catch(Exception e){
        GridPilot.getClassMgr().getLogFile().addMessage("Cannot load class " + dbClass, e);
        throw e;
      }
    }
    return ret;
  }

  public static String getFileName(String str){
    return getFileName("Enter file name", str);
  }
  
  public static String getFileName(String message, String str){

    JPanel panel = new JPanel(new GridBagLayout());
    JTextPane tp = new JTextPane();
    tp.setText("");
    tp.setEditable(false);
    tp.setOpaque(false);
    tp.setBorder(null);

    JTextField tf = new JTextField(str, 24);

    panel.add(tp, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
        GridBagConstraints.CENTER,
        GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));
    panel.add(new JLabel(message),
        new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));
    panel.add(tf, new GridBagConstraints(0, 2, 1, 1, 1.0, 1.0,
        GridBagConstraints.CENTER,
        GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));

    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), panel,
        "Enter file name", JOptionPane.OK_CANCEL_OPTION);

    if(choice!=JOptionPane.OK_OPTION){
      return null;
    }
    else{
      return tf.getText();
    }
    
  }
}
