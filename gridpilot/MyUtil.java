package gridpilot;

import gridfactory.common.ConfigFile;
import gridfactory.common.ConfirmBox;
import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.ResThread;
import gridfactory.common.Shell;
import gridfactory.common.StatusBar;
import gridfactory.common.TransferInfo;
import gridfactory.common.TransferStatusUpdateControl;
import gridfactory.common.Util;
import gridfactory.common.jobrun.RTEMgr;
import gridfactory.common.jobrun.RTECatalog.InstancePackage;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Vector;


import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JEditorPane;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.event.HyperlinkEvent;
import javax.swing.event.HyperlinkListener;
import javax.swing.plaf.UIResource;
import javax.swing.text.JTextComponent;

import org.dcache.srm.SRMException;
import org.globus.util.GlobusURL;

/**
 * @author Frederik.Orellana@cern.ch
 *
 */

public class MyUtil extends gridfactory.common.Util{

  public static final int OK_OPTION = 0;
  public static final int OK_SKIP_OPTION = 1;
  public static final int OK_ALL_SKIP_ALL_OPTION = 2;

  public static final String TMP_FILE_PREFIX = "gridpilot-";
  
  /** URL that will cause a blank browser window to be opened. */
  public static final String CHECK_URL = "http://check/";

  /**
   * Returns the text of a JComponent.
   */
  public static String getJTextOrEmptyString(JComponent comp){
    String text = "";
    if(comp.getClass().isInstance(new JTextArea()) ||
        JTextComponent.class.isAssignableFrom(comp.getClass()) ||
        comp.getClass().isInstance(createTextArea())){
      text = ((JTextComponent) comp).getText();
    }
    else if(comp.getClass().isInstance(new JTextField())){
      text = ((JTextField) comp).getText();
    }
    else if(comp.getClass().isInstance(new JComboBox()) ||
        comp.getClass().isInstance(new JExtendedComboBox())){
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
    if(comp.getClass().isInstance(new JTextField()) ||
        comp.getClass().isInstance(createTextArea())){
      Debug.debug("Setting text "+((JTextComponent) comp).getText()+"->"+text, 3);
      ((JTextComponent) comp).setText(text);
    }
    else if(comp.getClass().isInstance(new JTextField())){
      Debug.debug("Setting text "+((JTextField) comp).getText()+"->"+text, 3);
      ((JTextField) comp).setText(text);
    }
    else if(comp.getClass().isInstance(new JComboBox())){
      ((JComboBox) comp).setSelectedItem(text);
    }
    else{
      try{
        Debug.debug("Trying to set text "+((JTextComponent) comp).getText()+"->"+text, 3);
        ((JTextComponent) comp).setText(text);
      }
      catch(Exception e){
        Debug.debug("WARNING: component type "+comp.getClass().getName()+
            " not known. Failed to set text "+text, 3);
      }
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

  public static void setBackgroundColor(JComponent c){
    Color background = c.getBackground();
    if (background instanceof UIResource){
      c.setBackground(UIManager.getColor("TextField.inactiveBackground"));
    }
  }
 
  public static void launchCheckBrowser(final Frame frame, String url,
     final JTextComponent text, final boolean localFS, final boolean oneUrl,
     final boolean withNavigation, final boolean onlyDirs, boolean waitForThread){
    if(url.equals(CHECK_URL)){
      String httpScript = "";
      httpScript = text.getText();
      if(frame!=null){
        frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
      }
      final String finUrl = httpScript;
      ResThread rt = (new ResThread(){
        public void run(){
          String[] urls = null;
          if(oneUrl){
            urls = new String [] {finUrl};
          }
          else{
            try{
              urls = splitUrls(finUrl);
            }
            catch(Exception e){
              Debug.debug("Could not open URL(s) "+finUrl+". "+e.getMessage(), 1);
              e.printStackTrace();
              GridPilot.getClassMgr().getStatusBar().setLabel("Could not open URL(s) "+finUrl);
              return;
            }
          }
          if(urls.length==0){
            urls = new String [] {""};
          }
          boolean ok = true;
          for(int i=0; i<urls.length; ++i){
            urls[i] = handleUrl(urls, i, frame, onlyDirs, withNavigation, localFS);
            ok = ok && urls[i]!=null;
          }
          if(ok){
            setText(text, arrayToString(urls));
          }
          frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          //GridPilot.getClassMgr().getStatusBar().setLabel("");
        }
      });
      rt.start();
      if(waitForThread){
        myWaitForThread(rt, "File browser", 0, "launchCheckBrowser");
      }
    }
  }
 
 private static void setText(final JTextComponent comp, final String text){
   try{
     SwingUtilities.invokeAndWait(
       new Runnable(){
         public void run(){
           comp.setText(text);
         }
       }
     );
   }
   catch(Exception e){
     e.printStackTrace();
   }
 }

  private static String handleUrl(final String [] urls, int i, final Frame frame,
     final boolean onlyDirs, final boolean withNavigation, final boolean localFS){
    BrowserPanel wb = null;
    final String finBaseUrl = "";//url;
    String url = fixUrl(urls[i]);
    Debug.debug("URL: "+url, 3);
    try{
      if(SwingUtilities.isEventDispatchThread()){
        wb = launchBrowserPanel(frame, onlyDirs, url, finBaseUrl,
            withNavigation, localFS, urls);
      }
      else{
        final String tmpUrl = url;
        MyResThread rt = new MyResThread(){
          BrowserPanel wb = null;
          public void run(){
            try{
              wb = launchBrowserPanel(frame, onlyDirs, tmpUrl, finBaseUrl,
                  withNavigation, localFS, urls);
            }
            catch(Exception e){
             setException(e);
            }
          }
          public BrowserPanel getBrowserPanelRes(){
            return wb;
          }
        };
        SwingUtilities.invokeAndWait(rt);
        wb = rt.getBrowserPanelRes();
        if(rt.getException()!=null){
          throw rt.getException();
        }
      }
    }
    catch(Exception eee){
      url = null;
      handleCheckPanelException(eee, finBaseUrl);
    }
                    
    if(wb!=null && wb.getLastURL()!=null && wb.getLastURL().startsWith(finBaseUrl)){
      // Set the text: the URL browsed to with base URL removed
      //jt.setText(wb.lastURL.substring(finBaseUrl.length()));
      url = wb.getLastURL().substring(finBaseUrl.length());
      //GridPilot.getClassMgr().getStatusBar().setLabel("");
    }
    else{
      // Don't do anything if we cannot get a URL
      url = null;
      Debug.debug("ERROR: Could not open URL "+finBaseUrl, 1);
    }
    if(url!=null && !url.equals("") && isLocalFileName(url)){
      url = replaceWithTildeLocally(url);
    }
    return url;
  }
 
 private static BrowserPanel launchBrowserPanel(Frame frame, boolean onlyDirs,
     String url, String finBaseUrl, boolean withNavigation, boolean localFS,
     String [] urls) throws Exception {
   BrowserPanel wb = null;
   try{
     wb = new BrowserPanel(
         frame,
         onlyDirs?"Choose directory":"Choose file",
         //finUrl,
         url,
         finBaseUrl,
         true/*modal*/,
         !onlyDirs && withNavigation/*filter*/,
         withNavigation/*navigation*/,
         null,
         onlyDirs?"*/":null,
         localFS);
   }
   catch(Exception e){
     if(urls!=null && urls.length>1){
       throw e;
     }
   }
   if(wb==null){
     wb = new BrowserPanel(
         frame,
         onlyDirs?"Choose directory":"Choose file",
         //finUrl,
         "file:~/",
         finBaseUrl,
         true/*modal*/,
         !onlyDirs && withNavigation/*filter*/,
         withNavigation/*navigation*/,
         null,
         onlyDirs?"*/":null,
         localFS);
   }
   return wb;
}

private static String fixUrl(String _url){
   String url = _url;
   try{
     if(url.startsWith("/")){
       url = (new File(url)).toURI().toURL().toExternalForm();
     }
     else if(url.startsWith("~")){
       url = (new File(clearTildeLocally(url))).toURI().toURL().toExternalForm();
     }
     else if(url.startsWith("file://")){
       url = (new File(url.substring(6))).toURI().toURL().toExternalForm();
     }
     else if(url.startsWith("file://")){
       url = (new File(url.substring(5))).toURI().toURL().toExternalForm();
     }
   }
   catch(Exception ee){
     Debug.debug("Could not open URL "+url+". "+ee.getMessage(), 1);
     ee.printStackTrace();
     GridPilot.getClassMgr().getStatusBar().setLabel("Could not open URL "+url);
     url = null;
   }
   return url;
 }
 
 private static void handleCheckPanelException(Exception eee, String finBaseUrl){
   eee.printStackTrace();
   Debug.debug("Could not open URL "+finBaseUrl+". "+eee.getMessage(), 1);
   if(!GridPilot.IS_FIRST_RUN){
     GridPilot.getClassMgr().getStatusBar().setLabel("Could not open URL "+finBaseUrl+". "+eee.getMessage());
   }
   ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()/*,"",""*/); 
   try{
     confirmBox.getConfirm("URL could not be opened",
                          "The URL "+finBaseUrl+" could not be opened. \n"+eee.getMessage(),
                       new Object[] {mkOkObject(confirmBox.getOptionPane())});
   }
   catch(Exception eeee){
     Debug.debug("Could not get confirmation, "+eeee.getMessage(), 1);
   }
}
 
 /**
  * Create a JLabel with the text 'name' and a 'Browse' hyperlink
  * that fires up a browser for selecting a file (local or remote).
  * The JTextComponent 'jt' is filled with the selected file path.
  */
 public static JEditorPane createCheckPanel(
      final Frame frame, 
      final String name, final JTextComponent jt, final boolean oneUrl,
      final boolean withNavigation,
      final boolean onlyDirs,
      final boolean localFS){
    //final Frame frame = (Frame) SwingUtilities.getWindowAncestor(getRootPane());
    String markup = "<font size=-1 face=sans-serif><b>"+name+" : </b></font><br>"+
      "<a href=\""+CHECK_URL+"\">browse</a>";
    JEditorPane checkPanel = new JEditorPane("text/html", markup);
    checkPanel.setEditable(false);
    checkPanel.setOpaque(false);
    checkPanel.addHyperlinkListener(
      new HyperlinkListener(){
      public void hyperlinkUpdate(HyperlinkEvent e){
        if(e.getEventType()==HyperlinkEvent.EventType.ACTIVATED){
          launchCheckBrowser(frame, e.getURL().toExternalForm(), jt, localFS, oneUrl,
              withNavigation, onlyDirs, false);
        }
      }
    });
    return checkPanel;
  }
 
 /**
  * Like createCheckPanel, but with an button with an icon instead of a hyperlink.
  */
  public static JPanel createCheckPanel1(
     final Frame frame,
     final String name,
     final JTextComponent jt,
     final boolean oneUrl,
     final boolean withNavigation,
     final boolean onlyDirs,
     final boolean localFS){
    JButton bBrowse1 = MyUtil.mkButton1("open_folder.png", "Browse", "Open");
    bBrowse1.setPreferredSize(new java.awt.Dimension(22, 22));
    bBrowse1.setSize(new java.awt.Dimension(22, 22));
    bBrowse1.addMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent me){
        launchCheckBrowser(frame, CHECK_URL, jt, localFS, oneUrl, withNavigation, onlyDirs, false);
      }
    });

   JPanel fPanel = new JPanel(new BorderLayout());
   JPanel checkPanel = new JPanel(new FlowLayout());
   JLabel jlName = new JLabel(name);
   fPanel.add(jlName, BorderLayout.WEST);
   fPanel.add(new JLabel("   "));
   checkPanel.add(bBrowse1);
   fPanel.add(checkPanel, BorderLayout.EAST);
   fPanel.add(new JLabel(""));
   return fPanel;
 }
 
  /**
   * Loads class.
   * @argument className     name of the class
   * @argument argTypes      class names of the arguments of the class constructor
   * @argument args          arguments of the class constructor
   * 
   * @throws Throwable if an exception or an error occurs during loading
   */
  public static Object loadClass(String className, Class [] argTypes,
     Object [] args) throws Throwable{
    Debug.debug("Loading plugin: "+" : "+className, 2);
    // Arguments and class name for <DatabaseName>Database
    boolean loadfailed = false;
    Object ret = null;
    Debug.debug("argument types: "+arrayToString(argTypes), 3);
    Debug.debug("arguments: "+arrayToString(args), 3);
    try{
      //Class newClass = this.getClass().getClassLoader().loadClass(dbClass);
      Class newClass = (new MyClassLoader()).loadClass(className);
      ret = (newClass.getConstructor(argTypes).newInstance(args));
      Debug.debug("plugin " + "(" + className + ") loaded, "+ret.getClass(), 2);
    }
    catch(Exception e){
      Debug.debug("WARNING: failed to load class with standard method, trying findClass. "+
          e.getMessage(), 1);
      e.printStackTrace();
      loadfailed = true;
      //do nothing, will try with findClass.
    }
    if(loadfailed){
      try{
        // loading of this plug-in
       MyClassLoader mcl = new MyClassLoader();
       ret = (mcl.findClass(className).getConstructor(argTypes).newInstance(args)); 
       Debug.debug("plugin " + "(" + className + ") loaded", 2);
      }
      catch(IllegalArgumentException iae){
        GridPilot.getClassMgr().getLogFile().addMessage("Cannot load class " + className + ".\nThe plugin constructor " +
                          "must have one parameter (String)", iae);
        throw iae;
      }
      catch(Exception e){
        //GridPilot.getClassMgr().getLogFile().addMessage("Cannot load class " + dbClass, e);
        throw e;
      }
    }
    return ret;
  }

  public static String getName(String message, String str){

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
        str, JOptionPane.OK_CANCEL_OPTION);

    if(choice!=JOptionPane.OK_OPTION){
      return null;
    }
    else{
      return tf.getText();
    }
  }
  
  public static int getNumber(String message, String title, int initialValue){

    JPanel panel = new JPanel(new GridBagLayout());
    JSpinner sNum = new JSpinner();
    sNum.setPreferredSize(new Dimension(50, 21));
    sNum.setModel(new SpinnerNumberModel(initialValue, 1, 9999, 1));

    panel.add(new JLabel(message),
        new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0,
            GridBagConstraints.CENTER,
            GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));
    JPanel pNum = new JPanel();
    pNum.add(sNum);
    panel.add(pNum, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
        GridBagConstraints.CENTER,
        GridBagConstraints.NONE, new Insets(5, 5, 5, 5),
        0, 0));

    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), panel,
        title, JOptionPane.OK_CANCEL_OPTION);

    if(choice!=JOptionPane.OK_OPTION){
      return -1;
    }
    else{
      return((Integer) (sNum.getValue())).intValue();
    }
  }


  /**
   * Replaces the home directory path with ~.
   * @param str
   */
  public static String replaceWithTildeLocally(String str){
    if(str==null){
      return str;
    }
    if(str.startsWith(System.getProperty("user.home"))){
      str = "~"+str.substring(System.getProperty("user.home").length());
    }
    if(onWindows()){
      str =str.replaceAll("\\\\", "/");
    }
    return str;
  }

  // TODO: use this everywhere
  public static String addFile(String _line){
    String line = _line;
    line = line.replaceFirst("^/", "file:///");
    return line;
  }

  public static String getIPNumber(){ 
    long [] localip = null; 
    try{
      Enumeration e = NetworkInterface.getNetworkInterfaces();
      while(e.hasMoreElements()){ 
        NetworkInterface netface = (NetworkInterface) e.nextElement(); 
        Enumeration e2 = netface.getInetAddresses();
        while (e2.hasMoreElements()){
          InetAddress ip = (InetAddress) e2.nextElement(); 
          if(!ip.isLoopbackAddress() && ip.getHostAddress().indexOf(":")==-1){ 
            byte[] ipAddr = ip.getAddress();
            localip[0]=(ipAddr[0]&0xFF);
            localip[1]=(ipAddr[1]&0xFF);
            localip[2]=(ipAddr[2]&0xFF);
            localip[3]=(ipAddr[3]&0xFF);      
          }
        }
      }
    } 
    catch (Exception e){
      Debug.debug("ERROR: Could not get IP address", 1);
    }
    return Long.toString(localip[0])+"."+Long.toString(localip[1])+"."+
      Long.toString(localip[2])+"."+Long.toString(localip[3]);
  }  

  public static String getIPAddress(){ 
    // this should work from an applet.
    String host = GridPilot.getClassMgr().getGridPilot().getDocumentBase().getHost();
    /*String ipAddress = null;
    int port = GridPilot.getClassMgr().getGridPilot().getDocumentBase().getPort();
     try{
       ipAddress = (new Socket(host, port)).getLocalAddress().getHostAddress();
    }
    catch(UnknownHostException e){
      Debug.debug("ERROR: Could not get IP address. Unknown host. "+e.getMessage(), 1);
    }
    catch(IOException e){
      Debug.debug("ERROR: Could not get IP address. "+e.getMessage(), 1);
    }*/
    return host;
    //return ipAddress;
  }
  
  public static String dos2unix(String s){
    return s.replaceAll("\\r\\n","\n");
    //return s.replace('\r', ' ');
  }
  
  /**
   * Asks the user if he wants to interrupt a plug-in
   */
  private static boolean askForInterrupt(String name, String fct){
    
    if(!GridPilot.ASK_BEFORE_INTERRUPT){
      return !GridPilot.WAIT_FOREVER;
    }
    
    String msg = "No response from " + name + " for " + fct + "\n"+
                 "Do you want to interrupt it?";
    int choice = -1;
    
    final JCheckBox cbRemember = new JCheckBox("Remember decision", true);
    cbRemember.setSelected(false);
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    try{
      choice = confirmBox.getConfirm("No response from plugin",
          msg, new Object[] {"Yes, interrupt", "No, let it run", cbRemember});
    }
    catch(Exception e){
      e.printStackTrace();
      return true;
    }
    
    if(choice==JOptionPane.YES_OPTION){
      if(cbRemember.isSelected()){
        GridPilot.ASK_BEFORE_INTERRUPT = false;
      }
      return true;
    }
    else{
      if(cbRemember.isSelected()){
        GridPilot.ASK_BEFORE_INTERRUPT = false;
        GridPilot.WAIT_FOREVER = true;
      }
      GridPilot.getClassMgr().getGlobalFrame().setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      return false;
    }
  }

  /**
   * Waits the specified <code>MyThread</code> during maximum <code>timeOut</code> ms.
   * 0 means wait forever.
   * @return true if <code>t</code> ended normally, false if <code>t</code> has been interrupted
   * @throws InterruptedException 
   */
  public static boolean myWaitForThread(ResThread t, String name, int _timeOut,
      String function){
    return myWaitForThread(t, name, _timeOut, function, null);
  }

  public static boolean myWaitForThread(ResThread t, String name, int _timeOut,
      String function, Boolean _askForInterrupt){
    int timeOut = GridPilot.WAIT_FOREVER ? 0 : _timeOut;
    boolean ask = GridPilot.ASK_BEFORE_INTERRUPT;
    if(_askForInterrupt!=null){
      ask = _askForInterrupt.booleanValue();
    }
    do{
      try{
        t.join(timeOut);
      }
      catch(InterruptedException ie){
        ie.printStackTrace();
      }

      if(t.isAlive()){
        if(!ask || askForInterrupt(name, function)){
          GridPilot.getClassMgr().getLogFile().addMessage("No response from plugin " +
              name + " for " + function);
          t.requestStop();
          try{
            t.interrupt();
          }
          catch(Exception e){
            e.printStackTrace();
          }
          int i = 0;
          // Wait 5 seconds for thread to exit
          while(t.isAlive()){
            try{
              Debug.debug("Waiting for thread to exit...", 2);
              Thread.sleep(1000);
              if(!t.isInterrupted()){
                t.interrupt();
              }
              ++i;
              if(i>4){
                break;
              }
            }
            catch(InterruptedException e){
              break;
            }
          }         
          t.clearRequestStop();
          return false;
        }
      }
      else{
        break;
      }
    }
    while(true);
    return true;
  }
  
  /**
   * Returns a Vector which contains all elements from <code>v</code>, but in a
   * random order. <p>
   */
  public static Vector shuffle(Vector v){
    Vector w = new Vector();
    Random rand = new Random();
    while(v.size()>0){
      w.add(v.remove(rand.nextInt(v.size())));
    }
    return w;
  }

  /**
   * Returns an array which contains all elements from <code>v</code>, but in a
   * random order. <p>
   */
  public static Object [] shuffle(Object [] arr){
    ArrayList arl = new ArrayList();
    for(int i=0; i<arr.length; ++i){
      arl.add(arr[i]);
    }
    Collections.shuffle(arl);
    return arl.toArray();
  }

  /**
   * Presents a file browser to select a directory.
   */
  public static File getDownloadDir(Component parent){
    File file = null;
    JFileChooser fc = new JFileChooser();
    fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
    fc.setDialogTitle("Choose download directory");
    int returnVal = fc.showOpenDialog(parent);
    if(returnVal==JFileChooser.APPROVE_OPTION){
      file = fc.getSelectedFile();
      Debug.debug("Opening: " + file.getName(), 2);
    }
    else{
      Debug.debug("Not opening file", 3);
    }
    return file;
  }

  public static JTextArea createTextArea(){
    
    JTextArea ta = new JTextArea(){
      private static final long serialVersionUID=1L;
      public java.awt.Dimension getPreferredSize(){
        java.awt.Dimension dim = super.getPreferredSize();
        dim.setSize(0, dim.height);
        return dim;
      }
    };
    ta.setBorder(new JTextField().getBorder());
    ta.setWrapStyleWord(true);
    ta.setLineWrap(true);
    return ta;
  }
  
  public static JTextArea createTextArea(int size){
    JTextArea ta = new JTextArea(1, size){
      private static final long serialVersionUID=1L;
      public java.awt.Dimension getPreferredSize(){
        java.awt.Dimension dim = super.getPreferredSize();
        dim.setSize(0, dim.height);
        return dim;
      }
    };
    ta.setBorder(new JTextField().getBorder());
    ta.setWrapStyleWord(true);
    ta.setLineWrap(true);
    return ta;
  }
  
  public static JTextArea createGrayTextArea(String str){
    JTextArea jval = new JTextArea(str){
      private static final long serialVersionUID=1L;
      public java.awt.Dimension getPreferredSize(){
        java.awt.Dimension dim = super.getPreferredSize();
        dim.setSize(0, dim.height);
        return dim;
      }
    };
    ((JTextArea) jval).setLineWrap(true);
    ((JTextArea) jval).setWrapStyleWord(true);
    ((JTextArea) jval).setEditable(false);
    setBackgroundColor(jval);
    return jval;
  }
  
  private static Object[] createDisplayObjects(JOptionPane pane, int i, String cancelText){
    Object okObject = mkButton(pane, GridPilot.ICONS_PATH + "enter.png", "OK", "OK");
    Object skipObject = mkButton(pane, GridPilot.ICONS_PATH + "cancel.png", cancelText, cancelText);
    Object okAllObject = mkButton(pane, GridPilot.ICONS_PATH + "enter_all.png", "OK for all", "OK for all");
    Object skipAllObject = mkButton(pane, GridPilot.ICONS_PATH + "stop.png", cancelText+" all", cancelText+" all");
    Object[] showResultsOptions = null;
    switch(i){
      case OK_OPTION: showResultsOptions = new Object[] {okObject};
              break;
      case OK_SKIP_OPTION: showResultsOptions = new Object[] {okObject, skipObject};
              break;
      case OK_ALL_SKIP_ALL_OPTION: showResultsOptions = new Object[] {okObject, skipObject, okAllObject, skipAllObject};
    }
    return showResultsOptions;
  }

  public static int showResult(String [] cstAttrNames, String [] cstAttr, String title, int optionType,
      String cancelText){
    JPanel pResult = new JPanel(new GridBagLayout());
    int row = 0;
    JComponent jval;
    boolean noTextArea = true;
    for(int i =0; i<cstAttr.length; ++i, ++row){
      if(cstAttrNames[i].equalsIgnoreCase("initLines") ||
          cstAttrNames[i].equalsIgnoreCase("outFileMapping") ||
          cstAttrNames[i].equalsIgnoreCase("pfns") ||
          cstAttrNames[i].equalsIgnoreCase("metaData") ||
          cstAttrNames[i].equalsIgnoreCase("comment")){
        jval = createGrayTextArea(cstAttr[i]);
        noTextArea = false;
      }
      else{
        jval = new JTextField(cstAttr[i]==null?"":cstAttr[i]);
        ((JTextField) jval).setEditable(false);
      }
      pResult.add(new JLabel(cstAttrNames[i] + " : "),
          new GridBagConstraints(0, row, 1, 1, 0.0, 0.0 ,
              GridBagConstraints.CENTER, GridBagConstraints.BOTH,
              new Insets(5, 25, 5, 5), 0, 0));
      pResult.add(jval, new GridBagConstraints(1, row, 3, 1, 1.0, 0.0,
          GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
          new Insets(5, 5, 5, 5), 0, 0));
    }

    JScrollPane sp = new JScrollPane(pResult);
    int height = (int)pResult.getPreferredSize().getHeight() +
      (int)sp.getHorizontalScrollBar().getPreferredSize().getHeight() + 5;
    int width = (int)pResult.getPreferredSize().getWidth() +
      (int)sp.getVerticalScrollBar().getPreferredSize().getWidth() + 5;
    Dimension screenSize = new Dimension(Toolkit.getDefaultToolkit().getScreenSize());
    if(!noTextArea){
      width += 100;
    }
    if(height>screenSize.height){
      height = 700;
      Debug.debug("Screen height exceeded, setting "+height, 2);
    }
    if(!noTextArea){
      width += 200;
    }
    if(width>screenSize.width){
      width = 550;
      Debug.debug("Screen width exceeded, setting "+width, 2);
    }
    Debug.debug("Setting size "+width+":"+height, 3);
    sp.setPreferredSize(new Dimension(width, height));
    
    return showResult(null, sp, title, optionType, cancelText);
    
  }
  
  public static int showResult(Component parent, JComponent comp, String title, int optionType,
      String cancelText){

    JOptionPane op = new JOptionPane(comp);
    Object[] showResultsOptions = null;
    if(optionType==OK_OPTION){
      showResultsOptions = createDisplayObjects(op, 0, cancelText);
      op.setMessageType(JOptionPane.QUESTION_MESSAGE);
      op.setOptionType(JOptionPane.OK_OPTION);
      op.setOptions(showResultsOptions);
      op.setInitialValue(showResultsOptions[0]);
     }
    else if(optionType==OK_SKIP_OPTION){
      showResultsOptions = createDisplayObjects(op, 1, cancelText);
      op.setMessageType(JOptionPane.QUESTION_MESSAGE);
      op.setOptionType(JOptionPane.YES_NO_CANCEL_OPTION);
      op.setOptions(showResultsOptions);
      op.setInitialValue(showResultsOptions[0]);
   }
   else if(optionType==OK_ALL_SKIP_ALL_OPTION){
     showResultsOptions = createDisplayObjects(op, 2, "Skip");
     op.setMessageType(JOptionPane.QUESTION_MESSAGE);
     op.setOptionType(JOptionPane.YES_NO_CANCEL_OPTION);
     op.setOptions(showResultsOptions);
     op.setInitialValue(showResultsOptions[0]);
    }

    JDialog dialog = op.createDialog(parent!=null?parent:JOptionPane.getRootFrame(), title);    
    dialog.requestFocusInWindow();    
    dialog.setResizable(true);
    dialog.setVisible(true);
    dialog.dispose();

    Object selectedValue = op.getValue();
    
    Debug.debug("selectedValue "+selectedValue, 2);

    if(selectedValue==null){
      return JOptionPane.CLOSED_OPTION;
    }
    
    for(int i=0; i<showResultsOptions.length; ++i){
      if(showResultsOptions[i]==selectedValue){
        return i;
      }
    }
    
    return JOptionPane.CLOSED_OPTION;
    
  }
  
  /**
   * Create a JButton from the specified image.
   * If the image is null or not found, 'buttonText' is displayed.<br>
   * This method honors BUTTON_DISPLAY.
   * @param imageFile unqualified name of the image file
   * @param buttonText
   * @param mouseOverText
   * @return
   */
  public static JButton mkButton(String imageFile,  String buttonText, String mouseOverText) {
    JButton button = null;
    switch(BUTTON_DISPLAY){
    case ICON_ONLY:
      try{
        URL imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + imageFile);
        ImageIcon imgIcon = new ImageIcon(imgURL);
        button = new JButton(imgIcon);
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("Could not find image "+ GridPilot.ICONS_PATH + imageFile, 3);
        button = new JButton(buttonText);
      }
      break;
    case TEXT_ONLY:
      button = new JButton(buttonText);
      break;
    case ICON_AND_TEXT:
      try{
        URL imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + imageFile);
        ImageIcon imgIcon = new ImageIcon(imgURL);
        button = new JButton(buttonText, imgIcon);
      }
      catch(Exception ee){
        //ee.printStackTrace();
        Debug.debug("WARNING: Could not find image "+ GridPilot.ICONS_PATH + imageFile, 3);
        button = new JButton(buttonText);
      }
      break;
    }
    button.setToolTipText(mouseOverText);
    return button;
  }

  /**
   * Create a JButton from the specified image.
   * If the image is null or not found, 'fallbackText' is displayed.<br>
   * NOTICE that this method ignores BUTTON_DISPLAY and always displays only an image.
   * @param imageFile unqualified name of the image file
   * @param mouseOverText
   * @param fallbackText
   * @return
   */
  public static JButton mkButton1(String imageFile, String mouseOverText, String fallbackText) {
    JButton button = null;
    try{
      URL imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + imageFile);
      ImageIcon imgIcon = new ImageIcon(imgURL);
      button = new JButton(imgIcon);
    }
    catch(Exception e){
      e.printStackTrace();
      Debug.debug("Could not find image "+ GridPilot.ICONS_PATH + imageFile, 3);
      button = new JButton(fallbackText);
    }
    button.setToolTipText(mouseOverText);
    return button;
  }

  public static String [][] getValues(String dbName, String table, String id,
      String idValue, String [] fields){
    String req = "SELECT "+fields[0];
    String values [] = new String[fields.length];
    if(fields.length>1){
      for(int i=1; i<fields.length; ++i){
        req += ", "+fields[i];
      }
    }
    req += " FROM "+table;
    req += " WHERE "+id+" = '"+ idValue+"'";
    Vector resultVector = new Vector();
    String [][] resultArray = null;
    try{
      Debug.debug(dbName+" >> "+req, 3);
      DBResult rset = GridPilot.getClassMgr().getDBPluginMgr(dbName).executeQuery(dbName, req);
      while(rset.moveCursor()){
        for(int i=0; i<fields.length;i++){
          values[i] = rset.getString(fields[i]);
        }
        resultVector.add(values);
      }
      if(resultVector.size()==0){
        Debug.debug("WARNING: No record in "+table+" with "+ id +" "+idValue, 2);
      }
      resultArray = new String [resultVector.size()][fields.length];
      for(int i=0; i<resultVector.size(); ++i){
        for(int j=0; j<fields.length; ++j){
          resultArray[i][j] = ((String []) resultVector.get(i))[j];
          Debug.debug("Added value "+i+j+" "+resultArray[i][j], 3);
        }
      }
    }
    catch(SQLException e){
      Debug.debug("WARNING: No record found with "+ id +" "+idValue+". "+e.getMessage(), 2);
      return new String [fields.length][0];
    }
    return resultArray;
  }

  public static String [] getFileDatasetReference(String dbName){
    String [] ret = GridPilot.getClassMgr().getConfigFile().getValues(dbName,
      "file dataset reference");
    if(ret==null || ret.length<2){
      ret = new String [] {"name", "datasetName"};
    }
    Debug.debug("file dataset reference for "+dbName
        +" : "+arrayToString(ret), 2);
    return ret;
  }

  /**
   * Get the name of the table column holding the PFNs.
   */
  public static String getPFNsField(String dbName){
    String ret = GridPilot.getClassMgr().getConfigFile().getValue(dbName, "Pfns field");
    return ret;
  }

  /**
   * Get the name of the column holding the identifier.
   */
  public static String getIdentifierField(String dbName, String table){
    String ret = GridPilot.getClassMgr().getConfigFile().getValue(dbName, table+" identifier");
    if(ret==null || ret.equals("")){
      ret = "identifier";
    }
    //Debug.debug("Identifier for "+dbName+" - "+table+" : "+ret, 2);
    return ret;
  }

  /**
   * Get the name of the column holding the name.
   */
  public static String getNameField(String dbName, String table){
    String ret = GridPilot.getClassMgr().getConfigFile().getValue(dbName, table+" name");
    if(ret==null || ret.equals("")){
      ret = "name";
    }
    //Debug.debug("Name for "+dbName+" - "+table+" : "+ret, 2);
    return ret;
  }

  /**
   * Get the name of the column holding the file size.
   */
  public static String getFileSizeField(String dbName){
    String ret = GridPilot.getClassMgr().getConfigFile().getValue(dbName, "Bytes field");
    if(ret==null || ret.equals("")){
      ret = "bytes";
    }
    return ret;
  }

  /**
   * Get the name of the column holding the file checksum.
   */
  public static String getChecksumField(String dbName){
    String ret = GridPilot.getClassMgr().getConfigFile().getValue(dbName, "Checksum field");
    if(ret==null || ret.equals("")){
      ret = "checksum";
    }
    return ret;
  }

  /**
   * Get the name of the column holding the version.
   */
  public static String getVersionField(String dbName, String table){
    String ret = GridPilot.getClassMgr().getConfigFile().getValue(dbName, table+" version");
    if(ret==null || ret.equals("")){
      ret = "version";
    }
    //Debug.debug("Version for "+dbName+" - "+table+" : "+ret, 2);
    return ret;
  }

  public static String [] getJobDefDatasetReference(String dbName){
    String [] ret = GridPilot.getClassMgr().getConfigFile().getValues(dbName,
      "jobDefinition dataset reference");
    if(ret==null || ret.length<2){
      ret = new String [] {"name", "datasetName"};
    }
    Debug.debug("jobDefinition dataset reference for "+dbName
        +" : "+arrayToString(ret), 2);
    return ret;
  }

  public static String [] getDatasetTransformationReference(String dbName){
    String [] ret = GridPilot.getClassMgr().getConfigFile().getValues(dbName,
      "dataset transformation reference");
    if(ret==null || ret.length<2){
      ret = new String [] {"name", "transformationName"};
    }
    Debug.debug("dataset transformation reference for "+dbName
        +" : "+arrayToString(ret), 2);
    return ret;
  }

  public static String [] getDatasetTransformationVersionReference(String dbName){
    String [] ret = GridPilot.getClassMgr().getConfigFile().getValues(dbName,
      "dataset transformation version reference");
    if(ret==null || ret.length<2){
      ret = new String [] {"version", "transformationVersion"};
    }
    Debug.debug("dataset transformation version reference for "+dbName
        +" : "+arrayToString(ret), 2);
    return ret;
  }

  public static String [] getTransformationRuntimeReference(String dbName){
    String [] ret = GridPilot.getClassMgr().getConfigFile().getValues(dbName,
      "transformation runtime environment reference");
    if(ret==null || ret.length<2){
      ret = new String [] {"name", "runtimeEnvironmentName"};
    }
    Debug.debug("transformation runtime environment reference for "+dbName
        +" : "+arrayToString(ret), 2);
    return ret;
  }

  /**
   * Find closest in getSources() by checking with GridPilot.preferredFileServers
   */
  public static void setClosestSource(TransferInfo transfer){
    boolean ok = false;
    GlobusURL [] sources = transfer.getSources();
    if(GridPilot.PREFERRED_FILE_SERVERS!=null && GridPilot.PREFERRED_FILE_SERVERS.length>0){
      Debug.debug("Preferred file servers: "+arrayToString(GridPilot.PREFERRED_FILE_SERVERS), 2);
      int closeness = -1;
      for(int i=0; i<sources.length; ++i){
        Debug.debug("Checking source: "+sources[i].getURL()+" : "+sources[i].getHost(), 2);
        for(int j=0; j<GridPilot.PREFERRED_FILE_SERVERS.length; ++j){
          Debug.debug("Checking file server: "+GridPilot.PREFERRED_FILE_SERVERS[j], 2);
          if((sources[i].getURL().startsWith(GridPilot.PREFERRED_FILE_SERVERS[j]) ||
              sources[i].getHost().matches(".*"+
              GridPilot.PREFERRED_FILE_SERVERS[j].replaceAll("\\.", "\\\\.").replaceAll("\\*", "\\.\\*")+".*")) &&
              (j<closeness || closeness==-1)){
            closeness = j;
            transfer.setSource(sources[i]);
            ok = true;
            Debug.debug("Setting closest source : "+transfer.getSource().getURL(), 2);
          }
        }
      }
    }
    else{
      Debug.debug("WARNING: no preferred file servers defined. "+GridPilot.PREFERRED_FILE_SERVERS, 2);
    }
    if(!ok){
      transfer.setSource(sources[0]);
    }
  }

  public static HashMap parseMetaData(String str){
    HashMap hm = new HashMap();
    if(str==null){
      return hm;
    }
    // This is because hsqldb values don't have new-lines - \n is just \\n.
    str = str.replaceAll("\\\\n", "\n");
    try{
      InputStream is = new ByteArrayInputStream(str.getBytes());
      BufferedReader in = new BufferedReader(new InputStreamReader(is));
      String line;
      String key = null;
      String value = null;
      while((line = in.readLine())!=null){
        if(line.matches("^\\w+: .+$")){
          key = line.replaceFirst("^(\\w+): .+$", "$1");
          value = line.replaceFirst("^\\w+: (.+)$", "$1");
          Debug.debug("Adding metadata "+key+":"+value, 2);
          hm.put(key, value);
          hm.put(key.toLowerCase(), value);
        }
      }
      in.close();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    return hm;
  }
  
  /**
   * This method extracts the lines of a multi-line string
   * that are not of the form <field>: <value>
   */
  public static String getMetadataComments(String str){
    StringBuffer sb = new StringBuffer();
    try{
      InputStream is = new ByteArrayInputStream(str.getBytes());
      BufferedReader in = new BufferedReader(new InputStreamReader(is));
      String line;
      while((line = in.readLine())!=null){
        if(!line.matches("^\\w+: .+$")){
          sb.append(line);
          sb.append("\n");
        }
      }
      in.close();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    sb.trimToSize();
    return sb.toString();
  }
  
  public static String generateMetaDataText(HashMap data){
    StringBuffer sb = new StringBuffer();
    for(Iterator it=data.keySet().iterator(); it.hasNext();){
      sb.append((String) it.next());
      sb.append(": ");
      sb.append(data.get((String) it.next()));
      sb.append("\n");
    }
    sb.trimToSize();
    return sb.toString();
  }

  /**
   * Extracts the table names from an SQL statement.
   * 
   * @param the SQL string
   * @return and array of table names
   */
  public static String [] getTableNames(String sql){
    String table = null;
    table = sql.replaceFirst("^(?i)(?s)SELECT .* FROM (\\S+)$", "$1");
    if(table!=null && table.length()>0 && !table.equalsIgnoreCase(sql)){
      return split(table, ",");
    }
    table = sql.replaceFirst("^(?i)(?s)SELECT .* FROM (.+) WHERE .*$", "$1");
    if(table!=null && table.length()>0 && !table.equalsIgnoreCase(sql)){
      return split(table, ",");
    }
    table = sql.replaceFirst("^(?i)(?s)INSERT INTO (\\S+) .*$", "$1");
    if(table!=null && table.length()>0 && !table.equalsIgnoreCase(sql)){
      return new String []{table};
    }
    table = sql.replaceFirst("^(?i)(?s)UPDATE (\\S+) .*$", "$1");
    if(table!=null && table.length()>0 && !table.equalsIgnoreCase(sql)){
      return new String []{table};
    }
    table = sql.replaceFirst("^(?i)(?s)DELETE FROM (\\S+) .*$", "$1");
    if(table!=null && table.length()>0 && !table.equalsIgnoreCase(sql)){
      return new String []{table};
    }
    table = sql.replaceFirst("^(?i)(?s)CREATE TABLE (\\S+)\\s*\\(.*$", "$1");
    if(table!=null && table.length()>0 && !table.equalsIgnoreCase(sql)){
      return new String []{table};
    }
    return null;
  }

  public static String [] getColumnNames(String sql){
    String fields = null;
    fields = sql.replaceFirst("^(?i)SELECT (.*) FROM .*", "$1");
    // get rid of e.g. t_lfn in t_lfn.guid
    fields = fields.replaceAll("\\w+\\.(\\w+)", "$1");
    if(fields!=null){
      return split(fields, ",");
    }
    return null;
  }
  
  /**
   * Checks whether or not a URL is remote (i.e. does not point to a file/folder
   * on the local machine).
   * 
   * @param url the URL to check
   * @return true is the URL is remote, false if not
   */
  public static boolean urlIsRemote(String url){
    return !url.matches("^file:/*[^/]+.*") && url.matches("^[a-z]+:/*[^/]+.*");
  }
  
  /**
   * Reads the lines of a text file on a web server (or locally).
   * @param url the URL to read
   * @param transferControl MyTransferControl object to use for downloading
   * @param tmpFile local download location. If left empty, nothing is written to disk
   * @param commentTag a string like "#". If specified, lines starting with this string will be ignored
   * as will empty lines. If null, all lines will be kept.
   * @return an array of the lines of text
   */
  public static String [] readURL(URL url, MyTransferControl transferControl, File tmpFile, String commentTag) throws Exception{
    Debug.debug("Reading URL "+url, 3);
    String [] ret = null;
    BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
    BufferedWriter out = null;
    if(tmpFile!=null){
      out = new BufferedWriter(new FileWriter(tmpFile));
    }
    String line = null;
    Vector vec = new Vector();
    while((line=in.readLine())!=null){
      if(line!=null && (commentTag==null || !line.startsWith(commentTag) && !line.equals(""))){
        vec.add(line);
        if(tmpFile!=null){
          out.write(line+"\n");
        }
      }
    }
    in.close();
    if(tmpFile!=null){
      out.close();
    }
    ret = new String[vec.size()];
    Enumeration en = vec.elements();
    int i = 0;
    while(en.hasMoreElements()){
      ret[i] = (String) en.nextElement();
      ++i;
    }
    return ret;
  }
  
  /**
   * Reads the lines of a text file on a file server (or locally).
   * @param urlString the URL to read
   * @param transferControl MyTransferControl object to use for downloading
   * @param tmpFile local download location. If left empty, nothing is written to disk
   * @param commentTag a string like "#". If specified, lines starting with this string will be ignored
   * as will empty lines. If null, all lines will be kept.
   * @return an array of the lines of text
   */
  public static String [] readURL(String urlString, MyTransferControl transferControl, File tmpFile, String commentTag) throws Exception{
    String [] ret = null;
    if(urlString.startsWith("http:") || urlString.startsWith("http:") ||
        urlString.startsWith("ftp:") || urlString.startsWith("file:")){
      if(urlString.startsWith("file:")){
        urlString = clearTildeLocally(/*clearFile(*/urlString/*)*/);
      }
      URL url = new URL(urlString);
      ret = readURL(url, transferControl, tmpFile, commentTag);
    }
    else if(urlString.startsWith("gsiftp:") || urlString.startsWith("srm:")){
      transferControl.download(urlString, tmpFile);
      ret = readURL(tmpFile.toURL().toExternalForm(), transferControl, tmpFile, commentTag);
     }
    return ret;
  }
  
  /**
   * This method is used for putting a progress bar on the main panel
   * when doing PFN lookups.
   */
  public static JProgressBar setProgressBar(int maxVal, final String dbName){
    StatusBar statusBar = GridPilot.getClassMgr().getStatusBar();
    JProgressBar pb = new JProgressBar();
    ImageIcon cancelIcon;
    URL imgURL=null;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + "stop_small.png");
      cancelIcon = new ImageIcon(imgURL);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.ICONS_PATH + "stop_small.png", 3);
      cancelIcon = new ImageIcon();
    }
    statusBar.setProgressBar(pb);
    statusBar.setProgressBarMax(pb, maxVal);
    JButton bCancel = new JButton(cancelIcon);
    bCancel.setToolTipText("click here to stop PFN lookup");
    bCancel.addMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent me){
        GridPilot.getClassMgr().getDBPluginMgr(dbName).requestStopLookup();
      }
    });
    JPanel jpCancel = new JPanel(new FlowLayout(FlowLayout.RIGHT, 0, 0));
    jpCancel.add(bCancel);
    jpCancel.setPreferredSize(new java.awt.Dimension(6, 4));
    jpCancel.setSize(new java.awt.Dimension(6, 4));
    statusBar.setCenterComponent(jpCancel);
    pb.validate();
    return pb;
  }
  
  public static void showError(String text){
    showMessage("ERROR", text);
  }
  
  public static Object mkOkObject(JOptionPane pane){
    return mkButton(pane, GridPilot.ICONS_PATH + "enter.png", "OK", "OK");
  }

  public static Object mkOkAllObject(JOptionPane pane){
    return mkButton(pane, GridPilot.ICONS_PATH + "enter_all.png", "OK for all", "OK for all");
  }

  public static Object mkCancelObject(JOptionPane pane){
    return mkButton(pane, GridPilot.ICONS_PATH + "cancel.png", "Cancel", "Cancel");
  }

  public static Object mkSkipObject(JOptionPane pane){
    return mkButton(pane, GridPilot.ICONS_PATH + "next.png", "Skip", "Skip");
  }

  public static Object mkSkipAllObject(JOptionPane pane){
    return mkButton(pane, GridPilot.ICONS_PATH + "cancel.png", "Skip all", "Skip all");
  }

  private static void showMessage0(String title, String text){
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String confirmString = text;
    try{
      confirmBox.getConfirm(title, confirmString, new Object[] {mkOkObject(confirmBox.getOptionPane())});
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }

  public static void showMessage(final String title, final String text){
    if(SwingUtilities.isEventDispatchThread()){
      showMessage0(title, text);
      return;
    }
    // else
    try{
      SwingUtilities.invokeAndWait(
        new Runnable(){
          public void run(){
            showMessage0(title, text);
          }
        }
      );
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }

  public static void showLongMessage(String message, String title){
    JTextPane ta = new JTextPane();
    ta.setText(message);
  
    //ta.setLineWrap(true);
    //ta.setWrapStyleWord(true);
    ta.setOpaque(false);
    ta.setEditable(false);
  
    JOptionPane op = new JOptionPane(ta, JOptionPane.PLAIN_MESSAGE);
  
    JDialog dialog = op.createDialog(JOptionPane.getRootFrame(), title);
    dialog.setResizable(true);
    //ta.getPreferredSize(); // without this line, this dialog is too small !?
    dialog.pack();
    dialog.validate();
    //Dimension dim = dialog.getSize();
    //dialog.setSize(new Dimension(dim.height+50, dim.width));
    dialog.setVisible(true);
    dialog.dispose();
  
  }

  public static long getDateInMilliSeconds(String dateInput){
    return getDateInMilliSeconds(dateInput, GridPilot.DATE_FORMAT_STRING);
  }
  
/**
 * Write the local OS as an RTE. This is to make CSPluginMgr.submit happy when we are submitting
 * with local fork.
 * @param csName
 * @param toDeleteRtes
 * @param dbMgr
 */
  private static void createLocalOSRTE(String csName, HashMap toDeleteRtes,
      DBPluginMgr dbMgr){
    MyLogFile logFile = GridPilot.getClassMgr().getLogFile();
    try{
      dbMgr.createRuntimeEnv(
          new String [] {"name", "computingSystem"},
          new String [] {LocalStaticShell.getOS(), csName});
      // Find the ID of the newly created RTE and tag it for deletion
      String [] rteIds = dbMgr.getRuntimeEnvironmentIDs(LocalStaticShell.getOS(), csName);
      for(int j=0; j<rteIds.length; ++j){
        toDeleteRtes.put(rteIds[j], dbMgr.getDBName());
      }
    }
    catch(Exception e){
      e.printStackTrace();
      logFile.addMessage("WARNING: could not create RTE for local OS "+LocalStaticShell.getOS()+
          " on "+csName, e);
    }
  }
  
  /**
   * Create "Linux" and "Windows" RTEs. Selecting one of these as RTE amounts to requiring
   * Linux or Windows but not caring about the specific distro or version.
   * @param oses
   * @param csName
   * @param toDeleteRtes
   * @param dbMgr
   */
  public static void createBasicOSRTEs(String [] oses, String csName, HashMap toDeleteRtes,
      DBPluginMgr dbMgr){
    if(oses==null){
      return;
    }
    MyLogFile logFile = GridPilot.getClassMgr().getLogFile();
    for(int i=0; i<oses.length; ++i){
      try{
        dbMgr.createRuntimeEnv(
            new String [] {"name", "computingSystem"},
            new String [] {oses[i], csName});
        // Find the ID of the newly created RTE and tag it for deletion
        String [] rteIds = dbMgr.getRuntimeEnvironmentIDs(oses[i], csName);
        for(int j=0; j<rteIds.length; ++j){
          toDeleteRtes.put(rteIds[j], dbMgr.getDBName());
        }
      }
      catch(Exception e){
        e.printStackTrace();
        logFile.addMessage("WARNING: could not create RTE for local OS "+LocalStaticShell.getOS()+
            " on "+csName, e);
      }
    }
  }
  
  /**
   * Consolidates and cpies records from catalogs to the 'local' runtime DBs.
    */
  public static void syncRTEsFromCatalogs(String csName, String [] rteCatalogUrls, String [] localRuntimeDBs,
      HashMap toDeleteRtes, boolean mkLocalOS, boolean includeVMs, String [] basicOses,
      boolean createRteForEachProvides){
    
    MyLogFile logFile = GridPilot.getClassMgr().getLogFile();
    
    if(rteCatalogUrls==null){
      return;
    }
    
    DBPluginMgr dbMgr = null;
    
    RteRdfParser rteRdfParser = GridPilot.getClassMgr().getRteRdfParser(rteCatalogUrls);
    DBRecord row;
    Debug.debug("Syncing RTEs from catalogs to DBs: "+arrayToString(localRuntimeDBs), 2);
    
    String nameField;
    Object providesStr;
    String [] provides;
    DBResult allRtes;
    HashMap rtesMap = null;
    for(int ii=0; ii<localRuntimeDBs.length; ++ii){
      try{
        
        dbMgr = GridPilot.getClassMgr().getDBPluginMgr(localRuntimeDBs[ii]);  
        nameField = getNameField(dbMgr.getDBName(), "runtimeEnvironment");
        allRtes = dbMgr.getRuntimeEnvironments();
        rtesMap = new HashMap();
        for(int i=0; i<allRtes.values.length; ++i){
          rtesMap.put(allRtes.getValue(i, nameField), allRtes.getValue(i, "computingSystem"));
        }
        
        if(mkLocalOS){
          createLocalOSRTE(csName, toDeleteRtes, dbMgr);
        }
        createBasicOSRTEs(basicOses, csName, toDeleteRtes, dbMgr);
        
        DBResult rtes = rteRdfParser.getDBResult(dbMgr, csName);
        Debug.debug("Creating "+rtes.size()+" RTEs in DB "+dbMgr.getDBName(), 2);
        for(int i=0; i<rtes.values.length; ++i){
          row = rtes.get(i);
          createRte(dbMgr, row, csName, toDeleteRtes);
          if(createRteForEachProvides){
            providesStr = row.getValue("provides");
            if(providesStr!=null){
              provides = split((String) providesStr);
            }
            else{
              continue;
            }
            for(int j=0; j<provides.length; ++j){
              if(arrayContains(basicOses, provides[j]) ||
                  rtesMap.containsKey(provides[j]) && rtesMap.get(provides[j]).equals(csName) ||
                  // Exclude provide strings like tag:bla bla
                  provides[j].indexOf("/")<0){
                continue;
              }
              row.setValue(nameField, provides[j]);
              row.setValue("provides", "");
              Debug.debug("Creating runtimeEnvironment record for provided RTE "+provides[j], 3);
              createRte(dbMgr, row, csName, toDeleteRtes);
            }
          }
        }
      }
      catch(Exception e){
        String error = "Could not load local runtime DB "+localRuntimeDBs[ii]+". "+e.getMessage();
        logFile.addMessage(error, e);
        e.printStackTrace();
        continue;
      }
    }
  }
  
  private static void createRte(DBPluginMgr dbMgr, DBRecord row, String csName,
      HashMap toDeleteRtes){
    MyLogFile logFile = GridPilot.getClassMgr().getLogFile();
    Debug.debug("Checking RTE "+arrayToString(row.values), 3);
    if(dbMgr.createRuntimeEnvironment(row.values)){
      Debug.debug("Created RTE "+arrayToString(row.values), 2);
      // Tag for deletion
      String rteNameField = getNameField(dbMgr.getDBName(), "runtimeEnvironment");
      String name = (String) row.getValue(rteNameField);
      String [] newIds = dbMgr.getRuntimeEnvironmentIDs(name , csName);
      if(newIds!=null){
        for(int j=0; j<newIds.length; ++j){
          Debug.debug("Tagging for deletion "+name+":"+newIds[j], 3);
          toDeleteRtes.put(newIds[j], dbMgr.getDBName());
        }
      }
    }
    else{
      logFile.addMessage("WARNING: Failed creating RTE "+arrayToString(row.values)+
          " on "+csName);
    }
  }

  /**
   * Clean up runtime environment records copied from runtime catalog URLs.
   */
  public static void cleanupRuntimeEnvironments(String csName, String [] localRuntimeDBs,
      HashMap<String, String> toDeleteRtes){
    String id = "-1";
    boolean ok = true;
    DBPluginMgr localDBMgr = null;
    for(int i=0; i<localRuntimeDBs.length; ++i){
      localDBMgr = null;
      try{
        localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
            localRuntimeDBs[i]);
      }
      catch(Exception e){
        String error = "Could not load local runtime DB "+localRuntimeDBs[i]+"."+e.getMessage();
        GridPilot.getClassMgr().getLogFile().addMessage(error, e);
      }
      // Delete RTEs from catalog(s)
      Debug.debug("Cleaning up catalog RTEs "+arrayToString(toDeleteRtes.keySet().toArray()), 3);
      if(toDeleteRtes!=null && toDeleteRtes.keySet()!=null){
        for(Iterator<String> it=toDeleteRtes.keySet().iterator(); it.hasNext();){
          try{
            id = it.next();
            if(toDeleteRtes.get(id).equals(localRuntimeDBs[i])){
              Debug.debug("Deleting "+id, 3);
              ok = localDBMgr.deleteRuntimeEnvironment(id);
              if(!ok){
                String error = "WARNING: could not delete runtime environment " +
                id+" from database "+localDBMgr.getDBName();
                GridPilot.getClassMgr().getLogFile().addMessage(error);
              }
            }
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
    }
  }
  
  /**
   * Remove RTEs that need not be cared about once the VM has been booted.
   * @param rtes list of RTEs
   * @param osProvides RTEs provided by OS
   * @return
   */
  public static String [] removeBaseSystemAndVM(String [] rtes, String [] _osProvides){
    Debug.debug("Removing VMs from " + arrayToString(rtes), 2);
    Vector<String> newRTEs = new Vector<String>();
    Set<String> providedRTEs = new HashSet<String>();
    String[] osProvides;
    if(_osProvides==null){
      osProvides = new String [0];
    }
    else{
      osProvides = _osProvides;
    }
    for(int i=0; i<rtes.length; ++i){
      // TODO: consider using RTEMgr.isVM() instead of relying on people starting their
      //       VM RTE names with VM/
      if(!checkOS(rtes[i]) && !rtes[i].startsWith(RteRdfParser.VM_PREFIX) &&
          !MyUtil.arrayContains(osProvides, rtes[i])){
        newRTEs.add(rtes[i]);
      }
    }
    String rte;
    for(Iterator<String> it=providedRTEs.iterator(); it.hasNext();){
      rte = it.next();
      newRTEs.remove(rte);
    }
    return newRTEs.toArray(new String [newRTEs.size()]);
  }

  public static void setupJobRTEs(JobInfo job, Shell shell, RTEMgr rteMgr,
      TransferStatusUpdateControl transferStatusUpdateControl,
      String remoteRteDir, String localRteDir) throws Exception{
    String [] rteNames = job.getRTEs();
    Debug.debug("Setting up job RTES "+Util.arrayToString(rteNames), 2);
    Vector<String> rtes = new Vector<String>();
    Collections.addAll(rtes, rteNames);
    Vector<String> deps = rteMgr.getRteDepends(rtes, job.getOpSys(), false);
    InstancePackage ip = null;
    String name = null;
    String os = null;
    boolean osEntry = true;
    Debug.debug("Setting up RTEs "+Util.arrayToString(deps.toArray()), 2);
    for(Iterator<String> it=deps.iterator(); it.hasNext();){
      // The first dependency returned by getRteDepends is the OS; skip it. The fact
      // that the job landed on that system means it matches.
      if(osEntry){
        os = it.next();
        osEntry = false;
        if(!it.hasNext()){
          break;
        }
      }
      name = it.next();
      // Check if installation was already done.
      // This we need, because GridPilot's HTTPSFileTransfer does not cache.
      // GridFactory's does.
      //if(shell.existsFile(remoteRteDir+"/"+name+"/"+RTEInstaller.INSTALL_OK_FILE)){
      if(rteMgr.getRTEInstaller().isInstalled(name, shell)){
        GridPilot.getClassMgr().getLogFile().addInfo("Reusing existing installation of "+name);
        return;
      }
      ip = rteMgr.getRteInstancePackage(name, os);
      rteMgr.getRTEInstaller().install(name, ip, shell);
    }
  }

  public static void checkAndActivateSSL(String[] urls){
    for(int i=0; i<urls.length; ++i){
      if(urls[i].toLowerCase().startsWith("https://")){
        try{
          GridPilot.getClassMgr().getSSL().activateSSL();
        }
        catch(Exception e){
          e.printStackTrace();
          GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not activate SSL.");
        }
        break;
      }
    }
  }

  public static boolean isNumeric(String dep){
    try{
      Integer.parseInt(dep);
    }
    catch(Exception e){
      return false;
    }
    return true;
  }

  /**
   * Parse the file transfer ID into
   * {protocol, requestType (get|put|copy), requestId, statusIndex, srcTurl, destTurl, srmSurl, shortID}.
   * @param fileTransferID   the unique ID of this transfer.
   */
  // TODO: make a class SRMFileTransfer and use it instead of this hackish array solution.
  public static String [] parseSrmFileTransferID(String fileTransferID) throws SRMException {
    
    String protocol = null;
    String requestType = null;
    String requestId = null;
    String statusIndex = null;
    String srcTurl = null;
    String destTurl = null;
    String srmSurl = null;
    String shortID = null;

    String [] idArr = split(fileTransferID, "::");
    String [] head = split(idArr[0], "-");
    if(idArr.length<3){
      throw new SRMException("ERROR: malformed ID "+fileTransferID);
    }
    try{
      protocol = head[0];
      requestType = head[1];
      requestId = idArr[1];
      statusIndex = idArr[2];
      String turls = fileTransferID.replaceFirst(idArr[0]+"::", "");
      turls = turls.replaceFirst(idArr[1]+"::", "");
      turls = turls.replaceFirst(idArr[2]+"::", "");
      String [] turlArray = split(turls, "' '");
      srcTurl = turlArray[0].replaceFirst("'", "");
      destTurl = turlArray[1].replaceFirst("'", "");
      srmSurl = turlArray[2].replaceFirst("'", "");
    }
    catch(Exception e){
      e.printStackTrace();
      throw new SRMException("ERROR: could not parse ID "+fileTransferID+". "+e.getMessage());
    }
    try{
      // First resolve transport protocol
      GlobusURL url = null;
      String transportProtocol = null;
      if(requestType.equals("get")){
        url = new GlobusURL(srcTurl);
        transportProtocol = url.getProtocol();
        shortID = transportProtocol+"-copy"+"::'"+srcTurl+"' '"+destTurl+"'";
      }
      else if(requestType.equals("put")){
        url = new GlobusURL(destTurl);
        transportProtocol = url.getProtocol();
        shortID = transportProtocol+"-copy"+"::'"+srcTurl+"' '"+destTurl+"'";
      }
      Debug.debug("Found short ID: "+shortID, 3);
    }
    catch(Exception e){
      Debug.debug("WARNING: could not get short ID for "+fileTransferID+"; SRM probably not ready.", 2);
    }
    return new String [] {protocol, requestType, requestId, statusIndex, srcTurl, destTurl, srmSurl, shortID};
  }

  public static boolean isLocalFileName(String src){
    if(/*Linux local file*/(src.matches("^file:~[^:]*") || src.matches("^file:/[^:]*") || src.startsWith("/") || src.startsWith("~")) ||
        /*Windows local file*/(src.matches("^\\w:.*") || src.matches("^file:/*\\w:.*"))){
      return true;
    }
    return false;
  }
  
  public static String getURL(String url, JComponent jcb, boolean onlyDirs,
      String message) throws IOException{
    Debug.debug("URL: "+url, 3);
    JFrame frame = (JFrame) SwingUtilities.getWindowAncestor(GridPilot.getClassMgr().getGlobalFrame().getRootPane());
    frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    final String finUrl = url;
    final String finBaseUrl = "";//url;
    BrowserPanel wb = null;
    try{
      wb = new BrowserPanel(
                      GridPilot.getClassMgr().getGlobalFrame(),
                      message,
                      finUrl,
                      finBaseUrl,
                      true,
                      false,
                      true,
                      jcb,
                      onlyDirs?"*/":"",
                      false);
    }
    catch(Exception eee){
      frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      Debug.debug("Could not open URL "+finBaseUrl+". "+eee.getMessage(), 1);
      eee.printStackTrace();
      GridPilot.getClassMgr().getStatusBar().setLabel("Could not open URL "+finBaseUrl+". "+eee.getMessage());
      ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()/*,"",""*/); 
      try{
        confirmBox.getConfirm("URL could not be opened",
                             "The URL "+finBaseUrl+" could not be opened. \n"+eee.getMessage(),
                          new Object[] {mkOkObject(confirmBox.getOptionPane())});
      }
      catch(Exception eeee){
        Debug.debug("Could not get confirmation, "+eeee.getMessage(), 1);
      }
    }
    frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
    if(wb==null || wb.getLastURL()==null || !wb.getLastURL().startsWith(finBaseUrl)){
      Debug.debug("ERROR: Could not open URL "+finBaseUrl+":"+wb.getLastURL(), 1);
      throw new IOException("No download directory; "+wb.getLastURL());
    }
    //GridPilot.getClassMgr().getStatusBar().setLabel("");
    String ret = wb.getLastURL().substring(finBaseUrl.length());
    Debug.debug("Returning last URL "+ret, 2);
    if(onlyDirs && !ret.endsWith("/")){
      throw new IOException("ERROR: not a directory: "+ret);
    }
    return ret;
  }

  public static int getMaxSimultaneousRunningJobs(String csName) {
    MyLogFile logFile = GridPilot.getClassMgr().getLogFile();
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    int ret = -1;
    String tmp = configFile.getValue(csName, "max simultaneous running");
    try{
      ret = Integer.parseInt(tmp);
    }
    catch(Exception e){
      e.printStackTrace();
      logFile.addInfo("WARNING: Value of \"max simultaneous running\" is not"+
                                  " defined properly for "+csName);
    }
    tmp = configFile.getValue("Computing systems", "max simultaneous running");
    if(ret==-1){
      try{
        ret = Integer.parseInt(tmp);
      }
      catch(Exception e){
        logFile.addMessage("WARNING: Value of \"max simultaneous running\" is not"+
                                    " defined properly.", e);
      }
    }
    if(ret==-1){
      return 1;
    }
    return ret;
  }

  public static int getMaxSimultaneousPreprocessingJobs(String csName) {
    MyLogFile logFile = GridPilot.getClassMgr().getLogFile();
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    int ret = -1;
    String tmp = configFile.getValue(csName, "max simultaneous preprocessing");
    try{
      ret = Integer.parseInt(tmp);
    }
    catch(Exception e){
      e.printStackTrace();
      logFile.addInfo("WARNING: Value of \"max simultaneous running\" is not"+
                                  " defined properly for "+csName);
    }
    tmp = configFile.getValue("Computing systems", "max simultaneous preprocessing");
    if(ret==-1){
      try{
        ret = Integer.parseInt(tmp);
      }
      catch(Exception e){
        logFile.addMessage("WARNING: Value of \"max simultaneous preprocessing\" is not"+
                                    " defined properly.", e);
      }
    }
    if(ret==-1){
      return 1;
    }
    return ret;
  }

}
