package gridpilot;

import java.awt.Color;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;
//import java.net.Socket;
//import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JEditorPane;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.UIManager;
import javax.swing.event.HyperlinkEvent;
import javax.swing.event.HyperlinkListener;
import javax.swing.plaf.UIResource;
import javax.swing.text.JTextComponent;

import org.globus.gsi.CertUtil;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.OpenSSLKey;
import org.globus.gsi.X509ExtensionSet;
import org.globus.gsi.bc.BouncyCastleCertProcessingFactory;
import org.globus.gsi.bc.BouncyCastleOpenSSLKey;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

/**
 * @author Frederik.Orellana@cern.ch
 *
 */

public class Util{
  
  // Default when interrupting threads. Can be overridden by argument.
  private static boolean ASK_BEFORE_INTERRUPT = true;
  
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
    //StringTokenizer tok = new StringTokenizer(s, delim);
    //int len = tok.countTokens();
    //String [] res = new String[len];
    String [] res = s.split(delim);
    int len = res.length;
    for (int i=0 ; i<len ; i++){
      //res[i] = tok.nextToken();
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
 
  public static JEditorPane createCheckPanel(
      final JFrame frame, 
      final String name, final JTextComponent jt,
      final DBPluginMgr dbPluginMgr){
    //final Frame frame = (Frame) SwingUtilities.getWindowAncestor(getRootPane());
    String markup = "<font size=-1 face=sans-serif><b>"+name+" : </b></font><br>"+
      "<a href=\"http://check/\">browse</a>";
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
            frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            final String finUrl = httpScript;
            final String finBaseUrl = "";//url;
            (new MyThread(){
              public void run(){
                BrowserPanel wb = null;
                
                String [] urls = split(finUrl);
                if(urls.length==0){
                  urls = new String [] {""};
                }
                for(int i=0; i<urls.length; ++i){
                  try{
                    if(urls[i].startsWith("/")){
                      urls[i] = (new File(urls[i])).toURL().toExternalForm();
                    }
                    else if(urls[i].startsWith("file://")){
                      urls[i] = (new File(urls[i].substring(6))).toURL().toExternalForm();
                    }
                    else if(urls[i].startsWith("file://")){
                      urls[i] = (new File(urls[i].substring(5))).toURL().toExternalForm();
                    }
                  }
                  catch(Exception ee){
                    Debug.debug("Could not open URL "+urls[i]+". "+ee.getMessage(), 1);
                    ee.printStackTrace();
                    GridPilot.getClassMgr().getStatusBar().setLabel("Could not open URL "+urls[i]);
                    return;
                  }
                  Debug.debug("URL: "+urls[i], 3);
                  
                  try{
                    wb = new BrowserPanel(//GridPilot.getClassMgr().getGlobalFrame(),
                                    frame,
                                    "Choose file",
                                    //finUrl,
                                    urls[i],
                                    finBaseUrl,
                                    true,
                                    /*filter*/true,
                                    /*navigation*/true,
                                    null,
                                    null);
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
                    // Set the text: the URL browsed to with base URL removed
                    //jt.setText(wb.lastURL.substring(finBaseUrl.length()));
                    urls[i] = wb.lastURL.substring(finBaseUrl.length());
                    //GridPilot.getClassMgr().getStatusBar().setLabel("");
                  }
                  else{
                    // Don't do anything if we cannot get a URL
                    Debug.debug("ERROR: Could not open URL "+finBaseUrl, 1);
                  }
                }
                
                jt.setText(Util.arrayToString(urls));
                frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
                //GridPilot.getClassMgr().getStatusBar().setLabel("");
              }
            }).start();
          }
        }
      }
    });
    return checkPanel;
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
    Debug.debug("argument types: "+Util.arrayToString(argTypes), 3);
    Debug.debug("arguments: "+Util.arrayToString(args), 3);
    try{
      //Class newClass = this.getClass().getClassLoader().loadClass(dbClass);
      Class newClass = (new MyClassLoader()).loadClass(className);
      ret = (newClass.getConstructor(argTypes).newInstance(args));
      Debug.debug("plugin " + "(" + className + ") loaded, "+ret.getClass(), 2);
    }
    catch(Exception e){
      //e.printStackTrace();
      Debug.debug("WARNING: failed to load class with standard method, trying findClass. "+
          e.getMessage(), 1);
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

  public static String getFileName(String str){
    return getName("Enter file name", str);
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
  
  public static String clearFile(String _line){
    String line = _line;
    line = line.replaceFirst("^file:///", "/");
    line = line.replaceFirst("^file://", "/");
    line = line.replaceFirst("^file:/", "/");
    line = line.replaceFirst("^file:", "");
    return line;
  }

  public static String addFile(String _line){
    String line = _line;
    line = line.replaceFirst("^/", "file:///");
    return line;
  }

  public static String [] getGridPassword(String keyFile, String certFile, String password){
    
    JPanel panel = new JPanel(new GridBagLayout());
    JTextPane tp = new JTextPane();
    tp.setText("");
    tp.setEditable(false);
    tp.setOpaque(false);
    tp.setBorder(null);
    
    if(keyFile.startsWith("~")){
      try{
        keyFile = System.getProperty("user.home") + File.separator +
        keyFile.substring(2);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    if(certFile.startsWith("~")){
      try{
        certFile = System.getProperty("user.home") + File.separator +
        certFile.substring(2);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }

    JPasswordField passwordField = new JPasswordField(password, 24);
    JTextField keyField = new JTextField(keyFile, 24);
    JTextField certField = new JTextField(certFile, 24);

    panel.add(tp, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
        GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));
    panel.add(new JLabel("Password:"),
        new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0,
            GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));
    panel.add(passwordField, new GridBagConstraints(1, 1, 1, 1, 1.0, 1.0,
        GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));

    panel.add(new JLabel("Key file:"),
        new GridBagConstraints(0, 2, 1, 1, 1.0, 1.0,
        GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));
    panel.add(keyField, new GridBagConstraints(1, 2, 1, 1, 1.0, 1.0,
        GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));

    panel.add(new JLabel("Cert file:"),
        new GridBagConstraints(0, 3, 1, 1, 1.0, 1.0,
        GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));
    panel.add(certField, new GridBagConstraints(1, 3, 1, 1, 1.0, 1.0,
        GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));
    Debug.debug("showing dialog", 3);
    
    if(GridPilot.splash!=null){
      GridPilot.splash.hide();
    }
    
    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), panel,
        "Enter grid password", JOptionPane.OK_CANCEL_OPTION);
    Debug.debug("showing dialog done", 3);
    
    if(GridPilot.splash!=null){
      GridPilot.splash.show();
    }
    
    if(choice!=JOptionPane.OK_OPTION){
      throw new IllegalArgumentException("Cancelling");
    }
    else{
      return new String [] {
          new String(passwordField.getPassword()),
          keyField.getText(), certField.getText()
          };
    }
  }

  public static GlobusCredential createProxy(
      String userKeyFilename,
      String userCertFilename,
      String password,
      int lifetime,
      int strength)throws IOException, GeneralSecurityException{
    OpenSSLKey key;

    key = new BouncyCastleOpenSSLKey(userKeyFilename);

   // get user certificate
    X509Certificate userCert = CertUtil.loadCertificate(userCertFilename);
    return createProxy(key, userCert, password, lifetime, strength);

  }

  public static GlobusCredential createProxy(OpenSSLKey key,
     X509Certificate userCert, String password, int lifetime, int strength)
     throws InvalidKeyException, GeneralSecurityException{

    // decrypt the password
    if(key.isEncrypted()){
        key.decrypt(password);
      }

    // type of proxy. Hardcoded, as it's the only thing we'll use.
    int proxyType = GSIConstants.DELEGATION_FULL;

    // factory for proxy generation
    BouncyCastleCertProcessingFactory factory = BouncyCastleCertProcessingFactory.getDefault();

    GlobusCredential myCredentials = factory.createCredential(new X509Certificate[] { userCert }, key.getPrivateKey(), strength, lifetime,
            proxyType, (X509ExtensionSet) null);
    return myCredentials;
  }

  public static String setupDefaultCACertificates() throws IOException {
    try{
      // get a temp name
      File tmpFile = File.createTempFile(/*prefix*/"certificates", /*suffix*/"");
      String tmpDir = tmpFile.getAbsolutePath();
      tmpFile.delete();
      LocalStaticShellMgr.mkdirs(tmpDir);
      // hack to have the diretory deleted on exit
      GridPilot.tmpConfFile.put(tmpDir, new File(tmpDir));
      // fill the directory with certificates from resources/certificates
      Debug.debug("Reading list of files from "+
          GridPilot.resourcesPath+"ca_certs_list.txt", 3);
      Debug.debug("will save to "+tmpDir, 3);
      URL fileURL = GridPilot.class.getResource(
          GridPilot.resourcesPath+"ca_certs_list.txt");
      HashSet certFilesList = new HashSet();
      BufferedReader in = new BufferedReader(new InputStreamReader(fileURL.openStream()));
      String line;
      while((line = in.readLine())!=null){
        certFilesList.add(line);
      }
      in.close();
      String fileName;
      PrintWriter out = null;
      boolean ok = true;
      for(Iterator it=certFilesList.iterator(); it.hasNext();){
        fileName = it.next().toString();
        Debug.debug("extracting "+fileName, 3);
        try{
          fileURL = GridPilot.class.getResource(
              GridPilot.resourcesPath+"certificates/"+fileName);
          in = new BufferedReader(new InputStreamReader(fileURL.openStream()));
          out = new PrintWriter(
              new FileWriter(new File(tmpDir, fileName)));
          // if this is an X509 certificate with the
          // descripion in plain text first, just grab the
          // certificate part.
          // It is ridiculous, but to get the generateCertificate
          // to accept the file it must start with
          //-----BEGIN CERTIFICATE-----
          // and end with
          //-----END CERTIFICATE-----
          if(fileName.endsWith(".0")){
            ok = false;
          }
          else{
            ok = true;
          }
          while((line = in.readLine())!=null){
            if(fileName.endsWith(".0") &&
                line.matches(".*BEGIN CERTIFICATE.*")){
              ok = true;
            }
            if(ok){
              out.println(line);
            }
            if(fileName.endsWith(".0") &&
                line.matches(".*END CERTIFICATE.*")){
              ok = false;
            }
          }
          in.close();
          out.close();
        }
        catch(IOException e){
          Debug.debug("WARNING: Could not read CA certificate "+fileName+
              ". Skipping.", 2);
        }
        finally{
          try{
            in.close();
            out.close();
          }
          catch(Exception ee){
          }
        }
      }
      return tmpDir;
    }
    catch(IOException e){
      GridPilot.getClassMgr().getLogFile().addMessage(
          "ERROR: could not setup ca certificates. " +
          "Grid authentication will not work.", e);
      //e.printStackTrace();
      throw e;
    }
  }
    
  public static File getProxyFile(){
    return new File("/tmp/x509up_"+System.getProperty("user.name"));
  }
  
  public static GSSCredential initGridProxy() throws IOException, GSSException{
    
    ExtendedGSSManager manager = (ExtendedGSSManager) ExtendedGSSManager.getInstance();
    //String proxyFile = "/tmp/x509up_u501";
    File proxy = getProxyFile();
    GSSCredential credential = null;
        
    // first just try and load proxy file from default UNIX location
    try{
      if(proxy.exists()){
        byte [] data = new byte[(int) proxy.length()];
        FileInputStream in = new FileInputStream(proxy);
        Debug.debug("reading proxy "+proxy.getAbsolutePath(), 3);
        in.read(data);
        in.close();
        credential = 
          manager.createCredential(data,
                                   ExtendedGSSCredential.IMPEXP_OPAQUE,
                                               GSSCredential.DEFAULT_LIFETIME,
                                               // TODO: set proxy life time
                                               //GridPilot.proxyTimeValid,
                                               null, // use default mechanism - GSI
                                               GSSCredential.INITIATE_AND_ACCEPT);
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    // if credential ok, return
    if(credential!=null && credential.getRemainingLifetime()>=GridPilot.proxyTimeLeftLimit){
      Debug.debug("proxy ok", 3);
      return credential;
    }
    // if no valid proxy, init
    else{
      Debug.debug("proxy not ok: "+credential+": "+
          (credential!=null ? credential.getRemainingLifetime() : 0)+"<-->"+
          GridPilot.proxyTimeLeftLimit, 3);
      // Create new proxy
      Debug.debug("creating new proxy", 3);
      String [] password = null;
      GlobusCredential cred = null;
      FileOutputStream out = null;
      for(int i=0; i<=3; ++i){
        try{
          password = getGridPassword(GridPilot.keyFile, GridPilot.certFile,
              GridPilot.keyPassword);
        }
        catch(IllegalArgumentException e){
          // cancelling
          break;
        }
        try{
          cred = createProxy(password[1], password[2],
             password[0], GridPilot.proxyTimeValid, 512);
          credential = new GlobusGSSCredentialImpl(cred, GSSCredential.INITIATE_AND_ACCEPT) ;
          // Keep password in memory - needed by mysql plugin
          Debug.debug("Setting grid password to "+password[0], 3);
          GridPilot.keyPassword = password[0];
        }
        catch(Exception e){
          continue;
        }
        try{
          // if we managed to create proxy, save it to default location
          out = new FileOutputStream(proxy);
          cred.save(out);
          out.close();
          return credential;
        }
        catch(Exception e){
          Debug.debug("ERROR: problem saving proxy. "+e.getMessage(), 3);
          e.printStackTrace();
          break;
        }          
      }
      throw new IOException("ERROR: could not initialize grid proxy");
    }
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
  
  public static void dos2unix(File file) throws IOException{
    try{
      File tempFile = new File(file.getAbsolutePath() + ".tmp");
      BufferedReader in = new BufferedReader(new FileReader(file));
      BufferedWriter out = new BufferedWriter(new FileWriter(tempFile));
      int c;

      while((c = in.read()) != -1)
        {
          if(c != '\r')
            out.write(c);
        }

      in.close();
      out.close();

      file.delete();
      tempFile.renameTo(file);
    }
    catch(IOException e){
      throw e;
    }
  }
  
  /**
   * Asks the user if he wants to interrupt a plug-in
   */
  private static boolean askForInterrupt(String name, String fct){
    String msg = "No response from " + name +
                 " for " + fct + "\n"+
                 "Do you want to interrupt it ?";
    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), msg, "No response from plugin",
        JOptionPane.YES_NO_OPTION);
    if(choice==JOptionPane.YES_OPTION){
      return true;
    }
    else{
      return false;
    }
  }

  /**
   * Waits the specified <code>MyThread</code> during maximum <code>timeOut</code> ms.
   * @return true if <code>t</code> ended normally, false if <code>t</code> has been interrupted
   */
  public static boolean waitForThread(MyThread t, String name, int _timeOut,
      String function){
    return waitForThread(t, name, _timeOut, function, null);
  }

  public static boolean waitForThread(MyThread t, String name, int _timeOut,
      String function, Boolean _askForInterrupt){
    int timeOut = _timeOut;
    boolean askForInterrupt = ASK_BEFORE_INTERRUPT;
    if(_askForInterrupt!=null){
      askForInterrupt = _askForInterrupt.booleanValue();
    }
    do{
      try{
        t.join(timeOut);
      }
      catch(InterruptedException ie){}

      if(t.isAlive()){
        if(!askForInterrupt || askForInterrupt(name, function)){
          GridPilot.getClassMgr().getLogFile().addMessage("No response from plugin " +
              name + " for " + function);
          t.interrupt();
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
    Collections.addAll(arl, arr);
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
    fc.setDialogTitle("Choose dowload directory");
    int returnVal = fc.showOpenDialog(parent);
    if (returnVal==JFileChooser.APPROVE_OPTION){
      file = fc.getSelectedFile();
      Debug.debug("Opening: " + file.getName(), 2);
    }
    else{
      Debug.debug("Not opening file", 3);
    }
    return file;
  }

  public static JTextArea createTextArea(){
    JTextArea ta = new JTextArea();
    ta.setBorder(new JTextField().getBorder());
    ta.setWrapStyleWord(true);
    ta.setLineWrap(true);
    return ta;
  }
  
  public static JTextArea createGrayTextArea(String str){
    JTextArea jval = new JTextArea(str);
    ((JTextArea) jval).setLineWrap(true);
    ((JTextArea) jval).setWrapStyleWord(true);
    ((JTextArea) jval).setEditable(false);
    setBackgroundColor(jval);
    return jval;
  }

  public static void activateSsl(GlobusCredential globusCred) throws KeyStoreException,
  NoSuchAlgorithmException, CertificateException,
  IOException, GSSException {
 
    String tmpPwd = "whateva";
    FileInputStream fis = null;
    CertificateFactory cf = null;
    Certificate cert = null;
    
    // The key store
    //
    // We save the keystore to a temporary file.
    // This seems to be the only way to get connector/j to use it...
    // get a temp name
    File tmpFile = File.createTempFile(/*prefix*/"keystore", /*suffix*/"");
    String keystorePath = tmpFile.getAbsolutePath();
    // hack to have the diretory deleted on exit
    GridPilot.tmpConfFile.put(keystorePath, new File(keystorePath));
    // TODO: drop first (last?) element?
    X509Certificate [] chain = globusCred.getCertificateChain();
    X509Certificate [] idChain = new X509Certificate[chain.length-1];
    Vector idVector = new Vector();
    for(int i=0; i<chain.length; ++i){
      Debug.debug(chain[i].getSubjectDN().getName()+"<->"+ globusCred.getSubject(), 3);
      if(!chain[i].getSubjectDN().getName().equals(globusCred.getSubject())){
        idVector.add(chain[i]);
      }
      else{
        Debug.debug("Removing proxy certificate from chain", 2);
      }
    }
    for(int i=0; i<idChain.length; ++i){
      idChain[i] = (X509Certificate) idVector.get(i);
    }
    
    // This does NOT work: the proxy certificate cannot be used for
    // standard ssl authentication, because the user certificate does
    // not containg CA privileges.
    /*
    KeyStore ks = KeyStore.getInstance("JKS");
    PrivateKey mypriv = globusCred.getPrivateKey();
    ks.load(null, null);
    ks.setKeyEntry("mycert", mypriv, tmpPwd.toCharArray(), chain);
    FileOutputStream fos = new FileOutputStream(keystorePath);
    ks.store(fos, tmpPwd.toCharArray());
    fos.close();
    */
    
    // Instead we use the real certificate/proxy
    String keyFile = GridPilot.keyFile;
    if(keyFile.startsWith("~")){
      try{
        keyFile = System.getProperty("user.home") + File.separator +
        keyFile.substring(2);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    try{
      // Load the private key (in PKCS#8 DER encoding).
      /*
      File userKeyFile = new File(keyFile);
      Debug.debug("loading "+keyFile, 3);
      byte[] encodedKey = new byte[(int)userKeyFile.length()];
      fis = new FileInputStream(userKeyFile);
      fis.read(encodedKey);
      fis.close();
      KeyFactory rSAKeyFactory = KeyFactory.getInstance("RSA");
      PrivateKey privateKey = rSAKeyFactory.generatePrivate(
         new PKCS8EncodedKeySpec(encodedKey));
      */
      
      // We need the password to decrypt the pricate key.
      // If the proxy was loaded from a previous session, we have to
      // ask again.
      if(GridPilot.keyPassword==null){
        // delete proxy and reinitialize - this will set GridPilot.keyPassword
        Debug.debug("reinitializing grid proxy to get password", 3);
        Util.getProxyFile().delete();
        GridPilot.getClassMgr().gridProxyInitialized = Boolean.FALSE;
        GridPilot.getClassMgr().credential = null;
        GridPilot.getClassMgr().getGridCredential();
      }
      Debug.debug("Decrypting private key with password "+GridPilot.keyPassword, 3);
      BouncyCastleOpenSSLKey key = new BouncyCastleOpenSSLKey(keyFile);
      key.decrypt(GridPilot.keyPassword);     
      addToKeyStore(key.getPrivateKey(), GridPilot.keyPassword.toCharArray(), 
         idChain, keystorePath);
    }
    catch(Exception e){
      Debug.debug("Error adding certificate to keystore\n" + e, 2);
      e.printStackTrace();
      try{                   
        fis.close();
      }
      catch(Exception r){
      }
      return;
    }
    
    // activate the keystore
    System.setProperty("javax.net.ssl.keyStore", keystorePath);
    System.setProperty("javax.net.ssl.keyStorePassword", GridPilot.keyPassword);

    // The trust store
    //
    Debug.debug("Reading list of files from "+
        GridPilot.resourcesPath+"ca_certs_list.txt", 3);
    URL fileURL = GridPilot.class.getResource(
        GridPilot.resourcesPath+"ca_certs_list.txt");
    HashSet certFilesList = new HashSet();
    BufferedReader in = new BufferedReader(new InputStreamReader(fileURL.openStream()));
    String line;
    while((line = in.readLine())!=null){
      certFilesList.add(line);
    }
    in.close();
    
    // The truststore will be saved to a temporary file.
    // This seems to be the only way to get connector/j to use it...
    // get a temp name
    tmpFile = File.createTempFile(/*prefix*/"truststore", /*suffix*/"");
    String truststorePath = tmpFile.getAbsolutePath();
    // hack to have the diretory deleted on exit
    GridPilot.tmpConfFile.put(truststorePath, new File(truststorePath));

    String caCertsTmpdir = GridPilot.getClassMgr().getCaCertsTmpDir();
    String fileName = null;
    File caCertFile = null;
    for(Iterator it=certFilesList.iterator(); it.hasNext();){
      fileName = it.next().toString();
      if(!fileName.toLowerCase().endsWith(".0")){
        continue;
      }
      caCertFile =  new File(caCertsTmpdir, fileName);
      Debug.debug("loading "+caCertFile.getCanonicalPath(), 3);
      try{
        fis = new FileInputStream(caCertFile);
        cf = CertificateFactory.getInstance("X.509");
        cert = (X509Certificate) cf.generateCertificate(fis);
        String alias = fileName.substring(0, fileName.length()-2);
        Debug.debug("Adding the cert with alias " + alias, 2);
        addToTrustStore(tmpPwd.toCharArray(), alias, cert,
           truststorePath);
        fis.close();   
      }
      catch(Exception e){
        Debug.debug("Error adding certificate to keystore\n" + e, 2);
        e.printStackTrace();
        try{                   
          fis.close();
        }
        catch(Exception r){
        }
        return;
      }        
    }
    // activate truststore 
    System.setProperty("javax.net.ssl.trustStore", truststorePath);
    System.setProperty("javax.net.ssl.trustStorePassword", tmpPwd);    
  }


  private static void addToKeyStore(PrivateKey privateKey, char [] password,
     X509Certificate [] chain, String keyFilePath)
     throws KeyStoreException, FileNotFoundException,
     IOException, CertificateException, NoSuchAlgorithmException{
    KeyStore keystore = KeyStore.getInstance("JKS");
    // Load the keystore contents
    Debug.debug("Opening key file", 3);
    FileInputStream in = new FileInputStream(keyFilePath);
    Debug.debug("Loading input from "+keyFilePath+" into keystore", 2);
    Debug.debug("available "+in.available(), 3);
    if(in.available()==0){ 
      keystore.load(null, password);
    }
    else{ 
      keystore.load(in, password);
    }
    in.close();
    // Add the key
    keystore.setKeyEntry("mycert", privateKey, password, chain);
    // Save the new keystore contents
    FileOutputStream out = new FileOutputStream(keyFilePath);
    keystore.store(out, password);
    out.close();
  }

  private static void addToTrustStore(char [] password, String alias,
     Certificate cert, String keyFilePath)
     throws KeyStoreException, FileNotFoundException,
     IOException, CertificateException, NoSuchAlgorithmException{
    KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
    // Load the keystore contents
    Debug.debug("Opening key file", 3);
    FileInputStream in = new FileInputStream(keyFilePath);
    Debug.debug("Loading input from "+keyFilePath+" into keystore", 2);
    Debug.debug("available "+in.available(), 3);
    if(in.available()==0){ 
      keystore.load(null, password);
    }
    else{ 
      keystore.load(in, password);
    }
    in.close();
    // Add the certificate
    keystore.setCertificateEntry(alias, cert);
    // Save the new keystore contents
    FileOutputStream out = new FileOutputStream(keyFilePath);
    keystore.store(out, password);
    out.close();
  }

  public static Connection sqlConnection(String driver, String database,
      String user, String passwd, boolean gridAuth) throws SQLException{
    return sqlConnection(driver, database,
        user, passwd, gridAuth, null, null);
  }

  public static Connection sqlConnection(String driver, String database,
      String user, String passwd, boolean gridAuth, String _connectTimeout,
      String _socketTimeout) throws SQLException{
    Connection conn = null;
    // timeouts in milliseconds
    String connectTimeout = null;
    String socketTimeout = null;
    if(_connectTimeout==null){
      connectTimeout = "10000";
    }
    else{
      connectTimeout = _connectTimeout;
    }
    if(_socketTimeout==null){
      socketTimeout = "30000";
    }
    else{
      socketTimeout = _socketTimeout;
    }
    try{
      Class.forName(driver).newInstance();
    }
    catch(Exception e){
      String error = "Could not load the driver "+driver+". ";
      GridPilot.getClassMgr().getLogFile().addMessage(error, e);
      Debug.debug(error, 1);
      e.printStackTrace();
      throw new SQLException(error);
    }
    try{
      if(gridAuth){
        conn = DriverManager.getConnection(database+
            "?user="+user+"&password=&useSSL=true&" +
                    "connectionTimeout="+connectTimeout+
                    "&socketTimeout="+socketTimeout);
      }
      else{
        conn = DriverManager.getConnection(database+
            "?user="+user+"&password="+passwd+
            "&connectionTimeout="+connectTimeout+
            "&socketTimeout="+socketTimeout);
      }
    }
    catch(Exception e){
      String error = "Could not connect to database "+database+
          " with "+user+":"+passwd;
      e.printStackTrace();
      throw new SQLException(error);
    }  
    try{
      conn.setAutoCommit(true);
    }
    catch(Exception e){
      Debug.debug("failed setting auto commit to true: "+e.getMessage(), 2);
    }
    return conn;
  }
  
  public static int showResult(String [] cstAttrNames, String [] cstAttr, String title,
      int moreThanOne){
    
    Object[] showResultsOptions = null;
    Object[] showResultsOptions2 = {"OK", "Skip", "OK for all", "Skip all"};
    Object[] showResultsOptions1 = {"OK", "Skip"};
    Object[] showResultsOptions0 = {"OK"};

    JPanel pResult = new JPanel(new GridBagLayout());
    int row = 0;
    JComponent jval;
    boolean noTextArea = true;
    for(int i =0; i<cstAttr.length; ++i, ++row){
      if(cstAttrNames[i].equalsIgnoreCase("initLines") ||
          cstAttrNames[i].equalsIgnoreCase("outFileMapping") ||
          cstAttrNames[i].equalsIgnoreCase("pfns")){
        jval = Util.createGrayTextArea(cstAttr[i]);
        noTextArea = false;
      }
      else{
        jval = new JLabel(cstAttr[i].toString());
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

    JOptionPane op = null;
    if(moreThanOne==0){
      showResultsOptions = showResultsOptions0;
      op = new JOptionPane(sp,
          JOptionPane.QUESTION_MESSAGE,
          JOptionPane.OK_OPTION,
          null,
          showResultsOptions0,
          showResultsOptions2[0]);
     }
    else if(moreThanOne==1){
      showResultsOptions = showResultsOptions1;
      op = new JOptionPane(sp,
         JOptionPane.QUESTION_MESSAGE,
         JOptionPane.YES_NO_CANCEL_OPTION,
         null,
         showResultsOptions1,
         showResultsOptions2[0]);
   }
   else if(moreThanOne==2){
     showResultsOptions = showResultsOptions2;
     op = new JOptionPane(sp,
         JOptionPane.QUESTION_MESSAGE,
         JOptionPane.YES_NO_CANCEL_OPTION,
         null,
         showResultsOptions2,
         showResultsOptions2[0]);
    }

    JDialog dialog = op.createDialog(JOptionPane.getRootFrame(), title);    
    dialog.requestFocusInWindow();    
    dialog.setResizable(true);
    dialog.setVisible(true);
    dialog.dispose();

    Object selectedValue = op.getValue();

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

  public static String [][] getValues(Connection conn, String table, String id,
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
      Debug.debug(">> "+req, 3);
      ResultSet rset = conn.createStatement().executeQuery(req);
      while(rset.next()){
        for(int i=0; i<fields.length;i++){
          if(fields[i].endsWith("FK") || fields[i].endsWith("ID") &&
              !fields[i].equalsIgnoreCase("grid") ||
              fields[i].endsWith("COUNT")){
            int tmp = rset.getInt(fields[i]);
            values[i] = Integer.toString(tmp);
          }
          else{
            values[i] = rset.getString(fields[i]);
          }
        }
        resultVector.add(values);
      }
      rset.close();
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
    Debug.debug("jobDef dataset reference for "+dbName
        +" : "+arrayToString(ret), 2);
    return ret;
  }

  public static String getIdentifierField(String dbName, String table){
    String ret = GridPilot.getClassMgr().getConfigFile().getValue(dbName, table+" identifier");
    if(ret==null || ret.equals("")){
      ret = "identifier";
    }
    Debug.debug("Identifier for "+dbName+" - "+table+" : "+ret, 2);
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
    Debug.debug("Name for "+dbName+" - "+table+" : "+ret, 2);
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
    Debug.debug("Version for "+dbName+" - "+table+" : "+ret, 2);
    return ret;
  }

  public static String [] getJobDefDatasetReference(String dbName){
    String [] ret = GridPilot.getClassMgr().getConfigFile().getValues(dbName,
      "jobDefinition dataset reference");
    if(ret==null || ret.length<2){
      ret = new String [] {"name", "datasetName"};
    }
    Debug.debug("jobDef dataset reference for "+dbName
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
  
}
