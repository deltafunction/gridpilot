package gridpilot;

import java.awt.*;
import java.awt.event.*;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

import javax.swing.*;
import javax.swing.plaf.basic.BasicEditorPaneUI;
import javax.swing.text.Document;
import javax.swing.text.html.*;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.*;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Vector;

import org.globus.ftp.Buffer;
import org.globus.ftp.DataSink;
import org.globus.ftp.GridFTPClient;

import gridpilot.fsplugins.gridftp.GridFTPFileSystem;


/**
 * Box showing URL.
 */
public class BrowserPanel extends JDialog implements ActionListener{

  private static final long serialVersionUID = 1L;
  private JPanel panel = new JPanel(new BorderLayout());
  private JButton bOk = new JButton();
  private JButton bNew = new JButton();
  private JButton bDelete = new JButton();
  private JButton bUpload = new JButton();
  private JButton bDownload = new JButton();
  private JButton bSave = new JButton();
  protected JButton bCancel = new JButton();
  private JLabel currentUrlLabel = new JLabel("");
  private JTextField jtFilter = new JTextField("", 24);
  private JPanel pButton = new JPanel(new FlowLayout());
  private JEditorPane ep = new JEditorPane();
  private StatusBar statusBar = null;
  private String thisUrl;
  private String baseUrl;
  private String origUrl;
  private String filter = "";
  private boolean withFilter = false;
  private boolean withNavigation = false;
  protected String lastURL = null;
  private String currentUrlString = "";
  private JTextField currentUrlTextField = new JTextField("", 32);
  private GridFTPFileSystem gridftpFileSystem = null;

  public BrowserPanel(JFrame _parent, String title, String url, 
      String _baseUrl, boolean modal, boolean _withFilter,
      boolean _withNavigation) throws Exception{
    super(_parent);
    baseUrl = _baseUrl;
    origUrl = url;
    withFilter = _withFilter;
    withNavigation = _withNavigation;
    
    setModal(modal);
    
    gridftpFileSystem = new GridFTPFileSystem();
    
    try{
      initGUI(title, url);
    }
    catch(IOException e){
      //e.printStackTrace();
      throw e;
    }
  }
  
  //Component initialization
  private void initGUI(String title, String url) throws Exception{
    
    enableEvents(AWTEvent.WINDOW_EVENT_MASK);
    this.getContentPane().setLayout(new BorderLayout());
 
    Debug.debug("Creating BrowserPanel with baseUrl "+baseUrl, 3);
    
    requestFocusInWindow();
    this.setTitle(title);
    
    bOk.setText("Ok");
    bOk.addActionListener(this);
    
    bNew.setText("New");
    bNew.addActionListener(this);
    
    bDelete.setText("Delete");
    bDelete.addActionListener(this);

    bUpload.setText("Put");
    bUpload.addActionListener(this);
    
    bDownload.setText("Get");
    bDownload.addActionListener(this);
    
    bSave.setText("Save");
    bSave.addActionListener(this);
    
    bCancel.setText("Cancel");
    bCancel.addActionListener(this);

    pButton.add(bOk);
    pButton.add(bNew);
    pButton.add(bUpload);
    pButton.add(bDownload);
    pButton.add(bDelete);
    pButton.add(bSave);
    pButton.add(bCancel);
    panel.add(pButton, BorderLayout.SOUTH);
    
    bNew.setEnabled(false);
    bUpload.setEnabled(false);
    bDownload.setEnabled(false);

    JScrollPane sp = new JScrollPane();

    sp.getViewport().add(ep);
    panel.add(sp, BorderLayout.CENTER);
    
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white,new Color(165, 163, 151)), ""/*title*/));

    this.getContentPane().add(panel, BorderLayout.CENTER);

    panel.setPreferredSize(new Dimension(520, 400));
    setResizable(true);
    
    JPanel topPanel = new JPanel(new GridBagLayout()); 
    
    if(withNavigation){
      JPanel jpNavigation = new JPanel(new GridBagLayout());
      jpNavigation.add(new JLabel("URL: "));
      jpNavigation.add(currentUrlTextField);
      topPanel.add(jpNavigation, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0
          ,GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 5, 0, 5),
          0, 0));
      // Listen for enter key in text field
      currentUrlTextField.addKeyListener(new KeyAdapter(){
        public void keyPressed(KeyEvent e){
          switch(e.getKeyCode()){
            case KeyEvent.VK_ENTER:
              try{
                if(!currentUrlTextField.getText().equals("")){
                  statusBar.setLabel("Opening URL...");
                }
                setDisplay(currentUrlTextField.getText());
              }
              catch(/*IO*/Exception ee){
                statusBar.setLabel("ERROR: could not open "+currentUrlTextField.getText()+
                    ". "+ee.getMessage());
                Debug.debug("ERROR: could not open "+currentUrlTextField.getText()+
                    ". "+ee.getMessage(), 1);
                ee.printStackTrace();
              }
          }
        }
      });
    }
    
    if(withFilter){
      JPanel jpFilter = new JPanel(new GridBagLayout());      
      if(!withNavigation){
        topPanel.add(currentUrlLabel, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0
            ,GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(0, 5, 0, 5),
            0, 0));
      }
      jpFilter.add(new JLabel("Filter: "));
      jpFilter.add(jtFilter);
      topPanel.add(jpFilter, new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0
          ,GridBagConstraints.WEST, GridBagConstraints.BOTH, new Insets(0, 5, 0, 5),
          0, 0));
      topPanel.setPreferredSize(new Dimension(520, 56));
                
      panel.add(topPanel, BorderLayout.NORTH);

      // Listen for enter key in text field
      jtFilter.addKeyListener(new KeyAdapter(){
        public void keyPressed(KeyEvent e){
          if(!getUrl().endsWith("/")){
            return;
          }
          switch(e.getKeyCode()){
            case KeyEvent.VK_ENTER:
              try{
                setDisplay(getUrl());
              }
              catch(/*IO*/Exception ee){
                Debug.debug("ERROR: could not filter. "+ee.getMessage(), 1);
                ee.printStackTrace();
              }
          }
        }
      });
      
    }  
    
    if(withNavigation || withFilter){
      panel.add(topPanel, BorderLayout.NORTH);
    }
    
    setUrl(url);
    
    //HTMLDocument d = new HTMLDocument();
    ep.addHyperlinkListener(new HyperlinkListener(){
      public void hyperlinkUpdate(final HyperlinkEvent e){
        if(e.getEventType()==HyperlinkEvent.EventType.ACTIVATED){
          if(e instanceof HTMLFrameHyperlinkEvent){
            ((HTMLDocument) ep.getDocument()).processHTMLFrameHyperlinkEvent(
               (HTMLFrameHyperlinkEvent) e);
          }
          else{
            setUrl(e.getDescription());
            ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            statusBar.setLabel("Opening URL...");
            (new MyThread(){
              public void run(){
                try{
                  if(e.getURL()!=null){
                    setDisplay(e.getURL().toExternalForm());
                  }
                  else{
                    setDisplay(e.getDescription());
                  }
                }
                catch(Exception ioe){
                  ioe.printStackTrace();
                }
              }
            }).run();               
          }
        }
      }
    } );
    
    statusBar = new StatusBar();
    this.getContentPane().add(statusBar, BorderLayout.SOUTH);  
    setDisplay(url);    
    if(withNavigation){
      statusBar.setLabel("Type in URL and hit return.");
    }
    
    ep.addPropertyChangeListener(new PropertyChangeListener(){
      public void propertyChange(PropertyChangeEvent event) {
        Debug.debug("Property changed: "+event.getPropertyName(), 3);
        if(event.getPropertyName().equalsIgnoreCase("document") ||
            event.getPropertyName().equalsIgnoreCase("page")){
          statusBar.setLabel("Page loaded");
          Debug.debug("Page loaded", 2);
        }
      }
    });
    
    panel.validate();
    sp.validate();
    ep.validate();
    pack();

    setSize(panel.getPreferredSize());
    setVisible(true);
    
    // Close window with ctrl+w
    jtFilter.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        if(!e.isControlDown()){
          return;
        }
        switch(e.getKeyCode()){
          case KeyEvent.VK_W:
            exit();
        }
      }
    });
    
    currentUrlTextField.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        if(!e.isControlDown()){
          return;
        }
        switch(e.getKeyCode()){
          case KeyEvent.VK_W:
            exit();
        }
      }
    });
    
    ep.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        if(!e.isControlDown()){
          return;
        }
        switch(e.getKeyCode()){
          case KeyEvent.VK_W:
            exit();
        }
      }
    });

  }
  
  /**
   * Set the text of the navigation label or input field
   */
  private void setUrl(String url){
    currentUrlString = url;
    if(withNavigation){
      currentUrlTextField.setText(url);
    }
    else{
      currentUrlLabel.setText(url);
    }
  }
  
  /**
   * Set the text of the navigation label or input field
   */
  private String getUrl(){
    return currentUrlString;
  }
  
  /**
   * Displays the URL, using the method corresponding to
   * the protocol of the URL.
   */
  private void setDisplay(String url) throws /*IO*/Exception{
    try{
      Debug.debug("Checking URL, "+url, 3);
      // browse remote web directory
      if((url.startsWith("http://") ||
          url.startsWith("https://") ||
          url.startsWith("ftp://")) &&
         url.endsWith("/")){
        setHttpDirDisplay(url);
      }
      // local directory
      else if((url.startsWith("/") || url.startsWith("file:")) &&
          url.endsWith("/")){
        setLocalDirDisplay(url);
      }
      // remote gsiftp directory
      else if(url.startsWith("gsiftp://") &&
          url.endsWith("/")){
        setGsiftpDirDisplay(url);
      }
      // remote gsiftp text file
      else if(url.startsWith("gsiftp://") &&
          !url.endsWith("/") && !url.endsWith("htm") &&
          !url.endsWith("html") && !url.endsWith("gz")){
        setGsiftpTextEdit(url);
      }
      // html document
      else if((url.endsWith("htm") ||
          url.endsWith("html")) &&
          (url.startsWith("http://") || url.startsWith("file:"))){
        setHtmlDisplay(url);
      }
      // tarball on disk or web server
      else if(url.endsWith("gz") &&
          (url.startsWith("http://") || url.startsWith("file:"))){
        setGzipDisplay(url);
      }
      // tarball on gridftp server
      else if(url.endsWith("gz") &&
          (url.startsWith("gsiftp:/"))){
        setGsiftpGzipDisplay(url);
      }
      // text document on disk or web server
      else if(!url.endsWith("htm") &&
         !url.endsWith("html") &&
         !url.endsWith("/") &&
         (url.startsWith("http://") || url.startsWith("https://"))){
        setHttpTextDisplay(url);
      }
      // text document on disk
      else if(!url.endsWith("htm") &&
         !url.endsWith("html") &&
         !url.endsWith("/") &&
          (url.startsWith("file:") || url.startsWith("/")   
          )){
        setLocalTextEdit(url);
      }
      // blank page
      else if(url.equals("") && withNavigation){
      }
      // unknown protocol
      else{
        throw(new IOException("Unknown protocol for "+url));
      }
      // reset cursor to default when not editing
      if(url.endsWith("/")){
        ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      }
    }
    catch(/*IO*/Exception e){
      ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      Debug.debug("Could not initialize panel with URL "+url+". "+e.getMessage(), 1);
      //return;
      throw e;
    }
  }

  /**
   * Set the EditorPane to display the text page file url.
   * The page can then be edited and saved (when clicking bSave).
   */
  private void setGsiftpTextEdit(String url) throws IOException{
    Debug.debug("setGsiftpTextEdit "+url, 3);
    jtFilter.setEnabled(false);
    
    String localPath = null;
    String host = null;
    String hostAndPath = null;
    String port = "2811";
    String hostAndPort = null;
    File tmpFile = null;
    String localDir = null;
    if(url.startsWith("gsiftp://")){
      hostAndPath = url.substring(9);
    }
    else if(url.startsWith("gsiftp:/")){
      hostAndPath = url.substring(8);
    }
    else{
      return;
    }
    Debug.debug("Host+path: "+hostAndPath, 3);
    hostAndPort = hostAndPath.substring(0, hostAndPath.indexOf("/"));
    int colonIndex=hostAndPort.indexOf(":");
    if(colonIndex>0){
      host = hostAndPort.substring(0, hostAndPort.indexOf(":"));
      port = hostAndPort.substring(hostAndPort.indexOf(":")+1);
    }
    else{
      host = hostAndPort;
      port = "2811";
    }
    localPath = hostAndPath.substring(hostAndPort.length(), hostAndPath.length());
    localPath = localPath.replaceFirst("/[^\\/]*/\\.\\.", "");
    int lastSlash = localPath.lastIndexOf("/");
    if(lastSlash>0){
      localDir = localPath.substring(0, lastSlash);
    }
    else{
      localDir = "/";
    }
    Debug.debug("Host: "+host, 3);
    Debug.debug("Port: "+port, 3);
    Debug.debug("Path: "+localPath, 3);
    Debug.debug("Directory: "+localDir, 3);
    tmpFile = File.createTempFile("gridpilot", "txt");
    Debug.debug("Created temp file "+tmpFile, 3);
    try{
      GridFTPClient gridFtpClient = gridftpFileSystem.connect(host, Integer.parseInt(port));
      try{
        gridFtpClient.changeDir(localDir);
        if(gridFtpClient.getSize(localPath)>500000){
          throw new IOException("File too big");
        }
        gridFtpClient.get(localPath, tmpFile);
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("Could not read "+localPath, 1);
        ep.setText("ERROR!\n\nThe file "+localPath+" could not be read. "+
            e.getMessage());
        throw e;
      }
      pButton.updateUI();
    }
    catch(Exception e){
      Debug.debug("Could not set file display of "+localPath+". "+
         e.getMessage(), 1);
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    try{
      Debug.debug("Reading temp file "+tmpFile.getAbsolutePath(), 3);
      BufferedReader in = new BufferedReader(
        new InputStreamReader((new URL("file:"+tmpFile.getAbsolutePath())).openStream()));
      String text = "";
      String line;
      int lineNumber = 0;
      while((line=in.readLine())!=null){
        ++lineNumber;
        text += line+"\n";
      }
      in.close();
      tmpFile.delete();
      ep.setUI(new BasicEditorPaneUI());
      ep.setContentType("text/plain");
      ep.setText(text);
      ep.setEditable(true);
      //ep.setCursor(new Cursor(Cursor.TEXT_CURSOR));
      ep.getCaret().setBlinkRate(500);
      ep.updateUI();
      pButton.updateUI();
      
      bSave.setEnabled(true);
      bOk.setEnabled(true);
      bNew.setEnabled(false);
      bDelete.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      
      Debug.debug("Setting thisUrl, "+url, 3);
      thisUrl = url;
      setUrl(thisUrl);
    }
    catch(IOException e){
      Debug.debug("Could not set text editor for url "+url+". "+
         e.getMessage(), 1);
      throw e;
    }
  }

  /**
   * Set the EditorPane to display the text page file url.
   * The page can then be edited and saved (when clicking bSave).
   */
  private void setLocalTextEdit(String url) throws IOException{
    Debug.debug("setTextEdit "+url, 3);
    jtFilter.setEnabled(false);
    try{
      bSave.setEnabled(true);
      bNew.setEnabled(false);
      bDelete.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      BufferedReader in = new BufferedReader(
        new InputStreamReader((new URL(url)).openStream()));
      String text = "";
      String line;
      int lineNumber = 0;
      while((line=in.readLine())!=null){
        ++lineNumber;
        if(lineNumber>3000){
          throw new IOException("File too big");
        }
        text += line+"\n";
      }
      in.close();
      ep.setUI(new BasicEditorPaneUI());
      ep.setContentType("text/plain");
      ep.setPage(url);
      ep.setText(text);
      ep.setEditable(true);
      //ep.setCursor(new Cursor(Cursor.TEXT_CURSOR));
      ep.getCaret().setBlinkRate(500);
      ep.updateUI();
      pButton.updateUI();
      Debug.debug("Setting thisUrl, "+url, 3);
      thisUrl = url;
    }
    catch(IOException e){
      Debug.debug("Could not set text editor for url "+url+". "+
         e.getMessage(), 1);
      throw e;
    }
  }

  /**
   * Set the EditorPane to display the text page web url.
   * The page can then be edited and saved (when clicking bSave).
   */
  private void setHttpTextDisplay(String url) throws IOException{
    Debug.debug("setHttpTextDisplay "+url, 3);
    jtFilter.setEnabled(false);
    try{
      ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      bSave.setEnabled(false);
      bNew.setEnabled(false);
      bDelete.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      BufferedReader in = new BufferedReader(
        new InputStreamReader((new URL(url)).openStream()));
      String text = "";
      String line;
      int lineNumber = 0;
      while((line=in.readLine())!=null){
        ++lineNumber;
        if(lineNumber>3000){
          throw new IOException("File too big");
        }
        text += line+"\n";
      }
      in.close();
      ep.setPage(url);
      ep.setText(text);
      ep.setEditable(false);
      pButton.updateUI();
      Debug.debug("Setting thisUrl, "+url, 3);
      thisUrl = url;
    }
    catch(IOException e){
      Debug.debug("Could not set text editor for url "+url+". "+
         e.getMessage(), 1);
      throw e;
    }
  }

  /**
   * Set the EditorPane to display the HTML page url.
   */
  private void setHtmlDisplay(String url) throws IOException{
    Debug.debug("setHtmlDisplay "+url, 3);
    jtFilter.setEnabled(false);
    try{
      bSave.setEnabled(false);
      bNew.setEnabled(false);
      bDelete.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      ep.setPage(url);
      ep.setEditable(false);
      pButton.updateUI();
      Debug.debug("Setting thisUrl, "+thisUrl, 3);
      thisUrl = url;
    }
    catch(IOException e){
      Debug.debug("Could not set html display of url "+url+". "+
         e.getMessage(), 1);
      throw e;
    }
  }

  /**
   * Set the EditorPane to display a confirmation or disconfirmation
   * of the existence of the URL url.
   */
  private void setGzipDisplay(String url) throws IOException{
    Debug.debug("setGzipDisplay "+url, 3);
    jtFilter.setEnabled(false);
    try{
      bSave.setEnabled(false);
      bNew.setEnabled(false);
      bDelete.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      ep.setEditable(false);
      try {
        URLConnection connection = (new URL(url)).openConnection();
        DataInputStream dis;
        dis = new DataInputStream(connection.getInputStream());
              //while ((inputLine = dis.readLine()) != null){
              //    System.out.println(inputLine);
              //}
        dis.close();
        ep.setText("File found");
        Debug.debug("Setting thisUrl, "+thisUrl, 3);
        thisUrl = url;
      }
      catch(MalformedURLException me){
        ep.setText("File "+url+" NOT found");
      }
      catch(IOException ioe){
        ep.setText("File "+url+" NOT found");
      }
      pButton.updateUI();
    }
    catch(Exception e){
      Debug.debug("Could not set directory display of url "+url+". "+
         e.getMessage(), 1);
      //throw e;
    }
  }

  /**
   * Set the EditorPane to display a confirmation or disconfirmation
   * of the existence of the URL url.
   */
  private void setGsiftpGzipDisplay(String url) throws IOException{
    Debug.debug("setGsiftpGzipDisplay "+url, 3);
    jtFilter.setEnabled(false);
    
    bSave.setEnabled(false);
    bNew.setEnabled(false);
    bDelete.setEnabled(false);
    bUpload.setEnabled(false);
    bDownload.setEnabled(false);
    ep.setEditable(false);

    String localPath = null;
    String host = null;
    String hostAndPath = null;
    String port = "2811";
    String hostAndPort = null;
    String localDir = null;
    if(url.startsWith("gsiftp://")){
      hostAndPath = url.substring(9);
    }
    else if(url.startsWith("gsiftp:/")){
      hostAndPath = url.substring(8);
    }
    else{
      return;
    }
    Debug.debug("Host+path: "+hostAndPath, 3);
    hostAndPort = hostAndPath.substring(0, hostAndPath.indexOf("/"));
    int colonIndex=hostAndPort.indexOf(":");
    if(colonIndex>0){
      host = hostAndPort.substring(0, hostAndPort.indexOf(":"));
      port = hostAndPort.substring(hostAndPort.indexOf(":")+1);
    }
    else{
      host = hostAndPort;
      port = "2811";
    }
    localPath = hostAndPath.substring(hostAndPort.length(), hostAndPath.length());
    localPath = localPath.replaceFirst("/[^\\/]*/\\.\\.", "");
    int lastSlash = localPath.lastIndexOf("/");
    if(lastSlash>0){
      localDir = localPath.substring(0, lastSlash);
    }
    else{
      localDir = "/";
    }
    Debug.debug("Host: "+host, 3);
    Debug.debug("Port: "+port, 3);
    Debug.debug("Path: "+localPath, 3);
    Debug.debug("Directory: "+localDir, 3);
    try{
      GridFTPClient gridFtpClient = gridftpFileSystem.connect(host, Integer.parseInt(port));
      try{
        gridFtpClient.changeDir(localDir);
        if(gridFtpClient.getSize(localPath)==0){
          throw new IOException("File is empty");
        }
        ep.setText("File found");
        Debug.debug("Setting thisUrl, "+url, 3);
        thisUrl = url;
        setUrl(thisUrl);
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("Could not read "+localPath, 1);
        ep.setText("ERROR!\n\nThe file "+localPath+" could not be read. "+
            e.getMessage());
      }
      pButton.updateUI();
    }
    catch(Exception e){
      Debug.debug("Could not set gzip display of "+localPath+". "+
         e.getMessage(), 1);
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Set the EditorPane to display a directory listing
   * of the shell path fsPath.
   */
  private void setLocalDirDisplay(String fsPath) throws IOException{
    jtFilter.setEnabled(true);
    String localPath = "";
    if(fsPath.startsWith("file://")){
      localPath = fsPath.substring(6);
    }
    else if(fsPath.startsWith("file:/")){
      localPath = fsPath.substring(5);
    }
    /*else if(fsPath.startsWith("file:")){
      localPath = fsPath.substring(4);
    }*/
    else if(fsPath.toLowerCase().startsWith("c:\\")){
      localPath = fsPath.substring(2);
    }
    else{
      localPath = fsPath;
    }
    localPath = localPath.replaceFirst("/[^\\/]*/\\.\\.", "");
    localPath = localPath.replaceFirst("^/(\\w):", "$1:");
    localPath = localPath.replaceFirst("^file:(\\w):", "$1:");
    localPath = localPath.replaceFirst("^/(\\w):", "$1:");
    Debug.debug("setLocalDirDisplay "+localPath, 3);
    try{
      bSave.setEnabled(false);
      bNew.setEnabled(true);
      bDelete.setEnabled(true);
      bUpload.setEnabled(true);
      bDownload.setEnabled(true);
      String htmlText = "";
      try{
        String [] text = LocalStaticShellMgr.listFiles(localPath);
        int directories = 0;
        int files = 0;
        Vector textVector = new Vector();
        filter = jtFilter.getText();
        if(filter==null || filter.equals("")){
          filter = "*";
        }
        statusBar.setLabel("Filtering...");
        filter = filter.replaceAll("\\.", "\\\\.");
        filter = filter.replaceAll("\\*", ".*");
        Debug.debug("Filtering with "+filter, 3);
        
        for(int j=0; j<text.length; ++j){
          if(text[j].substring(localPath.length()).matches(filter)){
            if(LocalStaticShellMgr.isDirectory(text[j])){
              ++directories;
            }
            else{
              ++files;
            }
            textVector.add("<a href=file:"+text[j]+">" + 
                (((text[j].toLowerCase().startsWith("c:\\") ||
                    text[j].toLowerCase().startsWith("c:/")) &&
                    !localPath.toLowerCase().startsWith("c:\\") &&
                    !localPath.toLowerCase().startsWith("c:/")) ? 
                    text[j].substring(localPath.length()+2) :
                      text[j].substring(localPath.length())) +  "</a>");
          }
        }
        ep.setContentType("text/html");
        htmlText = "<html>\n";
        if(!localPath.equals("/")){
          htmlText += "<a href=file:"+localPath+"../>"/*+localPath*/+"../</a><br>\n";
        }
        htmlText += Util.arrayToString(textVector.toArray(), "<br>\n");
        htmlText += "\n</html>";
        ep.setText(htmlText);
        ep.setEditable(false);
        // if we don't get an exception, the directory got read...
        Debug.debug("Directory "+localPath, 2);
        Debug.debug("Setting thisUrl, "+localPath, 3);
        thisUrl = (new File(localPath)).toURL().toExternalForm();
        setUrl(thisUrl);
        statusBar.setLabel(directories+" directories, "+files+" files");
        return;
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("Could not read "+localPath, 1);
      }
      // if we got here we did not read the directory
      bSave.setEnabled(false);
      bOk.setEnabled(false);
      ep.setText("ERROR!\n\nThe directory "+localPath+" could not be read.");
      pButton.updateUI();
    }
    catch(Exception e){
      Debug.debug("Could not set directory display of "+localPath+". "+
          e.getMessage(), 1);
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
  }
  
   /**
   * Set the EditorPane to display a directory listing
   * of the URL url.
   */
  private void setGsiftpDirDisplay(String url) throws /*IO*/Exception{
    Debug.debug("setGsiftpDirDisplay "+url, 3);
    jtFilter.setEnabled(true);
    
    String localPath = null;
    String host = null;
    String hostAndPath = null;
    String port = "2811";
    String hostAndPort = null;
    if(url.startsWith("gsiftp://")){
      hostAndPath = url.substring(9);
    }
    else if(url.startsWith("gsiftp:/")){
      hostAndPath = url.substring(8);
    }
    else{
      return;
    }
    Debug.debug("Host+path: "+hostAndPath, 3);
    hostAndPort = hostAndPath.substring(0, hostAndPath.indexOf("/"));
    int colonIndex=hostAndPort.indexOf(":");
    if(colonIndex>0){
      host = hostAndPort.substring(0, hostAndPort.indexOf(":"));
      port = hostAndPort.substring(hostAndPort.indexOf(":")+1);
    }
    else{
      host = hostAndPort;
      port = "2811";
    }
    localPath = hostAndPath.substring(hostAndPort.length(), hostAndPath.length());
    localPath = localPath.replaceFirst("/[^\\/]*/\\.\\.", "");
    url = url.replaceFirst("/[^\\/]*/\\.\\.", "");
    Debug.debug("Host: "+host, 3);
    Debug.debug("Port: "+port, 3);
    Debug.debug("Path: "+localPath, 3);
    bSave.setEnabled(false);
    bNew.setEnabled(true);
    bDelete.setEnabled(true);
    bUpload.setEnabled(true);
    bDownload.setEnabled(true);
    String htmlText = "";
    Object [] textArr = null;
    GridFTPClient gridFtpClient = null;
    try{      
      /*try{
        // NorduGrid variant
        gridFtpClient = gsiftpConnect(host, Integer.parseInt(port));
        gridFtpClient.changeDir(localPath);
        textArr = gridFtpClient.mlsd().toArray();
      }
      catch(FTPException ee){*/
        //ee.printStackTrace();
        try{
          // LCG variant
          //gridFtpClient.close();         
          gridFtpClient = gridftpFileSystem.connect(host, Integer.parseInt(port));
          
          if(localPath.length()>1){
            gridFtpClient.changeDir(localPath.substring(0, localPath.length()-1));
          }
          Debug.debug("Current dir: "+gridFtpClient.getCurrentDir(), 3);
          final ByteArrayOutputStream received2 = new ByteArrayOutputStream(100000);
          DataSink dataSink = new DataSink(){
            public void write(Buffer buffer) 
            throws IOException{
              Debug.debug("received " + buffer.getLength() +
                 " bytes of directory listing", 1);
                 received2.write(buffer.getBuffer(),
                 0,
                 buffer.getLength());
              return;
            }
            public void close() throws IOException{
              Debug.debug("closing", 1);
            };                
          };
          gridFtpClient.list("", "", dataSink);
          textArr = Util.split(received2.toString(), "\n");
        }
        catch(/*FTP*/Exception e){
          Debug.debug("oops, failed listing!", 2);
          throw e;
        }
      //}
      Vector textVector = new Vector();
      String [] entryArr = null;
      String line = null;
      String fileName = null;
      int directories = 0;
      int files = 0;
      filter = jtFilter.getText();
      if(filter==null || filter.equals("")){
        filter = "*";
      }
      statusBar.setLabel("Filtering...");
      filter = filter.replaceAll("\\.", "\\\\.");
      filter = filter.replaceAll("\\*", ".*");
      Debug.debug("Filtering with "+filter, 3);
      for(int i=0; i<textArr.length; ++i){
        line = textArr[i].toString();
        //Debug.debug(line, 3);
        entryArr = Util.split(line);
        fileName = entryArr[entryArr.length-1];
        // If server is nice enough to provide file information, use it
        if(fileName.matches(filter)){
          if(line.matches("d[rwxsS-]* .*")){
            textVector.add(fileName+"/");
            ++directories;
            continue;
          }
          else if(line.matches("-[rwxsS-]* .*")){
            textVector.add(fileName);
            ++files;
            continue;
          }
          try{
            // File information not provided, we have to do it the slow way...
            // Check if fileName it is a directory by trying to cd into it
            Debug.debug("Checking file/directory information the slow way...", 2);
            gridFtpClient.changeDir(fileName);
            gridFtpClient.goUpDir();
            textVector.add(fileName+"/");
            ++directories;
          }
          catch(Exception e){
            textVector.add(fileName);
            ++files;
          }
        }
      }
      String text = "";
      // TODO: reconsider max entries and why listing more is so slow...
      // display max 500 entries
      int maxEntries = 500;
      int length = textVector.size()<maxEntries ? textVector.size() : maxEntries;
      for(int i=0; i<length; ++i){
        text += "<a href=gsiftp://"+host+":"+port+localPath+textVector.get(i)+">"+
        /*"gsiftp://"+host+":"+port+localPath+*/textVector.get(i)+"</a>";
        if(i<length-1){
          text += "<br>\n";
        }
        Debug.debug(textVector.get(i).toString(), 3);
      }
      ep.setContentType("text/html");
      htmlText = "<html>\n";
      
      if(!localPath.equals("/")){
        htmlText += "<a href=gsiftp://"+host+":"+port+localPath+"../>"+/*"gsiftp://"+host+":"+port+localPath+*/"../</a><br>\n";
      }
      Debug.debug("running arrayToString...", 3);
      htmlText += text;
      if(textVector.size()>maxEntries){
        htmlText += "<br>\n...<br>\n...<br>\n...<br>\n";
      }
      htmlText += "\n</html>";
      Debug.debug("done parsing, setting text", 3);
      ep.setText(htmlText);
      ep.setEditable(false);
      // if we don't get an exception, the directory got read...
      //Debug.debug("Directory "+localPath+" read", 2);
      //Debug.debug("Setting thisUrl, "+localPath, 3);
      //thisUrl = (new File(localPath)).toURL().toExternalForm();
      thisUrl = url;
      setUrl(thisUrl);
      statusBar.setLabel(directories+" directories, "+files+" files");
    }
    catch(Exception e){
      bSave.setEnabled(false);
      bOk.setEnabled(false);
      ep.setText("ERROR!\n\nThe directory "+localPath+" could not be read.");
      pButton.updateUI();
      throw e;
    }
    finally{
      try{
        gridFtpClient.close();
      }
      catch(Exception e){
      }
    }
  }

  /**
   * Set the EditorPane to display a directory listing
   * of the URL url.
   */
  private void setHttpDirDisplay(String url) throws IOException{
    Debug.debug("setHttpDirDisplay "+url, 3);
    jtFilter.setEnabled(false);
    try{
      bSave.setEnabled(false);
      bNew.setEnabled(false);
      bDelete.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(true);
      ep.setPage(url);
      // workaround for bug in java < 1.5
      // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4492274
      // Doesn't work...
      /*if (!ep.getPage().equals(url))
      {
        ep.getDocument().
          putProperty(Document.StreamDescriptionProperty, url);
      }*/
      ep.setEditable(false);
      pButton.updateUI();
      Debug.debug("Setting thisUrl, "+thisUrl, 3);
      thisUrl = url;
    }
    catch(IOException e){
      Debug.debug("Could not set directory display of url "+url+". "+
         e.getMessage(), 1);
      throw e;
    }
  }

    public Dimension getPreferredSize(){
    return panel.getPreferredSize();
  }

  public Dimension getMinimumSize(){
    return panel.getPreferredSize();
  }

  //Overridden so we can exit when window is closed
  protected void processWindowEvent(WindowEvent e){
    if (e.getID()==WindowEvent.WINDOW_CLOSING){
      cancel();
    }
    super.processWindowEvent(e);
  }

  //Close the dialog
  void cancel(){
    if(thisUrl==null || thisUrl.endsWith("/")){
      dispose();
      lastURL = origUrl;
    }
    else{
      try{
        String newUrl = thisUrl.substring(0, thisUrl.lastIndexOf("/")+1);
        setUrl(newUrl);
        setDisplay(newUrl);
      }
      catch(/*IO*/Exception ioe){
        ioe.printStackTrace();
        dispose();
        lastURL = origUrl;
      }
    }
  }

  //Set lastURL and close the dialog
  void exit(){
    //GridPilot.lastURL = ep.getPage();
    Debug.debug("Setting lastURL, "+thisUrl, 3);
    lastURL = thisUrl;
    dispose();
  }

  /**
   * Write file or directory fsPath in local file system containing text.
   */
  private void localWriteFile(String fsPath, String text){
    try{
      if(fsPath.endsWith("/") && (text==null || text.equals(""))){
        fsPath = fsPath.substring(0, fsPath.length()-1);
        LocalStaticShellMgr.mkdirs(fsPath);
      }
      else if(!fsPath.endsWith("/")){
        LocalStaticShellMgr.writeFile(fsPath, text, false);
      }
      else{
        throw new IOException("ERROR: Cannot write text to a directory.");
      }
      // if we don't get an exception, the file got written...
      statusBar.setLabel(fsPath+" written.");
      Debug.debug("File or directory "+fsPath+" written", 2);
      return;
    }
    catch(Exception e){
      Debug.debug("Could not write "+fsPath, 1);
    }
    // if we got here we did not save the file
    ep.setText("ERROR!\n\nThe file "+fsPath+" could not be saved.\n\n" +
        "Check if a shell manager is available.");
  }

  /**
   * Delete file or directory fsPath in local file system.
   */
  private void localDeleteFile(String fsPath){
    try{
      LocalStaticShellMgr.deleteFile(fsPath);
      // if we don't get an exception, the file got written...
      Debug.debug("File or directory "+fsPath+" deleted", 2);
      return;
    }
    catch(Exception e){
      Debug.debug("Could not delete "+fsPath, 1);
    }
    // if we got here we did not delete the file
    ep.setText("ERROR!\n\nThe file "+fsPath+" could not be deleted.\n\n" +
        "Check if a shell manager is available.");
  }

  /**
   * Copy file to directory fsPath.
   */
  private void localCopy(String fsPath, File file){
    if(!fsPath.endsWith("/")){
      fsPath = fsPath+"/";
    }
    // TODO: implement wildcard *
    try{
      String fileName = file.getName();
      int lastSlash = fileName.lastIndexOf("/");
      if(lastSlash>-1){
        fileName = fileName.substring(lastSlash + 1);
      }
      if(!LocalStaticShellMgr.isDirectory(fsPath)){
        throw new IOException("ERROR: "+fsPath+" is not a directory.");
      }
      LocalStaticShellMgr.copyFile(file.getAbsolutePath(),
          fsPath+fileName);
      // if we don't get an exception, the file got written...
      Debug.debug("File "+file.getAbsolutePath()+" written to " +
          fsPath+fileName, 2);
      return;
    }
    catch(Exception e){
      Debug.debug("Could not write "+fsPath, 1);
    }
    // if we got here we did not save the file
    ep.setText("ERROR!\n\nThe file "+fsPath+" could not be saved.\n\n" +
        "Check if a shell manager is available.");
  }
  
  /**
   * Create an empty file or directory in the directory fsPath.
   * Ask for the name.
   * If a name ending with a / is typed in, a directory is created.
   * (this path must match the URL url).
   */
  private void localCreate(String fsPath){
    String fileName = Util.getName("File name (end with a / to create a directory)", "");   
    if(fsPath==null || fileName==null){
      return;
    }  
    if(!fsPath.endsWith("/") && !fileName.startsWith("/")){
      fsPath = fsPath+"/";
    }
    localWriteFile(fsPath+fileName, "");
  }

  /**
   * Delete file or directory.
   * Ask for the name.
   */
  private void delete() throws IOException{
    String fileName = Util.getFileName(jtFilter.getText());
    if(fileName==null){
      return;
    }  
    if(thisUrl.startsWith("file:") || thisUrl.startsWith("/")){
      URL url = ep.getPage();
      String rootUrl = thisUrl.substring(0, thisUrl.lastIndexOf("/"));
      String fsPath = rootUrl;
      fsPath = fsPath.replaceFirst("^file://", "/");
      fsPath = fsPath.replaceFirst("^file:/", "/");
      fsPath = fsPath.replaceFirst("^file:", "");
      Debug.debug("Deleting file in "+fsPath, 3);
      if(fsPath==null || fileName==null){
        return;
      }  
      if(!fsPath.endsWith("/") && !fileName.startsWith("/")){
        fsPath = fsPath+"/";
      }
      localDeleteFile(fsPath+fileName);
      try{
        ep.getDocument().putProperty(
            Document.StreamDescriptionProperty, null);
        if(url!=null){
          setDisplay(url.toExternalForm());
        }
        else{
          setDisplay((new URL("file:"+fsPath)).toExternalForm());
        }
      }
      catch(/*IO*/Exception ioe){
        ioe.printStackTrace();
      }
    }
    else if(thisUrl.startsWith("gsiftp://")){
      String fullName = null;
      if(!thisUrl.endsWith("/") && !thisUrl.startsWith("/")){
        fullName = thisUrl+"/"+fileName;
      }
      else{
        fullName = thisUrl+fileName;
      }
      gridftpFileSystem.delete(fullName);
      // if we got here we did not delete the file
      ep.setText("ERROR!\n\nThe file "+fullName+" could not be deleted.");
      pButton.updateUI();
      try{
        setDisplay(thisUrl);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not display "+thisUrl, 1);
      }
    }
    else{
      throw(new IOException("Unknown protocol for "+thisUrl));
    }
  }

  /**
   * Upload file to fsPath.
   * Ask for the file with a FileChooser.
   */
  private void upload() throws IOException{
    try{
      
      if(!thisUrl.endsWith("/")){
        throw(new IOException("Upload location must be a directory. "+thisUrl));
      }

      File file = getInputFile();
      
      Debug.debug("Uploading file to "+thisUrl, 3);
      // local directory
      if(thisUrl.startsWith("file:")){
        String rootUrl = thisUrl.substring(0, thisUrl.lastIndexOf("/"));
        String fsPath = rootUrl;
        fsPath = fsPath.replaceFirst("^file://", "/");
        fsPath = fsPath.replaceFirst("^file:/", "/");
        fsPath = fsPath.replaceFirst("^file:", "");
        Debug.debug("Uploading file to "+fsPath, 3);        
        if(fsPath==null || file==null){
          return;
        }
        
        if(!fsPath.endsWith("/") && !file.getName().startsWith("/"))
          fsPath = fsPath+"/";
        
        localCopy(fsPath, file);
      }
      // remote gsiftp directory
      else if(thisUrl.startsWith("gsiftp://")){
        if(!thisUrl.endsWith("/")){
          throw(new IOException("Upload location must be a directory. "+thisUrl));
        }
        gridftpFileSystem.put(file, thisUrl+file.getName());
      }
      else{
        throw(new IOException("Unknown protocol for "+thisUrl));
      }
    }
    catch(IOException e){
      Debug.debug("Could not save to URL "+thisUrl+". "+e.getMessage(), 1);
      throw e;
    }

    try{
      ep.getDocument().putProperty(
          Document.StreamDescriptionProperty, null);
      setDisplay(thisUrl);
    }
    catch(/*IO*/Exception ioe){
      ioe.printStackTrace();
    }
  }
  
  /**
   * Download file to fsPath.
   * Ask for the file with a FileChooser.
   */
  private void download() throws IOException{
    try{
      
      if(!thisUrl.endsWith("/")){
        throw(new IOException("Download location must be a directory. "+thisUrl));
      }

      File dir = getDownloadDir();      
      if(thisUrl==null || dir==null){
        throw new IOException("ERROR: source or destination directory not given. "+
            thisUrl+":"+dir);
      }
      
      String fileName = Util.getFileName(jtFilter.getText());
      if(fileName==null){
        return;
      }
      
      statusBar.setLabel("Downloading "+fileName+" from "+thisUrl+
          " to "+dir);

      Debug.debug("Downloading file from "+thisUrl, 3);
      // local directory
      if(thisUrl.startsWith("file:")){
        String rootUrl = thisUrl.substring(0, thisUrl.lastIndexOf("/"));
        String fsPath = rootUrl;
        fsPath = fsPath.replaceFirst("^file://", "/");
        fsPath = fsPath.replaceFirst("^file:/", "/");
        fsPath = fsPath.replaceFirst("^file:", "");
        Debug.debug("Downloading file to "+dir.getAbsolutePath(), 3);        
        if(fsPath==null || dir==null){
          throw new IOException("ERROR: source or destination directory not given. "+
              fsPath+":"+dir);
        }        
        if(!fsPath.endsWith("/") && !fileName.startsWith("/")){
          fsPath = fsPath+"/";
        }
        localCopy(dir.getAbsolutePath(), new File(fsPath+fileName));
      }
      // remote gsiftp directory
      else if(thisUrl.startsWith("gsiftp://")){
        // construct remote location of file
        String rootUrl = thisUrl.substring(0, thisUrl.lastIndexOf("/"));
        String fsPath = rootUrl;
        if(!fsPath.endsWith("/") && !fileName.startsWith("/")){
          fsPath = fsPath+"/";
        }
        // construct local location of file
        Debug.debug("Downloading to "+dir.getAbsolutePath(), 3);        
        Debug.debug("Downloading "+fileName+" from "+thisUrl, 3);
        final String fName = fileName;
        final File dName = dir;
        ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        statusBar.setLabel("Downloading...");
        (new MyThread(){
          public void run(){
            try{
              gridftpFileSystem.get(thisUrl+fName, dName, statusBar);
            }
            catch(IOException e){
              Debug.debug("File download failed. "+e.getMessage(), 1);
              e.printStackTrace();
            }
            finally{
              ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
            }
          }
        }).run();               
      }
      else if(thisUrl.startsWith("http://") || thisUrl.startsWith("https://")){
        // construct remote location of file
        String rootUrl = thisUrl.substring(0, thisUrl.lastIndexOf("/"));
        String fsPath = rootUrl;
        if(!fsPath.endsWith("/") && !fileName.startsWith("/")){
          fsPath = fsPath+"/";
        }
        // construct local location of file
        Debug.debug("Downloading file to "+dir.getAbsolutePath(), 3);        
        String dirPath = dir.getAbsolutePath();
        if(!dirPath.endsWith("/") && !fileName.startsWith("/")){
          dirPath = dirPath+"/";
        }
        Debug.debug("Downloading from "+thisUrl+fileName+" to "+dirPath+fileName, 2);
        final String fName = fileName;
        final String dName = dirPath;
        // TODO: implement wildcard *
        ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
        statusBar.setLabel("Downloading...");
        (new MyThread(){
          public void run(){
            try{
              InputStream is = (new URL(thisUrl+fName)).openStream();
              DataInputStream dis = new DataInputStream(new BufferedInputStream(is));
              FileOutputStream os = new FileOutputStream(new File(dName+fName));
              // read data in chunks of 10 kB
              byte [] b = new byte[10000];
              while(dis.read(b)>-1){
                os.write(b);
              }
              dis.close();
              is.close();
              os.close();
            }
            catch(IOException e){
              Debug.debug("File download failed. "+e.getMessage(), 1);
              e.printStackTrace();
            }
          }
        }).run();               
      }
      else{
        throw(new IOException("Unknown protocol for "+thisUrl));
      }
    }
    catch(IOException e){
      Debug.debug("Could not get URL "+thisUrl+". "+e.getMessage(), 1);
      e.printStackTrace();
      throw e;
    }
        
    try{
      ep.getDocument().putProperty(
          Document.StreamDescriptionProperty, null);
      setDisplay(thisUrl);
    }
    catch(/*IO*/Exception ioe){
      ioe.printStackTrace();
    }
  }
  
  /**
   * Creates a file on a URL.
   */
  private void createNew() throws Exception{
    if(!thisUrl.endsWith("/")){
      throw new IOException("ERROR: URL "+thisUrl+" does not end with /.");
    }
    try{
      Debug.debug("Creating file in "+thisUrl, 3);
      // local directory
      if(thisUrl.startsWith("file:")){
        //String rootUrl = thisUrl.substring(0, thisUrl.lastIndexOf("/"));
        String fsPath = thisUrl;//rootUrl;
        fsPath = fsPath.replaceFirst("^file://", "/");
        fsPath = fsPath.replaceFirst("^file:/", "/");
        fsPath = fsPath.replaceFirst("^file:", "");
        Debug.debug("Creating file in "+fsPath, 3);
        localCreate(fsPath);
        ep.getDocument().putProperty(
            Document.StreamDescriptionProperty, null);
        setDisplay((new URL("file:"+fsPath)).toExternalForm());
      }
      // remote gsiftp directory
      else if(thisUrl.startsWith("gsiftp://")){
        Debug.debug("Creating file in "+thisUrl, 3);
        gridftpFileSystem.create(thisUrl);
        ep.getDocument().putProperty(
            Document.StreamDescriptionProperty, null);
        setDisplay(thisUrl);
      }
      else{
        throw(new IOException("Unknown protocol for "+thisUrl));
      }
    }
    catch(IOException e){
      Debug.debug("Could not save to URL "+thisUrl+". "+e.getMessage(), 1);
      throw e;
    }
  }

  //Close the dialog on a button event
  public void actionPerformed(ActionEvent e){
    try{
      if(e.getSource()==bOk){
        exit();
      }
      else if(e.getSource()==bCancel){
        cancel();
      }
      else if(e.getSource()==bSave){
        if(thisUrl.startsWith("file:") || thisUrl.startsWith("/")){
          String fsPath = ep.getPage().toExternalForm();
          fsPath = fsPath.replaceFirst("^file://", "/");
          fsPath = fsPath.replaceFirst("^file:/", "/");
          fsPath = fsPath.replaceFirst("^file:", "");
          String text = ep.getText();
          localWriteFile(fsPath, text);
        }
        else if(thisUrl.startsWith("gsiftp://")){
          gridftpFileSystem.writeFile(thisUrl, ep.getText());
          statusBar.setLabel(thisUrl+" written.");
        }
        else{
          throw(new IOException("Unknown protocol for "+thisUrl));
        }
      }
      else if(e.getSource()==bNew){
        createNew();
      }
      else if(e.getSource()==bDelete){
        delete();
      }
      else if(e.getSource()==bUpload){
        upload();
      }
      else if(e.getSource()==bDownload){
        download();
      }
    }
    catch(Exception ex){
      GridPilot.getClassMgr().getLogFile().addMessage("ERROR: ", ex);
      Debug.debug("ERROR: "+ex.getMessage(), 3);
      ex.printStackTrace();
    }
  }

  private File getInputFile(){
    File file = null;
    JFileChooser fc = new JFileChooser();
    fc.setDialogTitle("Choose file to upload");
    fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
    int returnVal = fc.showOpenDialog(this);
    if (returnVal==JFileChooser.APPROVE_OPTION){
      file = fc.getSelectedFile();
      Debug.debug("Opening: " + file.getName(), 2);
    }
    else{
      Debug.debug("Not opening file", 3);
    }
    return file;
  }

  private File getDownloadDir(){
    File file = null;
    JFileChooser fc = new JFileChooser();
    fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
    fc.setDialogTitle("Choose dowload directory");
    int returnVal = fc.showOpenDialog(this);
    if (returnVal==JFileChooser.APPROVE_OPTION){
      file = fc.getSelectedFile();
      Debug.debug("Opening: " + file.getName(), 2);
    }
    else{
      Debug.debug("Not opening file", 3);
    }
    return file;
  }
  
}