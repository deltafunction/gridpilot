package gridpilot;

import java.awt.*;
import java.awt.event.*;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

import javax.swing.*;
import javax.swing.plaf.basic.BasicEditorPaneUI;
import javax.swing.text.Document;
import javax.swing.text.JTextComponent;
import javax.swing.text.html.*;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.*;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.util.ListIterator;
import java.util.Vector;

import org.globus.ftp.GridFTPClient;
import org.globus.ftp.exception.FTPException;
import org.globus.util.GlobusURL;

import gridpilot.ftplugins.gsiftp.GSIFTPFileTransfer;


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
  private boolean withFilter = false;
  private boolean withNavigation = false;
  protected String lastURL = null;
  protected String [] lastUrlList = null;
  private String currentUrlString = "";
  private JComboBox currentUrlBox = null;
  private GSIFTPFileTransfer gsiftpFileTransfer = null;
  private boolean ok = true;
  private boolean saveUrlHistory = false;
  private boolean doingSearch = false;
  private JComponent jBox = null;
  private boolean localFS = false;
  
  public static int HISTORY_SIZE = 15;

  public BrowserPanel(JFrame _parent, String title, String url, 
      String _baseUrl, boolean modal, boolean _withFilter,
      boolean _withNavigation, JComponent _jBox, String _filter,
      boolean _localFS) throws Exception{
    super(_parent);
    init(title, url, _baseUrl, modal, _withFilter, _withNavigation, _jBox, _filter, _localFS);
  }
  
  public BrowserPanel(String title, String url, 
      String _baseUrl, boolean modal, boolean _withFilter,
      boolean _withNavigation, JComponent _jBox, String _filter,
      boolean _localFS) throws Exception{
    init(title, url, _baseUrl, modal, _withFilter, _withNavigation, _jBox, _filter, _localFS);
  }
  
  public void init(String title, String url, 
      String _baseUrl, boolean modal, boolean _withFilter,
      boolean _withNavigation, JComponent _jBox, String _filter, boolean _localFS) throws Exception{
    baseUrl = _baseUrl;
    origUrl = url;
    withFilter = _withFilter;
    withNavigation = _withNavigation;
    jBox = _jBox;
    localFS = _localFS;
    
    if(_filter!=null && !_filter.equals("")){
      jtFilter.setText(_filter);
    }
    
    setModal(modal);
    
    if(!localFS){
      gsiftpFileTransfer = new GSIFTPFileTransfer();
    }
    
    String urlHistory = null;
    Debug.debug("browser history file: "+GridPilot.browserHistoryFile, 2);
    if(GridPilot.browserHistoryFile!=null && !GridPilot.browserHistoryFile.equals("")){
      if(GridPilot.browserHistoryFile.startsWith("~")){
        String homeDir = System.getProperty("user.home");
        if(!homeDir.endsWith(File.separator)){
          homeDir += File.separator;
        }
        GridPilot.browserHistoryFile = homeDir+GridPilot.browserHistoryFile.substring(1);
      }
      try{
        if(!LocalStaticShellMgr.existsFile(GridPilot.browserHistoryFile)){
          Debug.debug("trying to create file", 2);
          LocalStaticShellMgr.writeFile(GridPilot.browserHistoryFile, "", false);
        }
        urlHistory = LocalStaticShellMgr.readFile(GridPilot.browserHistoryFile);
        saveUrlHistory = true;
      }
      catch(Exception e){
        Debug.debug("WARNING: could not use "+GridPilot.browserHistoryFile+
            " as history file.", 1);
        GridPilot.browserHistoryFile = null;
        urlHistory = null;
      }
    }
    
    if((GridPilot.getClassMgr().getUrlList()==null ||
        GridPilot.getClassMgr().getUrlList().size()==0) &&
        urlHistory!=null && !urlHistory.equals("")){
      BufferedReader in = null;
      try{
        Debug.debug("Reading file "+GridPilot.browserHistoryFile, 3);
        in = new BufferedReader(
          new InputStreamReader((new URL("file:"+GridPilot.browserHistoryFile)).openStream()));
      }
      catch(IOException ioe){
        Debug.debug("WARNING: could not use "+GridPilot.browserHistoryFile+
            " as history file.", 1);
        GridPilot.browserHistoryFile = null;
        urlHistory = null;
      }
      try{
        String line;
        int lineNumber = 0;
        while((line=in.readLine())!=null){
          ++lineNumber;
          line = line.replaceAll("\\r", "");
          line = line.replaceAll("\\n", "");
          line = URLDecoder.decode(line, "utf-8");
          GridPilot.getClassMgr().addUrl(line);
          Debug.debug("URL: "+line, 3);
        }
        in.close();
      }
      catch(IOException ioe){
        Debug.debug("WARNING: could not use "+GridPilot.browserHistoryFile+
            " as history file.", 1);
        GridPilot.browserHistoryFile = null;
        urlHistory = null;
      }
    }

    currentUrlBox = new JExtendedComboBox();
    currentUrlBox.setEditable(true);
    Dimension d = currentUrlBox.getPreferredSize();
    currentUrlBox.setPreferredSize(new Dimension(320, d.height));
    setUrl(url);
    
    try{
      initGUI(title, url);
    }
    catch(IOException e){
      //e.printStackTrace();
      throw e;
    }
  }
  
  public void okSetEnabled(boolean _ok){
    ok = _ok;
    bOk.setEnabled(ok);
  }
  
  private void addUrlKeyListener(){
    // Listen for enter key in text field
    JTextComponent editor = (JTextComponent) currentUrlBox.getEditor().getEditorComponent();
    editor.addKeyListener(new KeyAdapter(){
      public void keyReleased(KeyEvent e){
        if(!doingSearch && e.getKeyCode()==KeyEvent.VK_ENTER){
          doingSearch = true;
          Debug.debug("Detected ENTER", 3);
          if(currentUrlBox.getEditor().getItem()!=null &&
              !currentUrlBox.getEditor().getItem().toString().equals("") ||
              currentUrlBox.getSelectedItem()!=null && 
              !currentUrlBox.getSelectedItem().toString().equals("")){
            statusBar.setLabel("Opening URL...");
            ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            MyThread t = (new MyThread(){
              public void run(){
                try{
                  setDisplay(currentUrlBox.getSelectedItem().toString());
                }
                catch(Exception ee){
                  statusBar.setLabel("ERROR: could not open "+currentUrlBox.getSelectedItem().toString()+
                      ". "+ee.getMessage());
                  Debug.debug("ERROR: could not open "+currentUrlBox.getSelectedItem().toString()+
                      ". "+ee.getMessage(), 1);
                  ee.printStackTrace();
                }
                doingSearch = false;
              }
            });     
            SwingUtilities.invokeLater(t);
          }
        }
      }
    });
  }
  
  private void addFilterKeyListener(){
    // Listen for enter key in text field
    jtFilter.addKeyListener(new KeyAdapter(){
      public void keyReleased(KeyEvent e){
        if(!getUrl().endsWith("/")){
          return;
        }
        if(!doingSearch && e.getKeyCode()==KeyEvent.VK_ENTER){
          doingSearch = true;
          if(!currentUrlBox.getSelectedItem().toString().equals("")){
            Debug.debug("Detected ENTER", 3);
            statusBar.setLabel("Opening URL...");
            ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            MyThread t = (new MyThread(){
              public void run(){
                try{
                  setDisplay(getUrl());
                }
                catch(Exception ee){
                  statusBar.setLabel("ERROR: could not filter "+currentUrlBox.getSelectedItem().toString()+
                      ". "+ee.getMessage());
                  Debug.debug("ERROR: could not filter "+currentUrlBox.getSelectedItem().toString()+
                      ". "+ee.getMessage(), 1);
                  ee.printStackTrace();
                }
                doingSearch = false;
              }
            });
            SwingUtilities.invokeLater(t);
          }
        }
      }
    });
  }
  
  //Component initialization
  private void initGUI(String title, String url) throws Exception{
    
    enableEvents(AWTEvent.WINDOW_EVENT_MASK);
    this.getContentPane().setLayout(new BorderLayout());
 
    Debug.debug("Creating BrowserPanel with baseUrl "+baseUrl, 3);
    
    requestFocusInWindow();
    this.setTitle(title);
    
    bOk.setText("OK");
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

    if(jBox!=null){
      pButton.add(jBox);
    }
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
    bDelete.setEnabled(false);
    bSave.setEnabled(false);

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
      ImageIcon homeIcon;
      URL imgURL=null;
      try{
        imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "folder_home2.png");
        homeIcon = new ImageIcon(imgURL);
      }
      catch(Exception e){
        Debug.debug("Could not find image "+ GridPilot.resourcesPath + "folder_home2.png", 3);
        homeIcon = new ImageIcon();
      }
      ImageIcon enterIcon;
      imgURL=null;
      try{
        imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "key_enter.png");
        enterIcon = new ImageIcon(imgURL);
      }
      catch(Exception e){
        Debug.debug("Could not find image "+ GridPilot.resourcesPath + "key_enter.png", 3);
        enterIcon = new ImageIcon();
      }
      JButton bHome = new JButton(homeIcon);
      bHome.setToolTipText("go to grid home-URL");
      bHome.setPreferredSize(new java.awt.Dimension(22, 22));
      bHome.setSize(new java.awt.Dimension(22, 22));
      bHome.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent me){
          MyThread t = (new MyThread(){
            public void run(){
              try{
                statusBar.setLabel("Opening URL...");
                setDisplay(GridPilot.gridHomeURL);
              }
              catch(Exception ee){
                statusBar.setLabel("ERROR: could not open "+GridPilot.gridHomeURL+
                    ". "+ee.getMessage());
                Debug.debug("ERROR: could not open "+GridPilot.gridHomeURL+
                    ". "+ee.getMessage(), 1);
                ee.printStackTrace();
              }
              doingSearch = false;
            }
          });     
          SwingUtilities.invokeLater(t);
        }
      });
      JButton bEnter = new JButton(enterIcon);
      bEnter.setToolTipText("go!");
      bEnter.setPreferredSize(new java.awt.Dimension(22, 22));
      bEnter.setSize(new java.awt.Dimension(22, 22));
      bEnter.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent me){
          statusBar.setLabel("Opening URL...");
          ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
          MyThread t = (new MyThread(){
            public void run(){
              try{
                setDisplay(currentUrlBox.getSelectedItem().toString());
              }
              catch(Exception ee){
                statusBar.setLabel("ERROR: could not open "+currentUrlBox.getSelectedItem().toString()+
                    ". "+ee.getMessage());
                Debug.debug("ERROR: could not open "+currentUrlBox.getSelectedItem().toString()+
                    ". "+ee.getMessage(), 1);
                ee.printStackTrace();
              }
              doingSearch = false;
            }
          });     
          SwingUtilities.invokeLater(t);
        }
      });

      JPanel jpNavigation = new JPanel(new GridBagLayout());
      jpNavigation.add(bHome);
      jpNavigation.add(new JLabel(" "));
      jpNavigation.add(new JLabel("URL: "));
      jpNavigation.add(currentUrlBox);
      topPanel.add(jpNavigation, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
          GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
          new Insets(0, 5, 0, 5), 0, 0));
      jpNavigation.add(new JLabel(" "));
      jpNavigation.add(bEnter);
      GridPilot.getClassMgr().addUrl("");
      addUrlKeyListener();
    }
    
    if(withFilter){
      JPanel jpFilter = new JPanel(new GridBagLayout());      
      if(!withNavigation){
        topPanel.add(currentUrlLabel,
            new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
                GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(0, 5, 0, 5), 0, 0));
      }
      jpFilter.add(new JLabel("Filter: "));
      jpFilter.add(jtFilter);
      topPanel.add(jpFilter, new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0,
          GridBagConstraints.WEST, GridBagConstraints.BOTH,
          new Insets(0, 5, 0, 5), 0, 0));
      topPanel.setPreferredSize(new Dimension(520, 56));
                
      panel.add(topPanel, BorderLayout.NORTH);

      addFilterKeyListener();
    }  
    
    if(withNavigation || withFilter){
      panel.add(topPanel, BorderLayout.NORTH);
    }
        
    //HTMLDocument d = new HTMLDocument();
    ep.addHyperlinkListener(new HyperlinkListener(){
      public void hyperlinkUpdate(final HyperlinkEvent e){
        if(e.getEventType()==HyperlinkEvent.EventType.ACTIVATED){
          if(e instanceof HTMLFrameHyperlinkEvent){
            ((HTMLDocument) ep.getDocument()).processHTMLFrameHyperlinkEvent(
               (HTMLFrameHyperlinkEvent) e);
          }
          else{
            //setUrl(e.getDescription());
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
      statusBar.setLabel("Type in URL and hit return");
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
    
    Dimension dim = new Dimension(panel.getPreferredSize().width + (jBox==null?50:300),
        panel.getPreferredSize().height);

    setSize(dim);
    setVisible(true);
    
    // Close window with ctrl+w
    jtFilter.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        if(!e.isControlDown()){
          return;
        }
        switch(e.getKeyCode()){
          case KeyEvent.VK_W:
            cancel();
        }
      }
    });
    
    currentUrlBox.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        if(!e.isControlDown()){
          return;
        }
        switch(e.getKeyCode()){
          case KeyEvent.VK_W:
            cancel();
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
            cancel();
        }
      }
    });

    if(url!=null && !url.equals("")){
      setUrl(url);
    }

  }
  
  /**
   * Set the text of the navigation label or input field
   */
  private void setUrl(String url){
    try{
      url = URLDecoder.decode(url, "utf-8");
    }
    catch (UnsupportedEncodingException e){
      e.printStackTrace();
    }
    String newUrl = url;
    GridPilot.getClassMgr().removeUrl("");
    Vector urlList = GridPilot.getClassMgr().getUrlList();
    /* remove leading whitespace */
    newUrl = newUrl.replaceAll("^\\s+", "");
    /* remove trailing whitespace */
    newUrl = newUrl.replaceAll("\\s+$", "");
    newUrl = newUrl.replaceAll("\\\\", "/");
    newUrl = newUrl.replaceAll("file:C", "file:/C");
    
    // check if url is already in history and add if not
    boolean refresh = false;
    if(!urlList.contains(newUrl)){
      refresh = true;
      Debug.debug("Adding URL to history: "+newUrl, 2);
      if(urlList.size()>HISTORY_SIZE){
        Debug.debug("History size exceeded, removing first, "+
            urlList.size()+">"+HISTORY_SIZE, 2);
        GridPilot.getClassMgr().removeUrl(urlList.iterator().next().toString());
      }
      GridPilot.getClassMgr().addUrl(newUrl);
      Debug.debug("urlSet is now: "+Util.arrayToString(
          urlList.toArray(), " : "), 2);
    }

    if(refresh || currentUrlBox.getItemCount()==0 && urlList.size()>0){
      currentUrlBox.removeAllItems();
      urlList = GridPilot.getClassMgr().getUrlList();
      for(ListIterator it=urlList.listIterator(urlList.size()-1); it.hasPrevious();){
        currentUrlBox.addItem(it.previous().toString());
      }
      currentUrlBox.updateUI();
    }

    addUrlKeyListener();
    currentUrlString = url;
    if(withNavigation){
      currentUrlBox.setSelectedItem(url);
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
  private void setDisplay(String url) throws Exception{
    try{
      lastUrlList = null;
      // TODO: implement lastUrlList for other than gsiftp - that is, http - will have to wait...
      if(url.startsWith("file:/~")){
        url = "file:"+System.getProperty("user.home")+url.substring(7);
      }
      else if(url.startsWith("file:~")){
        url = "file:"+System.getProperty("user.home")+url.substring(6);
      }
      Debug.debug("Checking URL, "+url, 3);
      // browse remote web directory
      if((url.startsWith("http://") ||
          url.startsWith("https://") ||
          url.startsWith("ftp://")) &&
         url.endsWith("/")){
        setHttpDirDisplay(url);
      }
      // local directory
      else if((url.startsWith("/") || url.matches("\\w:.*") ||
          url.startsWith("file:")) &&
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
        try{
          setHttpTextDisplay(url);
        }
        catch(Exception e){
          setHttpDirDisplay(url);
        }
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
      // reset cursor to default
      if(url.endsWith("/")){
        ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      }
    }
    catch(Exception e){
      ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      Debug.debug("Could not initialize panel with URL "+url+". "+e.getMessage(), 1);
      throw e;
    }
  }

  /**
   * Set the EditorPane to display the text page file url.
   * The page can then be edited and saved (when clicking bSave).
   */
  private void setGsiftpTextEdit(String url) throws IOException,
     FTPException{
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
    tmpFile = File.createTempFile("gridpilot-", ".txt");
    Debug.debug("Created temp file "+tmpFile, 3);
    GridFTPClient gridFtpClient = null;
    try{
      gridFtpClient = gsiftpFileTransfer.connect(host, Integer.parseInt(port));
    }
    catch(Exception e){
      Debug.debug("Could not connect. "+
         e.getMessage(), 1);
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    try{
      gridFtpClient.changeDir(localDir);
      if(gridFtpClient.getSize(localPath)>500000){
        throw new IOException("File too big");
      }
      gridFtpClient.get(localPath, tmpFile);
    }
    catch(Exception e){
      Debug.debug("Could not read "+localPath, 1);
      e.printStackTrace();
      String error = "ERROR!\n\nThe file "+localPath+" could not be read. "+
      "\n\nIf it is a directory, please end with a /. "+
      e.getMessage();
      ep.setText(error);
      throw new IOException(error);
    }
    pButton.updateUI();
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
      //With this cursor, the caret doesn't blink...
      //ep.setCursor(new Cursor(Cursor.TEXT_CURSOR));
      ep.setCursor(new Cursor(Cursor.DEFAULT_CURSOR));
      ep.getCaret().setBlinkRate(500);
      ep.updateUI();
      pButton.updateUI();
      
      bSave.setEnabled(true);
      bOk.setEnabled(ok);
      bNew.setEnabled(false);
      bDelete.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      
      Debug.debug("Setting thisUrl, "+url, 3);
      thisUrl = url;
      lastUrlList = new String [] {thisUrl};
      setUrl(thisUrl);
      statusBar.setLabel("");
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
      // With this cursor, the caret doesn't blink...
      //ep.setCursor(new Cursor(Cursor.TEXT_CURSOR));
      ep.setCursor(new Cursor(Cursor.DEFAULT_CURSOR));
      ep.getCaret().setBlinkRate(500);
      ep.updateUI();
      pButton.updateUI();
      Debug.debug("Setting thisUrl, "+url, 3);
      thisUrl = url;
      setUrl(thisUrl);
      lastUrlList = new String [] {thisUrl};
      statusBar.setLabel("");
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
      setUrl(thisUrl);
      lastUrlList = new String [] {thisUrl};
      ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
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
      setUrl(thisUrl);
      lastUrlList = new String [] {thisUrl};
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
              //    Debug.debug(inputLine, 3);
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
      GridFTPClient gridFtpClient = gsiftpFileTransfer.connect(host, Integer.parseInt(port));
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
      lastUrlList = new String [] {thisUrl};
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
    else if(fsPath.startsWith("file:")){
      localPath = fsPath.substring(5);
    }
    else if(fsPath.matches("(\\w:\\\\).*")){
      localPath = fsPath.substring(2);
    }
    else{
      localPath = fsPath;
    }
    localPath = localPath.replaceFirst("/[^\\/]*/\\.\\.", "");
    localPath = localPath.replaceFirst("^/(\\w):", "$1:");
    localPath = localPath.replaceFirst("^file:(\\w):", "$1:");
    localPath = localPath.replaceFirst("^/(\\w):", "$1:");
    try{
      localPath = URLDecoder.decode(localPath, "utf-8");
    }
    catch (UnsupportedEncodingException e){
      e.printStackTrace();
    }
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
        String filter = jtFilter.getText();
        if(filter==null || filter.equals("")){
          filter = "*";
        }
        statusBar.setLabel("Filtering...");
        filter = filter.replaceAll("\\.", "\\\\.");
        filter = filter.replaceAll("\\*", ".*");
        Debug.debug("Filtering with "+filter, 3);
        Vector lastUrlVector = new Vector();
        for(int j=0; j<text.length; ++j){
          if(text[j].substring(localPath.length()).matches(filter)){
            if(LocalStaticShellMgr.isDirectory(text[j])){
              ++directories;
            }
            else{
              ++files;
            }
            textVector.add("<a href=\"file:"+text[j]+"\">" + 
                (((text[j].matches("(\\w:\\\\).*") ||
                    text[j].matches("\\w:/.*")) &&
                    !localPath.matches("(\\w:\\\\).*") &&
                    !localPath.matches("\\w:/.*")) ? 
                    text[j].substring(localPath.length()+2) :
                      text[j].substring(localPath.length())) +  "</a>");
            lastUrlVector.add("file:"+text[j]);
          }
        }
        lastUrlList = new String [lastUrlVector.size()];
        for(int j=0; j<lastUrlList.length; ++j){
          lastUrlList[j] = lastUrlVector.get(j).toString();
        }
        ep.setContentType("text/html");
        htmlText = "<html>\n";
        if(!localPath.equals("/")){
          htmlText += "<a href=\"file:"+localPath+"../\">"/*+localPath*/+"../</a><br>\n";
        }
        htmlText += Util.arrayToString(textVector.toArray(), "<br>\n");
        htmlText += "\n</html>";
        ep.setText(htmlText);
        ep.setEditable(false);
        // if we don't get an exception, the directory got read...
        Debug.debug("Directory "+localPath, 2);
        Debug.debug("Setting thisUrl, "+localPath, 3);
        thisUrl = (new File(localPath)).toURI().toURL().toExternalForm();
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
  private void setGsiftpDirDisplay(String url) throws Exception{
    Debug.debug("setGsiftpDirDisplay "+url, 3);
    
    jtFilter.setEnabled(true);
    String filter = jtFilter.getText();
    String htmlText = "";

    try{
      bSave.setEnabled(false);
      bNew.setEnabled(true);
      bDelete.setEnabled(true);
      bUpload.setEnabled(true);
      bDownload.setEnabled(true);

      url = url.replaceFirst("/[^\\/]*/\\.\\.", "");
      GlobusURL globusUrl = new GlobusURL(url);
      Debug.debug("Opening directory on gridftp server\n"+globusUrl.toString(), 3);
      String localPath = "/";
      if(globusUrl.getPath()!=null){
        localPath = globusUrl.getPath();
        if(!localPath.startsWith("/")){
          localPath = "/" + localPath;
        }
      }
      String host = globusUrl.getHost();
      int port = globusUrl.getPort();
      if(port<0){
        port = 2811;
      }
      
      Vector textVector = gsiftpFileTransfer.list(globusUrl, filter,
          this.statusBar);

      String text = "";
      // TODO: reconsider max entries and why listing more is so slow...
      // display max 500 entries
      // TODO: make this configurable
      int maxEntries = 500;
      int length = textVector.size()<maxEntries ? textVector.size() : maxEntries;
      String name = null;
      String bytes = null;
      String longName = null;
      String [] nameAndBytes = null;
      lastUrlList = new String [length];
      for(int i=0; i<length; ++i){
        nameAndBytes = null;
        longName = textVector.get(i).toString();
        try{
          nameAndBytes = Util.split(longName);
        }
        catch(Exception e){
        }
        if(nameAndBytes!=null && nameAndBytes.length>0){
          name = nameAndBytes[0];
          if(nameAndBytes.length>1){
            bytes = nameAndBytes[1];
          }
          else{
            bytes = "";
          }
        }
        else{
          name = longName;
          bytes = "";
        }
        text += "<a href=\"gsiftp://"+host+":"+port+localPath+name+"\">"+
        /*"gsiftp://"+host+":"+port+localPath+*/name+"</a> "+bytes;
        if(i<length-1){
          text += "<br>\n";
        }
        lastUrlList[i] = "gsiftp://"+host+":"+port+localPath+name;
        Debug.debug(textVector.get(i).toString(), 3);
      }
      ep.setContentType("text/html");
      htmlText = "<html>\n";
      
      if(!localPath.equals("/")){
        htmlText += "<a href=\"gsiftp://"+host+":"+port+localPath+"../\">"+/*"gsiftp://"+host+":"+port+localPath+*/"../</a><br>\n";
      }
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
    }
    catch(Exception e){
      bSave.setEnabled(false);
      bOk.setEnabled(false);
      ep.setText("ERROR!\n\nThe directory could not be read.");
      pButton.updateUI();
      throw e;
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
      setUrl(thisUrl);
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
      saveHistory();
      dispose();
      lastURL = origUrl;
      lastUrlList = null;
    }
    else{
      try{
        String newUrl = thisUrl.substring(0, thisUrl.lastIndexOf("/")+1);
        //thisUrl = newUrl;
        //setUrl(thisUrl);
        setDisplay(newUrl);
      }
      catch(Exception ioe){
        ioe.printStackTrace();
        dispose();
        lastUrlList = null;
        lastURL = origUrl;
      }
    }
  }

  //Set lastURL and close the dialog
  void exit(){
    //GridPilot.lastURL = ep.getPage();
    Debug.debug("Setting lastURL, "+thisUrl, 3);
    lastURL = thisUrl;
    saveHistory();
    dispose();
  }
  
  void saveHistory(){
    Debug.debug("Saving history", 3);
    if(saveUrlHistory){
      try{
        LocalStaticShellMgr.writeFile(GridPilot.browserHistoryFile,
           Util.arrayToString(GridPilot.getClassMgr().getUrlList().toArray(),"\n"), false);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not write history file. "+e.getMessage(), 1);
        e.printStackTrace();
      }
    }
  }

  /**
   * Write file or directory fsPath in local file system containing text.
   */
  private void localWriteFile(String fsPath, String text) throws IOException {
    if(fsPath.endsWith("/") && (text==null || text.equals(""))){
      fsPath = fsPath.substring(0, fsPath.length()-1);
      if(!LocalStaticShellMgr.mkdirs(fsPath)){
        throw new IOException("Could not make directory "+fsPath);
      }
    }
    else if(!fsPath.endsWith("/")){
      LocalStaticShellMgr.writeFile(fsPath, text, false);
    }
    else{
      throw new IOException("ERROR: Cannot write text to a directory.");
    }
    Debug.debug("File or directory "+fsPath+" written", 2);
  }

  /**
   * Delete file or directory fsPath in local file system.
   */
  private void localDeleteFile(String fsPath) throws IOException{
    if(!LocalStaticShellMgr.deleteFile(fsPath)){
      throw new IOException(fsPath+" could not be deleted.");
    }
    Debug.debug("File or directory "+fsPath+" deleted", 2);
  }
  
  /**
   * Create an empty file or directory in the directory fsPath.
   * Ask for the name.
   * If a name ending with a / is typed in, a directory is created.
   * (this path must match the URL url).
   */
  private String localCreate(String fsPath) throws IOException {
    String fileName = Util.getName("File name (end with a / to create a directory)", "");   
    if(fsPath==null || fileName==null){
      return null;
    }  
    if(!fsPath.endsWith("/") && !fileName.startsWith("/")){
      fsPath = fsPath+"/";
    }
    localWriteFile(fsPath+fileName, "");
    return fileName;
  }

  /**
   * Delete file or directory.
   * Ask for the name.
   */
  private void delete() throws IOException{
    String fileName = Util.getFileName(""/*jtFilter.getText()*/);
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
      fsPath = fsPath.replaceFirst("^/(\\w):", "$1:");
      Debug.debug("Deleting file in "+fsPath, 3);
      if(fsPath==null || fileName==null){
        return;
      }  
      if(!fsPath.endsWith("/") && !fileName.startsWith("/")){
        fsPath = fsPath+"/";
      }
      try{
        localDeleteFile(fsPath+fileName);
        statusBar.setLabel(fsPath+" deleted");
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
        catch(Exception ioe){
          ioe.printStackTrace();
        }
      }
      catch(Exception e){
        Debug.debug("ERROR: could not delete "+fsPath+fileName+". "+e.getMessage(), 1);
        e.printStackTrace();
        ep.setText("ERROR!\n\nThe file "+fsPath+fileName+" could not be deleted.\n\n"+
            //"If it is a directory, delete all files within first.\n\n"+
            e.getMessage());
        statusBar.setLabel("ERROR: The file "+fsPath+fileName+" could not be deleted.");
        //pButton.updateUI();
      }
    }
    else if(thisUrl.startsWith("gsiftp://")){
      String fullName = null;
      GlobusURL globusUrl = null;
      if(!thisUrl.endsWith("/") && !thisUrl.startsWith("/")){
        fullName = thisUrl+"/"+fileName;
      }
      else{
        fullName = thisUrl+fileName;
      }
      try{
        globusUrl = new GlobusURL(fullName);
        gsiftpFileTransfer.deleteFile(globusUrl);
        statusBar.setLabel(globusUrl.getPath()+" deleted");
        try{
          setDisplay(thisUrl);
        }
        catch(Exception e){
          Debug.debug("WARNING: could not display "+thisUrl, 1);
        }
      }
      catch(Exception e){
        Debug.debug("ERROR: could not delete "+fullName+". "+e.getMessage(), 1);
        e.printStackTrace();
        ep.setText("ERROR!\n\nThe file "+fullName+" could not be deleted.\n\n"+
            //"If it is a directory, delete all files within first.\n\n"+
            e.getMessage());
        statusBar.setLabel("ERROR: The file "+fullName+" could not be deleted.");
        //pButton.updateUI();
      }
    }
    else{
      throw(new IOException("Unknown protocol for "+thisUrl));
    }
  }
  
  /**
   * Creates a file on a URL.
   */
  private String createNew() throws Exception{
    if(!thisUrl.endsWith("/")){
      throw new IOException("ERROR: URL "+thisUrl+" does not end with /.");
    }
    String ret = null;
    try{
      Debug.debug("Creating file in "+thisUrl, 3);
      // local directory
      if(thisUrl.startsWith("file:")){
        String fsPath = thisUrl;
        fsPath = fsPath.replaceFirst("^file://", "/");
        fsPath = fsPath.replaceFirst("^file:/", "/");
        fsPath = fsPath.replaceFirst("^file:", "");
        fsPath = fsPath.replaceFirst("^/(\\w):", "$1:");
        Debug.debug("Creating file in "+fsPath, 3);
        ret = localCreate(fsPath);
        ep.getDocument().putProperty(
            Document.StreamDescriptionProperty, null);
        setDisplay((new URL("file:"+fsPath)).toExternalForm());
      }
      // remote gsiftp directory
      else if(thisUrl.startsWith("gsiftp://")){
        Debug.debug("Creating file in "+thisUrl, 3);
        GlobusURL globusUrl = new GlobusURL(thisUrl);
        ret = gsiftpFileTransfer.create(globusUrl);
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
    return ret;
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
          fsPath = fsPath.replaceFirst("^/(\\w):", "$1:");
          String text = ep.getText();
          localWriteFile(fsPath, text);
        }
        else if(thisUrl.startsWith("gsiftp://")){
          GlobusURL globusUrl = new GlobusURL(thisUrl);
          gsiftpFileTransfer.write(globusUrl, ep.getText());
        }
        else{
          throw(new IOException("Unknown protocol for "+thisUrl));
        }
        statusBar.setLabel(thisUrl+" saved");
      }
      else if(e.getSource()==bNew){
        String newFileOrDir = createNew();
        statusBar.setLabel(/*thisUrl+*/newFileOrDir+" created");
      }
      else if(e.getSource()==bDelete){
        delete();
      }
      else if(e.getSource()==bUpload){
        File file = getInputFile();
        TransferControl.upload(file, thisUrl, ep);
        try{
          ep.getDocument().putProperty(
              Document.StreamDescriptionProperty, null);
          setDisplay(thisUrl);
        }
        catch(Exception ioe){
          ioe.printStackTrace();
        }
        statusBar.setLabel(thisUrl+" uploaded");
      }
      else if(e.getSource()==bDownload){
        File dir = Util.getDownloadDir(this);      
        String fileName = Util.getFileName(jtFilter.getText());
        Debug.debug("Getting file : "+thisUrl+fileName+" -> "+dir.getAbsolutePath(), 3);
        TransferControl.download(thisUrl+fileName, dir, ep);
        try{
          ep.getDocument().putProperty(
              Document.StreamDescriptionProperty, null);
          setDisplay(thisUrl);
        }
        catch(Exception ioe){
          ioe.printStackTrace();
        }
      }
    }
    catch(Exception ex){
      statusBar.setLabel("ERROR: "+ex.getMessage());
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
  
}