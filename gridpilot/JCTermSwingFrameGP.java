package gridpilot;

import com.jcraft.jcterm.Connection;
import com.jcraft.jcterm.JCTermSwing;
import com.jcraft.jcterm.JCTermSwingFrame;
import com.jcraft.jcterm.Sftp;
import com.jcraft.jcterm.Term;
import com.jcraft.jsch.*;

import gridfactory.common.Debug;

import java.awt.*;
import java.awt.event.*;

import javax.swing.*;
import java.io.*;

public class JCTermSwingFrameGP extends JCTermSwingFrame {
  
  private static final long serialVersionUID = 1L;
  private String password=null;
  private String keyFile=null;
  private String keyPassphrase=null;
  private int port=22;

  public JCTermSwingFrameGP(String name, String _host, int _port, String _user,
      String _password, String _keyFile, String _keyPassphrase){
    
    host=_host;
    port=_port;
    user=_user;
    password=_password;
    keyFile=_keyFile;
    keyPassphrase=_keyPassphrase;

    enableEvents(AWTEvent.KEY_EVENT_MASK);
    addWindowListener(new WindowAdapter(){
      public void windowClosing(WindowEvent e){
        dispose();
      }
    });

    JMenuBar mb=getJMenuBar();
    setJMenuBar(mb);

    setTerm(new JCTermSwing());
    getContentPane().add("Center", (JCTermSwing) getTerm());
    pack();
    ((JCTermSwing) getTerm()).setVisible(true);

    ComponentListener l=new ComponentListener(){
      public void componentHidden(ComponentEvent e){
      }

      public void componentMoved(ComponentEvent e){
      }

      public void componentResized(ComponentEvent e){
        Debug.debug(e.toString(), 3);
        Component c=e.getComponent();
        int cw=c.getWidth();
        int ch=c.getHeight();
        int cwm=(c.getWidth()-((JFrame)c).getContentPane().getWidth());
        int chm=(c.getHeight()-((JFrame)c).getContentPane().getHeight());
        cw-=cwm;
        ch-=chm;
        ((JCTermSwing) getTerm()).setSize(cw, ch);
      }

      public void componentShown(ComponentEvent e){
      }
    };
    addComponentListener(l);

    openSession();
  }

  private Thread thread=null;

  public void kick(){
    this.thread=new Thread(this);
    this.thread.start();
  }
  
  public void actionPerformed(ActionEvent e){
    user=null;
    password=null;
    host=null;
    keyFile=null;
    keyPassphrase=null;
    super.actionPerformed(e);
  }
  
  public void quit(){
    Debug.debug("Quitting", 2);
    thread=null;
    if(connection!=null){
      connection.close();
      connection=null;
    }

    if(jschsession!=null){
      jschsession.dispose();
      jschsession=null;
    }

    this.dispose();
  }

  public void run(){
    int tries=0;
    int maxTries=3;
    while(thread!=null && tries<maxTries){
      try{
        if(host==null || host.equals("") ||
            user==null || user.equals("")){
          Debug.debug("Connecting...", 2);
          try{
            String _host=JOptionPane.showInputDialog((JCTermSwing) getTerm(),
                "Enter username@hostname", "");
            if(_host==null){
              break;
            }
            String _user=_host.substring(0, _host.indexOf('@'));
            _host=_host.substring(_host.indexOf('@')+1);
            if(_host==null||_host.length()==0){
              continue;
            }
            if(_host.indexOf(':')!=-1){
              try{
                port=Integer.parseInt(_host.substring(_host.indexOf(':')+1));
              }
              catch(Exception eee){
              }
              _host=_host.substring(0, _host.indexOf(':'));
            }
            user=_user;
            host=_host;
          }
          catch(Exception ee){
            ee.printStackTrace();
            continue;
          }
        }

        try{
          UserInfo ui=new MyUserInfo();
          
          jschsession=JSchSessionGP.getSession(user, password, host, port, ui, proxy,
              keyFile, keyPassphrase);
          java.util.Properties config=new java.util.Properties();
          if(getCompression()==0){
            config.put("compression.s2c", "none");
            config.put("compression.c2s", "none");
          }
          else{
            config.put("compression.s2c", "zlib,none");
            config.put("compression.c2s", "zlib,none");
          }
          jschsession.getSession().setConfig(config);
          jschsession.getSession().rekey();
        }
        catch(Exception e){
          e.printStackTrace();
          user=null;
          ++tries;
          continue;
        }

        Channel channel=null;
        OutputStream out=null;
        InputStream in=null;

        if(mode==SHELL){
          // TODO: find out about the input stream of Channel
          // unicode, ?...
          channel=jschsession.getSession().openChannel("shell");
          
          //((ChannelShell)channel).setPtyType("vt102");
          /*java.util.Hashtable env=new java.util.Hashtable();
          env.put("LANG", "da_DK");
          ((ChannelShell)channel).setEnv(env);*/

          if(xforwarding){
            jschsession.getSession().setX11Host(xhost);
            jschsession.getSession().setX11Port(xport+6000);
            channel.setXForwarding(true);
          }

          out=channel.getOutputStream();
          in=channel.getInputStream();
          
          channel.connect();
        }
        else if(mode==SFTP){

          out=new PipedOutputStream();
          in=new PipedInputStream();

          channel=jschsession.getSession().openChannel("sftp");

          channel.connect();

          (new Sftp((ChannelSftp)channel, (InputStream)(new PipedInputStream(
              (PipedOutputStream)out)), new PipedOutputStream(
              (PipedInputStream)in))).kick();
        }

        final OutputStream fout=out;
        final InputStream fin=in;
        final Channel fchannel=channel;

        connection=new Connection(){
          public InputStream getInputStream(){
            return fin;
          }

          public OutputStream getOutputStream(){
            return fout;
          }

          public void requestResize(Term term){
            if(fchannel instanceof ChannelShell){
              int c=term.getColumnCount();
              int r=term.getRowCount();
              ((ChannelShell)fchannel).setPtySize(c, r, c*term.getCharWidth(),
                  r*term.getCharHeight());
            }
          }

          public void close(){
            fchannel.disconnect();
          }
        };
        setTitle(user+"@"+host+(port!=22 ? new Integer(port).toString() : ""));
        ((JCTermSwing) getTerm()).requestFocus();
        ((JCTermSwing) getTerm()).start(connection);
      }
      catch(Exception e){
        e.printStackTrace();
      }
      break;
    }
    setTitle("JCTerm");
    thread=null;
  }

  public class MyUserInfo implements UserInfo, UIKeyboardInteractive{
    public boolean promptYesNo(String str){
      // This is only used to prompt: "The authenticity of the host ..."
      // Disable prompting.
      return true;
      /*Object[] options= {"yes", "no"};
      int foo=JOptionPane.showOptionDialog(JCTermSwingFrame.this.term, str,
          "Warning", JOptionPane.DEFAULT_OPTION, JOptionPane.WARNING_MESSAGE,
          null, options, options[0]);
      return foo==0;*/
    }

    String passwd=null;
    String passphrase=null;
    JTextField pword=new JPasswordField(20);

    public String getPassword(){
      return passwd;
    }

    public String getPassphrase(){
      return passphrase;
    }

    public boolean promptPassword(String message){
      Object[] ob= {pword};
      int result=JOptionPane.showConfirmDialog((JCTermSwing) getTerm(), ob,
          message, JOptionPane.OK_CANCEL_OPTION);
      if(result==JOptionPane.OK_OPTION){
        passwd=pword.getText();
        return true;
      }
      else{
        return false;
      }
    }

    public boolean promptPassphrase(String message){
      return true;
    }

    public void showMessage(String message){
      JOptionPane.showMessageDialog(null, message);
    }

    final GridBagConstraints gbc=new GridBagConstraints(0, 0, 1, 1, 1, 1,
        GridBagConstraints.NORTHWEST, GridBagConstraints.NONE, new Insets(0, 0,
            0, 0), 0, 0);
    private Container panel;

    public String[] promptKeyboardInteractive(String destination, String name,
        String instruction, String[] prompt, boolean[] echo){
      panel=new JPanel();
      panel.setLayout(new GridBagLayout());

      gbc.weightx=1.0;
      gbc.gridwidth=GridBagConstraints.REMAINDER;
      gbc.gridx=0;
      panel.add(new JLabel(instruction), gbc);
      gbc.gridy++;

      gbc.gridwidth=GridBagConstraints.RELATIVE;

      JTextField[] texts=new JTextField[prompt.length];
      for(int i=0; i<prompt.length; i++){
        gbc.fill=GridBagConstraints.NONE;
        gbc.gridx=0;
        gbc.weightx=1;
        panel.add(new JLabel(prompt[i]), gbc);

        gbc.gridx=1;
        gbc.fill=GridBagConstraints.HORIZONTAL;
        gbc.weighty=1;
        if(echo[i]){
          texts[i]=new JTextField(20);
        }
        else{
          texts[i]=new JPasswordField(20);
        }
        panel.add(texts[i], gbc);
        gbc.gridy++;
      }

      if(JOptionPane.showConfirmDialog((JCTermSwing) getTerm(), panel,
          destination+": "+name, JOptionPane.OK_CANCEL_OPTION,
          JOptionPane.QUESTION_MESSAGE)==JOptionPane.OK_OPTION){
        String[] response=new String[prompt.length];
        for(int i=0; i<prompt.length; i++){
          response[i]=texts[i].getText();
        }
        return response;
      }
      else{
        return null; // cancel
      }
    }
  }

  /* String name, String _host, int _port, String _user,
     String _password, String _keyFile, String _keyPassphrase */
  /* Example:
   
   public static void main(String[] arg){
    final JCTermSwingFrameGP frame=new JCTermSwingFrameGP(
        "GridPilot SSH terminal",
        "pato",
        22,
        "fjob",
        null,
        "C:\\Documents and Settings\\Frederik Orellana\\ssh\\paloma_id_rsa",
        null);
    frame.setVisible(true);
    frame.setResizable(true);

  }*/
}
