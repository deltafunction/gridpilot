package gridpilot;

import gridfactory.common.Debug;

import com.jcraft.jcterm.JSchSession;
import com.jcraft.jsch.*;

public class JSchSessionGP extends JSchSession{

  public static JSchSessionGP getSession(String username, String password,
      String hostname, int port, UserInfo userinfo, Proxy proxy,
      String keyFile, String keyPassphrase)
      throws JSchException{
    String key=getPoolKey(username, hostname, port);
    try{
      JSchSessionGP jschSession=(JSchSessionGP)pool.get(key);
      if(jschSession!=null&&!jschSession.getSession().isConnected()){
        pool.remove(key);
        jschSession=null;
      }
      if(jschSession==null){
        Session session=null;
        Debug.debug("Creating session with "+username+":"+password+":"+port+":"+keyFile+":"+keyPassphrase, 2);
        session=createSession(username, password, hostname, port, userinfo,
            proxy, keyFile, keyPassphrase);
        Debug.debug("Session created", 3);

        if(session==null)
          throw new JSchException("The JSch service is not available");

        JSchSessionGP schSession=new JSchSessionGP(session, key);
        pool.put(key, schSession);

        return schSession;
      }
      return jschSession;
    }
    catch(JSchException e){
      pool.remove(key);
      throw e;
    }
  }

  private static Session createSession(String username, String password,
      String hostname, int port, UserInfo userinfo, Proxy proxy,
      String keyFile, String keyPassphrase)
      throws JSchException{
    Session session=null;
    if(sessionFactory==null){
      session=getJSch().getSession(username, hostname, port);
    }
    else{
      session=sessionFactory.getSession(username, hostname, port);
    }
    session.setTimeout(60000);
    if(password!=null)
      session.setPassword(password);
    session.setUserInfo(userinfo);
    if(proxy!=null)
      session.setProxy(proxy);
    if(keyFile!=null && !keyFile.equals(""))
      getJSch().addIdentity(keyFile, (keyPassphrase==null?keyPassphrase:""));
    //KeyPair kp = KeyPair.load(jsch, "private key", "public key");
    session.connect(60000);
    session.setServerAliveInterval(60000);
    return session;
  }

  private JSchSessionGP(Session session, String key){
    super(session, key);
  }
}
