package gridpilot;

import java.net.*;
import java.io.*;


/**
 * This class allows to reload a plug-in from the file .class, without reboot the all
 * application, and without using the cached class.
 * This class will try to read from file the asked class, all will read each .class
 * in the same directory as well. If it is not possible (jar file, ...), this class
 * uses the "normal" class loader.
 */

class MyClassLoader extends ClassLoader{
  public Class findClass(String name)throws ClassNotFoundException{

    try{
      URL classUrl = ClassLoader.getSystemResource(name.replace('.', '/').concat(".class"));
      if(classUrl == null)
        throw new ClassNotFoundException();

      Debug.debug(classUrl.toString(), 2);
      File d = new File(classUrl.getFile()).getParentFile();
      File [] files = d.listFiles();
      if(files != null){
        for(int i=0; i<files.length; ++i){
          Debug.debug("tmp:"+files[i].getName(), 2);
          if(files[i].getName().endsWith(".class")){
            String className = name.substring(0, name.lastIndexOf(".")) + "." +
                  files[i].getName().substring(0, files[i].getName().indexOf(".class"));
//          Debug.debug("name : " + className, 3);
            if(!className.equals(name)){
              try{
                loadClass(files[i].toURL(), className);
                }catch(Throwable t){}
            }
          }
        }

        return loadClass(classUrl, name);
      }

    }catch(Exception ioe){
      ioe.printStackTrace();
      return null;
    }

    return super.loadClass(name);
  }

  private Class loadClass(URL classUrl, String name) throws ClassNotFoundException, IOException{
    if(classUrl.getProtocol().equals("file")){
      InputStream is = classUrl.openStream();
      byte b [] = new byte[is.available()];
      is.read(b);
      Class c = defineClass(name, b, 0, b.length);
      return c;
    }

    return loadClass(name);
  }

}