package gridpilot;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

public class MyFileAppender extends FileAppender {
  public void append(LoggingEvent event){
    if(event.getLevel()==Level.WARN ||
       event.getLevel()==Level.ERROR ||
       event.getLevel()==Level.FATAL){
      super.append(event);
    }
  }
}
