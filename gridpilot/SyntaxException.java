package gridpilot;

/**
 * Syntax error in arithmetic expression.
 *
 */

class SyntaxException extends Exception{
  public SyntaxException(String msg){
    super(msg);
  }
}
