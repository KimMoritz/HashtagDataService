package dataservice;

public class MongoDAOException extends Exception {

    private String errorCode;
    private Exception linkedException;

    public MongoDAOException(String reason, String errorCode) {
        super(reason);
        this.errorCode = errorCode;
        this.linkedException = null;
    }

    public MongoDAOException(String reason) {
        this(reason, (String)null);
        this.linkedException = null;
    }

    public String getErrorCode() {
        return this.errorCode;
    }

    public Exception getLinkedException() {
        return this.linkedException;
    }

    public synchronized void setLinkedException(Exception ex) {
        this.linkedException = ex;
    }
}
