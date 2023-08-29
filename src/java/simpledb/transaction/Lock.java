package simpledb.transaction;

public class Lock {
    public LockEnum getLockEnum() {
        return lockEnum;
    }

    public Lock(LockEnum lockEnum, TransactionId transactionId) {
        this.lockEnum = lockEnum;
        this.transactionId = transactionId;
    }

    public void setLockEnum(LockEnum lockEnum) {
        this.lockEnum = lockEnum;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    private LockEnum lockEnum;

    private TransactionId transactionId;

    public enum LockEnum {
        SHARE,
        EXCLUSIVE;
    }
}
