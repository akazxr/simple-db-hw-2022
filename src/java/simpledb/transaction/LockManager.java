package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    static Map<PageId, Map<TransactionId, Lock>> PAGELOCKS = new ConcurrentHashMap<>();

    private boolean acquireLock(Lock lock, TransactionId transactionId, PageId pageId) {
        Map<TransactionId, Lock> locks4OnePage = PAGELOCKS.get(pageId);
        if (locks4OnePage == null) {
            Map<TransactionId, Lock> map = new ConcurrentHashMap<>();
            map.put(transactionId, lock);
            PAGELOCKS.put(pageId, map);
            return true;
        }

        if (locks4OnePage.size() == 1) {
            if (locks4OnePage.get(transactionId).lockEnum.equals(Lock.LockEnum.SHARE)) {
                locks4OnePage.get(transactionId).setLockEnum(lock.lockEnum);
                return true;
            }

            if (locks4OnePage.get(transactionId).lockEnum.equals(Lock.LockEnum.EXCLUSIVE)) {
                r
            }
        }

        if (locks4OnePage.size() == 1 && lock.lockEnum.equals(Lock.LockEnum.EXCLUSIVE)) {
            throw new ConcurrentModificationException("exclude lock conflicted");
        }

        for (TransactionId)

    }

}
