package simpledb.transaction;

import com.sun.corba.se.impl.naming.cosnaming.TransientNameServer;

import simpledb.storage.PageId;

import org.omg.CORBA.TRANSACTION_UNAVAILABLE;

import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    public static Map<PageId, Map<TransactionId, Lock>> PAGELOCKS = new ConcurrentHashMap<>();

    public static synchronized boolean acquireLock(Lock acquiredLock, TransactionId transactionId, PageId pageId)
        throws TransactionAbortedException {
        Map<TransactionId, Lock> locksOnPage = PAGELOCKS.get(pageId);
        if (locksOnPage == null || locksOnPage.isEmpty()) {
            Map<TransactionId, Lock> map = new ConcurrentHashMap<>();
            map.put(transactionId, acquiredLock);
            PAGELOCKS.put(pageId, map);
            return true;
        }

        // ========== 如果锁中包含当前请求的事务id ==========
        if (locksOnPage.containsKey(acquiredLock.getTransactionId())) {
            // ----------- 锁中只有当前请求的事务id ----------
            if (locksOnPage.size() == 1) {
                if (acquiredLock.getLockEnum().equals(Lock.LockEnum.EXCLUSIVE) && locksOnPage.get(
                    acquiredLock.getTransactionId()).getLockEnum().equals(Lock.LockEnum.EXCLUSIVE)) {
                    // 请求写锁，页面上的锁是写锁，可重入
                    return true;
                }
                if (acquiredLock.getLockEnum().equals(Lock.LockEnum.EXCLUSIVE) && locksOnPage.get(
                    acquiredLock.getTransactionId()).getLockEnum().equals(Lock.LockEnum.SHARE)) {
                    // 请求写锁，页面上的锁是读锁，进行锁升级
                    locksOnPage.get(acquiredLock.getTransactionId()).setLockEnum(Lock.LockEnum.EXCLUSIVE);
                    return true;
                }
                // 请求读锁
                return true;
            }

            // ---------- 锁中包含其他的事务id ----------
            if (acquiredLock.getLockEnum().equals(Lock.LockEnum.EXCLUSIVE)) {
                if (locksOnPage.get(acquiredLock.getTransactionId()).getLockEnum().equals(Lock.LockEnum.EXCLUSIVE)) {
                    // 如果包含当前事务的写锁，获取成功 （理论上不存在此种情况）
                    return true;
                } else {
                    // 如果是包含当前事务的读锁，说明存在其他事务的读锁，等待重试
                    return false;
                }
            }
            // 请求读锁
            return true;
        }

        // ========== 锁中不包含当前请求的事务id ==========
        if (locksOnPage.size() > 1) {
            // 大于1说明都是读锁
            if (acquiredLock.getLockEnum().equals(Lock.LockEnum.SHARE)) {
                return true;
            } else {
                return false;
            }
        } else {
            // 等于1则分情况讨论
            // 如果page上的锁是读锁，只有读锁可以进入
            // 如果page上的锁是写锁，均无法进入
            TransactionId tid = locksOnPage.keySet().iterator().next();
            if (locksOnPage.get(tid).getLockEnum().equals(Lock.LockEnum.SHARE)) {
                if (acquiredLock.getLockEnum().equals(Lock.LockEnum.SHARE)) {
                    return true;
                }
                return false;
            } else {
                return false;
            }
        }
    }

    public static synchronized void releaseLock(TransactionId transactionId, PageId pageId) {
        Map<TransactionId, Lock> locksOnPage = PAGELOCKS.get(pageId);
        if (locksOnPage == null || locksOnPage.isEmpty()) {
            return;
        }
        if (!locksOnPage.containsKey(transactionId)) {
            return;
        }
        locksOnPage.remove(transactionId);
    }

    public static synchronized void releaseLocksOnPage(PageId pageId) {
        PAGELOCKS.remove(pageId);
    }

    public static boolean holdsLock(TransactionId tid, PageId p) {
        if (PAGELOCKS == null || PAGELOCKS.isEmpty()) {
            return false;
        }
        if (!PAGELOCKS.containsKey(p)) {
            return false;
        }
        if (PAGELOCKS.get(p) == null || PAGELOCKS.get(p).isEmpty()) {
            return false;
        }

        if (PAGELOCKS.get(p).containsKey(tid)) {
            return true;
        }
        return false;
    }
}