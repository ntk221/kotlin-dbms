package simpledb.tx.recovery

import simpledb.file.Page
import simpledb.log.LogManager
import simpledb.tx.Transaction

/**
 * Commit Log Record
 */
class CommitRecord(private val page: Page): LogRecord {
    private val transactionNumber: Int

    init {
        val transactionPosition = Integer.BYTES
        transactionNumber = page.getInt(transactionPosition)
    }

    override fun op(): Int {
        return Operator.COMMIT.id
    }

    override fun txNumber(): Int {
        return transactionNumber
    }

    /**
     * commit recordはやり直しを行う情報は持たない
     */
    override fun undo(transaction: Transaction) {}

    override fun toString(): String {
        return "<COMMIT $transactionNumber>"
    }

    /**
     * ログにCommit recordを書くメソッド
     * このログレコードはCOMMIT Operatorの後にトランザクションIDが含まれている。
     */
    companion object {
        fun writeToLog(logManager: LogManager, transactionNumber: Int): Int {
            val record = ByteArray(2 * Integer.BYTES)
            val page = Page(record)
            page.setInt(0, Operator.COMMIT.id)
            page.setInt(Integer.BYTES, transactionNumber)
            return logManager.append(record)
        }
    }
}