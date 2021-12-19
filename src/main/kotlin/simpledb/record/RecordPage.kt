package simpledb.record

import simpledb.file.BlockId
import simpledb.tx.Transaction

/**
 * 1つのブロック（スロットの配列）を表すクラス: サイズとして何バイトか決まっている
 * 1ブロックをスロットの配列として管理（スロットは1レコード+レコードの使用有無のフラグ: 0は空、1は使用済み）
 * Record Pageのイメージ: [slot0|slot1|.....|slot13|空き領域]
 * 1ブロック400バイト
 * スロット使用領域: 378バイト
 * 空き領域: 22バイト
 * slot0: [1|record0]
 * slot1: [0|record1]
 * slot13: [1|record13]
 * (1 record 26バイト（layoutが保持する情報）+ 使用済みかのフラグ 1バイト)* 14 slot
 * transactionを通して値に設定する
 */
class RecordPage(
    private val transaction: Transaction,
    val blockId: BlockId,
    private val layout: Layout,
) {
    init {
        transaction.pin(blockId)
    }

    fun getInt(slot: Int, fieldName: String): Int {
        val layoutOffset = layout.offset(fieldName) ?: throw RecordPageException()
        val fieldPosition = offset(slot) + layoutOffset
        return transaction.getInt(blockId, fieldPosition) ?: throw RecordPageException()
    }

    fun getString(slot: Int, fieldName: String): String {
        val layoutOffset = layout.offset(fieldName) ?: throw RecordPageException()
        val fieldPosition = offset(slot) + layoutOffset
        return transaction.getString(blockId, fieldPosition) ?: throw RecordPageException()
    }

    fun setInt(slot: Int, fieldName: String, value: Int) {
        val layoutOffset = layout.offset(fieldName) ?: throw RecordPageException()
        val fieldPosition = offset(slot) + layoutOffset
        transaction.setInt(blockId, fieldPosition, value, true)
    }

    fun setString(slot: Int, fieldName: String, value: String) {
        val layoutOffset = layout.offset(fieldName) ?: throw RecordPageException()
        val fieldPosition = offset(slot) + layoutOffset
        transaction.setString(blockId, fieldPosition, value, true)
    }

    fun delete(slot: Int) {
        setFlag(slot, RecordPageState.EMPTY.id)
    }

    fun format() {
        var slot = 0
        while (isValidSlot(slot)) {
            transaction.setInt(blockId, offset(slot), RecordPageState.EMPTY.id, false)
            val schema = layout.schema()
            for (fieldName in schema.fields) {
                val layoutOffset = layout.offset(fieldName) ?: throw RecordPageException()
                val fieldPosition = offset(slot) + layoutOffset
                if (schema.type(fieldName) == java.sql.Types.INTEGER) {
                    transaction.setInt(blockId, fieldPosition, 0, false)
                } else {
                    transaction.setString(blockId, fieldPosition, "", false)
                }
            }
            slot++
        }
    }

    fun nextAfter(slot: Int): Int {
        return searchAfter(slot, RecordPageState.USED.id)
    }

    fun insertAfter(slot: Int): Int {
        val newSlot = searchAfter(slot, RecordPageState.EMPTY.id)
        if (newSlot >= 0) setFlag(newSlot, RecordPageState.USED.id)
        return newSlot
    }

    private fun searchAfter(slot: Int, flag: Int): Int {
        var nextSlot = slot + 1
        while (isValidSlot(nextSlot)) {
            val transactionInt = transaction.getInt(blockId, offset(nextSlot))
            if (transactionInt != null && transactionInt == flag) {
                return nextSlot
            }
            nextSlot++
        }
        return -1
    }

    private fun isValidSlot(slot: Int): Boolean {
        return offset(slot+1) <= transaction.blockSize()
    }

    // Private auxiliary methods
    private fun setFlag(slot: Int, flag: Int) {
        transaction.setInt(blockId, offset(slot), flag, true)
    }

    private fun offset(slot: Int): Int {
        return slot * layout.slotSize()
    }
}