package simpledb.file

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * Disk blockに保存されている内容（値）を保持するクラス
 *
 * @property charset 文字列に変換するときのコード: ASCII
 * @property bb ByteBuffer バイト型配列のラップして、配列内の値にアクセスする
 *
 * @constructor Buffer Managerから使用される。
 * 使用するBlockサイズを指定してPageを作成する。
 *
 * @constructor Log Managerから使用される。
 * Kotlin(Java)のバイト型配列からPageを作成する。
 */
class Page {
    // ByteBuffer オブジェクトは、バイト配列をラップし、配列の任意の位置に値を読み書きするメソッドを提供します。
    // これらの値はプリミティブ値（整数など）だけでなく、より小さなバイト配列でもあります。
    // ByteBuffer クラスには文字列を読み書きするメソッドが存在しないため、Page は文字列値を blob として書き込むことを選択します。
    private var bb: ByteBuffer
    private val charset = StandardCharsets.US_ASCII

    constructor(blockSize: Int) {
        bb = ByteBuffer.allocateDirect(blockSize)
    }

    constructor(b: ByteArray) {
        bb = ByteBuffer.wrap(b)
    }

    /**
     * Page内の[offset]で指定した場所の数値を返す
     * @return Page 内の数値
     */
    fun getInt(offset: Int): Int {
        return bb.getInt(offset)
    }

    /**
     * Page内の[offset]で指定した場所に数値[n]を保存する
     */
    fun setInt(offset: Int, n: Int) {
        bb.putInt(offset, n)
    }

    /**
     * Page内の[offset]で指定した場所のバイナリを返す
     * @return Page 内のバイナリ
     */
    fun getBytes(offset: Int): ByteArray {
        bb.position(offset)
        val length = bb.int
        val b = ByteArray(length)
        bb.get(b)
        return b
    }

    /**
     * Page内の[offset]で指定した場所にバイナリ[b]を保存する
     */
    fun setBytes(offset: Int, b: ByteArray) {
        bb.position(offset)
        bb.putInt(b.size)
        bb.put(b)
    }

    /**
     * Page内の[offset]で指定した場所の文字列を返す
     * @return Page 内の文字列
     */
    fun getString(offset: Int): String {
        val b = getBytes(offset)
        return String(b, charset)
    }

    /**
     * Page内の[offset]で指定した場所に文字列[s]を保存する
     * PageはByteBufferを使用しているので、文字列はバイト列に変換してからsetする
     * 文字列とそのバイト表現との変換は文字エンコーディング(charset)によって決まります
     */
    fun setString(offset: Int, s: String) {
        val b = s.toByteArray(charset)
        setBytes(offset, b)
    }

    /**
     * 文字列のサイズ[strSize]を受け取りバイナリとしての長さを返す
     * @return 数値
     */
    companion object {
        private val charset = StandardCharsets.US_ASCII
        fun maxLength(strSize: Int): Int {
            val bytesPerChar = charset.newEncoder().maxBytesPerChar()
            return Integer.BYTES + (strSize * (bytesPerChar.toInt()))
        }
    }

    /**
     * FileManagerから利用され、Pageの内容を返す
     * @return ByteBuffer
     */
    fun contents(): ByteBuffer {
        bb.position(0)
        return bb
    }
}