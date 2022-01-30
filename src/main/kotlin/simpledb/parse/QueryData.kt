package simpledb.parse

import simpledb.query.Predicate

class QueryData(
    private val fields: List<String>,
    private val tables: Collection<String>,
    private val predicate: Predicate,
) {
    override fun toString(): String {
        var result = "select "
        for (filedName in fields) {
            result += "$filedName, "
        }
        result = result.substring(0, result.length-2) //zap final comma
        result += " from "
        for (tableName in tables) {
            result += "$tableName, "
        }
        result = result.substring(0, result.length-2) // zap final comma
        val predicateString = predicate.toString()
        if (predicateString != "") {
            result += " where $predicateString"
        }
        return result
    }
}