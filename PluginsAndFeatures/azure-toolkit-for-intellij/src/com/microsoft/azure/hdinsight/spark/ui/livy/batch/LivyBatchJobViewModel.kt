/*
 * Copyright (c) Microsoft Corporation
 *
 * All rights reserved.
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
 * to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 *
 * THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.hdinsight.spark.ui.livy.batch

import com.intellij.util.ui.ColumnInfo
import com.intellij.util.ui.SortableColumnModel
import com.microsoft.azure.Page
import com.microsoft.azure.PagedList
import javax.swing.RowSorter
import javax.swing.table.AbstractTableModel

interface LivyBatchJobTablePage : Page<UniqueColumnNameTableSchema.RowDescriptor> {
    override fun items(): List<UniqueColumnNameTableSchema.RowDescriptor>?
}

class LivyBatchJobPagedList : PagedList<UniqueColumnNameTableSchema.RowDescriptor>() {
    var firstJobPage : LivyBatchJobTablePage? = null
        set(value) {
            field = value

            clear()
            setCurrentPage(value)
        }

    override fun nextPage(p0: String?): Page<UniqueColumnNameTableSchema.RowDescriptor>? {
        // TODO: To implement pagination later
        return null
    }

    // Override by an empty method since we won't like to load all jobs at once.
    // And all results of size(), toArray(), lastIndexOf() are the currently cached
    // jobs.
    override fun loadAll() { }
}

class LivyBatchJobTableModel(private val schema: UniqueColumnNameTableSchema? = null)
    : AbstractTableModel(), SortableColumnModel {

    private val pagedJobs = LivyBatchJobPagedList()

    var firstPage: LivyBatchJobTablePage? = null
        get() = pagedJobs.firstJobPage
        set(value) {
            if (field != value) {
                pagedJobs.firstJobPage = value
            }
        }


//    private var myDefaultSortKey: RowSorter.SortKey? = null

    override fun getRowCount(): Int = pagedJobs.size

    override fun getColumnName(column: Int): String =
            if ((columnInfos[column] as? UniqueColumnNameTableSchema.HideableHeaderColumn)?.doesHide == true) "" else columnInfos[column].name

    override fun getColumnCount(): Int = columnInfos.size

    override fun getColumnClass(columnIndex: Int): Class<*> {
//        return if (columnIndex == 0) LivyBatchJobActions::class.java else super.getColumnClass(columnIndex)
        return columnInfos[columnIndex].columnClass
    }

    override fun isCellEditable(rowIndex: Int, columnIndex: Int): Boolean {
        return columnInfos[columnIndex].isCellEditable(getJobDescriptor(rowIndex))
    }

    override fun getValueAt(rowIndex: Int, columnIndex: Int): Any? =
            schema?.columns?.get(columnIndex)?.valueOf(getJobDescriptor(rowIndex))

//    fun getObjectAt(row: Int, column: Int): Any? = pagedJobs[row].getObjectAt(column)

    override fun isSortable(): Boolean = true

    override fun getColumnInfos(): Array<ColumnInfo<Any, Any>> =
            schema?.columns?.filterIsInstance<ColumnInfo<Any, Any>>()?.toTypedArray() ?: arrayOf()

    override fun setSortable(ignored: Boolean) { }

    override fun getDefaultSortKey(): RowSorter.SortKey? = null //myDefaultSortKey

    // For object
    override fun getRowValue(row: Int): Any = getJobDescriptor(row)

    fun getJobDescriptor(row: Int): UniqueColumnNameTableSchema.RowDescriptor = pagedJobs[row]
}

data class LivyBatchJobViewModel(
        var tableModel: LivyBatchJobTableModel
)
