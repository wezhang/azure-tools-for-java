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

import com.intellij.openapi.Disposable
import com.intellij.openapi.editor.EditorFactory
import com.intellij.ui.components.JBScrollPane
import com.intellij.ui.table.JBTable
import com.intellij.util.ui.ColumnInfo
import com.intellij.util.ui.UIUtil
import com.microsoft.azure.hdinsight.common.mvc.IdeaSettableControl
import java.awt.Component
import java.awt.Graphics
import javax.swing.JSplitPane
import javax.swing.JSplitPane.HORIZONTAL_SPLIT
import javax.swing.table.TableCellEditor
import javax.swing.table.TableCellRenderer

class LivyBatchJobTable : JBTable(LivyBatchJobTableModel()) {
    val jobTableModel: LivyBatchJobTableModel
            get() = model as? LivyBatchJobTableModel
                    ?: throw IllegalArgumentException("LivyBatchJobTable only supports LivyBatchJobTableModel")

    fun getColumnInfoAt(column: Int): ColumnInfo<Any, Any> {
        return jobTableModel.columnInfos[column]
    }
    // Override getCellEditor method since no need to edit the job table, but needs to perform actions
    override fun getCellEditor(row: Int, column: Int): TableCellEditor {
        return getColumnInfoAt(column).getEditor(jobTableModel.getJobDescriptor(row)) ?: super.getCellEditor(row, column)
    }

    override fun getCellRenderer(row: Int, column: Int): TableCellRenderer {
        return getColumnInfoAt(column).getRenderer(jobTableModel.getJobDescriptor(row)) ?: super.getCellRenderer(row, column)
    }

    override fun paint(g: Graphics) {
        super.paint(g)
        UIUtil.fixOSXEditorBackground(this)
    }
}

class LivyBatchJobView : Disposable, IdeaSettableControl<LivyBatchJobViewModel> {
    private val jobDetailNotSetMessage = "<Click the job item row to get details>"

    private var jobsMainTable = LivyBatchJobTable()
    private val jobDetailDocument = EditorFactory.getInstance().createDocument(jobDetailNotSetMessage)
    private val jobDetailViewer = EditorFactory.getInstance().createViewer(jobDetailDocument)

    val control = LivyBatchJobViewControl(this)

    val component: Component by lazy {
        JSplitPane(HORIZONTAL_SPLIT, JBScrollPane(jobsMainTable), JBScrollPane(jobDetailViewer.component)).apply {
            dividerSize = 6
            dividerLocation = 600
        }
    }

    override fun dispose() {
        EditorFactory.getInstance().releaseEditor(jobDetailViewer)
    }

    override fun getData(to: LivyBatchJobViewModel) {
        to.tableModel = jobsMainTable.model as? LivyBatchJobTableModel
                ?: throw IllegalArgumentException("Broken Spark Batch Job view model")
        to.jobDetail = jobDetailDocument.text
    }

    override fun setDataInDispatch(from: LivyBatchJobViewModel) {
        if (jobsMainTable.model != from.tableModel) {
            jobsMainTable.model = from.tableModel
        }

        jobDetailDocument.setText(from.jobDetail ?: jobDetailNotSetMessage)
    }
}