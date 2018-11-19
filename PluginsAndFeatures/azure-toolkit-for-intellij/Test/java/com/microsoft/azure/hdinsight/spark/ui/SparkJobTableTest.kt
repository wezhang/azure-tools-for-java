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

package com.microsoft.azure.hdinsight.spark.ui

import com.intellij.icons.AllIcons
import com.intellij.openapi.actionSystem.AnActionEvent
import com.microsoft.azure.hdinsight.spark.common.SparkUITest
import com.microsoft.azure.hdinsight.spark.ui.livy.batch.*
import com.microsoft.azuretools.ijidea.utility.AzureAnAction
import org.junit.Ignore
import org.junit.Test
import com.microsoft.azure.hdinsight.spark.ui.livy.batch.UniqueColumnNameTableSchema.*

@Ignore
class SparkJobTableTest : SparkUITest() {
    @Test
    fun testLivyTable() {
        class KillLivyJobAction : AzureAnAction(AllIcons.Actions.Cancel) {
            override fun onActionPerformed(anActionEvent: AnActionEvent?) {
                System.out.println("Clicked ${anActionEvent?.place} kill job button")
            }
        }

        class RestartLivyJobAction : AzureAnAction(AllIcons.Actions.Restart) {
            override fun onActionPerformed(anActionEvent: AnActionEvent?) {
                System.out.println("Clicked ${anActionEvent?.place} restart job button")
            }
        }

        val jobView = LivyBatchJobView()
        val columnInfos = arrayOf(
                UniqueTableActionsColumnInfo("KillAction"),
                UniqueTableActionsColumnInfo("RestartAction"),
                UniqueTablePlainColumnInfo("id"),
                UniqueTablePlainColumnInfo("appId"),
                UniqueTablePlainColumnInfo("state")
        )
        val tableSchema = UniqueColumnNameTableSchema(columnInfos)
        val model = LivyBatchJobViewModel(LivyBatchJobTableModel(tableSchema))

        model.tableModel.firstPage = object : LivyBatchJobTablePage {
            override fun nextPageLink(): String? {
                return null
            }

            override fun items(): List<UniqueColumnNameTableSchema.RowDescriptor>? {
                return listOf(
                        // varargs
                        tableSchema.RowDescriptor(
                                "KillAction" to KillLivyJobAction(),
                                "RestartAction" to RestartLivyJobAction(),
                                "id" to "id-1",
                                "appId" to "appId-1",
                                "state" to "state-1"
                        ),
                        // null cell values
                        tableSchema.RowDescriptor(
                                "KillAction" to KillLivyJobAction(),
                                "RestartAction" to RestartLivyJobAction(),
                                "id" to "id-2",
                                "appId" to null,
                                "state" to "state-2"
                        ),
                        tableSchema.RowDescriptor(
                                "KillAction" to KillLivyJobAction(),
                                "RestartAction" to RestartLivyJobAction(),
                                "id" to "id-3",
                                "state" to "state-3"
                        ),
                        tableSchema.RowDescriptor(
                                "KillAction" to KillLivyJobAction(),
                                "RestartAction" to RestartLivyJobAction(),
                                "id" to "id-4",
                                "appId" to "appId-4"
                        )
                )
            }
        }

        jobView.setData(model)

        dialog!!.apply {
            contentPane.add(jobView.component)
            pack()
            isVisible = true
        }
    }
}