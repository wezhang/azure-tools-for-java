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
import com.microsoft.azure.hdinsight.spark.common.SparkSubmitResponse
import com.microsoft.azure.hdinsight.spark.common.SparkUITest
import com.microsoft.azure.hdinsight.spark.ui.livy.batch.*
import com.microsoft.azuretools.ijidea.utility.AzureAnAction
import org.junit.Ignore
import org.junit.Test
import rx.Observable
import java.awt.event.WindowAdapter
import java.awt.event.WindowEvent
import java.util.concurrent.TimeUnit

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

const val killActionColName = "KillAction"
const val restartActionColName = "RestartAction"
const val idColName = "ID"
const val appIdColName = "AppID"
const val stateColName = "State"

class MockSparkLivyJobsTableSchema
    : UniqueColumnNameTableSchema(arrayOf(
        UniqueTableActionsColumnInfo(killActionColName),
        UniqueTableActionsColumnInfo(restartActionColName),
        UniqueTablePlainColumnInfo(idColName),
        UniqueTablePlainColumnInfo(appIdColName),
        UniqueTablePlainColumnInfo(stateColName))) {

    inner class MockSparkJobDescriptor(val jobStatus: SparkSubmitResponse) : RowDescriptor(
            killActionColName to KillLivyJobAction(),
            restartActionColName to RestartLivyJobAction(),
            idColName to jobStatus.id,
            appIdColName to jobStatus.appId,
            stateColName to jobStatus.state)

}

class MockSparkBatchJobViewerControl(private val view: MockSparkBatchJobViewer) : LivyBatchJobViewer.LivyBatchJobViewerControl {
    override fun onJobSelected(jobSelected: UniqueColumnNameTableSchema.RowDescriptor?) {
        val sparkJobDesc = (jobSelected as? MockSparkLivyJobsTableSchema.MockSparkJobDescriptor)?.let { arrayOf(it)}
            ?: emptyArray()

        Observable.from(sparkJobDesc)
                .delay(500, TimeUnit.MILLISECONDS)
                .subscribe { view.getModel(LivyBatchJobViewerModel::class.java).apply {
                    jobDetail = """{"message":"hello ${it.jobStatus.appId}!","error no": 0, "id": ${it.jobStatus.id}}"""

                    view.setData(this)
                }}
    }
}


class MockSparkBatchJobViewer : LivyBatchJobViewer() {
    override val jobViewerControl: LivyBatchJobViewerControl by lazy { MockSparkBatchJobViewerControl(this@MockSparkBatchJobViewer) }
}

@Ignore
class SparkJobTableTest : SparkUITest() {
    @Test
    fun testLivyTable() {

        val jobView = MockSparkBatchJobViewer()
        val tableSchema = MockSparkLivyJobsTableSchema()
        val model = LivyBatchJobViewerModel(LivyBatchJobTableModel(tableSchema))

        model.tableModel?.apply {
            firstPage = object : LivyBatchJobTablePage {
                override fun nextPageLink(): String? {
                    return null
                }

                override fun items(): List<UniqueColumnNameTableSchema.RowDescriptor>? {
                    return listOf(
                            tableSchema.MockSparkJobDescriptor(SparkSubmitResponse.parseJSON("""{
                           "id": 1,
                           "appId": "application-134124194-1",
                           "state": "running"
                        }""".trimIndent())),
                            tableSchema.MockSparkJobDescriptor(SparkSubmitResponse.parseJSON("""{
                           "id": 2,
                           "appId": null,
                           "state": "dead"
                        }""".trimIndent())),
                            tableSchema.MockSparkJobDescriptor(SparkSubmitResponse.parseJSON("""{
                           "id": 3,
                           "state": "success"
                        }""".trimIndent())),
                            tableSchema.MockSparkJobDescriptor(SparkSubmitResponse.parseJSON("""{
                           "id": 4,
                           "appId": "application-134124194-4"
                        }""".trimIndent()))
                    )
                }
            }
        }

        jobView.setData(model)

        dialog!!.apply {
            contentPane.add(jobView.component)
            pack()

            addWindowListener(object: WindowAdapter() {
                override fun windowClosing(e: WindowEvent?) {
                    jobView.dispose()
                }
            })
            isVisible = true
        }
    }
}