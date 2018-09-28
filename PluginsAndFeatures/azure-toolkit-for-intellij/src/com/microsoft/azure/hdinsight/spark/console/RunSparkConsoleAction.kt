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

package com.microsoft.azure.hdinsight.spark.console

import com.intellij.openapi.actionSystem.AnActionEvent
import com.microsoft.azure.hdinsight.common.logger.ILogger
import com.microsoft.azure.hdinsight.spark.console.ScalaPluginUtils.isScalaPluginEnabled
import com.microsoft.azuretools.ijidea.utility.AzureAnAction
import java.lang.reflect.Method

// The Action is a bridge to connect Scala related actions with dependent Scala Plugin actions by reflection
open class RunSparkConsoleAction(private val sparkScalaActionClassName: String) : AzureAnAction(), ILogger {
    val isEnabled
        get() = isScalaPluginEnabled() && updateMethod != null && actionPerformedMethod != null

    private val actionClass: Class<*>?
        get() = try {
            val clazz = Class.forName(sparkScalaActionClassName)

            log().debug("class ${clazz.canonicalName} is loaded from ${clazz.protectionDomain.codeSource.location}")
            clazz
        } catch (err: Exception) {
            log().debug("Class $sparkScalaActionClassName is not found", err)
            null
        }

    private val updateMethod: Method?
        get() = getAnActionMethod("update")

    private val actionPerformedMethod: Method?
        get() = getAnActionMethod("actionPerformed")

    private fun getAnActionMethod(methodName: String): Method? = try {
                actionClass?.getMethod(methodName, AnActionEvent::class.java)
            } catch (err: Exception) {
                log().debug("Method `$methodName` is not found", err)
                null
            }

    private val sparkScalaAction: Any by lazy { actionClass!!.newInstance() }

    override fun update(actionEvent: AnActionEvent?) {
        val presentation = actionEvent?.presentation ?: return

        // Hide for the Scala plugin not installed
        presentation.isVisible = isEnabled
        presentation.isEnabled = isEnabled

        if (isEnabled) {
            updateMethod?.invoke(sparkScalaAction, actionEvent)
        }
    }

    override fun onActionPerformed(actionEvent: AnActionEvent) {
        actionPerformedMethod?.invoke(sparkScalaAction, actionEvent)
    }
}

class RunSparkLocalConsoleAction : RunSparkConsoleAction("com.microsoft.azure.hdinsight.spark.console.RunSparkScalaLocalConsoleAction")
class RunSparkLivyConsoleAction : RunSparkConsoleAction("com.microsoft.azure.hdinsight.spark.console.RunSparkScalaLivyConsoleAction")
