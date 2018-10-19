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

import com.intellij.execution.ExecutionException
import com.intellij.execution.Executor
import com.intellij.execution.configurations.JavaCommandLineState
import com.intellij.execution.configurations.JavaParameters
import com.intellij.execution.configurations.RunProfileState
import com.intellij.execution.runners.ExecutionEnvironment
import com.intellij.jarRepository.JarRepositoryManager
import com.intellij.jarRepository.RemoteRepositoriesConfiguration
import com.intellij.openapi.project.Project
import com.intellij.openapi.roots.OrderRootType
import com.intellij.openapi.roots.impl.libraries.LibraryEx
import com.intellij.openapi.roots.impl.libraries.ProjectLibraryTable
import com.intellij.openapi.roots.libraries.Library
import com.intellij.openapi.roots.libraries.NewLibraryConfiguration
import com.intellij.openapi.roots.ui.configuration.libraryEditor.NewLibraryEditor
import com.intellij.psi.JavaPsiFacade
import com.intellij.psi.search.ProjectScope
import com.microsoft.azure.hdinsight.spark.run.SparkBatchLocalRunState
import com.microsoft.azure.hdinsight.spark.run.configuration.RemoteDebugRunConfiguration
import com.microsoft.azuretools.ijidea.ui.ErrorWindow
import com.microsoft.intellij.util.runInWriteAction
import org.jetbrains.plugins.scala.console.ScalaConsoleRunConfiguration
import javax.swing.Action

class SparkScalaLocalConsoleRunConfiguration(
        project: Project,
        configurationFactory: SparkScalaLocalConsoleRunConfigurationFactory,
        name: String)
    : ScalaConsoleRunConfiguration(project, configurationFactory, name) {

    private val sparkCoreCoodRegex = """.*\b(org.apache.spark:spark-)(core)(_.+:.+)""".toRegex()
    private val replMain = "org.apache.spark.repl.Main"

    lateinit var batchRunConfiguration: RemoteDebugRunConfiguration

    override fun mainClass(): String = "org.apache.spark.deploy.SparkSubmit"

    override fun createParams(): JavaParameters {
        val localRunParams = SparkBatchLocalRunState(project, batchRunConfiguration.model.localRunConfigurableModel).createParams()
        val params = super.createParams()
        params.classPath.clear()
        val replLibraryCoord = findReplCoord() ?: throw ExecutionException("""
                The library org.apache.spark:spark-core is not in project dependencies.
                The project may not be a Spark Application project.
                Please create it from the wizard or add Spark related libraries into dependencies.
                ( Refer to https://www.jetbrains.com/help/idea/library.html#add-library-to-module-dependencies )
        """.trimIndent())

        // Check repl dependence and prompt the user to fix it
        checkReplDependenceAndTryToFix(replLibraryCoord)

        val library = getReplLibrary(replLibraryCoord) ?: throw ExecutionException("""
                The library $replLibraryCoord is not in project dependencies, please add it as the top one of list.
                ( Refer to https://www.jetbrains.com/help/idea/library.html#add-library-to-module-dependencies )
                """.trimIndent())

        params.classPath.addAll(library.getFiles(OrderRootType.CLASSES).map { it.presentableUrl })
        params.classPath.addAll(localRunParams.classPath.pathList)
        params.mainClass = mainClass()

        params.vmParametersList.addAll(localRunParams.vmParametersList.parameters)

        // FIXME!!! To support local mock filesystem
        // params.mainClass = localRunParams.mainClass
        //
        // localRunParams.programParametersList.parameters.takeWhile { it.trim().startsWith("--master") }
        //         .forEach { params.programParametersList.add(it) }
        // params.programParametersList.add(mainClass())

        params.programParametersList.add("--class", "org.apache.spark.repl.Main")
        params.programParametersList.add("--name", "Spark shell")
        params.programParametersList.add("spark-shell")

        params.addEnv("SPARK_SUBMIT_OPTS", "-Dscala.usejavacp=true")
        localRunParams.env.forEach { name, value -> params.addEnv(name, value) }

        return params
    }

    private fun findReplCoord(): String? {
        val iterator = ProjectLibraryTable.getInstance(project).libraryIterator

        while (iterator.hasNext()) {
            val libEntryName = iterator.next().name ?: continue

            // Replace `core` to `repl` with the title removed, such as:
            //     Maven: org.apache.spark:spark-core_2.11:2.1.0 => org.apache.spark:spark-repl_2.11:2.1.0
            //     ^^^^^^^                       ^^^^                                      ^^^^
            val replCoord = sparkCoreCoodRegex.replace(libEntryName) { "${it.groupValues[1]}repl${it.groupValues[3]}" }

            if (replCoord != libEntryName) {
                // Found and replaced
                return replCoord
            }
        }

        return null
    }

    private fun checkReplDependenceAndTryToFix(replLibraryCoord: String) {
        if (getReplLibrary(replLibraryCoord) == null
                && JavaPsiFacade.getInstance(project).findClass(replMain, ProjectScope.getLibrariesScope(project)) == null) {
            // `repl.Main` is not in the project class path
            val toFixDialog = object : ErrorWindow(
                    project,
                    "The library $replLibraryCoord is not in project dependencies, would you like to auto fix it?",
                    "Auto fix dependency issue to confirm") {
                init {
                    setOKButtonText("Auto Fix")
                }

                override fun createActions(): Array<Action> {
                    return arrayOf(okAction, cancelAction)
                }
            }

            val toFix = toFixDialog.showAndGet()

            if (toFix) {
                fixReplDependence(replLibraryCoord)
            }
        }
    }

    private fun fixReplDependence(replLibraryCoord: String) {
        runInWriteAction {
            val projectRepositories = RemoteRepositoriesConfiguration.getInstance(project).repositories
            val newLibConf: NewLibraryConfiguration = JarRepositoryManager.resolveAndDownload(
                    project, replLibraryCoord, false, false, true, null, projectRepositories) ?: return@runInWriteAction
            val libraryType = newLibConf.libraryType
            val library = ProjectLibraryTable.getInstance(project).createLibrary("Spark Console(auto-fix): $replLibraryCoord")

            val editor = NewLibraryEditor(libraryType, newLibConf.properties)
            newLibConf.addRoots(editor)
            val model = library.modifiableModel
            editor.applyTo(model as LibraryEx.ModifiableModelEx)
            model.commit()
        }
    }

    private fun getReplLibrary(replLibraryCoord: String): Library? = ProjectLibraryTable.getInstance(project)
            .libraries.firstOrNull { it.name?.endsWith(replLibraryCoord) == true }

    override fun getState(executor: Executor, env: ExecutionEnvironment): RunProfileState? {
        val state = object : JavaCommandLineState(env) {
            override fun createJavaParameters() : JavaParameters {
                val params = createParams()

                params.programParametersList.addParametersString(consoleArgs())
                return params
            }
        }

        state.consoleBuilder = SparkScalaConsoleBuilder(project)

        return state
    }
}
