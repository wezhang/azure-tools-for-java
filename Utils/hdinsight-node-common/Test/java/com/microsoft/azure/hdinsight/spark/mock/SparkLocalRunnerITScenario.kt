/*
 * Copyright (c) Microsoft Corporation
 * <p/>
 * All rights reserved.
 * <p/>
 * MIT License
 * <p/>
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
 * to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * <p/>
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 * <p/>
 * THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.hdinsight.spark.mock

import com.microsoft.azure.hdinsight.spark.common.SparkLocalJvmProcess
import cucumber.api.java.en.And
import cucumber.api.java.en.Given
import cucumber.api.java.en.Then
import cucumber.api.DataTable
import org.assertj.core.api.Assertions.*

class SparkLocalRunnerITScenario {
    private var sparkLocalJob: ProcessBuilder? = null
    private val jvmProcess = SparkLocalJvmProcess()

    @And("^enable Spark Job debugging")
    fun enableLocalJobDebug() {
        // Enable it for Spark Job locally debugging
        jvmProcess.isDebugEnabled = true
    }

    @Given("^locally run job '(.*)' with args")
    fun localRunJob(mainClass: String, jobArgs: List<String>) {
        val args = arrayOf("--master local[1]", mainClass) + jobArgs

        sparkLocalJob = jvmProcess.createProcess("", SparkLocalRunner::class.java, args)
    }

    private fun runToGetStdoutLines(): List<String> {
        assertThat(sparkLocalJob)
                .describedAs("Run Spark Job locally firstly")
                .isNotNull()

        sparkLocalJob!!.redirectOutput(ProcessBuilder.Redirect.PIPE)

        val process = sparkLocalJob!!.start()
        val outputLines = process.inputStream.reader().readLines()

        assertThat(process.waitFor())
                .describedAs("Spark job exist with error.")
                .isEqualTo(0)

        return outputLines
    }

    @Then("^locally run stand output should be")
    fun checkLocallyRunStdout(expectOutputs: List<String>) {
        val outputLines = runToGetStdoutLines()

        assertThat(outputLines).containsAll(expectOutputs)
        assertThat(outputLines).hasSameSizeAs(expectOutputs)
    }

    @Then("^locally run stand output table should be")
    fun checkLocallyRunStdoutTable(expectOutputs: DataTable) {
        val outputTable = runToGetStdoutLines()
                .filter { !it.matches("""^\+[-+]*\+$""".toRegex()) }
                .map {
                    it.split("|")
                        .toList()
                        .map { it.trim() }
                        .filter { it.isNotEmpty() }
                }
                .filter { it.isNotEmpty() }

        assertThat(outputTable).isEqualTo(expectOutputs.raw())
    }
}
