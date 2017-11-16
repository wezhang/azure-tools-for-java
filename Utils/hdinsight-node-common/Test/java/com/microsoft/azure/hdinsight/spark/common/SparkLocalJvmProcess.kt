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

package com.microsoft.azure.hdinsight.spark.common

import mockit.integration.junit4.JMockit
import java.io.File
import java.nio.file.Paths
import kotlin.test.fail

class SparkLocalJvmProcess : JvmProcess() {
    val targetDir = File(javaClass.protectionDomain.codeSource.location.file).parentFile
    val projectDir = targetDir.parentFile
    val dataRootDir = Paths.get(
                targetDir.path,
                "data"
            ).toFile()

    val userDefaultDir = Paths.get(dataRootDir.path, "__default__", "user", "current").toFile()

    override var workingDirectory: String = userDefaultDir.path

    override fun createProcess(jvmOptions: String, mainClass: String, arguments: Array<String>): ProcessBuilder {
        val jMockitClass = JMockit::class.java
        val uri = jMockitClass.getResource(
                "/${jMockitClass.canonicalName.replace('.', '/')}.class")
        val jarPathRegex = """(zip:|jar:file:)(/.*)!/(.*)""".toRegex()

        return jarPathRegex.matchEntire(uri.toString())
                           ?.groups
                           ?.get(2)
                           ?.let { File(it.value) }
                           ?.let {
                               val newJvmOptions = "-javaagent:$it $jvmOptions" +
                                       if (isDebugEnabled)
                                           " -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
                                       else
                                           ""

                               super.createProcess(newJvmOptions, mainClass, arguments)
                           }
                ?: fail("Can't find JMockit jar file in classpaths")
    }

    var isDebugEnabled = false

    init {
        // Prepare data directories
        dataRootDir.deleteRecursively()

        val testDataResource = Paths.get(projectDir.path, "Test", "resources", "data").toFile()
        testDataResource.copyRecursively(dataRootDir, true)
        userDefaultDir.mkdirs()
    }
}