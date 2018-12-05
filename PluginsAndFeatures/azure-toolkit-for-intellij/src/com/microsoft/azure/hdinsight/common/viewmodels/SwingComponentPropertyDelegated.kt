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

package com.microsoft.azure.hdinsight.common.viewmodels

import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.progress.ProcessCanceledException
import com.intellij.openapi.ui.ComponentWithBrowseButton
import com.microsoft.azure.hdinsight.common.logger.ILogger
import javax.swing.ComboBoxModel
import javax.swing.JComboBox
import kotlin.reflect.KProperty

abstract class SwingComponentPropertyDelegated<T>: ILogger {
    operator fun setValue(ref: Any, property: KProperty<*>, v: T) {
        try {
            ApplicationManager.getApplication().invokeAndWait {
                setValueInDispatch(ref, property, v)
            }
        } catch (ex: ProcessCanceledException) {
            log().debug(ex.message)
        }
    }

    abstract fun setValueInDispatch(ref: Any, property: KProperty<*>, v: T)
}

class ComponentWithBrowseButtonEnabledDelegated(private val componentWithBrowseButton: ComponentWithBrowseButton<*>) {
    operator fun getValue(ref: Any, property: KProperty<*>): Boolean {
        return componentWithBrowseButton.button.isEnabled
    }

    operator fun setValue(ref: Any, property: KProperty<*>, v: Boolean) {
        componentWithBrowseButton.setButtonEnabled(v)
    }
}

class ComboBoxSelectionDelegated<T>(private val comboBox: JComboBox<T>): SwingComponentPropertyDelegated<T>() {
    operator fun getValue(ref: Any, property: KProperty<*>): T? {
        return comboBox.selectedItem as? T
    }

    override fun setValueInDispatch(ref: Any, property: KProperty<*>, v: T) {
        comboBox.selectedItem = v
    }

}

class ComboBoxModelDelegated<T>(private val comboBox: JComboBox<T>) {
    operator fun getValue(ref: Any, property: KProperty<*>): ComboBoxModel<T> {
        return comboBox.model
    }

    operator fun setValue(ref: Any, property: KProperty<*>, v: ComboBoxModel<T>) {
        if (comboBox.model != v) {
            comboBox.model = v
        }
    }
}

