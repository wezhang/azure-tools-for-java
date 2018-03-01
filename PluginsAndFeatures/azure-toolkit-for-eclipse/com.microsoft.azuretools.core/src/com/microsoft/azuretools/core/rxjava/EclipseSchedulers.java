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

package com.microsoft.azuretools.core.rxjava;

import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.jobs.Job;
import org.eclipse.swt.widgets.Display;

import com.microsoft.azuretools.azurecommons.helpers.NotNull;

import rx.Scheduler;
import rx.schedulers.Schedulers;

public class EclipseSchedulers {
    public static Scheduler processBarVisibleAsync(@NotNull String title) {
        return Schedulers.from(command -> {
            Job job = Job.create(title, monitor -> {
                try {
                    command.run();
                } catch (Exception ex) {
                    return new Status(IStatus.ERROR, "unknown", ex.getMessage(), ex);
                }

                return Status.OK_STATUS;
            });

            job.schedule();
        });
    }

    public static Scheduler processBarVisibleSync(@NotNull String title) {
        return Schedulers.from(command -> {
            Job job = Job.create(title, monitor -> {
                try {
                    command.run();
                } catch (Exception ex) {
                    return new Status(IStatus.ERROR, "unknown", ex.getMessage(), ex);
                }

                return Status.OK_STATUS;
            });

            job.schedule();

            // Waiting for job finished
            try {
                job.join();
            } catch (InterruptedException ignore) {
            }
        });
    }

    public static Scheduler dispatchThread() {
        return Schedulers.from(command -> Display.getDefault().asyncExec(command));
    }
}
