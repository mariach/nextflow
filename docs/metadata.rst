.. metadata-page:

***********************
Workflow introspection
***********************


Runtime metadata
--------------------

The implicit ``workflow`` object allows you to access some workflow and runtime metadata in your Nextflow scripts.
For example::

    println "Project : $workflow.projectDir"
    println "Git info: $workflow.repository - $workflow.revision [$workflow.commitId]"
    println "Cmd line: $workflow.commandLine"


.. tip:: To shortcut the access to multiple ``workflow`` properties you can use the Groovy
    `with <http://docs.groovy-lang.org/latest/html/groovy-jdk/java/lang/Object.html#with(groovy.lang.Closure)>`_ method.


The following table lists the properties that can be accessed on the ``workflow`` object:

=========================== ===========================
Name                        Description
=========================== ===========================
repository                  Project repository Git remote URL.
commitId                    Git commit ID of the executed workflow repository.
revision                    Git branch/tag of the executed workflow repository.
projectDir                  Directory where the workflow project is stored in the computer.
launchDir                   Directory where the workflow execution has been launched.
workDir                     Workflow working directory.
container                   Docker image used to run workflow tasks. When more than one image is used
                            it returns a map object containing `[process name, image name]` pair entries.
commandLine                 Command line as entered by the user to launch the workflow execution.
profile                     Used configuration profile.
sessionId                   Unique identifier (UUID) associated to current execution.
resume                      Returns ``true`` whenever the current instance is resumed from a previous execution.
start                       Timestamp of workflow at execution start.
:sup:`✝` complete           Timestamp of workflow when execution is completed.
:sup:`✝` duration           Time elapsed to complete workflow execution.
:sup:`*` success            Reports if the execution completed successfully.
:sup:`*` exitStatus         Exit status of the task that caused the workflow execution to fail.
:sup:`*` errorMessage       Error message of the task that caused the workflow execution to fail.
:sup:`*` errorReport        Detailed error of the task that caused the workflow execution to fail.
nextflow.version            Nextflow runtime version number.
nextflow.build              Nextflow runtime build number.
nextflow.timestamp          Nextflow runtime compile timestamp.
=========================== ===========================


Properties marked with a `✝` are accessible only in the workflow completion handler.
Properties marked with a `*` are accessible only in the workflow completion and error handlers.
See the following section for details.


Completion handler
------------------

Due to the asynchronous nature of Nextflow the termination of a script does not correspond to the termination
of the running workflow. Thus some information, only available on execution completion, needs to be accessed by
using an asynchronous handler.

The ``onComplete`` event handler is invoked by the framework when the workflow execution is completed. It allows one
to access the workflow termination status and other useful information. For example::

    workflow.onComplete {
        println "Pipeline completed at: $workflow.complete"
        println "Execution status: ${ workflow.success ? 'OK' : 'failed' }"
    }


Error handler
-------------

The ``onError`` even handler is invoked by Nextflow when a runtime or process error caused the pipeline execution to stop.
For example::

    workflow.onError {
        println "Oops... Pipeline execution stopped with the following message: ${workflow.errorMessage}"
    }

.. note:: Both the ``onError`` and ``onComplete`` handlers are invoked when an error condition is encountered.
    However the first is called as soon as the error is raised, while the second just before the pipeline execution
    is going terminate. When using the ``finish`` :ref:`process-page-error-strategy`, between the two there could be
    a significant time gap depending by the time required to complete any pending job.

Notification message
--------------------

Nextflow does not provide a built-in mechanism to send emails or other messages. However the ``mail`` standard Linux
tool (or an equivalent one) can easily be used to send a notification message when the workflow execution is completed,
as shown below::


    workflow.onComplete {
        def subject = 'My pipeline execution'
        def recipient = 'me@gmail.com'

        ['mail', '-s', subject, recipient].execute() << """

        Pipeline execution summary
        ---------------------------
        Completed at: ${workflow.complete}
        Duration    : ${workflow.duration}
        Success     : ${workflow.success}
        workDir     : ${workflow.workDir}
        exit status : ${workflow.exitStatus}
        Error report: ${workflow.errorReport ?: '-'}
        """
    }



Decoupling metadata
-----------------------

The workflow event handlers can be defined also in the ``nextflow.config`` file. This is useful to
decouple the handling of pipeline events from the main script logic.

When the event handlers are included in a configuration file the only difference is that the ``onComplete`` and
the ``onError` closures have to be defined by using the assignment operator as shown below::

    workflow.onComplete = {
        // any workflow property can be used here
        println "Pipeline complete"
        println "Command line: $workflow.commandLine"
    }


    workflow.onError = {
        println "Oops .. something when wrong"
    }


.. note:: It is possible to define a workflow event handlers both in the pipeline script **and** in the
  configuration file.

