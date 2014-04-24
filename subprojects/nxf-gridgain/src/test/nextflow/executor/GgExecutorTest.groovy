/*
 * Copyright (c) 2013-2014, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2014, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.executor

import java.nio.file.Files
import java.nio.file.Paths

import nextflow.processor.FileHolder
import nextflow.processor.TaskRun
import nextflow.script.FileInParam
import nextflow.script.FileOutParam
import nextflow.script.TokenVar
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class GgExecutorTest extends Specification {


    def testCopyToTarget() {

        given:
        def targetDir = Files.createTempDirectory('store-dir')

        def task = [ getTargetDir:{ targetDir } ] as GgBaseTask
        task.scratchDir = Files.createTempDirectory('just-a-dir')
        task.scratchDir.resolve('file1').text = 'file 1'
        task.scratchDir.resolve('file_x_1').text = 'x 1'
        task.scratchDir.resolve('file_x_2').text = 'x 2'
        task.scratchDir.resolve('file_z_3').text = 'z 3'

        when:
        task.copyToTargetDir('file1')
        then:
        targetDir.resolve('file1').text == 'file 1'

        when:
        task.copyToTargetDir('file_x*')
        then:
        targetDir.resolve('file_x_1').text == 'x 1'
        targetDir.resolve('file_x_2').text == 'x 2'
        !targetDir.resolve('file_z_3').exists()

        cleanup:
        task?.scratchDir?.deleteDir()
    }


    def testGgBashTask() {

        given:
        def sourcePath = Files.createTempDirectory('stage-test')
        def sourceFile1 = sourcePath.resolve('file1')
        def sourceFile2 = sourcePath.resolve('file2')

        def targetPath = Files.createTempDirectory('target-path')

        sourceFile1.text = 'Content for file1'
        sourceFile2.text = 'Content for file2'

        def binding = new Binding()
        def task = new TaskRun(id: 123, name: 'TestRun', workDirectory: Paths.get('/some/path'), storeDir: targetPath)
        def s1 = new FileInParam(binding, []).bind( new TokenVar('x') )
        task.setInput(s1, [ new FileHolder(sourceFile1), new FileHolder(sourceFile2) ])

        task.setOutput( new FileOutParam(binding, []).bind('x'), 'file1' )
        task.setOutput( new FileOutParam(binding, []).bind('y'), 'file2' )
        task.setOutput( new FileOutParam(binding, []).bind('z'), 'file3' )

        when:
        def bash = new GgBashTask(task, ['echo', 'do this', 'do that'])
        then:
        bash.taskId == 123
        bash.name == 'TestRun'
        bash.workDir == Paths.get('/some/path')
        bash.targetDir == targetPath
        bash.inputFiles == [ file1:sourceFile1, file2: sourceFile2 ]
        bash.outputFiles == task.getOutputFilesNames()

        when:
        bash.stage()
        then:
        bash.scratchDir != null
        bash.scratchDir.resolve('file1').text ==  sourceFile1.text
        bash.scratchDir.resolve('file2').text ==  sourceFile2.text

        when:
        bash.scratchDir.resolve('x').text = 'File x'
        bash.scratchDir.resolve('y').text = 'File y'
        bash.scratchDir.resolve('z').text = 'File z'
        bash.unstage()
        then:
        targetPath.exists()
        targetPath.resolve('x').text == 'File x'
        targetPath.resolve('y').text == 'File y'
        targetPath.resolve('z').text == 'File z'

        cleanup:
        sourcePath?.deleteDir()
        targetPath?.deleteDir()
        bash?.scratchDir?.deleteDir()

    }

}
