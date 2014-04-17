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

package nextflow.splitter

import nextflow.Channel
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class BytesSplitterTest extends Specification {

    def bytes = [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6 ] as byte[]

    def testCount() {
        expect:
        new BytesSplitter(count: 6).split(bytes).count() == 3

    }

    def testList() {

        expect:
        new BytesSplitter(count: 5).split(bytes).list() == [ [0, 1, 2, 3, 4] as byte[], [5, 6, 7, 8, 9] as byte[] , [ 0, 1, 2, 3, 4] as byte[], [5, 6] as byte[] ]

    }

    def testChannel() {

        when:
        def c = new BytesSplitter(count: 5).split(bytes).channel()
        then:
        c.val == [0, 1, 2, 3, 4] as byte[]
        c.val == [5, 6, 7, 8, 9] as byte[]
        c.val == [ 0, 1, 2, 3, 4] as byte[]
        c.val == [5, 6] as byte[]
        c.val == Channel.STOP


    }

}
