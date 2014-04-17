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

import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class CsvSplitterTest extends Specification {

    def text = '''
        alpha,beta,delta
        gamma,,zeta
        eta,theta,iota
        mu,nu,xi
        pi,rho,sigma
        '''
        .stripIndent().trim()

    def testSplitRows() {

        when:
        def items = new CsvSplitter().split(text).list()

        then:
        items.size() == 5
        items[0] == ['alpha', 'beta', 'delta']
        items[1] == ['gamma', '', 'zeta']
        items[2] == ['eta', 'theta', 'iota']
        items[3] == ['mu', 'nu', 'xi']
        items[4] == ['pi', 'rho', 'sigma']

    }

    def testSplitWithCount() {

        when:
        def groups = new CsvSplitter(count:3).split(text).list()

        then:
        groups.size() == 2

        groups[0][0] == ['alpha', 'beta', 'delta']
        groups[0][1] == ['gamma', '', 'zeta']
        groups[0][2] == ['eta', 'theta', 'iota']

        groups[1][0] == ['mu', 'nu', 'xi']
        groups[1][1] == ['pi', 'rho', 'sigma']

    }

}
