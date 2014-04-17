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
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class SplitterFactory {

    /**
     *
     * @param strategy The splitting strategy e.g. {@code 'fasta'}, {@code 'fastq'}, etc
     * @param object
     */
    static SplitterStrategy create( String strategy, Map options = [:] ) {
        assert strategy
        String name = strategy.contains('.') ? strategy : "nextflow.splitter.${strategy}Splitter"
        def clazz = (Class<SplitterStrategy>) Class.forName(name)
        create(clazz, options)
    }

    static SplitterStrategy create( Class<? extends SplitterStrategy> strategy, Map options = [:] ) {
        (SplitterStrategy) strategy.newInstance( [options] as Object[] )
    }
}
