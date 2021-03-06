/*
 * Copyright (c) 2013-2016, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2016, Paolo Di Tommaso and the respective authors.
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

import groovyx.gpars.dataflow.DataflowQueue

/**
 * --== Separation ==--
 *
 * + Separation is the opposite operation to merge. The supplied closure returns a list of values,
 *   each of which will be output into an output channel with the corresponding position index.
 *
 */

queue1 = new DataflowQueue<>()
queue2 = new DataflowQueue<>()
queue3 = new DataflowQueue<>()
queue4 = new DataflowQueue<>()

queue1 << 1 << 2 << 3

queue1.separate([queue2, queue3, queue4]) {a -> [a-1, a, a+1]}

//println "queue1: ${queue1.getVal()} - ${queue1.getVal()} - ${queue1.getVal()} "
println "queue2: ${queue2.getVal()} - ${queue2.getVal()} - ${queue2.getVal()} "
println "queue3: ${queue3.getVal()} - ${queue3.getVal()} - ${queue3.getVal()} "
println "queue4: ${queue4.getVal()} - ${queue4.getVal()} - ${queue4.getVal()} "