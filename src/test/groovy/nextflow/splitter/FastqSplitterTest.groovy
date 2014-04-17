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
class FastqSplitterTest extends Specification {

    def testFastqRead() {

        given:
        def text = '''
            @HWI-EAS209_0006_FC706VJ:5:58:5894:21141#ATCACG/1
            TTAATTGGTAAATAAATCTCCTAATAGCTTAGATNTTACCTTNNNNNNNNNNTAGTTTCTTGAGATTTGTTGGGGGAGACATTTTTGTGATTGCCTTGAT
            +HWI-EAS209_0006_FC706VJ:5:58:5894:21141#ATCACG/1
            efcfffffcfeefffcffffffddf`feed]`]_Ba_^__[YBBBBBBBBBBRTT\\]][]dddd`ddd^dddadd^BBBBBBBBBBBBBBBBBBBBBBBB
            '''
            .stripIndent().trim()

        when:
        def items = new FastqSplitter().split(text).list()
        then:
        items.size() == 1
        items.get(0) == '''
            @HWI-EAS209_0006_FC706VJ:5:58:5894:21141#ATCACG/1
            TTAATTGGTAAATAAATCTCCTAATAGCTTAGATNTTACCTTNNNNNNNNNNTAGTTTCTTGAGATTTGTTGGGGGAGACATTTTTGTGATTGCCTTGAT
            +HWI-EAS209_0006_FC706VJ:5:58:5894:21141#ATCACG/1
            efcfffffcfeefffcffffffddf`feed]`]_Ba_^__[YBBBBBBBBBBRTT\\]][]dddd`ddd^dddadd^BBBBBBBBBBBBBBBBBBBBBBBB
            '''.stripIndent().trim()

        when:
        def entries = new FastqSplitter().split(text).options(record:true).list()
        then:
        entries[0].readHeader == 'HWI-EAS209_0006_FC706VJ:5:58:5894:21141#ATCACG/1'
        entries[0].readString == 'TTAATTGGTAAATAAATCTCCTAATAGCTTAGATNTTACCTTNNNNNNNNNNTAGTTTCTTGAGATTTGTTGGGGGAGACATTTTTGTGATTGCCTTGAT'
        entries[0].baseQualityHeader == 'HWI-EAS209_0006_FC706VJ:5:58:5894:21141#ATCACG/1'
        entries[0].baseQualityString == 'efcfffffcfeefffcffffffddf`feed]`]_Ba_^__[YBBBBBBBBBBRTT\\]][]dddd`ddd^dddadd^BBBBBBBBBBBBBBBBBBBBBBBB'

    }

    def testFastqSplit() {

        given:
        def text = '''
            @SRR636272.19519409/1
            GGCCCGGCAGCAGGATGATGCTCTCCCGGGCCAAGCCGGCTGTGGGGAGCACCCCGCCGCAGGGGGACAGGCGGAGGAAGAAAGGGAAGAAGGTGCCACAGATCG
            +
            CCCFFFFDHHD;FF=GGDHGGHIIIGHIIIBDGBFCAHG@E=6?CBDBB;?BB@BD8BB;BDB<>>;@?BB<9>&5<?288AAABDBBBBACBCAC?@AD?CAC?
            @SRR636272.13995011/1
            GCAGGATGATGCTCTCCCGGGCCAAGCCGGCTGTGGGGAGCACCCCGCCGCAGGGGGACAGGCGGAGGAAGAAAGGGAGATCGGAAGAGCACACGTCTGAACTCC
            +
            BBCFDFDEFFHHFIJIHGHGHGIIFIJJJJIGGBFHHIEGBEFEFFCDDDD:@@<BB8BBDDDDDDBBB?AA?CDABDD5?CDDDBB<A<>ACBB8ACDCD@CD>
            @SRR636272.21107783/1
            CGGGGAGCGCGGGCCCGGCAGCAGGATGATGCTCTCCCGGGCCAAGCCGGCTGTAGGGAGCACCCCGCCGCAGGGGGACAGGCGAGATCGGAAGAGCACACGTCT
            +
            BCCFFDFFHHHHHJJJJJIJHHHHFFFFEEEEEEEDDDDDDBDBDBBDBBDBBB(:ABCDDDDDDDDDDDDDDDD@BBBDDDDDDDDDDDDBDDDDDDDDDDADC
            '''.stripIndent().trim()

        when:
        def items = new FastqSplitter().split(text).list()

        then:
        items.size() == 3
        items.get(0) == '''
            @SRR636272.19519409/1
            GGCCCGGCAGCAGGATGATGCTCTCCCGGGCCAAGCCGGCTGTGGGGAGCACCCCGCCGCAGGGGGACAGGCGGAGGAAGAAAGGGAAGAAGGTGCCACAGATCG
            +
            CCCFFFFDHHD;FF=GGDHGGHIIIGHIIIBDGBFCAHG@E=6?CBDBB;?BB@BD8BB;BDB<>>;@?BB<9>&5<?288AAABDBBBBACBCAC?@AD?CAC?
            ''' .stripIndent() .trim()

        items.get(1) == '''
            @SRR636272.13995011/1
            GCAGGATGATGCTCTCCCGGGCCAAGCCGGCTGTGGGGAGCACCCCGCCGCAGGGGGACAGGCGGAGGAAGAAAGGGAGATCGGAAGAGCACACGTCTGAACTCC
            +
            BBCFDFDEFFHHFIJIHGHGHGIIFIJJJJIGGBFHHIEGBEFEFFCDDDD:@@<BB8BBDDDDDDBBB?AA?CDABDD5?CDDDBB<A<>ACBB8ACDCD@CD>
            ''' .stripIndent() .trim()

        items.get(2) == '''
            @SRR636272.21107783/1
            CGGGGAGCGCGGGCCCGGCAGCAGGATGATGCTCTCCCGGGCCAAGCCGGCTGTAGGGAGCACCCCGCCGCAGGGGGACAGGCGAGATCGGAAGAGCACACGTCT
            +
            BCCFFDFFHHHHHJJJJJIJHHHHFFFFEEEEEEEDDDDDDBDBDBBDBBDBBB(:ABCDDDDDDDDDDDDDDDD@BBBDDDDDDDDDDDDBDDDDDDDDDDADC
            ''' .stripIndent() .trim()


        when:
        def records = new FastqSplitter().split(text).options(record: true).list()

        then:
        records.size() == 3
        records[0].readHeader == 'SRR636272.19519409/1'
        records[0].readString == 'GGCCCGGCAGCAGGATGATGCTCTCCCGGGCCAAGCCGGCTGTGGGGAGCACCCCGCCGCAGGGGGACAGGCGGAGGAAGAAAGGGAAGAAGGTGCCACAGATCG'
        records[0].baseQualityString == 'CCCFFFFDHHD;FF=GGDHGGHIIIGHIIIBDGBFCAHG@E=6?CBDBB;?BB@BD8BB;BDB<>>;@?BB<9>&5<?288AAABDBBBBACBCAC?@AD?CAC?'
        records[0].baseQualityHeader == null

        records[1].readHeader == 'SRR636272.13995011/1'
        records[1].readString == 'GCAGGATGATGCTCTCCCGGGCCAAGCCGGCTGTGGGGAGCACCCCGCCGCAGGGGGACAGGCGGAGGAAGAAAGGGAGATCGGAAGAGCACACGTCTGAACTCC'
        records[1].baseQualityString == 'BBCFDFDEFFHHFIJIHGHGHGIIFIJJJJIGGBFHHIEGBEFEFFCDDDD:@@<BB8BBDDDDDDBBB?AA?CDABDD5?CDDDBB<A<>ACBB8ACDCD@CD>'
        records[1].baseQualityHeader == null

        records[2].readHeader == 'SRR636272.21107783/1'
        records[2].readString == 'CGGGGAGCGCGGGCCCGGCAGCAGGATGATGCTCTCCCGGGCCAAGCCGGCTGTAGGGAGCACCCCGCCGCAGGGGGACAGGCGAGATCGGAAGAGCACACGTCT'
        records[2].baseQualityString == 'BCCFFDFFHHHHHJJJJJIJHHHHFFFFEEEEEEEDDDDDDBDBDBBDBBDBBB(:ABCDDDDDDDDDDDDDDDD@BBBDDDDDDDDDDDDBDDDDDDDDDDADC'
        records[2].baseQualityHeader == null



    }

}
