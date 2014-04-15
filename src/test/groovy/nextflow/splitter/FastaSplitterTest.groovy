package nextflow.splitter

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.operator.PoisonPill
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class FastaSplitterTest extends Specification {




    def 'test chop Fasta ' () {

        when:
        def fasta = """\
                >prot1
                LCLYTHIGRNIYYGS1
                EWIWGGFSVDKATLN
                ;
                ; comment
                ;
                >prot2
                LLILILLLLLLALLS
                GLMPFLHTSKHRSMM
                IENY
                """.stripIndent()

        def count = 0
        def q = fasta.chopFasta(into: new DataflowQueue()) { count++; it }

        then:
        count == 2
        q.val == ">prot1\nLCLYTHIGRNIYYGS1\nEWIWGGFSVDKATLN\n"
        q.val == ">prot2\nLLILILLLLLLALLS\nGLMPFLHTSKHRSMM\nIENY\n"
        q.val == PoisonPill.instance

    }

    def 'test chop Fasta record ' () {

        when:
        def fasta = """\
                >1aboA
                NLFVALYDFVASGDNTLSITKGEKLRVLGYNHNGEWCEAQTKNGQGWVPS
                NYITPVN
                >1ycsB
                KGVIYALWDYEPQNDDELPMKEGDCMTIIHREDEDEIEWWWARLNDKEGY
                VPRNLLGLYP
                ; comment
                >1pht
                GYQYRALYDYKKEREEDIDLHLGDILTVNKGSLVALGFSDGQEARPEEIG
                WLNGYNETTGERGDFPGTYVE
                YIGRKKISP
                """.stripIndent()

        def q = fasta.chopFasta(into: new DataflowQueue(), record: [id:true, seq:true, width: 999])

        then:
        q.val == [id:'1aboA', seq: 'NLFVALYDFVASGDNTLSITKGEKLRVLGYNHNGEWCEAQTKNGQGWVPSNYITPVN']
        q.val == [id:'1ycsB', seq: 'KGVIYALWDYEPQNDDELPMKEGDCMTIIHREDEDEIEWWWARLNDKEGYVPRNLLGLYP']
        q.val == [id:'1pht', seq: 'GYQYRALYDYKKEREEDIDLHLGDILTVNKGSLVALGFSDGQEARPEEIGWLNGYNETTGERGDFPGTYVEYIGRKKISP']
        q.val == PoisonPill.instance

    }

    def 'test chop Fasta file' () {

        setup:
        def file = File.createTempFile('chunk','test')
        file.deleteOnExit()
        def fasta = """\
                >prot1
                AA
                >prot2
                BB
                CC
                >prot3
                DD
                >prot4
                EE
                FF
                GG
                >prot5
                LL
                NN
                """.stripIndent()


        when:
        def result = fasta.chopFasta(count:2, into: [])

        then:
        result[0] == ">prot1\nAA\n>prot2\nBB\nCC\n"
        result[1] == ">prot3\nDD\n>prot4\nEE\nFF\nGG\n"
        result[2] == ">prot5\nLL\nNN\n"


        when:
        def result2 = fasta.chopFasta(into: [], record: [id:true, seq: true] ) {
            [ it.id, it.seq.size() ]
        }

        then:
        result2[0] == [ 'prot1', 2 ]
        result2[1] == [ 'prot2', 5 ]
        result2[2] == [ 'prot3', 2 ]

    }

    def 'test chop string' () {

        expect:
        '012345678901234567'.chopString(count:5, into:[]) == ['01234','56789','01234','567']
        '012345678901234567'.chopString(count:5, into:[]) {it.reverse()} == ['43210','98765','43210','765']

        when:
        def q = '012345678901234567'.chopString(count:5, into:new DataflowQueue())
        then:
        q.val == '01234'
        q.val == '56789'
        q.val == '01234'
        q.val == '567'
        q.val == PoisonPill.instance
    }


    def 'test chop bytes' () {

        setup:
        def bytes = [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6 ] as byte[]

        expect:
        new ByteArrayInputStream(bytes).chopBytes(count: 10) == [0, 1, 2, 3, 4, 5, 6] as byte[]
        new ByteArrayInputStream(bytes).chopBytes(count:5, into:[]) == [ [0, 1, 2, 3, 4] as byte[], [5, 6, 7, 8, 9] as byte[] , [ 0, 1, 2, 3, 4] as byte[], [5, 6] as byte[] ]

    }



}
