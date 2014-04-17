package nextflow.splitter

import groovyx.gpars.dataflow.operator.PoisonPill
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class FastaSplitterTest extends Specification {



    def testFastaRecord() {
        def fasta = /
            ;

            >1aboA  xyz|beta|gamma
            NLFVALYDFVASGDNTLSITKGEKLRVLGYNHNGEWCEAQTKNGQGWVPS
            NYITPVN
            /.stripIndent()

        expect:
        FastaSplitter.parseFastaRecord(fasta, [id:true]) .id == '1aboA'
        FastaSplitter.parseFastaRecord(fasta, [seq:true]) .seq == 'NLFVALYDFVASGDNTLSITKGEKLRVLGYNHNGEWCEAQTKNGQGWVPS\nNYITPVN'
        FastaSplitter.parseFastaRecord(fasta, [seq:true, width: 20 ]) .seq == 'NLFVALYDFVASGDNTLSIT\nKGEKLRVLGYNHNGEWCEAQ\nTKNGQGWVPSNYITPVN'
        FastaSplitter.parseFastaRecord(fasta, [head:true]) .head == '>1aboA  xyz|beta|gamma'
        FastaSplitter.parseFastaRecord(fasta, [seq:true, width: 0]) .seq == 'NLFVALYDFVASGDNTLSITKGEKLRVLGYNHNGEWCEAQTKNGQGWVPSNYITPVN'
        FastaSplitter.parseFastaRecord(fasta, [text:true]) .text == fasta
        FastaSplitter.parseFastaRecord(fasta, [desc:true]) .desc == 'xyz|beta|gamma'

    }



    def testSplitFasta () {

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
        def q = new FastaSplitter().options(each:{ count++; it }).split(fasta).channel()

        then:
        count == 2
        q.val == ">prot1\nLCLYTHIGRNIYYGS1\nEWIWGGFSVDKATLN\n"
        q.val == ">prot2\nLLILILLLLLLALLS\nGLMPFLHTSKHRSMM\nIENY\n"
        q.val == PoisonPill.instance

    }

    def testSplitFastaRecord() {

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

        def q = new FastaSplitter().options(record: [id:true, seq:true, width: 999]).split(fasta) .channel()

        then:
        q.val == [id:'1aboA', seq: 'NLFVALYDFVASGDNTLSITKGEKLRVLGYNHNGEWCEAQTKNGQGWVPSNYITPVN']
        q.val == [id:'1ycsB', seq: 'KGVIYALWDYEPQNDDELPMKEGDCMTIIHREDEDEIEWWWARLNDKEGYVPRNLLGLYP']
        q.val == [id:'1pht', seq: 'GYQYRALYDYKKEREEDIDLHLGDILTVNKGSLVALGFSDGQEARPEEIGWLNGYNETTGERGDFPGTYVEYIGRKKISP']
        q.val == PoisonPill.instance

    }

    def testSplitFastaFile () {

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
        def result = new FastaSplitter().options(count:2).split(fasta).list()

        then:
        result[0] == ">prot1\nAA\n>prot2\nBB\nCC\n"
        result[1] == ">prot3\nDD\n>prot4\nEE\nFF\nGG\n"
        result[2] == ">prot5\nLL\nNN\n"


        when:
        def result2 = new FastaSplitter()
                .options(record: [id:true, seq: true], each:{ [ it.id, it.seq.size() ]} )
                .split(fasta)
                .list()

        then:
        result2[0] == [ 'prot1', 2 ]
        result2[1] == [ 'prot2', 5 ]
        result2[2] == [ 'prot3', 2 ]

    }




}
