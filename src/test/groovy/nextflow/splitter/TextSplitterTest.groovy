package nextflow.splitter

import groovyx.gpars.dataflow.operator.PoisonPill
import spock.lang.Specification
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class TextSplitterTest extends Specification {

    def testTextByLine() {

        expect:
        new TextSplitter().split("Hello\nworld\n!").list() == ['Hello','world','!']
        new TextSplitter().split("Hello\nworld\n!").options(each:{ it.reverse() }) .list()  == ['olleH','dlrow','!']

    }


    def testSplitLinesByCount () {

        expect:
        new TextSplitter().split("Hello\nHola\nHalo").list() == ['Hello', 'Hola', 'Halo']
        new TextSplitter().options(count:3).split("11\n22\n33\n44\n55").list() == [ '11\n22\n33', '44\n55' ]
        new TextSplitter().options(count:2).split("11\n22\n33\n44\n55").list() == [ '11\n22', '33\n44', '55' ]

    }

    def 'test chop file by line' () {

        setup:
        def file = File.createTempFile('chunk','test')
        file.deleteOnExit()
        file.text = '''\
        line1
        line2
        line3
        line4
        line5
        '''.stripIndent()

        when:
        def lines = new TextSplitter().split(file).list()

        then:
        lines[0] == 'line1'
        lines[1] == 'line2'
        lines[2]== 'line3'
        lines[3] == 'line4'
        lines[4] == 'line5'

        when:
        def channel = new TextSplitter().split(file).options(count:2).channel()
        then:
        channel.val == 'line1\nline2'
        channel.val == 'line3\nline4'
        channel.val == 'line5'

    }

    def testSplitChannel() {


        when:   '*into* is a Dataflow channel, get the chopped items, closing by a poison-pill'
        def channel = new TextSplitter().split("Hello\nworld\n!").channel()
        then:
        channel.val == 'Hello'
        channel.val == 'world'
        channel.val == '!'
        channel.val == PoisonPill.instance

    }
}
