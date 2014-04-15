package nextflow.splitter

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.operator.PoisonPill
import spock.lang.Specification


/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class TextSplitterTest extends Specification {

    def 'test chop text by line' () {

        expect:
        // no params are specified, it returns the last line chopped
        "Hello".chopLines() == 'Hello'
        "Hello\nworld".chopLines() == 'world'
        // when *into* is a List object, it return that list populated
        "Hello\nworld\n!".splitText(into:[]) == ['Hello','world','!']
        "Hello\nworld\n!".splitText(into:[]) { it.reverse() } == ['olleH','dlrow','!']

        when:   '*into* is a Dataflow channel, get the chopped items, closing by a poison-pill'
        def channel = "Hello\nworld\n!".splitText(into: new DataflowQueue())
        then:
        channel.val == 'Hello'
        channel.val == 'world'
        channel.val == '!'
        channel.val == PoisonPill.instance


        expect:
        "Hello\nHola\nHalo".splitText(into:[]) == ['Hello', 'Hola', 'Halo']
        "11\n22\n33\n44\n55".splitText(count:3, into:[]) == [ '11\n22\n33', '44\n55' ]
        "11\n22\n33\n44\n55".splitText(count:2, into:[]) == [ '11\n22', '33\n44', '55' ]

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
        def lines = file.chopLines(into:[])

        then:
        lines[0] == 'line1'
        lines[1] == 'line2'
        lines[2]== 'line3'
        lines[3] == 'line4'
        lines[4] == 'line5'

        when:
        def channel = new DataflowQueue()
        file.chopLines(count:2, into: channel)

        then:
        channel.val == 'line1\nline2'
        channel.val == 'line3\nline4'
        channel.val == 'line5'

    }
}
