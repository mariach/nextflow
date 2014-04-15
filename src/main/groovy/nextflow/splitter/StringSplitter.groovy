package nextflow.splitter

import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.operator.PoisonPill

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@Singleton(strict = false)
class StringSplitter extends AbstractTextSplitter {

    @Override
    def apply(Reader reader, Map options, Closure<String> closure) {
        assert reader != null
        assert options != null

        log.trace "${this.class.simpleName} options: ${options}"
        final count = options.count ?: 1
        final into = options.into
        final ignoreNewLine = options.ignoreNewLine == true ?: false
        if( into && !(into instanceof Collection) && !(into instanceof DataflowQueue) )
            throw new IllegalArgumentException("Argument 'into' can be a subclass of Collection or a DataflowQueue type -- Entered value type: ${into.class.name}")

        def result = null
        def index = 0
        def buffer = new StringBuilder()
        int c = 0
        def ch

        try {

            while( (ch=reader.read()) != -1 ) {
                if( ignoreNewLine && ( ch == '\n' as char || ch == '\r' as char ))
                    continue
                buffer.append( (char)ch )
                if ( ++c == count ) {
                    c = 0
                    result = splitInvoke(closure, buffer.toString(), index++ )
                    if( into != null )
                        into << result

                    buffer.setLength(0)
                }
            }

        }
        finally {
            reader.closeQuietly()
        }

        /*
         * if there's something remaining in the buffer it's supposed
         * to be the last entry
         */
        if ( buffer.size() ) {
            result = splitInvoke(closure, buffer.toString(), index++ )
            if( into != null )
                into << result
        }

        /*
         * now close and return the result
         * - when the target it's a channel, send stop message
         * - when it's a list return it
         * - otherwise return the last value
         */
        if( into instanceof DataflowWriteChannel ) {
            into << PoisonPill.instance
            return into
        }
        if( into != null )
            return into

        return result
    }
}
