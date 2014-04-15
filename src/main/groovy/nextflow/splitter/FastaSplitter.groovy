package nextflow.splitter

import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.operator.PoisonPill
import nextflow.extension.NextflowExtensions


/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@Singleton(strict = false)
class FastaSplitter extends AbstractTextSplitter {


    @Override
    final apply(Reader reader, Map options, Closure<String> closure) {
        assert reader != null
        assert options != null

        log.trace "${this.class.simpleName} options: $options"
        final count = options.count ?: 1
        final into = options.into
        final rec = options.record && options.record instanceof Map

        if( into && !(into instanceof Collection) && !(into instanceof DataflowQueue) )
            throw new IllegalArgumentException("Argument 'into' can be a subclass of Collection or a DataflowQueue type -- Entered value type: ${into.class.name}")

        if( rec && count>1 )
            throw new IllegalArgumentException("When using 'record' option 'count' cannot be greater than 1")

        BufferedReader reader0 = reader instanceof BufferedReader ? reader : new BufferedReader(reader)

        def result = null
        String line
        StringBuilder buffer = new StringBuilder()
        int index = 0
        int blockCount=0
        boolean openBlock = false

        try {

            while( (line = reader0.readLine()) != null ) {

                if( line.startsWith(';')) continue

                if ( line == '' ) {
                    buffer << '\n'
                }
                else if ( !openBlock && line.charAt(0)=='>' ) {
                    openBlock = true
                    buffer << line << '\n'
                }
                else if ( openBlock && line.charAt(0)=='>') {
                    // another block is started

                    if ( ++blockCount == count ) {
                        // invoke the closure, passing the read block as parameter
                        def record = rec ? NextflowExtensions.parseFastaRecord(buffer.toString(), (Map)options.record) : buffer.toString()
                        result = splitInvoke( closure, record, index++ )
                        if( into != null ) {
                            into << result
                        }

                        buffer.setLength(0)
                        blockCount=0
                    }

                    buffer << line << '\n'

                }
                else {
                    buffer << line << '\n'
                }
            }

        }
        finally {
            reader0.closeQuietly()
        }


        /*
         * if there's something remaining in the buffer it's supposed
         * to be the last entry
         */
        if ( buffer.size() ) {
            def record = rec ? NextflowExtensions.parseFastaRecord(buffer.toString(), (Map)options.record) : buffer.toString()
            result = splitInvoke(closure, record, index )
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
