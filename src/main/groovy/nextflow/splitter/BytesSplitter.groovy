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
class BytesSplitter extends AbstractBinarySplitter {


    @Override
    def apply(InputStream stream, Map options, Closure<byte[]> closure) {
        assert stream != null
        assert options != null

        log.trace "${this.class.simpleName} options: ${options}"
        final count = options.count ?: 1
        final into = options.into
        if( into && !(into instanceof Collection) && !(into instanceof DataflowQueue) )
            throw new IllegalArgumentException("Argument 'into' can be a subclass of Collection or a DataflowQueue type -- Entered value type: ${into.class.name}")

        def result = null

        int c=0
        int index = 0
        byte[] buffer = new byte[count]
        byte item

        try {

            while( (item=stream.read()) != -1 ) {
                buffer[c] = (byte)item

                if ( ++c == count ) {
                    c = 0
                    result = splitInvoke(closure, buffer, index++ )
                    if( into != null )
                        into << result

                    buffer = new byte[count]
                }
            }

        }
        finally {
            stream.closeQuietly()
        }


        /*
         * if there's something remaining in the buffer it's supposed
         * to be the last entry
         */

        if ( c ) {
            if( c != count ) {
                def copy = new byte[c]
                System.arraycopy(buffer,0,copy,0,c)
                buffer = copy
            }

            result = splitInvoke(closure, buffer, index++ )
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
