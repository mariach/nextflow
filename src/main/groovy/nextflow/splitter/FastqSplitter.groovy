package nextflow.splitter
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.operator.PoisonPill
import net.sf.picard.fastq.FastqReader
import net.sf.picard.fastq.FastqRecord

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@Singleton(strict = false)
class FastqSplitter extends AbstractTextSplitter {

    static recordToBuffer( FastqReader record, StringBuilder buffer ) {

    }

    static Map recordToMap( FastqRecord record ) {
        def result = [:]
        result.readHeader = record.readHeader
        result.readString = record.readString
        result.baseQualityHeader = record.baseQualityHeader
        result.baseQualityString = record.baseQualityString
    }

    static void recordToBuffer( FastqRecord record, StringBuilder buffer ) {
        buffer << record.readHeader << '\n'
        buffer << record.readString << '\n'
        buffer << record.baseQualityHeader << '\n'
        buffer << record.baseQualityString << '\n'
    }

    @Override
    def apply(Reader reader, Map options, Closure<String> closure) {

        log.trace "${this.class.simpleName} options: $options"
        final count = options.count ?: 1
        final into = options.into
        final recordMode = options.record && options.record instanceof Map

        if( into && !(into instanceof Collection) && !(into instanceof DataflowQueue) )
            throw new IllegalArgumentException("Argument 'into' can be a subclass of Collection or a DataflowQueue type -- Entered value type: ${into.class.name}")

        if( recordMode && count>1 )
            throw new IllegalArgumentException("When using 'record' option 'count' cannot be greater than 1")

        BufferedReader reader0 = reader instanceof BufferedReader ? reader : new BufferedReader(reader)

        final fastq = new FastqReader(reader0)
        final StringBuilder buffer = new StringBuilder()
        int index = 0
        int blockCount=0
        def result = null

        try {
            while( fastq.hasNext() ) {
                def record = fastq.next()

                if( !recordMode )
                    recordToBuffer(record,buffer)

                if ( ++blockCount == count ) {
                    // invoke the closure, passing the read block as parameter
                    def splitArg = recordMode ? recordToMap(record) : buffer.toString()

                    result = splitInvoke( closure, splitArg, index++ )
                    if( into != null ) {
                        into << result
                    }

                    buffer.setLength(0)
                    blockCount=0
                }


            }
        }
        finally {
            fastq.closeQuietly()
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
