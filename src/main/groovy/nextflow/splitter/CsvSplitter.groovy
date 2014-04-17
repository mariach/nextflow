package nextflow.splitter
import groovy.transform.InheritConstructors
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.operator.PoisonPill
import nextflow.extension.FilesExtensions
import org.apache.commons.lang.StringUtils

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

@Slf4j
@InheritConstructors
class CsvSplitter extends AbstractTextSplitter {

    protected String delim

    CsvSplitter options(Map options) {
        super.options(options)
        delim = options.delim ?: ','
        return this
    }

    @Override
    def splitImpl() {
        assert targetObject
        BufferedReader reader0 = (BufferedReader)(targetObject instanceof BufferedReader ? targetObject : new BufferedReader(targetObject))

        def result = null
        String line
        int index = 0
        int c=0

        List buffer = count > 1 ? [] : null

        try {
            while( (line = reader0.readLine()) != null ) {

                def tokens = StringUtils.splitPreserveAllTokens(line, delim)
                if( buffer!=null )
                    buffer.add(tokens)

                if ( ++c == count ) {
                    c = 0
                    result = splitCall(closure, buffer ?: tokens, index++ )
                    if( into != null )
                        append(into,result)

                    if( buffer!=null )
                        buffer = []
                }
            }

        }
        finally {
            FilesExtensions.closeQuietly(reader0)
        }

        /*
         * if there's something remaining in the buffer it's supposed
         * to be the last entry
         */
        if ( buffer ) {
            result = splitCall(closure, buffer, index )
            if( into != null )
                append(into, result)
        }

        /*
         * now close and return the result
         * - when the target it's a channel, send stop message
         * - when it's a list return it
         * - otherwise return the last value
         */
        if( into instanceof DataflowWriteChannel ) {
            append(into, PoisonPill.instance)
            return into
        }
        if( into != null )
            return into

        return result
    }

}
