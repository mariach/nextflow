package nextflow.splitter

import groovy.transform.InheritConstructors
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowWriteChannel
import groovyx.gpars.dataflow.operator.PoisonPill
import nextflow.extension.FilesExtensions
/**
 * Split a text file by one or more lines at times
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@InheritConstructors
class TextSplitter extends AbstractTextSplitter {

    @Override
    def splitImpl( )  {
        assert targetObject != null

        BufferedReader reader0 = (BufferedReader)(targetObject instanceof BufferedReader ? targetObject : new BufferedReader(targetObject))

        def result = null
        String line
        int index = 0
        StringBuilder buffer = new StringBuilder()
        int c=0

        try {

            while( (line = reader0.readLine()) != null ) {
                if ( c ) buffer << '\n'
                buffer << line
                if ( ++c == count ) {
                    c = 0
                    result = splitCall(closure, buffer.toString(), index++ )
                    if( into != null )
                        append(into,result)

                    buffer.setLength(0)
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
        if ( buffer.size() ) {
            result = splitCall(closure, buffer.toString(), index )
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
