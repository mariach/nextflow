package nextflow.splitter

import java.nio.file.Files
import java.nio.file.Path

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
class TextSplitter extends AbstractTextSplitter {

    protected TextSplitter() { }


    @Override
    def apply(Reader reader, Map options, Closure<String> closure) {
        assert reader != null
        assert options != null

        log.trace "${this.class.simpleName} options: ${options}"
        final size = options.count ?: 1
        final into = options.into
        if( into && !(into instanceof Collection) && !(into instanceof DataflowQueue) )
            throw new IllegalArgumentException("Argument 'into' can be a subclass of Collection or a DataflowQueue type -- Entered value type: ${into.class.name}")

        BufferedReader reader0 = reader instanceof BufferedReader ? reader : new BufferedReader(reader)

        def result = null
        String line
        int index = 0
        StringBuilder buffer = new StringBuilder()
        int c=0

        try {

            while( (line = reader0.readLine()) != null ) {
                if ( c ) buffer << '\n'
                buffer << line
                if ( ++c == size ) {
                    c = 0
                    result = splitInvoke(closure, buffer.toString(), index++ )
                    if( into != null )
                        into << result

                    buffer.setLength(0)
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
            result = splitInvoke(closure, buffer.toString(), index )
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


    def apply( CharSequence text, Map options = [:], Closure closure ) {
        assert text != null
        apply( new StringReader(text.toString()), options, closure )
    }

    def apply( Path path, Map options = [:], Closure closure ) {
        assert path != null
        final charset = NextflowExtensions.getCharset(options)
        apply( Files.newBufferedReader(path, charset), options, closure )
    }

    def apply( File file, Map options = [:], Closure closure ) {
        assert file != null
        apply( new FileReader(file), options, closure )
    }


    def apply( InputStream obj, Map options = [:], Closure closure ) {
        assert obj != null
        final charset = NextflowExtensions.getCharset(options)
        apply( new InputStreamReader(obj,charset), options, closure )
    }



}
