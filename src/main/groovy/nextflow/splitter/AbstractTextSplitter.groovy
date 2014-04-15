package nextflow.splitter

import java.nio.file.Files
import java.nio.file.Path

import nextflow.extension.NextflowExtensions

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */

abstract class AbstractTextSplitter extends SplitterTrait {

    abstract apply (Reader reader, Map options, Closure<String> closure)

    def apply( obj, Object[] args ) {

        Closure closure = null
        Map options = null

        if( args.size() == 1 ) {
            if( args[0] instanceof Closure )
                closure = args[0] as Closure
            else if( args[0] instanceof Map )
                options = args[0] as Map
            else
                throw new IllegalArgumentException()
        }
        else if( args.size() == 2 ) {
            options = args[0] as Map
            closure = args[1] as Closure
        }
        else if( args.size()>2 )
            throw new IllegalArgumentException()

        if( options == null )
            options = [:]

        if( obj instanceof CharSequence )
            return apply( new StringReader(obj.toString()), options, closure )

        if( obj instanceof Path ) {
            def charset = NextflowExtensions.getCharset(options.charset)
            return apply( Files.newBufferedReader(obj, charset), options, closure )
        }

        if( obj instanceof Reader )
            return apply( obj, options, closure )

        if( obj instanceof InputStream ) {
            def charset = NextflowExtensions.getCharset(options)
            return apply( new InputStreamReader(obj,charset), options, closure )
        }

        if( obj instanceof File )
            return apply( new FileReader(obj), options, closure )

        throw new IllegalAccessException("Object of class '${obj.class.name}' does not support 'chopString' method")

    }


}
